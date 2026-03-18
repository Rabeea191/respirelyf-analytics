"""
App Store Connect — daily downloads via Analytics Reports API (ONE_TIME_SNAPSHOT).

4-step async process:
  1. POST /v1/analyticsReportRequests       → create ONE_TIME_SNAPSHOT request
  2. Poll /v1/analyticsReportRequests/{id}/reports → wait for "App Downloads" READY
  3. GET  /v1/analyticsReports/{id}/instances     → get instance for target date
  4. GET  /v1/analyticsReportInstances/{id}/segments → download gzip TSV data

No fallbacks — only exact Analytics API data is stored.
If API unavailable (403) or data not ready → skip silently (nothing stored).

Run: python -m pipeline.fetch_app_store
"""
import gzip
import sys
import time
from datetime import date, timedelta

import jwt
import requests

from pipeline.config import (
    APPSTORE_APP_ID,
    APPSTORE_ISSUER_ID,
    APPSTORE_KEY_ID,
    APPSTORE_PRIVATE_KEY,
)
from pipeline.store import upsert

BASE = "https://api.appstoreconnect.apple.com/v1"


# ── Auth ───────────────────────────────────────────────────────────────────────

def _make_token() -> str:
    now = int(time.time())
    payload = {
        "iss": APPSTORE_ISSUER_ID,
        "iat": now,
        "exp": now + 1200,  # 20 min
        "aud": "appstoreconnect-v1",
    }
    return jwt.encode(
        payload,
        APPSTORE_PRIVATE_KEY,
        algorithm="ES256",
        headers={"kid": APPSTORE_KEY_ID, "alg": "ES256", "typ": "JWT"},
    )


def _hdr() -> dict:
    """Fresh JWT on every call — avoids expiry during polling."""
    return {
        "Authorization": f"Bearer {_make_token()}",
        "Content-Type": "application/json",
    }


# ── Step 1: Create ONE_TIME_SNAPSHOT request ───────────────────────────────────

def _create_report_request() -> str | None:
    """Get existing ONE_TIME_SNAPSHOT or create new one. Returns request ID."""
    # Check for existing request first (409 = already exists)
    r = requests.get(
        f"{BASE}/analyticsReportRequests",
        headers=_hdr(),
        params={"filter[app]": str(APPSTORE_APP_ID),
                "filter[accessType]": "ONE_TIME_SNAPSHOT"},
        timeout=30,
    )
    if r.status_code == 200:
        data = r.json().get("data", [])
        if data:
            req_id = data[0]["id"]
            print(f"[app_store] Using existing ONE_TIME_SNAPSHOT request: {req_id}")
            return req_id

    # No existing request — create new one
    print("[app_store] Creating ONE_TIME_SNAPSHOT analytics request...")
    body = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": "ONE_TIME_SNAPSHOT"},
            "relationships": {
                "app": {"data": {"type": "apps", "id": str(APPSTORE_APP_ID)}}
            },
        }
    }
    r2 = requests.post(f"{BASE}/analyticsReportRequests",
                       headers=_hdr(), json=body, timeout=30)
    if r2.status_code == 403:
        print("[app_store] Analytics API 403 — account needs Analytics entitlement. Skipping.")
        return None
    if r2.status_code == 409:
        # Another key already created a ONE_TIME_SNAPSHOT for this app — find and reuse it
        print("[app_store] 409 — snapshot exists (another key). Fetching existing request...")
        r3 = requests.get(
            f"{BASE}/analyticsReportRequests",
            headers=_hdr(),
            params={"filter[app]": str(APPSTORE_APP_ID)},
            timeout=30,
        )
        if r3.status_code == 200:
            all_data = r3.json().get("data", [])
            if all_data:
                req_id = all_data[0]["id"]
                print(f"[app_store] Reusing existing request: {req_id}")
                return req_id
        print("[app_store] 409 and cannot find existing request — skipping.")
        return None
    if r2.status_code not in (200, 201):
        print(f"[app_store] Create request error {r2.status_code}: {r2.text[:300]}")
        return None
    req_id = r2.json()["data"]["id"]
    print(f"[app_store] Report request created: {req_id}")
    return req_id


# ── Step 2: Poll until "App Downloads" report is READY ────────────────────────

def _wait_for_report(request_id: str,
                     target_name: str = "App Downloads",
                     max_attempts: int = 10) -> str | None:
    """Poll reports endpoint until target report appears. Returns report ID.
    Apple's API has no 'state' field on reports — presence in list = available."""
    url = f"{BASE}/analyticsReportRequests/{request_id}/reports"
    for attempt in range(1, max_attempts + 1):
        r = requests.get(url, headers=_hdr(), timeout=30)
        if r.status_code == 403:
            print("[app_store] 403 while polling reports — skipping.")
            return None
        if r.status_code != 200:
            print(f"[app_store] Poll error {r.status_code}: {r.text[:200]}")
            return None

        reports = r.json().get("data", [])
        # Find App Downloads Standard or Detailed (no state check — presence = available)
        for rep in reports:
            name = rep.get("attributes", {}).get("name", "")
            if target_name.lower() in name.lower():
                print(f"[app_store] [{attempt}/{max_attempts}] Found: {name} ✓")
                return rep["id"]

        count = len(reports)
        print(f"[app_store] [{attempt}/{max_attempts}] {count} reports, App Downloads not yet — waiting 30s...")
        if attempt < max_attempts:
            time.sleep(30)

    print("[app_store] App Downloads report not found after polling — skipping.")
    return None


# ── Step 3: Get instance for target date ──────────────────────────────────────

def _get_instance(report_id: str, target_date: date) -> str | None:
    """GET instances filtered by processingDate + DAILY granularity."""
    r = requests.get(
        f"{BASE}/analyticsReports/{report_id}/instances",
        headers=_hdr(),
        params={
            "filter[processingDate]": target_date.isoformat(),
            "filter[granularity]":    "DAILY",
        },
        timeout=30,
    )
    if r.status_code != 200:
        print(f"[app_store] Instances error {r.status_code}: {r.text[:200]}")
        return None

    instances = r.json().get("data", [])
    if not instances:
        print(f"[app_store] No instance for {target_date} — data may not be ready yet.")
        return None

    inst_id = instances[0]["id"]
    print(f"[app_store] Instance found: {inst_id}")
    return inst_id


# ── Step 4: Download segments and parse App Units ─────────────────────────────

def _download_and_parse(instance_id: str, target_date: date) -> int | None:
    """Download gzip TSV segments, parse App Units for our app."""
    r = requests.get(
        f"{BASE}/analyticsReportInstances/{instance_id}/segments",
        headers=_hdr(), timeout=30,
    )
    if r.status_code != 200:
        print(f"[app_store] Segments error {r.status_code}: {r.text[:200]}")
        return None

    segments = r.json().get("data", [])
    if not segments:
        print("[app_store] No segments found.")
        return None

    unit_cols = ["App Units", "AppUnits", "Units", "app_units"]
    id_cols   = ["App Apple ID", "AppAppleId", "Apple ID", "app_apple_id"]
    total     = 0

    for seg in segments:
        url = seg.get("attributes", {}).get("url")
        if not url:
            continue
        # Signed URL — no auth header needed
        dl = requests.get(url, timeout=60)
        if dl.status_code != 200:
            continue
        raw   = gzip.decompress(dl.content).decode("utf-8")
        lines = raw.strip().splitlines()
        if not lines:
            continue
        hdrs = lines[0].split("\t")
        rows = [dict(zip(hdrs, line.split("\t"))) for line in lines[1:] if line]

        for row in rows:
            # Filter by app ID if column exists
            app_id = ""
            for ic in id_cols:
                if ic in row:
                    app_id = str(row[ic]).strip()
                    break
            if app_id and app_id != str(APPSTORE_APP_ID):
                continue
            # Sum App Units
            for uc in unit_cols:
                if uc in row:
                    try:
                        total += int(float(row[uc] or 0))
                    except (ValueError, TypeError):
                        pass
                    break

    return total


# ── Main ───────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    missing = [k for k, v in {
        "APPSTORE_ISSUER_ID":   APPSTORE_ISSUER_ID,
        "APPSTORE_KEY_ID":      APPSTORE_KEY_ID,
        "APPSTORE_PRIVATE_KEY": APPSTORE_PRIVATE_KEY,
        "APPSTORE_APP_ID":      APPSTORE_APP_ID,
    }.items() if not v]
    if missing:
        print(f"[app_store] SKIP — missing credentials: {', '.join(missing)}")
        return

    # Determine date range: last 7 days with 2-day processing lag
    end_date = date.today() - timedelta(days=2)
    if target_date is not None:
        # Single-date mode (e.g. manual CLI override)
        start_date = target_date
        end_date = target_date
    else:
        start_date = end_date - timedelta(days=6)

    print(f"[app_store] Fetching App Units {start_date} → {end_date} ...")

    # Step 1 — one shared request for all dates
    request_id = _create_report_request()
    if not request_id:
        return

    # Step 2 — poll up to 10 min (10 × 30s)
    report_id = _wait_for_report(request_id)
    if not report_id:
        return

    # Steps 3+4 — loop over each day in range
    current = start_date
    while current <= end_date:
        instance_id = _get_instance(report_id, current)
        if instance_id:
            downloads = _download_and_parse(instance_id, current)
            if downloads is not None:
                print(f"[app_store] {current}: {downloads} App Units (Analytics API ✓ exact)")
                upsert("app_store_daily", [{"date": current.isoformat(),
                                            "downloads": downloads, "redownloads": 0}])
        current += timedelta(days=1)


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
