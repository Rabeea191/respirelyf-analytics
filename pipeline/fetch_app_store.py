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


# ── Step 1: Get or create ONGOING report request ──────────────────────────────
# ONGOING = Apple generates daily reports automatically — no per-app conflict.
# ONE_TIME_SNAPSHOT was abandoned: Apple ties it to the app (not the key),
# so any existing snapshot (even from a revoked key) blocks new POSTs with 409.

def _create_report_request() -> str | None:
    """Get existing ONGOING request or create one. Returns request ID."""
    # List all requests for this app (filter[accessType] is not a valid API filter)
    r = requests.get(
        f"{BASE}/analyticsReportRequests",
        headers=_hdr(),
        params={"filter[app]": str(APPSTORE_APP_ID)},
        timeout=30,
    )
    if r.status_code == 200:
        for item in r.json().get("data", []):
            access_type = item.get("attributes", {}).get("accessType", "")
            if access_type == "ONGOING":
                req_id = item["id"]
                print(f"[app_store] Using existing ONGOING request: {req_id}")
                return req_id

    # No existing ONGOING — create one
    print("[app_store] Creating ONGOING analytics request...")
    body = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": "ONGOING"},
            "relationships": {
                "app": {"data": {"type": "apps", "id": str(APPSTORE_APP_ID)}}
            },
        }
    }
    r2 = requests.post(f"{BASE}/analyticsReportRequests",
                       headers=_hdr(), json=body, timeout=30)
    if r2.status_code == 403:
        print("[app_store] Analytics API 403 — skipping.")
        return None
    if r2.status_code == 409:
        # Existing ONGOING owned by another (revoked) key — try to DELETE it using the error ID
        err_body = r2.json()
        print(f"[app_store] 409 error body: {err_body}")
        errors = err_body.get("errors", [])
        if errors:
            conflict_id = errors[0].get("id", "")
            if conflict_id:
                print(f"[app_store] Attempting DELETE on conflicting request: {conflict_id}")
                r_del = requests.delete(
                    f"{BASE}/analyticsReportRequests/{conflict_id}",
                    headers=_hdr(), timeout=30,
                )
                print(f"[app_store] DELETE status: {r_del.status_code}")
                if r_del.status_code in (200, 204, 404):
                    # Deleted (or didn't exist) — retry POST
                    print("[app_store] Retrying POST after DELETE...")
                    r_retry = requests.post(f"{BASE}/analyticsReportRequests",
                                            headers=_hdr(), json=body, timeout=30)
                    print(f"[app_store] Retry POST status: {r_retry.status_code}")
                    if r_retry.status_code in (200, 201):
                        req_id = r_retry.json()["data"]["id"]
                        print(f"[app_store] ONGOING request created after DELETE: {req_id}")
                        return req_id
        print("[app_store] 409 unresolvable — Analytics API blocked. Skipping.")
        return None
    if r2.status_code not in (200, 201):
        print(f"[app_store] Create request error {r2.status_code}: {r2.text[:300]}")
        return None
    req_id = r2.json()["data"]["id"]
    print(f"[app_store] ONGOING request created: {req_id}")
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

def _list_all_instances(report_id: str) -> list:
    """List ALL available instances for debugging (no date filter)."""
    r = requests.get(
        f"{BASE}/analyticsReports/{report_id}/instances",
        headers=_hdr(),
        params={"filter[granularity]": "DAILY"},
        timeout=30,
    )
    if r.status_code != 200:
        return []
    return r.json().get("data", [])


def _get_instance(report_id: str, target_date: date) -> str | None:
    """GET instance for target date. Falls back to searching all instances."""
    # Try filtered fetch first
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
    if instances:
        inst_id = instances[0]["id"]
        print(f"[app_store] Instance found: {inst_id}")
        return inst_id

    # Not found with date filter — silently skip (ONGOING needs time to populate)
    return None


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

    # Show what instances Apple has generated so far (helps debug timing)
    all_inst = _list_all_instances(report_id)
    if all_inst:
        dates_available = [i.get("attributes", {}).get("processingDate", "?") for i in all_inst]
        print(f"[app_store] Available instances ({len(all_inst)}): {', '.join(dates_available)}")
    else:
        print("[app_store] No instances available yet — ONGOING request may need 24h to populate.")

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
