"""
App Store Connect — daily downloads via Analytics Reports API.

Primary:  analyticsReportRequests → instances → download (exact App Units)
Fallback: salesReports API (if analytics not ready yet — first 24h)

Analytics API matches App Store Connect dashboard exactly (unique devices).
Sales Reports may overcount due to reinstalls/updates.

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
    APPSTORE_VENDOR_NUMBER,
)
from pipeline.store import upsert

BASE = "https://api.appstoreconnect.apple.com/v1"


# ── Auth ───────────────────────────────────────────────────────────────────────

def _make_token() -> str:
    now = int(time.time())
    payload = {
        "iss": APPSTORE_ISSUER_ID,
        "iat": now,
        "exp": now + 1100,
        "aud": "appstoreconnect-v1",
    }
    return jwt.encode(
        payload,
        APPSTORE_PRIVATE_KEY,
        algorithm="ES256",
        headers={"kid": APPSTORE_KEY_ID},
    )


def _hdr(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ── Analytics Reports API ──────────────────────────────────────────────────────

def _get_or_create_report_request(token: str) -> str | None:
    """Get existing ONGOING report request or create one. Returns request ID."""
    r = requests.get(
        f"{BASE}/analyticsReportRequests",
        headers=_hdr(token),
        params={
            "filter[app]":        str(APPSTORE_APP_ID),
            "filter[accessType]": "ONGOING",
        },
        timeout=30,
    )
    if r.status_code == 403:
        print(f"[app_store] Analytics API 403 — key may need Analytics access. Falling back.")
        return None
    if r.status_code == 200:
        data = r.json().get("data", [])
        if data:
            req_id = data[0]["id"]
            print(f"[app_store] Found existing report request ✓")
            return req_id
        print("[app_store] No existing requests — creating new ONGOING request...")
    else:
        print(f"[app_store] List requests error {r.status_code}: {r.text[:200]}")
        return None

    # Create new ONGOING request
    body = {
        "data": {
            "type": "analyticsReportRequests",
            "attributes": {"accessType": "ONGOING"},
            "relationships": {
                "app": {"data": {"type": "apps", "id": str(APPSTORE_APP_ID)}}
            },
        }
    }
    r2 = requests.post(
        f"{BASE}/analyticsReportRequests",
        headers={**_hdr(token), "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    if r2.status_code in (200, 201):
        req_id = r2.json()["data"]["id"]
        print(f"[app_store] Created report request ✓ (first data available in ~24h)")
        return req_id
    print(f"[app_store] Create request error {r2.status_code}: {r2.text[:200]}")
    return None


def _get_best_instance(token: str, request_id: str, target_date: date) -> tuple[str | None, str]:
    """Find best report instance for target_date. Returns (instance_id, proc_date)."""
    # List all report types for this request
    r = requests.get(
        f"{BASE}/analyticsReportRequests/{request_id}/reports",
        headers=_hdr(token),
        timeout=30,
    )
    if r.status_code != 200:
        print(f"[app_store] Reports list error {r.status_code}: {r.text[:200]}")
        return None, ""

    reports = r.json().get("data", [])
    if not reports:
        print("[app_store] No report types available yet (may take ~24h after first request)")
        return None, ""

    report_names = [d.get("attributes", {}).get("name", "") for d in reports]
    print(f"[app_store] Available report types: {report_names}")

    # Prefer APP_USAGE (has App Units), fall back to first available
    report_id = None
    for rep in reports:
        name = rep.get("attributes", {}).get("name", "")
        if name in ("APP_USAGE", "APP_UNITS", "appUsage"):
            report_id = rep["id"]
            print(f"[app_store] Using report type: {name}")
            break
    if not report_id:
        report_id = reports[0]["id"]
        print(f"[app_store] Using first available report: {report_names[0]}")

    # Get instances
    r2 = requests.get(
        f"{BASE}/analyticsReports/{report_id}/instances",
        headers=_hdr(token),
        timeout=30,
    )
    if r2.status_code != 200:
        print(f"[app_store] Instances error {r2.status_code}: {r2.text[:200]}")
        return None, ""

    instances = r2.json().get("data", [])
    if not instances:
        print("[app_store] No instances available yet")
        return None, ""

    # Find exact date match first
    for inst in instances:
        proc_date = inst.get("attributes", {}).get("processingDate", "")
        if proc_date == target_date.isoformat():
            return inst["id"], proc_date

    # Use most recent
    inst = instances[0]
    proc_date = inst.get("attributes", {}).get("processingDate", "?")
    print(f"[app_store] No instance for {target_date}, using most recent: {proc_date}")
    return inst["id"], proc_date


def _download_instance(token: str, instance_id: str) -> list[dict]:
    """Download all segments for an instance. Returns parsed TSV rows."""
    r = requests.get(
        f"{BASE}/analyticsReportInstances/{instance_id}/segments",
        headers=_hdr(token),
        timeout=30,
    )
    if r.status_code != 200:
        print(f"[app_store] Segments error {r.status_code}: {r.text[:200]}")
        return []

    segments = r.json().get("data", [])
    rows = []
    for seg in segments:
        url = seg.get("attributes", {}).get("url")
        if not url:
            continue
        dl = requests.get(url, timeout=60)
        if dl.status_code != 200:
            continue
        raw = gzip.decompress(dl.content).decode("utf-8")
        lines = raw.strip().splitlines()
        if not lines:
            continue
        headers = lines[0].split("\t")
        for line in lines[1:]:
            if line:
                rows.append(dict(zip(headers, line.split("\t"))))
    return rows


def _parse_app_units(rows: list[dict], target_date: date) -> int:
    """Extract App Units for our app. Tries multiple column name variants."""
    date_str  = target_date.isoformat()
    unit_cols = ["App Units", "AppUnits", "Units", "app_units"]
    id_cols   = ["App Apple ID", "AppAppleId", "Apple ID", "app_apple_id"]
    date_cols = ["Date", "date", "Report Date"]

    total = 0
    for row in rows:
        # Filter by date if column exists
        for dc in date_cols:
            if dc in row and row[dc] and row[dc] != date_str:
                break
        # Filter by app ID if column exists
        app_id = ""
        for ic in id_cols:
            if ic in row:
                app_id = str(row[ic]).strip()
                break
        if app_id and app_id != str(APPSTORE_APP_ID):
            continue
        # Sum units
        for uc in unit_cols:
            if uc in row:
                try:
                    total += int(float(row[uc] or 0))
                except (ValueError, TypeError):
                    pass
                break
    return total


# ── Sales Reports Fallback ─────────────────────────────────────────────────────

def _sales_reports_fallback(token: str, target_date: date) -> None:
    """Fallback: Sales Reports API. Note: may overcount (includes reinstalls)."""
    print("[app_store] Falling back to Sales Reports API (may include reinstalls)...")

    def _is_download(t: str) -> bool:
        return t.endswith("F") and not t.startswith("IA")

    def _is_redownload(t: str) -> bool:
        return t.endswith("7") and not t.startswith("IA")

    for days_back in range(0, 4):
        check_date = target_date - timedelta(days=days_back)
        date_str   = check_date.isoformat()
        print(f"[app_store] trying sales report for {date_str} ...")

        r = requests.get(
            f"{BASE}/salesReports",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/a-gzip"},
            params={
                "filter[frequency]":     "DAILY",
                "filter[reportType]":    "SALES",
                "filter[reportSubType]": "SUMMARY",
                "filter[vendorNumber]":  APPSTORE_VENDOR_NUMBER,
                "filter[reportDate]":    date_str,
                "filter[version]":       "1_0",
            },
            timeout=60,
        )
        if r.status_code in (404, 204):
            print(f"[app_store] no sales report for {date_str}")
            continue
        r.raise_for_status()

        raw   = gzip.decompress(r.content).decode("utf-8")
        lines = raw.strip().splitlines()
        if not lines:
            continue
        headers = lines[0].split("\t")
        rows    = [dict(zip(headers, line.split("\t"))) for line in lines[1:] if line]

        downloads = redownloads = 0
        for row in rows:
            if str(row.get("Apple Identifier", "")).strip() != str(APPSTORE_APP_ID):
                continue
            t = row.get("Product Type Identifier", "").strip()
            u = int(float(str(row.get("Units", "0")).replace(",", "") or 0))
            if _is_download(t):
                downloads += u
            elif _is_redownload(t):
                redownloads += u

        print(f"[app_store] {date_str}: {downloads} downloads (Sales Reports fallback)")
        upsert("app_store_daily", [{"date": date_str, "downloads": downloads, "redownloads": redownloads}])
        return

    print("[app_store] no data available for past 4 days")


# ── Main ───────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    missing = [k for k, v in {
        "APPSTORE_ISSUER_ID":   APPSTORE_ISSUER_ID,
        "APPSTORE_KEY_ID":      APPSTORE_KEY_ID,
        "APPSTORE_PRIVATE_KEY": APPSTORE_PRIVATE_KEY,
        "APPSTORE_APP_ID":      APPSTORE_APP_ID,
    }.items() if not v]
    if missing:
        print(f"[app_store] SKIP — missing: {', '.join(missing)}")
        return

    # Analytics API has ~2 day processing lag — try 2 days back first
    analytics_date = date.today() - timedelta(days=2)  # March 15 today
    if target_date is None:
        target_date = analytics_date

    token = _make_token()
    print(f"[app_store] fetching for {target_date} (Analytics lag: trying {analytics_date}) ...")

    # ── Try Analytics Reports API first (exact unique-device count) ──────────
    request_id = _get_or_create_report_request(token)
    if request_id:
        instance_id, proc_date = _get_best_instance(token, request_id, target_date)
        if instance_id:
            rows = _download_instance(token, instance_id)
            if rows:
                downloads = _parse_app_units(rows, target_date)
                print(f"[app_store] {proc_date}: {downloads} App Units (Analytics API ✓ exact)")
                upsert("app_store_daily", [{"date": proc_date or target_date.isoformat(),
                                            "downloads": downloads, "redownloads": 0}])
                return
        print("[app_store] Analytics instances not ready yet — using Sales Reports")
    else:
        print("[app_store] Analytics API unavailable — using Sales Reports")

    # ── Sales Reports fallback (App Store Installs — may include re-installs) ─
    _sales_reports_fallback(token, target_date)


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
