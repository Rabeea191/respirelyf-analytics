"""
App Store Connect — daily downloads via Sales Reports API.

API: GET /v1/salesReports
  filter[frequency]=DAILY
  filter[reportType]=SALES
  filter[reportSubType]=SUMMARY
  filter[vendorNumber]={VENDOR_NUMBER}
  filter[reportDate]=YYYY-MM-DD
  filter[version]=1_0

Returns gzip-compressed TSV. Filters by Apple Identifier == APPSTORE_APP_ID.
Product Type F  = free download  → downloads
Product Type 7  = redownload     → redownloads

Writes to: app_store_daily (date, downloads, redownloads)
Run:  python -m pipeline.fetch_app_store
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

# Free download types we count as "downloads"
DOWNLOAD_TYPES  = {"F", "1"}    # F=free, 1=paid
REDOWNLOAD_TYPES = {"7"}         # 7=redownload


# ── Auth ──────────────────────────────────────────────────────────────────────

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


# ── Sales Report Fetch ─────────────────────────────────────────────────────────

def _fetch_report(date_str: str, token: str) -> list[dict] | None:
    """Download daily SALES SUMMARY report. Returns rows list or None if not available."""
    params = {
        "filter[frequency]":     "DAILY",
        "filter[reportType]":    "SALES",
        "filter[reportSubType]": "SUMMARY",
        "filter[vendorNumber]":  APPSTORE_VENDOR_NUMBER,
        "filter[reportDate]":    date_str,
        "filter[version]":       "1_0",
    }
    r = requests.get(
        f"{BASE}/salesReports",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept":        "application/a-gzip",
        },
        params=params,
        timeout=60,
    )
    if r.status_code in (404, 204):
        return None   # Report not available yet for this date
    r.raise_for_status()

    raw = gzip.decompress(r.content).decode("utf-8")
    lines = raw.strip().splitlines()
    if not lines:
        return []
    headers = lines[0].split("\t")
    return [dict(zip(headers, line.split("\t"))) for line in lines[1:] if line]


def _safe_int(v: str) -> int:
    try:
        return int(float(str(v).replace(",", "")))
    except (ValueError, AttributeError):
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────

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

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    token = _make_token()

    # Try yesterday, fall back up to 3 days (Apple has 1-2 day processing lag)
    for days_back in range(0, 4):
        check_date = target_date - timedelta(days=days_back)
        date_str   = check_date.isoformat()
        print(f"[app_store] trying sales report for {date_str} ...")

        rows = _fetch_report(date_str, token)
        if rows is None:
            print(f"[app_store] no report available for {date_str}")
            continue

        # Debug: show all Apple Identifiers found in this report
        all_ids = {str(r.get("Apple Identifier", "")).strip() for r in rows}
        print(f"[app_store] report has {len(rows)} rows, identifiers: {all_ids}")

        # Filter to our app and sum units by product type
        downloads   = 0
        redownloads = 0
        for row in rows:
            apple_id = str(row.get("Apple Identifier", "")).strip()
            if apple_id != str(APPSTORE_APP_ID):
                continue
            prod_type = row.get("Product Type Identifier", "").strip()
            units     = _safe_int(row.get("Units", "0"))
            print(f"[app_store] matched row — type={prod_type!r} units={units}")
            if prod_type in DOWNLOAD_TYPES:
                downloads += units
            elif prod_type in REDOWNLOAD_TYPES:
                redownloads += units

        record = {"date": date_str, "downloads": downloads, "redownloads": redownloads}
        print(f"[app_store] {date_str}: {downloads} downloads, {redownloads} redownloads")
        upsert("app_store_daily", [record])
        return

    print("[app_store] no sales report available for past 4 days — skipping")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
