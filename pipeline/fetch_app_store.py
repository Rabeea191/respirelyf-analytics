"""
App Store Connect — daily impressions, page views, downloads.

API flow (Analytics Report Requests):
  1. JWT auth
  2. GET or POST an ONGOING analyticsReportRequest for the app
  3. List available reports → find APP_STORE_ENGAGEMENT + APP_STORE_ACQUISITION
  4. Get daily instances for target date (yesterday; falls back up to 3 days)
  5. Download gzip-TSV segments
  6. Aggregate and upsert to app_store_daily

Run:  python -m pipeline.fetch_app_store
"""
import gzip
import io
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


# ── Auth ──────────────────────────────────────────────────────────────────────

def _make_token() -> str:
    now = int(time.time())
    payload = {
        "iss": APPSTORE_ISSUER_ID,
        "iat": now,
        "exp": now + 1100,          # 18 min (max 20)
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


# ── Report Request ─────────────────────────────────────────────────────────────

def _get_or_create_request(app_id: str, token: str) -> str:
    """Return the ID of the ONGOING analyticsReportRequest, creating one if needed."""
    url = f"{BASE}/apps/{app_id}/analyticsReportRequests"
    r = requests.get(url, headers=_hdr(token), params={"filter[accessType]": "ONGOING"}, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    if data:
        req_id = data[0]["id"]
        print(f"[app_store] existing report request: {req_id}")
        return req_id

    # Create new
    body = {"data": {"type": "analyticsReportRequests", "attributes": {"accessType": "ONGOING"}}}
    r = requests.post(url, json=body, headers=_hdr(token), timeout=30)
    r.raise_for_status()
    req_id = r.json()["data"]["id"]
    print(f"[app_store] created new report request: {req_id}")
    return req_id


def _get_reports(request_id: str, token: str) -> list[dict]:
    url = f"{BASE}/analyticsReportRequests/{request_id}/reports"
    r = requests.get(url, headers=_hdr(token), timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


def _get_instance(report_id: str, date_str: str, token: str) -> dict | None:
    """Return the daily instance for the given date (YYYY-MM-DD), or None."""
    url = f"{BASE}/analyticsReports/{report_id}/instances"
    params = {"filter[granularity]": "DAILY", "filter[processingDate]": date_str}
    r = requests.get(url, headers=_hdr(token), params=params, timeout=30)
    r.raise_for_status()
    instances = r.json().get("data", [])
    return instances[0] if instances else None


def _get_segments(instance_id: str, token: str) -> list[dict]:
    url = f"{BASE}/analyticsReportInstances/{instance_id}/segments"
    r = requests.get(url, headers=_hdr(token), timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


def _download_tsv(download_url: str, token: str) -> list[dict]:
    """Download a gzip-TSV segment and return list of row dicts."""
    r = requests.get(download_url, headers=_hdr(token), timeout=60)
    r.raise_for_status()
    raw = gzip.decompress(r.content).decode("utf-8")
    lines = raw.strip().splitlines()
    if not lines:
        return []
    headers = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        vals = line.split("\t")
        rows.append(dict(zip(headers, vals)))
    return rows


# ── Parse TSV rows ─────────────────────────────────────────────────────────────

def _safe_int(v: str) -> int:
    try:
        return int(float(v.replace(",", "")))
    except (ValueError, AttributeError):
        return 0


def _parse_engagement(rows: list[dict], target_date: str) -> dict:
    """Sum impressions + page views across all source-type breakdown rows."""
    agg = {"date": target_date, "impressions": 0, "impressions_unique": 0,
           "page_views": 0, "page_views_unique": 0}
    for row in rows:
        agg["impressions"]        += _safe_int(row.get("Impressions", "0"))
        agg["impressions_unique"] += _safe_int(row.get("Impressions Unique Devices", "0"))
        agg["page_views"]         += _safe_int(row.get("Product Page Views", "0"))
        agg["page_views_unique"]  += _safe_int(row.get("Product Page Views Unique Devices", "0"))
    return agg


def _parse_acquisition(rows: list[dict], target_date: str) -> dict:
    """Sum app units (new downloads) + redownloads."""
    agg = {"date": target_date, "downloads": 0, "redownloads": 0}
    for row in rows:
        agg["downloads"]   += _safe_int(row.get("App Units", "0"))
        agg["redownloads"] += _safe_int(row.get("Re-Downloads", "0"))
    return agg


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    # Default: yesterday; Apple typically has 1-2 day lag so fall back up to 3 days
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    token = _make_token()
    request_id = _get_or_create_request(APPSTORE_APP_ID, token)

    # Fetch all available reports for this request
    reports = _get_reports(request_id, token)
    print(f"[app_store] {len(reports)} report type(s) available")

    engagement_report = next(
        (r for r in reports if r["attributes"].get("name") == "APP_STORE_ENGAGEMENT"), None
    )
    acquisition_report = next(
        (r for r in reports if r["attributes"].get("name") == "APP_STORE_ACQUISITION"), None
    )

    if not engagement_report and not acquisition_report:
        print("[app_store] No reports ready yet — request may need up to 24h to activate")
        return

    # Try target date, then fall back 1–3 days if Apple hasn't processed it yet
    row: dict = {}
    for days_back in range(0, 4):
        check_date = target_date - timedelta(days=days_back)
        date_str = check_date.isoformat()
        found_data = False

        if engagement_report:
            inst = _get_instance(engagement_report["id"], date_str, token)
            if inst:
                segs = _get_segments(inst["id"], token)
                all_rows: list[dict] = []
                for seg in segs:
                    dl_url = seg["attributes"]["url"]
                    all_rows.extend(_download_tsv(dl_url, token))
                eng = _parse_engagement(all_rows, date_str)
                row.update(eng)
                found_data = True

        if acquisition_report:
            inst = _get_instance(acquisition_report["id"], date_str, token)
            if inst:
                segs = _get_segments(inst["id"], token)
                all_rows = []
                for seg in segs:
                    dl_url = seg["attributes"]["url"]
                    all_rows.extend(_download_tsv(dl_url, token))
                acq = _parse_acquisition(all_rows, date_str)
                row.update(acq)
                found_data = True

        if found_data:
            row["date"] = date_str
            print(f"[app_store] fetched data for {date_str}: {row}")
            break
    else:
        print(f"[app_store] No data available in past 4 days — skipping")
        return

    upsert("app_store_daily", [row])


if __name__ == "__main__":
    # Allow passing a specific date: python -m pipeline.fetch_app_store 2025-03-07
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
