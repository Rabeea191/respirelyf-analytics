"""
Apple Search Ads — daily keyword performance.

Needs credentials:  APPLE_ADS_CLIENT_ID, APPLE_ADS_TEAM_ID,
                    APPLE_ADS_KEY_ID, APPLE_ADS_PRIVATE_KEY (.p8 contents)

Writes to:  apple_ads_keywords (date, keyword, impressions, taps, installs, spend, cpt)

Run:  python -m pipeline.fetch_apple_ads
"""
import sys
import time
from datetime import date, timedelta

import jwt
import requests

from pipeline.config import (
    APPLE_ADS_CLIENT_ID,
    APPLE_ADS_KEY_ID,
    APPLE_ADS_PRIVATE_KEY,
    APPLE_ADS_TEAM_ID,
)
from pipeline.store import upsert

BASE = "https://api.searchads.apple.com/api/v5"

# ── Credential check ──────────────────────────────────────────────────────────

def _check_creds() -> bool:
    missing = [k for k, v in {
        "APPLE_ADS_CLIENT_ID": APPLE_ADS_CLIENT_ID,
        "APPLE_ADS_TEAM_ID":   APPLE_ADS_TEAM_ID,
        "APPLE_ADS_KEY_ID":    APPLE_ADS_KEY_ID,
        "APPLE_ADS_PRIVATE_KEY": APPLE_ADS_PRIVATE_KEY,
    }.items() if not v]
    if missing:
        print(f"[apple_ads] SKIP — missing credentials: {', '.join(missing)}")
        return False
    return True


# ── Auth ──────────────────────────────────────────────────────────────────────

def _make_client_secret() -> str:
    """Build the ES256 JWT client secret for Apple Search Ads OAuth."""
    now = int(time.time())
    payload = {
        "sub": APPLE_ADS_CLIENT_ID,
        "aud": "https://appleid.apple.com",
        "iat": now,
        "exp": now + 86400,  # 24 h
        "iss": APPLE_ADS_TEAM_ID,
    }
    return jwt.encode(
        payload,
        APPLE_ADS_PRIVATE_KEY,
        algorithm="ES256",
        headers={"kid": APPLE_ADS_KEY_ID, "alg": "ES256"},
    )


def _get_access_token() -> str:
    secret = _make_client_secret()
    r = requests.post(
        "https://appleid.apple.com/auth/oauth2/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     APPLE_ADS_CLIENT_ID,
            "client_secret": secret,
            "scope":         "searchadsorg",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _hdr(token: str) -> dict:
    return {
        "Authorization":   f"Bearer {token}",
        "X-AP-Context":    f"orgId={APPLE_ADS_TEAM_ID}",
    }


# ── Keyword report ────────────────────────────────────────────────────────────

def _get_campaigns(token: str) -> list[dict]:
    r = requests.get(f"{BASE}/campaigns", headers=_hdr(token), timeout=30)
    r.raise_for_status()
    return r.json().get("data", [])


def _keyword_report(campaign_id: str, date_str: str, token: str) -> list[dict]:
    body = {
        "startTime": date_str,
        "endTime":   date_str,
        "granularity": "DAILY",
        "selector": {
            "orderBy": [{"field": "impressions", "sortOrder": "DESCENDING"}],
            "pagination": {"offset": 0, "limit": 1000},
        },
        "returnRowTotals": False,
        "returnRecordsWithNoMetrics": False,
    }
    url = f"{BASE}/reports/campaigns/{campaign_id}/keywords"
    r = requests.post(url, json=body, headers=_hdr(token), timeout=30)
    r.raise_for_status()
    return r.json().get("data", {}).get("reportingDataResponse", {}).get("row", [])


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    if not _check_creds():
        return

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    date_str = target_date.isoformat()
    token = _get_access_token()
    campaigns = _get_campaigns(token)
    print(f"[apple_ads] {len(campaigns)} campaign(s) found")

    rows = []
    for camp in campaigns:
        camp_id = camp["id"]
        kw_rows = _keyword_report(camp_id, date_str, token)
        for r in kw_rows:
            meta  = r.get("metadata", {})
            total = r.get("total", {})
            rows.append({
                "date":     date_str,
                "keyword":  meta.get("keywordText", "(unknown)"),
                "impressions": int(total.get("impressions", 0)),
                "taps":        int(total.get("taps", 0)),
                "installs":    int(total.get("installs", 0)),
                "spend":       float(total.get("localSpend", {}).get("amount", 0)),
                "cpt":         float(total.get("avgCPT", {}).get("amount", 0)),
            })

    print(f"[apple_ads] {len(rows)} keyword row(s) for {date_str}")
    upsert("apple_ads_keywords", rows)


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
