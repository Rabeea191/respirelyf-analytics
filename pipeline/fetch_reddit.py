"""
Reddit Ads — daily campaign metrics via Reddit Ads API v3.

Credentials (GitHub Secrets):
  REDDIT_APP_ID     — Reddit app client ID
  REDDIT_APP_SECRET — Reddit app secret

Auth: client_credentials (no user login needed for ads data)

Writes to:
  reddit_ads_daily — per-campaign daily metrics
    date, campaign_id, campaign_name, impressions, clicks, spend_usd, cpm, cpc

Run: python -m pipeline.fetch_reddit
"""
import sys
from datetime import date, timedelta

import requests

from pipeline.config import (
    REDDIT_APP_ID, REDDIT_APP_SECRET,
    REDDIT_USERNAME, REDDIT_PASSWORD,
)
from pipeline.store import upsert

ADS_BASE  = "https://ads-api.reddit.com/api/v3"
TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
USER_AGENT = "RespireLYF Analytics/1.0"


# ── Auth ───────────────────────────────────────────────────────────────────────

def _get_token() -> str | None:
    """Get OAuth2 token via password grant (script app flow)."""
    if not REDDIT_USERNAME or not REDDIT_PASSWORD:
        print("[reddit] REDDIT_USERNAME / REDDIT_PASSWORD not set — skipping.")
        return None
    r = requests.post(
        TOKEN_URL,
        auth=(REDDIT_APP_ID, REDDIT_APP_SECRET),
        data={
            "grant_type": "password",
            "username":   REDDIT_USERNAME,
            "password":   REDDIT_PASSWORD,
        },
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"[reddit] Token error {r.status_code}: {r.text[:200]}")
        return None
    token = r.json().get("access_token")
    if not token:
        print(f"[reddit] No access_token in response: {r.json()}")
        return None
    print("[reddit] Token obtained ✓")
    return token


def _hdr(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
    }


# ── Step 1: Get ad accounts ────────────────────────────────────────────────────

def _get_accounts(token: str) -> list[dict]:
    r = requests.get(f"{ADS_BASE}/me/accounts", headers=_hdr(token), timeout=30)
    if r.status_code != 200:
        print(f"[reddit] Accounts error {r.status_code}: {r.text[:200]}")
        return []
    data = r.json().get("data", [])
    print(f"[reddit] Ad accounts found: {len(data)}")
    return data


# ── Step 2: Get campaigns for account ─────────────────────────────────────────

def _get_campaigns(token: str, account_id: str) -> list[dict]:
    r = requests.get(
        f"{ADS_BASE}/accounts/{account_id}/campaigns",
        headers=_hdr(token),
        timeout=30,
    )
    if r.status_code != 200:
        print(f"[reddit] Campaigns error {r.status_code}: {r.text[:200]}")
        return []
    data = r.json().get("data", [])
    print(f"[reddit] Campaigns found: {len(data)}")
    return data


# ── Step 3: Get campaign insights (date range) ─────────────────────────────────

def _get_insights(token: str, account_id: str,
                  start: date, end: date) -> list[dict]:
    """Fetch daily campaign-level insights for the date range."""
    r = requests.get(
        f"{ADS_BASE}/accounts/{account_id}/insights",
        headers=_hdr(token),
        params={
            "date_start":   start.isoformat(),
            "date_stop":    end.isoformat(),
            "granularity":  "DAY",
            "breakdown":    "campaign",
        },
        timeout=60,
    )
    if r.status_code == 403:
        print("[reddit] Insights 403 — check app permissions / ads account access.")
        return []
    if r.status_code != 200:
        print(f"[reddit] Insights error {r.status_code}: {r.text[:300]}")
        return []
    data = r.json().get("data", [])
    print(f"[reddit] Insight rows returned: {len(data)}")
    return data


# ── Main ───────────────────────────────────────────────────────────────────────

def run(start_date: date | None = None, end_date: date | None = None) -> None:
    if not REDDIT_APP_ID or not REDDIT_APP_SECRET or not REDDIT_USERNAME or not REDDIT_PASSWORD:
        print("[reddit] SKIP — REDDIT_APP_ID / REDDIT_APP_SECRET / REDDIT_USERNAME / REDDIT_PASSWORD not set.")
        return

    # Default: last 7 days
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=6)

    print(f"[reddit] Fetching ads data {start_date} → {end_date} ...")

    token = _get_token()
    if not token:
        return

    accounts = _get_accounts(token)
    if not accounts:
        print("[reddit] No ad accounts found — skipping.")
        return

    rows = []
    for account in accounts:
        account_id = account.get("id") or account.get("account_id", "")
        if not account_id:
            continue

        insights = _get_insights(token, account_id, start_date, end_date)

        for row in insights:
            # Normalise field names (Reddit API may vary)
            attrs  = row.get("attributes", row)  # some endpoints wrap in attributes
            c_date = attrs.get("date") or attrs.get("date_start", "")
            rows.append({
                "date":          c_date,
                "campaign_id":   str(attrs.get("campaign_id", "")),
                "campaign_name": attrs.get("campaign_name", ""),
                "impressions":   int(attrs.get("impressions", 0) or 0),
                "clicks":        int(attrs.get("clicks", 0) or 0),
                "spend_usd":     float(attrs.get("spend", 0) or 0),
                "cpm":           float(attrs.get("ecpm", 0) or 0),
                "cpc":           float(attrs.get("ecpc", 0) or 0),
            })

    if rows:
        print(f"[reddit] Upserting {len(rows)} rows...")
        upsert("reddit_ads_daily", rows)
    else:
        print("[reddit] No ad data returned — either no campaigns ran or date range has no spend.")


if __name__ == "__main__":
    s = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    e = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else None
    run(s, e)
