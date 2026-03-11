"""
Google Ads — daily campaign performance.

Needs credentials:  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET,
                    GOOGLE_REFRESH_TOKEN, GOOGLE_ADS_CUSTOMER_ID,
                    GOOGLE_ADS_DEV_TOKEN

Writes to:  google_ads_daily (date, campaign, impressions, clicks, spend, conversions)

Run:  python -m pipeline.fetch_google_ads
"""
import sys
from datetime import date, timedelta

from pipeline.config import (
    GOOGLE_ADS_CUSTOMER_ID,
    GOOGLE_ADS_DEV_TOKEN,
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REFRESH_TOKEN,
)
from pipeline.store import upsert


def _check_creds() -> bool:
    missing = [k for k, v in {
        "GOOGLE_CLIENT_ID":       GOOGLE_CLIENT_ID,
        "GOOGLE_CLIENT_SECRET":   GOOGLE_CLIENT_SECRET,
        "GOOGLE_REFRESH_TOKEN":   GOOGLE_REFRESH_TOKEN,
        "GOOGLE_ADS_CUSTOMER_ID": GOOGLE_ADS_CUSTOMER_ID,
        "GOOGLE_ADS_DEV_TOKEN":   GOOGLE_ADS_DEV_TOKEN,
    }.items() if not v]
    if missing:
        print(f"[google_ads] SKIP — missing credentials: {', '.join(missing)}")
        return False
    return True


def run(target_date: date | None = None) -> None:
    if not _check_creds():
        return

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    date_str = target_date.isoformat()

    from google.ads.googleads.client import GoogleAdsClient
    import requests

    # Get access token via refresh token
    token_resp = requests.post("https://oauth2.googleapis.com/token", data={
        "grant_type":    "refresh_token",
        "client_id":     GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
    }, timeout=30)
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]

    client = GoogleAdsClient.load_from_dict({
        "developer_token": GOOGLE_ADS_DEV_TOKEN,
        "client_id":       GOOGLE_CLIENT_ID,
        "client_secret":   GOOGLE_CLIENT_SECRET,
        "refresh_token":   GOOGLE_REFRESH_TOKEN,
        "use_proto_plus":  True,
    })

    gaql = f"""
        SELECT
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions
        FROM campaign
        WHERE segments.date = '{date_str}'
          AND campaign.status = 'ENABLED'
    """

    ga_service = client.get_service("GoogleAdsService")
    customer_id = GOOGLE_ADS_CUSTOMER_ID.replace("-", "")
    response = ga_service.search_stream(customer_id=customer_id, query=gaql)

    rows = []
    for batch in response:
        for row in batch.results:
            rows.append({
                "date":        date_str,
                "campaign":    row.campaign.name,
                "impressions": int(row.metrics.impressions),
                "clicks":      int(row.metrics.clicks),
                "spend":       round(row.metrics.cost_micros / 1_000_000, 2),
                "conversions": int(row.metrics.conversions),
            })

    print(f"[google_ads] {len(rows)} campaign row(s) for {date_str}")
    upsert("google_ads_daily", rows)


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
