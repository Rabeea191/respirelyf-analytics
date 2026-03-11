"""
Meta — daily ad performance + Facebook Page organic reach.

Needs credentials:  META_ACCESS_TOKEN  (long-lived system user token)
                    META_AD_ACCOUNT_ID  (act_xxxxxxxxxx)
                    META_PAGE_ID        (numeric page ID)

Permissions needed: ads_read, pages_read_engagement, read_insights

Writes to:
  meta_ads_daily   (date, campaign, impressions, clicks, spend, reach, cpm)
  meta_page_daily  (date, reach, impressions, engaged_users, page_views)

Run:  python -m pipeline.fetch_meta
"""
import sys
from datetime import date, timedelta

import requests

from pipeline.config import META_ACCESS_TOKEN, META_AD_ACCOUNT_ID, META_PAGE_ID
from pipeline.store import upsert

GRAPH = "https://graph.facebook.com/v19.0"


def _check_creds() -> bool:
    missing = [k for k, v in {
        "META_ACCESS_TOKEN":  META_ACCESS_TOKEN,
        "META_AD_ACCOUNT_ID": META_AD_ACCOUNT_ID,
        "META_PAGE_ID":       META_PAGE_ID,
    }.items() if not v]
    if missing:
        print(f"[meta] SKIP — missing credentials: {', '.join(missing)}")
        return False
    return True


def _p(extra: dict | None = None) -> dict:
    base = {"access_token": META_ACCESS_TOKEN}
    if extra:
        base.update(extra)
    return base


# ── Ads ───────────────────────────────────────────────────────────────────────

def _fetch_ad_insights(date_str: str) -> list[dict]:
    url = f"{GRAPH}/{META_AD_ACCOUNT_ID}/insights"
    params = _p({
        "fields":         "campaign_name,impressions,clicks,spend,reach,cpm",
        "level":          "campaign",
        "time_range":     f'{{"since":"{date_str}","until":"{date_str}"}}',
        "time_increment": "1",
        "limit":          100,
    })
    rows = []
    while url:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        for item in body.get("data", []):
            rows.append({
                "date":        item.get("date_start", date_str),
                "campaign":    item.get("campaign_name", "(unknown)"),
                "impressions": int(item.get("impressions", 0)),
                "clicks":      int(item.get("clicks", 0)),
                "spend":       float(item.get("spend", 0)),
                "reach":       int(item.get("reach", 0)),
                "cpm":         float(item.get("cpm", 0)),
            })
        url    = body.get("paging", {}).get("next")
        params = {}  # next URL includes all params already
    return rows


# ── Page organic ──────────────────────────────────────────────────────────────

def _fetch_page_insights(date_str: str) -> dict | None:
    metrics = "page_impressions,page_reach,page_engaged_users,page_views_total"
    url = f"{GRAPH}/{META_PAGE_ID}/insights"
    params = _p({
        "metric":  metrics,
        "period":  "day",
        "since":   date_str,
        "until":   date_str,
    })
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return None

    result = {"date": date_str, "reach": 0, "impressions": 0,
              "engaged_users": 0, "page_views": 0}
    key_map = {
        "page_impressions":    "impressions",
        "page_reach":          "reach",
        "page_engaged_users":  "engaged_users",
        "page_views_total":    "page_views",
    }
    for item in data:
        field = key_map.get(item.get("name"))
        if field:
            values = item.get("values", [])
            # 'since' date is index 0
            result[field] = int(values[0].get("value", 0)) if values else 0
    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    if not _check_creds():
        return

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    date_str = target_date.isoformat()
    print(f"[meta] fetching {date_str}")

    ad_rows = _fetch_ad_insights(date_str)
    print(f"[meta] {len(ad_rows)} ad campaign row(s)")
    upsert("meta_ads_daily", ad_rows)

    page_row = _fetch_page_insights(date_str)
    if page_row:
        upsert("meta_page_daily", [page_row])
        print(f"[meta] page: {page_row}")
    else:
        print("[meta] no page insights returned")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
