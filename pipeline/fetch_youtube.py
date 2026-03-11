"""
YouTube Analytics — daily channel stats + top video performance.

Needs credentials:  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET,
                    GOOGLE_REFRESH_TOKEN, YOUTUBE_CHANNEL_ID

Writes to:
  youtube_channel_daily  (date, views, impressions, ctr, watch_time_min, subscribers)
  youtube_videos         (video_id, title, views, impressions, ctr, published_at)

Run:  python -m pipeline.fetch_youtube
"""
import sys
from datetime import date, timedelta

import requests

from pipeline.config import (
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REFRESH_TOKEN,
    YOUTUBE_CHANNEL_ID,
)
from pipeline.store import upsert

YT_ANALYTICS = "https://youtubeanalytics.googleapis.com/v2/reports"
YT_DATA      = "https://www.googleapis.com/youtube/v3"
TOKEN_URL    = "https://oauth2.googleapis.com/token"


# ── Credential check ──────────────────────────────────────────────────────────

def _check_creds() -> bool:
    missing = [k for k, v in {
        "GOOGLE_CLIENT_ID":     GOOGLE_CLIENT_ID,
        "GOOGLE_CLIENT_SECRET": GOOGLE_CLIENT_SECRET,
        "GOOGLE_REFRESH_TOKEN": GOOGLE_REFRESH_TOKEN,
        "YOUTUBE_CHANNEL_ID":   YOUTUBE_CHANNEL_ID,
    }.items() if not v]
    if missing:
        print(f"[youtube] SKIP — missing credentials: {', '.join(missing)}")
        return False
    return True


# ── OAuth refresh ─────────────────────────────────────────────────────────────

def _get_access_token() -> str:
    r = requests.post(TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "client_id":     GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
    }, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


# ── Analytics API ─────────────────────────────────────────────────────────────

def _channel_daily(access_token: str, start: str, end: str) -> list[dict]:
    """Fetch views, estimatedMinutesWatched, subscribersGained per day."""
    params = {
        "ids":        f"channel=={YOUTUBE_CHANNEL_ID}",
        "startDate":  start,
        "endDate":    end,
        "metrics":    "views,estimatedMinutesWatched,subscribersGained,subscribersLost",
        "dimensions": "day",
        "sort":       "day",
    }
    r = requests.get(YT_ANALYTICS, params=params,
                     headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    body = r.json()
    cols = [h["name"] for h in body.get("columnHeaders", [])]
    rows = []
    for row in body.get("rows", []):
        d = dict(zip(cols, row))
        rows.append({
            "date":           d["day"],
            "views":          int(d.get("views", 0)),
            "watch_time_min": float(d.get("estimatedMinutesWatched", 0)),
            "subscribers":    int(d.get("subscribersGained", 0)) - int(d.get("subscribersLost", 0)),
        })
    return rows


def _impressions_daily(access_token: str, start: str, end: str) -> list[dict]:
    """Fetch impressions + CTR per day (requires YouTube Studio access level)."""
    params = {
        "ids":        f"channel=={YOUTUBE_CHANNEL_ID}",
        "startDate":  start,
        "endDate":    end,
        "metrics":    "impressions,impressionsClickThroughRate",
        "dimensions": "day",
        "sort":       "day",
    }
    r = requests.get(YT_ANALYTICS, params=params,
                     headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if r.status_code == 403:
        print("[youtube] impressions API not accessible — skipping CTR data")
        return []
    r.raise_for_status()
    body = r.json()
    cols = [h["name"] for h in body.get("columnHeaders", [])]
    rows = []
    for row in body.get("rows", []):
        d = dict(zip(cols, row))
        rows.append({
            "date":        d["day"],
            "impressions": int(d.get("impressions", 0)),
            "ctr":         round(float(d.get("impressionsClickThroughRate", 0)) * 100, 2),
        })
    return rows


def _top_videos(access_token: str, start: str, end: str, limit: int = 20) -> list[dict]:
    """Fetch top videos by views over the period."""
    params = {
        "ids":        f"channel=={YOUTUBE_CHANNEL_ID}",
        "startDate":  start,
        "endDate":    end,
        "metrics":    "views,impressions,impressionsClickThroughRate",
        "dimensions": "video",
        "sort":       "-views",
        "maxResults": limit,
    }
    r = requests.get(YT_ANALYTICS, params=params,
                     headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    body = r.json()
    cols = [h["name"] for h in body.get("columnHeaders", [])]
    analytics = {}
    for row in body.get("rows", []):
        d = dict(zip(cols, row))
        vid_id = d["video"]
        analytics[vid_id] = {
            "views":       int(d.get("views", 0)),
            "impressions": int(d.get("impressions", 0)),
            "ctr":         round(float(d.get("impressionsClickThroughRate", 0)) * 100, 2),
        }

    if not analytics:
        return []

    # Fetch titles + publish dates from Data API
    vid_ids = ",".join(analytics.keys())
    dr = requests.get(f"{YT_DATA}/videos", params={
        "part": "snippet",
        "id":   vid_ids,
        "key":  "",  # not needed with OAuth
    }, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    dr.raise_for_status()
    items = dr.json().get("items", [])

    rows = []
    for item in items:
        vid_id = item["id"]
        snip   = item.get("snippet", {})
        stats  = analytics.get(vid_id, {})
        rows.append({
            "video_id":     vid_id,
            "title":        snip.get("title", ""),
            "published_at": snip.get("publishedAt", ""),
            "views":        stats.get("views", 0),
            "impressions":  stats.get("impressions", 0),
            "ctr":          stats.get("ctr", 0.0),
        })
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    if not _check_creds():
        return

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    # Fetch a 30-day window ending on target_date to keep video rankings current
    start = (target_date - timedelta(days=29)).isoformat()
    end   = target_date.isoformat()

    token = _get_access_token()
    print(f"[youtube] fetching {start} → {end}")

    # Daily channel stats
    daily = _channel_daily(token, start, end)
    impr  = _impressions_daily(token, start, end)

    # Merge impressions into daily rows
    impr_map = {r["date"]: r for r in impr}
    for row in daily:
        d = row["date"]
        row["impressions"] = impr_map.get(d, {}).get("impressions", 0)
        row["ctr"]         = impr_map.get(d, {}).get("ctr", 0.0)

    upsert("youtube_channel_daily", daily)
    print(f"[youtube] {len(daily)} daily channel row(s)")

    # Top videos
    videos = _top_videos(token, start, end)
    if videos:
        upsert("youtube_videos", videos)
        print(f"[youtube] {len(videos)} video(s) upserted")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
