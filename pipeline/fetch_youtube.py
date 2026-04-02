"""
YouTube Analytics — daily channel stats + per-video depth data.

Needs credentials:  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET,
                    GOOGLE_REFRESH_TOKEN, YOUTUBE_CHANNEL_ID

Writes to:
  youtube_channel_daily  (date, views, impressions, ctr, watch_time_min, subscribers)
  youtube_videos         (video_id, title, views, impressions, ctr, published_at)
  youtube_video_daily    (video_id, date, title, views, watch_time_min, impressions,
                          ctr, likes, comments, avg_view_duration)  ← per-video depth

Run:  python -m pipeline.fetch_youtube
"""
import os
import sys
from datetime import date, timedelta

# How many days of history to fetch — override via FETCH_DAYS env var
# Default 30 for daily runs; set to 90+ for backfill
_FETCH_DAYS = int(os.environ.get("FETCH_DAYS", "30"))

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
    if not r.ok:
        print(f"[youtube] token exchange failed {r.status_code}: {r.text}")
    r.raise_for_status()
    return r.json()["access_token"]


# ── Analytics API ─────────────────────────────────────────────────────────────

def _channel_daily(access_token: str, start: str, end: str) -> list[dict]:
    """Fetch views, estimatedMinutesWatched, subscribersGained per day."""
    params = {
        "ids":        "channel==MINE",
        "startDate":  start,
        "endDate":    end,
        "metrics":    "views,estimatedMinutesWatched,subscribersGained,subscribersLost",
        "dimensions": "day",
        "sort":       "day",
    }
    r = requests.get(YT_ANALYTICS, params=params,
                     headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if r.status_code == 403:
        print(f"[youtube] channel daily 403 — YouTube Analytics API may not be enabled in Google Cloud Console")
        print(f"[youtube] hint: console.cloud.google.com → APIs & Services → Library → 'YouTube Analytics API' → Enable")
        print(f"[youtube] response: {r.text[:300]}")
        return []
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
    """Fetch impressions + CTR per day using correct metric names."""
    # Try with day dimension first
    params = {
        "ids":        "channel==MINE",
        "startDate":  start,
        "endDate":    end,
        "metrics":    "videoThumbnailImpressions,videoThumbnailImpressionsClickRate",
        "dimensions": "day",
        "sort":       "day",
    }
    r = requests.get(YT_ANALYTICS, params=params,
                     headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if r.status_code == 400:
        # Try without day dimension (aggregate for period)
        params2 = {k: v for k, v in params.items() if k not in ("dimensions", "sort")}
        r2 = requests.get(YT_ANALYTICS, params=params2,
                          headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
        if r2.status_code != 200:
            print(f"[youtube] impressions API not available ({r2.status_code}) — skipping CTR")
            return []
        body = r2.json()
        cols = [h["name"] for h in body.get("columnHeaders", [])]
        rows_raw = body.get("rows", [])
        if not rows_raw:
            return []
        d = dict(zip(cols, rows_raw[0]))
        total_impr = int(d.get("videoThumbnailImpressions", 0))
        total_ctr  = round(float(d.get("videoThumbnailImpressionsClickRate", 0)) * 100, 2)
        print(f"[youtube] impressions aggregate: {total_impr} impr, {total_ctr}% CTR")
        return [{"date": None, "impressions": total_impr, "ctr": total_ctr}]
    if r.status_code == 403:
        print(f"[youtube] impressions API 403 — insufficient scope")
        return []
    r.raise_for_status()
    body = r.json()
    cols = [h["name"] for h in body.get("columnHeaders", [])]
    rows = []
    for row in body.get("rows", []):
        d = dict(zip(cols, row))
        rows.append({
            "date":        d["day"],
            "impressions": int(d.get("videoThumbnailImpressions", 0)),
            "ctr":         round(float(d.get("videoThumbnailImpressionsClickRate", 0)) * 100, 2),
        })
    print(f"[youtube] impressions daily: {len(rows)} rows")
    return rows


def _top_videos(access_token: str, start: str, end: str, limit: int = 20) -> list[dict]:
    """Fetch top videos by views over the period."""
    params = {
        "ids":        "channel==MINE",
        "startDate":  start,
        "endDate":    end,
        "metrics":    "views",
        "dimensions": "video",
        "sort":       "-views",
        "maxResults": limit,
    }
    r = requests.get(YT_ANALYTICS, params=params,
                     headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    if r.status_code in (400, 403):
        print(f"[youtube] top_videos {r.status_code} — skipping: {r.text[:200]}")
        return []
    r.raise_for_status()
    body = r.json()
    cols = [h["name"] for h in body.get("columnHeaders", [])]
    analytics = {}
    for row in body.get("rows", []):
        d = dict(zip(cols, row))
        vid_id = d["video"]
        analytics[vid_id] = {
            "views": int(d.get("views", 0)),
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
            "impressions":  0,
            "ctr":          0.0,
        })
    return rows


# ── Per-video daily depth ─────────────────────────────────────────────────────

def _video_daily(
    access_token: str,
    start: str,
    end: str,
    title_map: dict[str, str],
) -> list[dict]:
    """
    Fetch per-video aggregated metrics over the window (dimensions=video).
    dimensions=video,day is not a supported report type in YouTube Analytics API.
    Stores with date=end so each daily run upserts that day's 30-day window totals.
    """
    params = {
        "ids":        "channel==MINE",
        "startDate":  start,
        "endDate":    end,
        "metrics":    "views,estimatedMinutesWatched,averageViewDuration,averageViewPercentage,likes,comments",
        "dimensions": "video",
        "sort":       "-views",
        "maxResults": 50,
    }
    r = requests.get(
        YT_ANALYTICS, params=params,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if r.status_code == 403:
        print("[youtube] per-video depth API not accessible — skipping")
        return []
    if r.status_code != 200:
        print(f"[youtube] per-video depth error {r.status_code}: {r.text[:200]}")
        return []

    body = r.json()
    cols = [h["name"] for h in body.get("columnHeaders", [])]
    rows = []
    for row in body.get("rows", []):
        d = dict(zip(cols, row))
        vid_id = d.get("video", "")
        rows.append({
            "video_id":          vid_id,
            "date":              end,   # period end date — upserts on each run
            "title":             title_map.get(vid_id, ""),
            "views":             int(d.get("views", 0)),
            "watch_time_min":    round(float(d.get("estimatedMinutesWatched", 0)), 1),
            "impressions":       0,
            "ctr":               0.0,
            "likes":             int(d.get("likes", 0)),
            "comments":          int(d.get("comments", 0)),
            "avg_view_duration": round(float(d.get("averageViewDuration", 0)), 1),
            "avg_view_pct":      round(float(d.get("averageViewPercentage", 0)), 2),
        })
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    if not _check_creds():
        return

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    # Fetch a window ending on target_date — width set by FETCH_DAYS (default 30)
    start = (target_date - timedelta(days=_FETCH_DAYS - 1)).isoformat()
    end   = target_date.isoformat()

    token = _get_access_token()
    print(f"[youtube] fetching {start} → {end}")

    # Daily channel stats + impressions/CTR
    daily = _channel_daily(token, start, end)
    impr  = _impressions_daily(token, start, end)
    impr_map = {r["date"]: r for r in impr if r.get("date")}
    for row in daily:
        d = row["date"]
        row["impressions"] = impr_map.get(d, {}).get("impressions", 0)
        row["ctr"]         = impr_map.get(d, {}).get("ctr", 0.0)

    upsert("youtube_channel_daily", daily)
    print(f"[youtube] {len(daily)} daily channel row(s)")

    # Top videos (aggregated over window)
    videos = _top_videos(token, start, end)
    if videos:
        upsert("youtube_videos", videos)
        print(f"[youtube] {len(videos)} video(s) upserted")

    # Per-video daily depth ← NEW
    title_map = {v["video_id"]: v["title"] for v in videos} if videos else {}
    video_daily = _video_daily(token, start, end, title_map)
    if video_daily:
        upsert("youtube_video_daily", video_daily)
        print(f"[youtube] {len(video_daily)} per-video-day row(s) upserted")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
