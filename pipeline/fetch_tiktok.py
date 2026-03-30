"""
TikTok Organic Profile Analytics.

Fetches RespireLYF TikTok account stats and per-video insights
using the TikTok Content API (open.tiktokapis.com/v2).

Credentials (GitHub Secrets):
  TIKTOK_CLIENT_KEY     — App client key from developer.tiktok.com
  TIKTOK_CLIENT_SECRET  — App client secret
  TIKTOK_ACCESS_TOKEN   — OAuth access token (refreshed automatically)
  TIKTOK_REFRESH_TOKEN  — OAuth refresh token (long-lived)
  TIKTOK_OPEN_ID        — TikTok user open_id

Writes to:
  tiktok_account_daily   (date, followers, total_likes, video_count)
  tiktok_video_insights  (video_id, date, title, views, likes, comments, shares)

Run: python -m pipeline.fetch_tiktok
"""
import sys
from datetime import date, timedelta

import requests

from pipeline.config import (
    TIKTOK_CLIENT_KEY,
    TIKTOK_CLIENT_SECRET,
    TIKTOK_ACCESS_TOKEN,
    TIKTOK_REFRESH_TOKEN,
    TIKTOK_OPEN_ID,
)
from pipeline.store import upsert

API_BASE   = "https://open.tiktokapis.com/v2"
TOKEN_URL  = f"{API_BASE}/oauth/token/"


# ── Auth ──────────────────────────────────────────────────────────────────────

def _refresh_access_token() -> str | None:
    """Exchange refresh token for a new access token."""
    if not TIKTOK_REFRESH_TOKEN or not TIKTOK_CLIENT_KEY or not TIKTOK_CLIENT_SECRET:
        return TIKTOK_ACCESS_TOKEN  # fall back to existing token
    r = requests.post(TOKEN_URL, data={
        "client_key":     TIKTOK_CLIENT_KEY,
        "client_secret":  TIKTOK_CLIENT_SECRET,
        "grant_type":     "refresh_token",
        "refresh_token":  TIKTOK_REFRESH_TOKEN,
    }, timeout=30)
    if r.status_code != 200:
        print(f"[tiktok] token refresh failed {r.status_code}: {r.text[:200]}")
        return TIKTOK_ACCESS_TOKEN  # fall back
    data = r.json().get("data", r.json())
    token = data.get("access_token")
    print(f"[tiktok] token refreshed ✓")
    return token or TIKTOK_ACCESS_TOKEN


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }


# ── Fetch ─────────────────────────────────────────────────────────────────────

def _fetch_profile(token: str) -> dict | None:
    """Fetch account-level stats: followers, likes, video count."""
    fields = "display_name,follower_count,likes_count,video_count"
    r = requests.get(
        f"{API_BASE}/user/info/",
        headers=_headers(token),
        params={"fields": fields},
        timeout=30,
    )
    if r.status_code != 200:
        print(f"[tiktok] profile error {r.status_code}: {r.text[:300]}")
        return None
    data = r.json().get("data", {}).get("user", {})
    return {
        "followers":    int(data.get("follower_count", 0)),
        "total_likes":  int(data.get("likes_count", 0)),
        "video_count":  int(data.get("video_count", 0)),
    }


def _fetch_videos(token: str, open_id: str) -> list[dict]:
    """Fetch per-video stats (most recent 20 videos)."""
    fields = "id,title,create_time,share_url,video_description,duration,cover_image_url,share_count,view_count,like_count,comment_count,play_count"
    rows   = []
    cursor = None

    for _ in range(3):  # max 3 pages (60 videos)
        payload = {
            "max_count": 20,
            "fields":    fields,
        }
        if cursor:
            payload["cursor"] = cursor

        r = requests.post(
            f"{API_BASE}/video/list/",
            headers=_headers(token),
            json=payload,
            timeout=30,
        )
        if r.status_code != 200:
            print(f"[tiktok] video list error {r.status_code}: {r.text[:300]}")
            break

        resp     = r.json().get("data", {})
        videos   = resp.get("videos", [])
        today_str = date.today().isoformat()

        for v in videos:
            stats = v.get("statistics", v)  # some API versions nest, some don't
            rows.append({
                "video_id":   v.get("id", ""),
                "date":       today_str,
                "title":      (v.get("title") or v.get("video_description", ""))[:200],
                "views":      int(stats.get("view_count",    v.get("view_count",    0))),
                "likes":      int(stats.get("like_count",    v.get("like_count",    0))),
                "comments":   int(stats.get("comment_count", v.get("comment_count", 0))),
                "shares":     int(stats.get("share_count",   v.get("share_count",   0))),
                "play_count": int(stats.get("play_count",    v.get("play_count",    0))),
            })

        if not resp.get("has_more"):
            break
        cursor = resp.get("cursor")

    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    missing = [k for k, v in {
        "TIKTOK_CLIENT_KEY":    TIKTOK_CLIENT_KEY,
        "TIKTOK_CLIENT_SECRET": TIKTOK_CLIENT_SECRET,
        "TIKTOK_REFRESH_TOKEN": TIKTOK_REFRESH_TOKEN,
    }.items() if not v]
    if missing:
        print(f"[tiktok] SKIP — missing credentials: {', '.join(missing)}")
        return

    if target_date is None:
        target_date = date.today()

    print(f"[tiktok] fetching for {target_date.isoformat()}")

    # Always refresh token first
    token = _refresh_access_token()
    if not token:
        print("[tiktok] no token — aborting")
        return

    open_id = TIKTOK_OPEN_ID or ""

    # ── Profile / account daily ──────────────────────────────
    profile = _fetch_profile(token)
    if profile:
        profile["date"] = target_date.isoformat()
        upsert("tiktok_account_daily", [profile])
        print(f"[tiktok] profile — followers: {profile['followers']}, likes: {profile['total_likes']}")

    # ── Per-video insights ───────────────────────────────────
    videos = _fetch_videos(token, open_id)
    if videos:
        upsert("tiktok_video_insights", videos)
        print(f"[tiktok] videos upserted: {len(videos)} ✓")
    else:
        print("[tiktok] no videos returned")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
