"""
YouTube Details — One-time CSV uploader to Supabase.

Reads the 3 CSVs from youtube_details/ folder and upserts into:
  yt_totals       (period, engaged_views)
  yt_chart_data   (period, video_id, video_title, publish_time, duration_seconds, engaged_views)
  yt_video_stats  (video_id, video_title, publish_time, duration_seconds, engaged_views, ...)

Run:
  python -m pipeline.upload_yt_details

Requirements: SUPABASE_URL + SUPABASE_SERVICE_KEY in env or .env file
"""
import csv
import os
import sys
from pathlib import Path

from pipeline.store import upsert

# Folder containing the 3 CSVs — relative to project root
CSV_DIR = Path(__file__).parent.parent / "youtube_details"


def _float(val: str) -> float | None:
    try:
        return float(val) if val.strip() else None
    except (ValueError, AttributeError):
        return None


def _int(val: str) -> int | None:
    try:
        return int(float(val)) if val.strip() else None
    except (ValueError, AttributeError):
        return None


def _str(val: str) -> str | None:
    s = val.strip() if val else ""
    return s if s else None


# ── Totals ────────────────────────────────────────────────────────────────────

def upload_totals():
    path = CSV_DIR / "Totals.csv"
    if not path.exists():
        print(f"[yt_details] Totals.csv not found at {path} — skipping")
        return
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            period = _str(row.get("Date", ""))
            ev     = _int(row.get("Engaged views", ""))
            if period:
                rows.append({"period": period, "engaged_views": ev or 0})
    upsert("yt_totals", rows)


# ── Chart Data ────────────────────────────────────────────────────────────────

def upload_chart_data():
    path = CSV_DIR / "Chart data.csv"
    if not path.exists():
        print(f"[yt_details] Chart data.csv not found at {path} — skipping")
        return
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            video_id = _str(row.get("Content", ""))
            if not video_id:
                continue
            rows.append({
                "period":           _str(row.get("Date", "")) or "",
                "video_id":         video_id,
                "video_title":      _str(row.get("Video title", "")),
                "publish_time":     _str(row.get("Video publish time", "")),
                "duration_seconds": _int(row.get("Duration", "")),
                "engaged_views":    _int(row.get("Engaged views", "")) or 0,
            })
    upsert("yt_chart_data", rows)


# ── Video Stats ───────────────────────────────────────────────────────────────

def upload_video_stats():
    path = CSV_DIR / "Table data.csv"
    if not path.exists():
        print(f"[yt_details] Table data.csv not found at {path} — skipping")
        return
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            video_id_raw = _str(row.get("Content", ""))
            # First row is the aggregate "Total" row — use sentinel key
            video_id = video_id_raw if video_id_raw else "_TOTAL_"
            rows.append({
                "video_id":                 video_id,
                "video_title":              _str(row.get("Video title", "")),
                "publish_time":             _str(row.get("Video publish time", "")),
                "duration_seconds":         _int(row.get("Duration", "")),
                "engaged_views":            _int(row.get("Engaged views", "")),
                "avg_view_duration":        _str(row.get("Average view duration", "")),
                "avg_pct_viewed":           _float(row.get("Average percentage viewed (%)", "")),
                "stayed_to_watch_pct":      _float(row.get("Stayed to watch (%)", "")),
                "unique_viewers":           _int(row.get("Unique viewers", "")),
                "avg_views_per_viewer":     _float(row.get("Average views per viewer", "")),
                "new_viewers":              _int(row.get("New viewers", "")),
                "returning_viewers":        _int(row.get("Returning viewers", "")),
                "casual_viewers":           _int(row.get("Casual viewers", "")),
                "regular_viewers":          _int(row.get("Regular viewers", "")),
                "subs_gained":              _int(row.get("Subscribers gained", "")),
                "likes":                    _int(row.get("Likes", "")),
                "dislikes":                 _int(row.get("Dislikes", "")),
                "likes_vs_dislikes_pct":    _float(row.get("Likes (vs dislikes) (%)", "")),
                "shares":                   _int(row.get("Shares", "")),
                "comments_added":           _int(row.get("Comments added", "")),
                "playlist_watch_time_hrs":  _float(row.get("Playlist watch time (hours)", "")),
                "views_from_playlist":      _int(row.get("Views from playlist", "")),
                "post_subscribers":         _int(row.get("Post subscribers", "")),
                "views":                    _int(row.get("Views", "")),
                "watch_time_hours":         _float(row.get("Watch time (hours)", "")),
                "subscribers":              _int(row.get("Subscribers", "")),
                "impressions":              _int(row.get("Impressions", "")),
                "impressions_ctr_pct":      _float(row.get("Impressions click-through rate (%)", "")),
            })
    upsert("yt_video_stats", rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    print(f"[yt_details] Reading CSVs from: {CSV_DIR}")
    if not CSV_DIR.exists():
        print(f"[yt_details] ERROR: folder not found → {CSV_DIR}")
        sys.exit(1)
    upload_totals()
    upload_chart_data()
    upload_video_stats()
    print("[yt_details] Done ✓")


if __name__ == "__main__":
    run()
