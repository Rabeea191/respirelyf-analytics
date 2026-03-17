"""
Meta — Facebook Page + Instagram + Ads daily insights.
Also fetches per-post engagement for in-depth analysis.

Credentials (GitHub Secrets):
  META_APP_ID         — Meta App ID (for token exchange)
  META_APP_SECRET     — Meta App Secret (for token exchange)
  META_ACCESS_TOKEN   — Short or long-lived user token (from Graph API Explorer)
  META_AD_ACCOUNT_ID  — act_xxxxxxxxxx
  META_PAGE_ID        — numeric Page ID

Permissions needed:
  ads_read, pages_read_engagement, pages_show_list,
  read_insights, instagram_basic, instagram_manage_insights

Writes to:
  meta_page_daily    — daily FB page metrics (reach, impressions, engaged, views)
  meta_post_insights — per-post engagement (FB + IG combined)
  meta_ads_daily     — campaign-level ad spend/reach

Run: python -m pipeline.fetch_meta
"""
import sys
from datetime import date, timedelta

import requests

from pipeline.config import (
    META_ACCESS_TOKEN,
    META_AD_ACCOUNT_ID,
    META_PAGE_ID,
    META_APP_ID,
    META_APP_SECRET,
)
from pipeline.store import upsert

GRAPH = "https://graph.facebook.com/v19.0"


# ── Credentials check ──────────────────────────────────────────────────────────

def _check_creds() -> bool:
    missing = [k for k, v in {
        "META_ACCESS_TOKEN": META_ACCESS_TOKEN,
        "META_PAGE_ID":      META_PAGE_ID,
    }.items() if not v]
    if missing:
        print(f"[meta] SKIP — missing credentials: {', '.join(missing)}")
        return False
    return True


# ── Token exchange: short-lived → long-lived → permanent page token ────────────

def _get_long_lived_token(user_token: str) -> str:
    """Exchange short-lived user token for long-lived (60 days)."""
    if not META_APP_ID or not META_APP_SECRET:
        print("[meta] No APP_ID/APP_SECRET — using token as-is (may expire in 1hr)")
        return user_token
    r = requests.get(f"{GRAPH}/oauth/access_token", params={
        "grant_type":        "fb_exchange_token",
        "client_id":         META_APP_ID,
        "client_secret":     META_APP_SECRET,
        "fb_exchange_token": user_token,
    }, timeout=30)
    if r.status_code != 200:
        print(f"[meta] Token exchange failed: {r.text[:200]} — using original token")
        return user_token
    long_token = r.json().get("access_token", user_token)
    print("[meta] Token exchanged → long-lived (60 days) ✓")
    return long_token


def _get_page_token(long_user_token: str, page_id: str) -> str:
    """Get Page Access Token.
    Attempt 1: /me/accounts  (requires pages_show_list + pages_manage_metadata)
    Attempt 2: direct /{page_id}?fields=access_token  (works for NPE pages)
    """
    # ── Attempt 1: /me/accounts ───────────────────────────────────────────────
    r = requests.get(f"{GRAPH}/me/accounts", params={
        "access_token": long_user_token,
    }, timeout=30)
    if r.status_code == 200:
        pages = r.json().get("data", [])
        for page in pages:
            if str(page.get("id")) == str(page_id):
                print("[meta] Page Access Token obtained via /me/accounts ✓")
                return page["access_token"]
        # Log page names found (helps diagnose ID mismatch)
        for p in pages:
            print(f"[meta] Found page: name='{p.get('name')}' id={p.get('id')}")
        # Fallback: if only one page exists, use it directly
        if len(pages) == 1:
            print(f"[meta] Using only available page token (ID mismatch — check META_PAGE_ID)")
            return pages[0]["access_token"]
        print(f"[meta] Page {page_id} not found in /me/accounts "
              f"({len(pages)} pages returned) — trying direct page node...")
    else:
        print(f"[meta] /me/accounts error {r.status_code} — trying direct page node...")

    # ── Attempt 2: direct page node (bypasses /me/accounts, works for NPE) ────
    r2 = requests.get(f"{GRAPH}/{page_id}", params={
        "fields":       "id,name,access_token",
        "access_token": long_user_token,
    }, timeout=30)
    if r2.status_code == 200:
        token = r2.json().get("access_token")
        if token:
            print("[meta] Page Access Token obtained via direct page node ✓")
            return token
        print("[meta] Direct page node returned no access_token — "
              "add 'pages_manage_metadata' permission to your token")
    else:
        print(f"[meta] Direct page node error {r2.status_code}: {r2.text[:200]}")

    print("[meta] WARNING: using user token — page insights may fail (New Page Experience)")
    return long_user_token


def _get_instagram_id(page_token: str, page_id: str) -> str | None:
    """Get Instagram Business Account ID linked to the Facebook Page."""
    r = requests.get(f"{GRAPH}/{page_id}", params={
        "fields":       "instagram_business_account",
        "access_token": page_token,
    }, timeout=30)
    if r.status_code != 200:
        return None
    ig = r.json().get("instagram_business_account") or {}
    ig_id = ig.get("id")
    if ig_id:
        print(f"[meta] Instagram Account ID: {ig_id} ✓")
    else:
        print("[meta] No Instagram Business Account linked to Page")
    return ig_id


# ── Facebook Page daily insights ───────────────────────────────────────────────

def _fetch_page_insights(page_token: str, page_id: str, date_str: str) -> dict | None:
    # page_engaged_users deprecated for New Page Experience — use NPE-compatible metrics
    metrics = "page_impressions,page_impressions_unique,page_post_engagements,page_views_total"
    # Facebook requires until > since by at least 1 day
    until_str = (date.fromisoformat(date_str) + timedelta(days=1)).isoformat()
    r = requests.get(f"{GRAPH}/{page_id}/insights", params={
        "metric":       metrics,
        "period":       "day",
        "since":        date_str,
        "until":        until_str,
        "access_token": page_token,
    }, timeout=30)
    if r.status_code != 200:
        print(f"[meta] Page insights error {r.status_code}: {r.text[:300]}")
        return None
    data = r.json().get("data", [])
    if not data:
        return None
    result = {"date": date_str, "reach": 0, "impressions": 0,
              "engaged_users": 0, "page_views": 0}
    key_map = {
        "page_impressions":      "impressions",
        "page_reach":            "reach",
        "page_engaged_users":    "engaged_users",
        "page_post_engagements": "page_views",
    }
    for item in data:
        field = key_map.get(item.get("name"))
        if field:
            values = item.get("values", [])
            result[field] = int(values[0].get("value", 0)) if values else 0
    return result


# ── Facebook per-post insights ─────────────────────────────────────────────────

def _detect_fb_post_type(post: dict) -> str:
    attachments = post.get("attachments", {}).get("data", [])
    if attachments:
        t = attachments[0].get("type", "")
        if "video" in t:
            return "video"
        if "photo" in t:
            return "photo"
    return "status"


def _fetch_fb_posts(page_token: str, page_id: str, since_date: str) -> list[dict]:
    """Fetch FB posts since given date + per-post engagement metrics."""
    r = requests.get(f"{GRAPH}/{page_id}/posts", params={
        "fields":       "id,message,created_time,attachments",
        "since":        since_date,
        "limit":        50,
        "access_token": page_token,
    }, timeout=30)
    if r.status_code != 200:
        print(f"[meta] FB posts error {r.status_code}: {r.text[:400]}")
        return []
    posts = r.json().get("data", [])
    print(f"[meta] FB posts found: {len(posts)}")

    rows = []
    for post in posts:
        post_id   = post["id"]
        post_date = post["created_time"][:10]
        caption   = (post.get("message") or post.get("story") or "")[:200]

        row = {
            "post_id":     post_id,
            "platform":    "facebook",
            "date":        post_date,
            "post_type":   _detect_fb_post_type(post),
            "message":     caption,
            "impressions": 0,
            "reach":       0,
            "likes":       0,
            "comments":    0,
            "shares":      0,
            "saves":       0,
            "engagement":  0,
            "video_views": 0,
        }

        # Per-post insight metrics
        ir = requests.get(f"{GRAPH}/{post_id}/insights", params={
            "metric":       "post_impressions,post_reach,post_engaged_users,"
                            "post_reactions_by_type_total,post_clicks,post_shares",
            "access_token": page_token,
        }, timeout=30)

        if ir.status_code == 200:
            for m in ir.json().get("data", []):
                name = m.get("name")
                val  = m.get("values", [{}])[0].get("value", 0)
                if name == "post_impressions":
                    row["impressions"] = int(val or 0)
                elif name == "post_reach":
                    row["reach"] = int(val or 0)
                elif name == "post_engaged_users":
                    row["engagement"] = int(val or 0)
                elif name == "post_reactions_by_type_total":
                    row["likes"] = int(sum((val or {}).values()))
                elif name == "post_shares":
                    row["shares"] = int(val or 0)

        rows.append(row)

    return rows


# ── Instagram per-media insights ───────────────────────────────────────────────

def _fetch_ig_media(page_token: str, ig_id: str, since_date: str) -> list[dict]:
    """Fetch IG posts/reels since given date + per-media engagement metrics."""
    r = requests.get(f"{GRAPH}/{ig_id}/media", params={
        "fields":       "id,caption,media_type,timestamp,like_count,comments_count",
        "since":        since_date,
        "limit":        50,
        "access_token": page_token,
    }, timeout=30)
    r.raise_for_status()
    medias = r.json().get("data", [])
    print(f"[meta] IG media found: {len(medias)}")

    rows = []
    for media in medias:
        media_id   = media["id"]
        media_date = media["timestamp"][:10]
        media_type = media.get("media_type", "IMAGE").lower()

        # Reels have 'plays', others don't
        metrics = "impressions,reach,likes,comments,shares,saved"
        if media_type in ("video", "reel"):
            metrics += ",plays"

        row = {
            "post_id":     media_id,
            "platform":    "instagram",
            "date":        media_date,
            "post_type":   media_type,
            "message":     (media.get("caption") or "")[:200],
            "impressions": 0,
            "reach":       0,
            "likes":       int(media.get("like_count", 0)),
            "comments":    int(media.get("comments_count", 0)),
            "shares":      0,
            "saves":       0,
            "engagement":  0,
            "video_views": 0,
        }

        ir = requests.get(f"{GRAPH}/{media_id}/insights", params={
            "metric":       metrics,
            "access_token": page_token,
        }, timeout=30)

        if ir.status_code == 200:
            for m in ir.json().get("data", []):
                name = m.get("name")
                val  = int(m.get("values", [{}])[0].get("value", 0) or 0)
                if name == "impressions": row["impressions"] = val
                elif name == "reach":     row["reach"]       = val
                elif name == "likes":     row["likes"]       = val
                elif name == "comments":  row["comments"]    = val
                elif name == "shares":    row["shares"]      = val
                elif name == "saved":     row["saves"]       = val
                elif name == "plays":     row["video_views"] = val

        row["engagement"] = (
            row["likes"] + row["comments"] + row["shares"] + row["saves"]
        )
        rows.append(row)

    return rows


# ── Meta Ads insights ──────────────────────────────────────────────────────────

def _fetch_ad_insights(token: str, date_str: str) -> list[dict]:
    if not META_AD_ACCOUNT_ID:
        print("[meta] No AD_ACCOUNT_ID — skipping ads")
        return []
    url    = f"{GRAPH}/{META_AD_ACCOUNT_ID}/insights"
    params = {
        "fields":         "campaign_name,impressions,clicks,spend,reach,cpm",
        "level":          "campaign",
        "time_range":     f'{{"since":"{date_str}","until":"{date_str}"}}',
        "time_increment": "1",
        "limit":          100,
        "access_token":   token,
    }
    rows = []
    while url:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 403:
            print(f"[meta] Ads insights 403 — token missing 'ads_read' permission. Skipping ads.")
            return []
        if r.status_code != 200:
            print(f"[meta] Ads insights error {r.status_code}: {r.text[:300]}")
            return []
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
        params = {}
    return rows


# ── Main ───────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    if not _check_creds():
        return

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    date_str  = target_date.isoformat()
    since_30d = (target_date - timedelta(days=30)).isoformat()
    print(f"[meta] fetching {date_str} (posts since {since_30d})")

    # 1. Token exchange → permanent page token
    long_token = _get_long_lived_token(META_ACCESS_TOKEN)
    page_token = _get_page_token(long_token, META_PAGE_ID)

    # 2. Instagram Account ID (from Page)
    ig_id = _get_instagram_id(page_token, META_PAGE_ID)

    # 3. FB Page daily insights
    page_row = _fetch_page_insights(page_token, META_PAGE_ID, date_str)
    if page_row:
        upsert("meta_page_daily", [page_row])
        print(f"[meta] page: reach={page_row['reach']}, impressions={page_row['impressions']} ✓")
    else:
        print("[meta] no page insights for this date")

    # 4. FB per-post insights (last 30 days)
    fb_posts = _fetch_fb_posts(page_token, META_PAGE_ID, since_30d)
    if fb_posts:
        upsert("meta_post_insights", fb_posts)
        print(f"[meta] FB posts upserted: {len(fb_posts)} ✓")

    # 5. Instagram per-media insights (last 30 days)
    if ig_id:
        ig_posts = _fetch_ig_media(page_token, ig_id, since_30d)
        if ig_posts:
            upsert("meta_post_insights", ig_posts)
            print(f"[meta] IG posts upserted: {len(ig_posts)} ✓")

    # 6. Meta Ads
    ad_rows = _fetch_ad_insights(long_token, date_str)
    if ad_rows:
        upsert("meta_ads_daily", ad_rows)
        print(f"[meta] ad campaigns: {len(ad_rows)} ✓")
    else:
        print("[meta] no active ad campaigns today")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
