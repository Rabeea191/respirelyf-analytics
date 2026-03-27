"""
Internal User Journey API — RespireLYF backend analytics.

Fetches per-user journey data from the internal LYFSuite API and
upserts into the firebase_user_journey Supabase table.

Run: python -m pipeline.fetch_user_journey
"""
import os
import sys
from datetime import date, timedelta, datetime, timezone

import requests

from pipeline.store import upsert

# ── Config ────────────────────────────────────────────────────────────────────

API_BASE   = "https://apis.lyfsuite.com/pr/rl/api/v1/internal"
API_KEY    = os.environ.get("ANALYTICS_API_KEY", "")
HEADERS    = {
    "Authorization": f"Bearer {API_KEY}",
    "X-App-Source":  "internal_dashboard",
    "X-Os-Source":   "web",
    "X-App-Version": "1.0.0",
    "X-User-Timezone": "UTC",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_dt(val: str | None) -> str | None:
    """Return ISO string or None."""
    if not val:
        return None
    return val  # already ISO format from API

def _days_since(dt_str: str | None) -> int:
    """Days since a datetime string (UTC). Returns 999 if None."""
    if not dt_str:
        return 999
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).days
    except Exception:
        return 999

def _compute_journey_stage(u: dict) -> str:
    """Classify user into journey stage based on profile + activity."""
    ps = u.get("profile_setup", {})
    hd = u.get("health_determinants", {})
    hi = u.get("health_indicators", {})
    md = u.get("mdric", {})

    profile_complete = sum([
        ps.get("profile_about_you", False),
        ps.get("profile_symptoms", False),
        ps.get("profile_root_causes", False),
        ps.get("profile_medication", False),
    ])

    total_logs = (
        sum(hd.values()) + sum(hi.values())
    )
    meeps = md.get("mdric_meeps", 0)

    if profile_complete == 0:
        return "onboarding"
    elif profile_complete < 4:
        return "setup"
    elif total_logs == 0:
        return "activated"
    elif total_logs < 10:
        return "early_logger"
    elif meeps > 0:
        return "engaged_mdric"
    else:
        return "active_logger"

def _compute_days_active(timing: dict) -> int:
    """Estimate days active from signup → last log."""
    signed_up = timing.get("signed_up")
    last_log  = timing.get("last_log") or timing.get("last_meep")
    if not signed_up or not last_log:
        return 0
    try:
        t0 = datetime.fromisoformat(signed_up.replace("Z", "+00:00"))
        t1 = datetime.fromisoformat(last_log.replace("Z", "+00:00"))
        return max(0, (t1 - t0).days + 1)
    except Exception:
        return 0

def _compute_status(timing: dict) -> str:
    """active if last_login within 7 days, else inactive."""
    last_login = timing.get("last_meep") or timing.get("last_log")
    if _days_since(last_login) <= 7:
        return "active"
    return "inactive"

# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_user_journey(start_date: str, end_date: str) -> list[dict]:
    url = f"{API_BASE}/user-journey"
    r = requests.get(url, headers=HEADERS, params={
        "start_date": start_date,
        "end_date":   end_date,
    }, timeout=60)

    if r.status_code != 200:
        print(f"[user_journey] API error {r.status_code}: {r.text[:300]}")
        return []

    data  = r.json()
    users = data.get("users", [])
    print(f"[user_journey] {len(users)} user(s) returned for {start_date} → {end_date}")
    return users

# ── Map → Supabase row ────────────────────────────────────────────────────────

def _map_row(u: dict) -> dict:
    timing = u.get("timing", {})
    ps     = u.get("profile_setup", {})
    tr     = u.get("treatments", {})
    hd     = u.get("health_determinants", {})
    hi     = u.get("health_indicators", {})
    md     = u.get("mdric", {})
    sess   = u.get("session", {})

    total_logs = sum(hd.values()) + sum(hi.values())

    return {
        "user_id":              u["user_id"],
        # Timing
        "signed_up_at":         _parse_dt(timing.get("signed_up")),
        "first_log_at":         _parse_dt(timing.get("first_log")),
        "last_log_at":          _parse_dt(timing.get("last_log")),
        "last_meep_at":         _parse_dt(timing.get("last_meep")),
        "last_login_at":        _parse_dt(sess.get("last_login_at")),
        # Profile
        "profile_about_you":    bool(ps.get("profile_about_you", False)),
        "profile_symptoms":     bool(ps.get("profile_symptoms", False)),
        "profile_root_causes":  bool(ps.get("profile_root_causes", False)),
        "profile_medication":   bool(ps.get("profile_medication", False)),
        # Treatments
        "inhalers":             int(tr.get("inhalers", 0)),
        "medications":          int(tr.get("medications", 0)),
        "supplements":          int(tr.get("supplements", 0)),
        # Health determinants
        "hd_food":              int(hd.get("hd_food", 0)),
        "hd_hydration":         int(hd.get("hd_hydration", 0)),
        "hd_sleep":             int(hd.get("hd_sleep", 0)),
        "hd_activity":          int(hd.get("hd_activity", 0)),
        "hd_stress":            int(hd.get("hd_stress", 0)),
        # Health indicators
        "hi_symptoms":          int(hi.get("hi_symptoms", 0)),
        "hi_flareups":          int(hi.get("hi_flareups", 0)),
        "hi_peak_flow":         int(hi.get("hi_peak_flow", 0)),
        "hi_vitals":            int(hi.get("hi_vitals", 0)),
        "hi_surveys":           int(hi.get("hi_surveys", 0)),
        # MDRIC
        "mdric_meeps":          int(md.get("mdric_meeps", 0)),
        "mdric_weekly":         int(md.get("mdric_weekly", 0)),
        "mdric_monthly":        int(md.get("mdric_monthly", 0)),
        "mdric_report":         int(md.get("mdric_report", 0)),
        "mdric_memories":       int(md.get("mdric_memories", 0)),
        # Session
        "has_session":          bool(sess.get("has_session", False)),
        "session_country":      sess.get("session_country"),
        "city":                 sess.get("city"),
        "region":               sess.get("region"),
        "auth_platform":        sess.get("auth_platform"),
        "app_source":           sess.get("app_source"),
        "device_type":          sess.get("device_type"),
        "os_version":           sess.get("os_version"),
        "app_version":          sess.get("app_version"),
        "language":             sess.get("language"),
        "device_country":       sess.get("device_country"),
        # Derived
        "journey_stage":        _compute_journey_stage(u),
        "current_status":       _compute_status(timing),
        "days_active":          _compute_days_active(timing),
        "total_logs":           total_logs,
        "updated_at":           datetime.now(timezone.utc).isoformat(),
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    if not API_KEY:
        print("[user_journey] SKIP — ANALYTICS_API_KEY not set")
        sys.exit(0)

    today      = date.today()
    start_date = (today - timedelta(days=90)).isoformat()  # last 90 days
    end_date   = today.isoformat()

    users = fetch_user_journey(start_date, end_date)
    if not users:
        print("[user_journey] no users returned — skipping")
        return

    rows = [_map_row(u) for u in users]
    upsert("firebase_user_journey", rows)
    print(f"[user_journey] {len(rows)} user(s) upserted ✓")

if __name__ == "__main__":
    run()
