"""
Central config — reads all env vars.
Required vars raise immediately if missing.
Optional vars (for fetchers not yet connected) return None.
"""
import os
from dotenv import load_dotenv

load_dotenv()

def _req(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        raise EnvironmentError(f"Required env var missing: {key}")
    return v

def _opt(key: str) -> str | None:
    return os.environ.get(key) or None

# ── App Store Connect ──────────────────────────────────────
# Optional at import — fetch_app_store.py validates these itself at runtime
APPSTORE_ISSUER_ID     = _opt("APPSTORE_ISSUER_ID")
APPSTORE_KEY_ID        = _opt("APPSTORE_KEY_ID")
APPSTORE_APP_ID        = _opt("APPSTORE_APP_ID")
APPSTORE_VENDOR_NUMBER = os.environ.get("APPSTORE_VENDOR_NUMBER", "93247691")
# Private key may have literal \n from GitHub Secrets — normalise them
_raw_appstore_key    = _opt("APPSTORE_PRIVATE_KEY")
APPSTORE_PRIVATE_KEY = _raw_appstore_key.replace("\\n", "\n") if _raw_appstore_key else None

# ── Firebase / BigQuery ────────────────────────────────────
# Optional at import — fetch_firebase.py validates these itself at runtime
GCP_SERVICE_ACCOUNT_JSON = _opt("GCP_SERVICE_ACCOUNT_JSON")
FIREBASE_PROJECT_ID      = _opt("FIREBASE_PROJECT_ID")
BIGQUERY_DATASET_ID      = _opt("BIGQUERY_DATASET_ID")

# ── Apple Search Ads (optional until .p8 key added) ───────
APPLE_ADS_CLIENT_ID   = _opt("APPLE_ADS_CLIENT_ID")
APPLE_ADS_TEAM_ID     = _opt("APPLE_ADS_TEAM_ID")
APPLE_ADS_KEY_ID      = _opt("APPLE_ADS_KEY_ID")
_raw_apple_ads_key    = _opt("APPLE_ADS_PRIVATE_KEY")
APPLE_ADS_PRIVATE_KEY = _raw_apple_ads_key.replace("\\n", "\n") if _raw_apple_ads_key else None

# ── Google OAuth (YouTube + Google Ads) ───────────────────
GOOGLE_CLIENT_ID       = _opt("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET   = _opt("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN   = _opt("GOOGLE_REFRESH_TOKEN")
YOUTUBE_CHANNEL_ID     = _opt("YOUTUBE_CHANNEL_ID")
GOOGLE_ADS_CUSTOMER_ID = _opt("GOOGLE_ADS_CUSTOMER_ID")
GOOGLE_ADS_DEV_TOKEN   = _opt("GOOGLE_ADS_DEV_TOKEN")

# ── Meta ──────────────────────────────────────────────────
META_APP_ID        = _opt("META_APP_ID")        # for token exchange
META_APP_SECRET    = _opt("META_APP_SECRET")    # for token exchange
META_ACCESS_TOKEN  = _opt("META_ACCESS_TOKEN")
META_AD_ACCOUNT_ID = _opt("META_AD_ACCOUNT_ID")
META_PAGE_ID       = _opt("META_PAGE_ID")

# ── Reddit Ads ────────────────────────────────────────────
REDDIT_APP_ID     = _opt("REDDIT_APP_ID")
REDDIT_APP_SECRET = _opt("REDDIT_APP_SECRET")

# ── Supabase ──────────────────────────────────────────────
# URL is hardcoded in workflow — keep as fallback for local dev
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "https://jxanvdhpqzxehupxkmee.supabase.co")
# SERVICE_KEY is optional at import time — store.py will warn if missing when writing
SUPABASE_SERVICE_KEY = _opt("SUPABASE_SERVICE_KEY")
