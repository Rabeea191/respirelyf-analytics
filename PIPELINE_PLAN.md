# RespireLYF — Real-Time Analytics Pipeline Plan

## Context
The existing dashboard (`RespireLYF_unified_dashboard.html`) has all data hardcoded as static JS arrays.
Goal: Replace all hardcoded data with a live pipeline that fetches from every connected platform daily,
stores in a database, and serves to a hosted team dashboard that auto-updates on page load.

**User:** Full developer · wants a hosted shareable URL · daily refresh sufficient ·
already has credentials for: App Store, Apple Ads, YouTube, Google Ads, Firebase/BigQuery, Meta Ads.

---

## Architecture

```
GitHub Actions (cron: daily 02:00 UTC)
    │
    ├── pipeline/fetch_apple_ads.py      → Apple Search Ads Campaign API v5
    ├── pipeline/fetch_app_store.py      → App Store Connect Analytics API
    ├── pipeline/fetch_youtube.py        → YouTube Analytics API + Data API v3
    ├── pipeline/fetch_google_ads.py     → Google Ads API
    ├── pipeline/fetch_meta.py           → Meta Marketing API + Graph API (page organic)
    ├── pipeline/fetch_firebase.py       → BigQuery (Firebase events export)
    └── pipeline/fetch_social.py         → Twitter API v2, Reddit API (optional)
         │
         └── pipeline/store.py  →  Supabase (PostgreSQL)
                  │
                  └── dashboard/index.html  ←  fetches Supabase REST API on load
                            │
                            └── Hosted: Vercel (free) — shareable team URL
```

**Cost: $0** — GitHub Actions free tier (2,000 min/month), Supabase free tier (500 MB),
Vercel free tier (unlimited static). Total infra cost = $0/month.

---

## Project Structure (new GitHub repo: `respirelyf-analytics`)

```
respirelyf-analytics/
├── .github/
│   └── workflows/
│       └── fetch-data.yml          # Daily cron + manual trigger
├── pipeline/
│   ├── config.py                   # Reads all env vars / secrets
│   ├── store.py                    # Supabase upsert client (shared)
│   ├── fetch_app_store.py          # → impressions, page views, downloads
│   ├── fetch_apple_ads.py          # → keywords, spend, taps, installs
│   ├── fetch_youtube.py            # → channel daily + per-video stats
│   ├── fetch_google_ads.py         # → campaigns, spend, clicks, conversions
│   ├── fetch_meta.py               # → ad performance + page organic reach
│   ├── fetch_firebase.py           # → events, sessions, user properties
│   └── fetch_social.py             # → Twitter impressions, Reddit (optional)
├── supabase/
│   └── schema.sql                  # All table definitions + RLS policies
├── dashboard/
│   └── index.html                  # Live version — fetches from Supabase REST API
├── requirements.txt
├── .env.example                    # All var names, no values
└── vercel.json                     # Static site deploy config
```

---

## Supabase Database Tables

| Table | Key Columns | Source |
|---|---|---|
| `daily_metrics` | date, channel, impressions, clicks, spend, installs | all fetchers (unified view) |
| `app_store_daily` | date, impressions, page_views, downloads, redownloads | fetch_app_store |
| `apple_ads_keywords` | date, keyword, impressions, taps, installs, spend, cpt | fetch_apple_ads |
| `youtube_channel_daily` | date, views, impressions, ctr, watch_time_min, subscribers | fetch_youtube |
| `youtube_videos` | video_id, title, views, impressions, ctr, published_at | fetch_youtube |
| `google_ads_daily` | date, campaign, impressions, clicks, spend, conversions | fetch_google_ads |
| `meta_ads_daily` | date, campaign, impressions, clicks, spend, reach, cpm | fetch_meta |
| `meta_page_daily` | date, reach, impressions, engaged_users, page_views | fetch_meta |
| `firebase_events` | date, event_name, event_count, unique_users | fetch_firebase |
| `firebase_user_props` | date, property, value, user_count | fetch_firebase |

All tables use `ON CONFLICT (date, ...) DO UPDATE` — re-runs are safe and idempotent.

---

## GitHub Actions Workflow

```yaml
# .github/workflows/fetch-data.yml
on:
  schedule:
    - cron: '0 2 * * *'   # 02:00 UTC daily (ready for morning review)
  workflow_dispatch:        # Manual trigger for testing

jobs:
  fetch-all:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.12' }
      - run: pip install -r requirements.txt
      - run: python pipeline/fetch_app_store.py
      - run: python pipeline/fetch_apple_ads.py
      - run: python pipeline/fetch_youtube.py
      - run: python pipeline/fetch_google_ads.py
      - run: python pipeline/fetch_meta.py
      - run: python pipeline/fetch_firebase.py
    env:
      APPLE_ADS_CLIENT_ID:      ${{ secrets.APPLE_ADS_CLIENT_ID }}
      APPLE_ADS_TEAM_ID:        ${{ secrets.APPLE_ADS_TEAM_ID }}
      APPLE_ADS_KEY_ID:         ${{ secrets.APPLE_ADS_KEY_ID }}
      APPLE_ADS_PRIVATE_KEY:    ${{ secrets.APPLE_ADS_PRIVATE_KEY }}
      APPSTORE_KEY_ID:          ${{ secrets.APPSTORE_KEY_ID }}
      APPSTORE_ISSUER_ID:       ${{ secrets.APPSTORE_ISSUER_ID }}
      APPSTORE_PRIVATE_KEY:     ${{ secrets.APPSTORE_PRIVATE_KEY }}
      APPSTORE_APP_ID:          ${{ secrets.APPSTORE_APP_ID }}
      GOOGLE_CLIENT_ID:         ${{ secrets.GOOGLE_CLIENT_ID }}
      GOOGLE_CLIENT_SECRET:     ${{ secrets.GOOGLE_CLIENT_SECRET }}
      GOOGLE_REFRESH_TOKEN:     ${{ secrets.GOOGLE_REFRESH_TOKEN }}
      YOUTUBE_CHANNEL_ID:       ${{ secrets.YOUTUBE_CHANNEL_ID }}
      GOOGLE_ADS_CUSTOMER_ID:   ${{ secrets.GOOGLE_ADS_CUSTOMER_ID }}
      GOOGLE_ADS_DEV_TOKEN:     ${{ secrets.GOOGLE_ADS_DEV_TOKEN }}
      GCP_SERVICE_ACCOUNT_JSON: ${{ secrets.GCP_SERVICE_ACCOUNT_JSON }}
      FIREBASE_PROJECT_ID:      ${{ secrets.FIREBASE_PROJECT_ID }}
      BIGQUERY_DATASET_ID:      ${{ secrets.BIGQUERY_DATASET_ID }}
      META_ACCESS_TOKEN:        ${{ secrets.META_ACCESS_TOKEN }}
      META_AD_ACCOUNT_ID:       ${{ secrets.META_AD_ACCOUNT_ID }}
      META_PAGE_ID:             ${{ secrets.META_PAGE_ID }}
      SUPABASE_URL:             ${{ secrets.SUPABASE_URL }}
      SUPABASE_SERVICE_KEY:     ${{ secrets.SUPABASE_SERVICE_KEY }}
```

---

## Dashboard HTML Changes (dashboard/index.html)

Remove all hardcoded JS arrays. Replace with:

```js
const SUPABASE_URL  = 'https://xxxx.supabase.co';
const SUPABASE_ANON = 'eyJ...anon-key...';
const H = { headers: { apikey: SUPABASE_ANON, Authorization: `Bearer ${SUPABASE_ANON}` } };

async function loadDashboard() {
  const [appStore, kwds, ytDaily, ytVideos, events, meta, gads] = await Promise.all([
    fetch(`${SUPABASE_URL}/rest/v1/app_store_daily?order=date.asc`, H).then(r=>r.json()),
    fetch(`${SUPABASE_URL}/rest/v1/apple_ads_keywords?order=impressions.desc`, H).then(r=>r.json()),
    fetch(`${SUPABASE_URL}/rest/v1/youtube_channel_daily?order=date.asc`, H).then(r=>r.json()),
    fetch(`${SUPABASE_URL}/rest/v1/youtube_videos?order=views.desc&limit=10`, H).then(r=>r.json()),
    fetch(`${SUPABASE_URL}/rest/v1/firebase_events?order=date.desc`, H).then(r=>r.json()),
    fetch(`${SUPABASE_URL}/rest/v1/meta_ads_daily?order=date.asc`, H).then(r=>r.json()),
    fetch(`${SUPABASE_URL}/rest/v1/google_ads_daily?order=date.asc`, H).then(r=>r.json()),
  ]);
  renderAllCharts({ appStore, kwds, ytDaily, ytVideos, events, meta, gads });
}
loadDashboard();
```

All `renderAllCharts()` functions receive live arrays with the same shape as current hardcoded arrays —
chart code stays identical, only the data source changes.

---

## Credentials Checklist — What You Need to Provide

### 1. Apple Search Ads
**Where:** searchads.apple.com → Settings → API → Create Certificate
**Provide:**
- `APPLE_ADS_CLIENT_ID` — shown after creating cert
- `APPLE_ADS_TEAM_ID` — your Apple team ID
- `APPLE_ADS_KEY_ID` — shown after creating cert
- `APPLE_ADS_PRIVATE_KEY` — contents of downloaded `.p8` file

### 2. App Store Connect
**Where:** appstoreconnect.apple.com → Users & Access → Integrations → App Store Connect API
**Create key with:** App Analytics permission (read only is fine)
**Provide:**
- `APPSTORE_ISSUER_ID` — 44db1147-6013-426e-917e-4258d76ea82b
- `APPSTORE_KEY_ID` — P7Z7627972
- `APPSTORE_PRIVATE_KEY` — -----BEGIN PRIVATE KEY-----
MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQgNRC3WbLVj8btl1fF
5WjL4UzROKulv5Dk+uU46rgNp72gCgYIKoZIzj0DAQehRANCAAQROGwOD5AGXIo6
DJA7Z1PQ6hG3AsCQNYpuupqDcDaShjp1D0F73zkUkaYowshE/IcExCxb0x/yoDOS
Ny8tcYU2
-----END PRIVATE KEY-----
- `APPSTORE_APP_ID` — 6752850093

### 3. Google Cloud (YouTube + Google Ads)
**Where:** console.cloud.google.com
**Steps:**
1. Enable APIs: YouTube Analytics API, YouTube Data API v3, Google Ads API
2. Create → OAuth2 credentials (Desktop app type)
3. Run one-time OAuth consent flow to get refresh token (I'll provide a helper script)
4. Google Ads: your manager account → Admin → API Center → Developer token

**Provide:**
- `GOOGLE_CLIENT_ID`
- `GOOGLE_CLIENT_SECRET`
- `GOOGLE_REFRESH_TOKEN` (from OAuth flow)
- `YOUTUBE_CHANNEL_ID` (UCxxxxxxx format — from your channel URL)
- `GOOGLE_ADS_CUSTOMER_ID` (10-digit, no dashes)
- `GOOGLE_ADS_DEV_TOKEN`

### 4. Firebase / BigQuery
**Where:** console.firebase.google.com → Project Settings → Service Accounts
**Steps:**
1. Generate new private key → download JSON
2. Integrations → BigQuery → Enable (if not already — 24hr first sync delay)

**Provide:**
- `GCP_SERVICE_ACCOUNT_JSON` — {
  "type": "service_account",
  "project_id": "respire-lyf",
  "private_key_id": "192a020d77f5fefd6b696bc6dd3a5b8c7e4f403b",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCmZnNTGzii104/\nN4F/IHwJCFCLN7uJR7FLnFtOQiTT8ci7z2sIQe7RpJNLZ1PryAu7Vb8LY8l1G28K\nSn1T1CPf9wkFlwpsoIjx/ZglaTSK4thSFKcnvIO5HvoqPe+dgqJHRI2O1IEcThSt\npVuDwVTpJYLOb7UHI82PBdB7c8rp+3aQWIk2HiFFZ+AZRRKDP53d38UQPLCWaIyO\nWKp/iJXi33xLwSdw8oz7uK1VOwrnSif6FR+6RVfpWxzVJ2fOqhAz1AxRAdf0xRJ9\nU1afzy7mz/+rsWo9t4pervZbCapxlnjczeKsNHDSXcsFcbfONWAi/9Binh4WGwtd\nLYT+sILtAgMBAAECggEAI5MvMDMXaG8eDjNkduSfyAGUULNFX5vpengO6DwJahuc\n1hRJ9n28HGtyRzJr1XKTGYXDPqDimuwW0Q0sXq3xzonezqhkROWMaaoVK1hi8Jd/\ndoruUrdBtSP7iWjs79CUN6bfGcxyXvYNgzNhTZmZeO/5V3+35PMhWSS91grjNKMj\nRmbffJQ2lNvlORjHdMiNfXqg/RU2r0FWaB34O6l52y9eKXQZshBYo/vbHJLWhDKg\n6eSxdit4uSc3YXVYyB9edTetz11lnVnUj7bxbHSeiMrSLz/bfZwsNsWsOCm6R0An\nYXixAv2MDDMg8SwYJ05p+ADpC/vU1HRiBvYLU9YJcQKBgQDTj4cJjHjFq1x/G/J8\niooXC11a49ehhPN6FWXF99fB8E2gbVb9NNNqEgy79AUjIEhUZ9VIFyVJOI91RVPc\ngQ2NN6aToRV082yWWKCxNOXut/ZXX/dPlrC7jDB4ebgwKYr7GZCcwYxWnOsUg/C1\nkJpsMRrCLqMjGDOb0uaskp7p3QKBgQDJWnZkaGH509q7EghkCSAdZwX51IHlEXdO\nsdHUVb44YpEtLseVANFSJYE5NYHWkL58jO9kn6HR+UOcxQOD0l2uQP0HpL4HaHWP\ncPmJBY+ZeaQJc2NW+GTGQ2W/knPL2lQdwCsihbZy8lldJC3t0ex5JZTFioTPti6J\nqn8UD5xUUQKBgQCXIjeTyLgWbj6bx3lJIydXIxVD8vR+PJ47HsIf1NhbgbHS44l7\nRZuqcM6MDt0EpPFG6w5ge7h1QGSroCUypzbpJVJosHDFpYhzN0lEjseb6+udq77f\nKoWC1blit4GvVl4k17SJZ3M8BMmjVEZHTtAibknxYqPEVGu568ZCjlPcOQKBgBTj\nlKrABYIUj3me2k3+gF1shbswZ7VrBXSdkAY2SwCwgR0t4Di+F/ksuOZFfc3TwF9N\ng7xtry0IbhJCgiXX2i4swdNcLPa5yJB0CP9FG2uxqentFZ49ML882L5yJB8+7EEV\n4aO4OYBdUhYOndExWRYMZw3DXBnsbZGMpZhgMjixAoGBAKB6GaCgjhAJZbmauC4B\njQhqHosagbyA3/wDaFhTlNcpFU59MKTRKiB5/YL7FwpExyHFeQxW3mfYX89fE6Io\nuUGCpplMwEi2iQu7cyOSCPZ2j7Yu1vRs7SdGsRpyuFUVerZF18wbgTTQFVc9jV03\nsKBCrm29V+8S3AogbeRO+NVm\n-----END PRIVATE KEY-----\n",
  "client_email": "respirelyf-analytics@respire-lyf.iam.gserviceaccount.com",
  "client_id": "110988271049055689101",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/respirelyf-analytics%40respire-lyf.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

- `FIREBASE_PROJECT_ID` —  "respire-lyf"
- `BIGQUERY_DATASET_ID` — analytics_489687866

### 5. Meta Business Manager
**Where:** business.facebook.com → Business Settings → System Users
**Steps:**
1. Create system user (Admin role)
2. Assign Ad Account (full control) + Facebook Page (at minimum: Analyst)
3. Generate token → permissions: `ads_read`, `pages_read_engagement`, `read_insights`, `instagram_basic`

**Provide:**
- `META_ACCESS_TOKEN` — long-lived system user token
- `META_AD_ACCOUNT_ID` — format: `act_1234567890`
- `META_PAGE_ID` — numeric ID from page settings

### 6. Supabase (you create this — free)
**Where:** supabase.com → New project
**Provide:**
- `SUPABASE_URL` — from Project Settings → API
- `SUPABASE_SERVICE_KEY` — `service_role` key (for pipeline writes)
- `SUPABASE_ANON_KEY` — `anon` key (for dashboard reads — safe to embed in HTML)

---

## API Rate Limits & Cost

| API | Rate Limit | Daily calls needed | Cost |
|---|---|---|---|
| Apple Search Ads | 500 req/hr | ~5 | Free |
| App Store Connect | No published limit | ~3 | Free |
| YouTube Analytics | 10,000 units/day | ~50 units | Free |
| Google Ads | 15,000 req/day | ~10 | Free |
| Meta Marketing | 200 calls/hr per token | ~8 | Free |
| BigQuery | 1 TB/month free | ~0.001 TB/day | Free |
| GitHub Actions | 2,000 min/month | ~3 min/day | Free |
| Supabase | 500 MB free | <1 MB/day | Free |
| Vercel | Unlimited static | — | Free |

**Total monthly infra cost: $0**

---

## Implementation Order

| Step | What | Est. time |
|---|---|---|
| 1 | Create Supabase project + run schema.sql | 20 min |
| 2 | Create GitHub repo, add `.env.example`, push skeleton | 10 min |
| 3 | Write + test `fetch_app_store.py` locally | 1 hr |
| 4 | Write + test `fetch_apple_ads.py` locally | 1 hr |
| 5 | Write + test `fetch_youtube.py` locally | 1 hr |
| 6 | Write + test `fetch_google_ads.py` locally | 1.5 hr |
| 7 | Write + test `fetch_meta.py` locally | 1 hr |
| 8 | Write + test `fetch_firebase.py` locally | 1 hr |
| 9 | Add GitHub Actions workflow + all secrets | 30 min |
| 10 | Trigger manual workflow run — verify Supabase data | 15 min |
| 11 | Modify `dashboard/index.html` to fetch from Supabase | 2 hr |
| 12 | Deploy to Vercel — get shareable URL | 10 min |
| **Total** | | **~10 hrs** |

---

## Python Dependencies (requirements.txt)

```
supabase==2.4.0
requests==2.31.0
google-auth==2.28.0
google-auth-oauthlib==1.2.0
google-api-python-client==2.118.0
google-ads==24.1.0
google-cloud-bigquery==3.17.2
PyJWT==2.8.0
cryptography==42.0.0
python-dotenv==1.0.1
```

---

## What I Build Next (once you confirm)

1. `supabase/schema.sql` — full table definitions
2. `pipeline/config.py` + `pipeline/store.py` — shared base
3. All 6 fetcher scripts with real API calls
4. `.github/workflows/fetch-data.yml`
5. Modified `dashboard/index.html` wired to Supabase
6. `vercel.json` + deployment instructions
7. Helper script to generate Google OAuth refresh token (one-time)
8. `.env.example` with all variable names

**Start whenever you have the credentials ready — or start with just App Store + Firebase
(no paid API keys needed) to prove the pipeline works, then add the rest.**
