# 🚀 Deploy RespireLYF Dashboard — Step by Step

**Time needed: ~20 minutes. Cost: $0/month.**

After this, every team member visits one URL and gets fresh data every morning automatically.

---

## What's already done ✅

- `dashboard/index.html` — live dashboard, wired to your Supabase project
- `supabase/schema.sql` — all tables + Row Level Security
- `pipeline/` — all 6 data fetchers written
- `.github/workflows/fetch-data.yml` — daily cron (02:00 UTC) + manual trigger
- `vercel.json` — Vercel static hosting config

---

## Step 1 — Supabase: Run the Schema (5 min)

Your project: **https://supabase.com/dashboard/project/jxanvdhpqzxehupxkmee**

1. Go to: **SQL Editor → New query**
2. Open `supabase/schema.sql` from this folder
3. Paste the entire contents → click **Run**
4. You should see: `Success. No rows returned`

This creates all tables (app_store_daily, youtube_channel_daily, etc.) and the RLS read policies.

**Get your Service Role key** (needed for GitHub secrets):
- Supabase Dashboard → **Project Settings → API**
- Copy the `service_role` key (starts with `eyJ...`, labeled "secret")

---

## Step 2 — GitHub: Push the Repo (5 min)

```bash
# From your project folder:
cd /path/to/your/respirelyf-analytics

git init
git add .
git commit -m "Initial: full pipeline + live dashboard"

# Create a new GitHub repo (private recommended):
# github.com → New repository → Name: respirelyf-analytics → Create

git remote add origin https://github.com/YOUR_USERNAME/respirelyf-analytics.git
git push -u origin main
```

---

## Step 3 — GitHub Secrets: Add Your Credentials (5 min)

Go to: **GitHub → your repo → Settings → Secrets and variables → Actions → New repository secret**

Add these secrets one by one:

| Secret Name | Where to find it |
|---|---|
| `SUPABASE_SERVICE_KEY` | Supabase → Project Settings → API → service_role key |
| `APPSTORE_ISSUER_ID` | App Store Connect → Users & Access → Keys |
| `APPSTORE_KEY_ID` | App Store Connect → Users & Access → Keys |
| `APPSTORE_PRIVATE_KEY` | The .p8 file contents (paste the whole thing) |
| `APPSTORE_APP_ID` | Your app's numeric ID from App Store Connect |
| `GOOGLE_CLIENT_ID` | Google Cloud Console → OAuth 2.0 Credentials |
| `GOOGLE_CLIENT_SECRET` | Google Cloud Console → OAuth 2.0 Credentials |
| `GOOGLE_REFRESH_TOKEN` | Run `get_youtube_token.py` to generate |
| `YOUTUBE_CHANNEL_ID` | YouTube Studio → Settings → Channel → Basic Info |
| `GCP_SERVICE_ACCOUNT_JSON` | GCP → IAM → Service Accounts → your key → JSON contents |
| `FIREBASE_PROJECT_ID` | Firebase Console → Project Settings |
| `BIGQUERY_DATASET_ID` | BigQuery → your Firebase export dataset name |

**Don't have all of them yet?** That's fine. Each pipeline step has `continue-on-error: true` so missing secrets just skip that fetcher — the rest still run.

---

## Step 4 — Run the Pipeline Once to Test (2 min)

1. GitHub → your repo → **Actions** tab
2. Click **"Fetch Analytics Data"**
3. Click **"Run workflow"** → **Run workflow** (green button)
4. Watch the logs — each step either succeeds or shows why it skipped

After it runs, go to Supabase → **Table Editor** and check if rows appeared in `app_store_daily`, `youtube_channel_daily`, etc.

---

## Step 5 — Deploy to Vercel (3 min)

1. Go to **https://vercel.com** → Sign up / Log in with GitHub
2. Click **"Add New Project"**
3. Import your `respirelyf-analytics` GitHub repo
4. Vercel auto-detects `vercel.json` — no config needed
5. Click **Deploy**

You'll get a URL like: `https://respirelyf-analytics.vercel.app`

**That's your shareable link.** Share it with your team. Anyone who opens it gets live data from Supabase (fetched on page load, no login needed).

---

## How It Works After Deployment

```
Every day at 02:00 UTC:
  GitHub Actions runs all 6 pipeline fetchers
       ↓
  Fresh data lands in Supabase tables
       ↓
  Anyone who opens the Vercel URL sees today's stats
  (the dashboard fetches Supabase on every page load)
```

**No rebuild needed.** Vercel serves the static HTML — the HTML itself talks to Supabase on load. So data is always fresh without redeploying.

---

## Optional: Custom Domain

In Vercel → your project → **Settings → Domains** → add your own domain (e.g. `analytics.respirelyf.com`). Free on Vercel.

---

## What the Dashboard Shows (Live)

| Tab | Data Source | Updates |
|---|---|---|
| Overview | All channels combined | Daily |
| Paid Ads | Apple Ads + Meta + Google + Reddit | Daily |
| Organic | YouTube channel stats | Daily |
| App Store | Impressions, downloads, page views | Daily |
| Firebase | Sessions, active users, events | Daily |
| Social | Meta page organic reach | Daily |
| Funnel | End-to-end conversion | Daily |
| Insights | Strategic recommendations | Static |

---

*Dashboard wired: March 25, 2026 · Supabase: jxanvdhpqzxehupxkmee · Vercel: free tier*
