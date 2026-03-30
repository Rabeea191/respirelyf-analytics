-- ============================================================
-- RespireLYF Analytics — Supabase Schema
-- Run this in: Supabase Dashboard → SQL Editor → New query
-- ============================================================

-- ── App Store Connect ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS app_store_daily (
    date                    DATE    PRIMARY KEY,
    impressions             INTEGER NOT NULL DEFAULT 0,
    impressions_unique      INTEGER NOT NULL DEFAULT 0,
    page_views              INTEGER NOT NULL DEFAULT 0,
    page_views_unique       INTEGER NOT NULL DEFAULT 0,
    downloads               INTEGER NOT NULL DEFAULT 0,
    redownloads             INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Apple Search Ads ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS apple_ads_keywords (
    date        DATE    NOT NULL,
    keyword     TEXT    NOT NULL,
    impressions INTEGER NOT NULL DEFAULT 0,
    taps        INTEGER NOT NULL DEFAULT 0,
    installs    INTEGER NOT NULL DEFAULT 0,
    spend       NUMERIC(10,2) NOT NULL DEFAULT 0,
    cpt         NUMERIC(10,2) NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, keyword)
);

-- ── YouTube ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS youtube_channel_daily (
    date            DATE    PRIMARY KEY,
    views           INTEGER NOT NULL DEFAULT 0,
    impressions     INTEGER NOT NULL DEFAULT 0,
    ctr             NUMERIC(6,2) NOT NULL DEFAULT 0,
    watch_time_min  NUMERIC(12,1) NOT NULL DEFAULT 0,
    subscribers     INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS youtube_video_daily (
    video_id          TEXT        NOT NULL,
    date              DATE        NOT NULL,
    title             TEXT,
    views             INTEGER     NOT NULL DEFAULT 0,
    watch_time_min    NUMERIC(12,1) NOT NULL DEFAULT 0,
    impressions       INTEGER     NOT NULL DEFAULT 0,
    ctr               NUMERIC(6,2) NOT NULL DEFAULT 0,
    likes             INTEGER     NOT NULL DEFAULT 0,
    comments          INTEGER     NOT NULL DEFAULT 0,
    avg_view_duration NUMERIC(8,1) NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (video_id, date)
);

CREATE TABLE IF NOT EXISTS youtube_videos (
    video_id     TEXT PRIMARY KEY,
    title        TEXT,
    views        INTEGER NOT NULL DEFAULT 0,
    impressions  INTEGER NOT NULL DEFAULT 0,
    ctr          NUMERIC(6,2) NOT NULL DEFAULT 0,
    published_at TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Google Ads ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS google_ads_daily (
    date         DATE NOT NULL,
    campaign     TEXT NOT NULL,
    impressions  INTEGER NOT NULL DEFAULT 0,
    clicks       INTEGER NOT NULL DEFAULT 0,
    spend        NUMERIC(10,2) NOT NULL DEFAULT 0,
    conversions  INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, campaign)
);

-- ── Meta Post Insights (FB + IG per-post) ──────────────────
CREATE TABLE IF NOT EXISTS meta_post_insights (
    post_id                     TEXT        NOT NULL,
    platform                    TEXT        NOT NULL,   -- 'facebook' or 'instagram'
    date                        DATE        NOT NULL,
    post_type                   TEXT,                   -- 'photo', 'video', 'reel', 'status'
    message                     TEXT,                   -- caption (truncated to 200 chars)
    impressions                 INTEGER     NOT NULL DEFAULT 0,
    reach                       INTEGER     NOT NULL DEFAULT 0,
    likes                       INTEGER     NOT NULL DEFAULT 0,
    comments                    INTEGER     NOT NULL DEFAULT 0,
    shares                      INTEGER     NOT NULL DEFAULT 0,
    saves                       INTEGER     NOT NULL DEFAULT 0,
    engagement                  INTEGER     NOT NULL DEFAULT 0,  -- post_engaged_users / total_interactions
    clicks                      INTEGER     NOT NULL DEFAULT 0,  -- post_clicks (FB)
    video_views                 INTEGER     NOT NULL DEFAULT 0,  -- post_video_views / video_views
    video_view_time_ms          BIGINT      NOT NULL DEFAULT 0,  -- post_video_view_time (FB, milliseconds)
    video_complete_views        INTEGER     NOT NULL DEFAULT 0,  -- post_video_complete_video_views (FB)
    reels_avg_watch_time_ms     BIGINT      NOT NULL DEFAULT 0,  -- ig_reels_avg_watch_time (IG, milliseconds)
    reels_total_watch_time_ms   BIGINT      NOT NULL DEFAULT 0,  -- ig_reels_video_view_total_time (IG, milliseconds)
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (post_id, date)
);

-- Run these if the table already exists (adds missing columns safely):
ALTER TABLE meta_post_insights ADD COLUMN IF NOT EXISTS clicks                    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meta_post_insights ADD COLUMN IF NOT EXISTS video_view_time_ms        BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE meta_post_insights ADD COLUMN IF NOT EXISTS video_complete_views      INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meta_post_insights ADD COLUMN IF NOT EXISTS reels_avg_watch_time_ms   BIGINT  NOT NULL DEFAULT 0;
ALTER TABLE meta_post_insights ADD COLUMN IF NOT EXISTS reels_total_watch_time_ms BIGINT  NOT NULL DEFAULT 0;

-- ── Meta Ads ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS meta_ads_daily (
    date         DATE NOT NULL,
    campaign     TEXT NOT NULL,
    impressions  INTEGER NOT NULL DEFAULT 0,
    clicks       INTEGER NOT NULL DEFAULT 0,
    spend        NUMERIC(10,2) NOT NULL DEFAULT 0,
    reach        INTEGER NOT NULL DEFAULT 0,
    cpm          NUMERIC(10,2) NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, campaign)
);

CREATE TABLE IF NOT EXISTS meta_page_daily (
    date           DATE PRIMARY KEY,
    reach          INTEGER NOT NULL DEFAULT 0,
    impressions    INTEGER NOT NULL DEFAULT 0,
    engaged_users  INTEGER NOT NULL DEFAULT 0,
    page_views     INTEGER NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Firebase / BigQuery ────────────────────────────────────
CREATE TABLE IF NOT EXISTS firebase_events (
    date          DATE NOT NULL,
    event_name    TEXT NOT NULL,
    event_count   INTEGER NOT NULL DEFAULT 0,
    unique_users  INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, event_name)
);

CREATE TABLE IF NOT EXISTS firebase_user_props (
    date        DATE NOT NULL,
    property    TEXT NOT NULL,
    value       TEXT NOT NULL,
    user_count  INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, property, value)
);

-- ── Firebase User Behavior (BigQuery events_intraday_* query) ─
CREATE TABLE IF NOT EXISTS firebase_user_behavior (
    user_pseudo_id          TEXT PRIMARY KEY,
    city                    TEXT,
    region                  TEXT,
    country                 TEXT,
    first_seen_date         DATE,
    last_seen_date          DATE,
    total_day_span          INTEGER NOT NULL DEFAULT 0,
    days_actually_active    INTEGER NOT NULL DEFAULT 0,
    total_events            INTEGER NOT NULL DEFAULT 0,
    unique_features_used    INTEGER NOT NULL DEFAULT 0,
    total_sessions          INTEGER NOT NULL DEFAULT 0,
    onboarding_events       INTEGER NOT NULL DEFAULT 0,
    login_events            INTEGER NOT NULL DEFAULT 0,
    otp_events              INTEGER NOT NULL DEFAULT 0,
    health_profile_events   INTEGER NOT NULL DEFAULT 0,
    today_tab_visits        INTEGER NOT NULL DEFAULT 0,
    peak_flow_logs          INTEGER NOT NULL DEFAULT 0,
    symptom_logs            INTEGER NOT NULL DEFAULT 0,
    sleep_logs              INTEGER NOT NULL DEFAULT 0,
    treatment_views         INTEGER NOT NULL DEFAULT 0,
    progress_views          INTEGER NOT NULL DEFAULT 0,
    journey_stage           TEXT,
    current_status          TEXT,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE firebase_user_behavior ENABLE ROW LEVEL SECURITY;
CREATE POLICY IF NOT EXISTS "anon read firebase_user_behavior"
    ON firebase_user_behavior FOR SELECT TO anon USING (TRUE);

-- ── Reddit Ads ─────────────────────────────────────────────
-- ── Internal User Journey (backend API) ───────────────────
CREATE TABLE IF NOT EXISTS firebase_user_journey (
    user_id              TEXT        PRIMARY KEY,
    signed_up_at         TIMESTAMPTZ,
    first_log_at         TIMESTAMPTZ,
    last_log_at          TIMESTAMPTZ,
    last_meep_at         TIMESTAMPTZ,
    last_login_at        TIMESTAMPTZ,
    -- Profile setup
    profile_about_you    BOOLEAN     NOT NULL DEFAULT FALSE,
    profile_symptoms     BOOLEAN     NOT NULL DEFAULT FALSE,
    profile_root_causes  BOOLEAN     NOT NULL DEFAULT FALSE,
    profile_medication   BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Treatments
    inhalers             INTEGER     NOT NULL DEFAULT 0,
    medications          INTEGER     NOT NULL DEFAULT 0,
    supplements          INTEGER     NOT NULL DEFAULT 0,
    -- Health determinants (log counts)
    hd_food              INTEGER     NOT NULL DEFAULT 0,
    hd_hydration         INTEGER     NOT NULL DEFAULT 0,
    hd_sleep             INTEGER     NOT NULL DEFAULT 0,
    hd_activity          INTEGER     NOT NULL DEFAULT 0,
    hd_stress            INTEGER     NOT NULL DEFAULT 0,
    -- Health indicators (log counts)
    hi_symptoms          INTEGER     NOT NULL DEFAULT 0,
    hi_flareups          INTEGER     NOT NULL DEFAULT 0,
    hi_peak_flow         INTEGER     NOT NULL DEFAULT 0,
    hi_vitals            INTEGER     NOT NULL DEFAULT 0,
    hi_surveys           INTEGER     NOT NULL DEFAULT 0,
    -- MDRIC AI engagement
    mdric_meeps          INTEGER     NOT NULL DEFAULT 0,
    mdric_weekly         INTEGER     NOT NULL DEFAULT 0,
    mdric_monthly        INTEGER     NOT NULL DEFAULT 0,
    mdric_report         INTEGER     NOT NULL DEFAULT 0,
    mdric_memories       INTEGER     NOT NULL DEFAULT 0,
    -- Session / device info
    has_session          BOOLEAN     NOT NULL DEFAULT FALSE,
    session_country      TEXT,
    city                 TEXT,
    region               TEXT,
    auth_platform        TEXT,
    app_source           TEXT,
    device_type          TEXT,
    os_version           TEXT,
    app_version          TEXT,
    language             TEXT,
    device_country       TEXT,
    -- Derived journey fields (computed by fetcher)
    journey_stage        TEXT,
    current_status       TEXT        NOT NULL DEFAULT 'inactive',
    days_active          INTEGER     NOT NULL DEFAULT 0,
    total_logs           INTEGER     NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- RLS: anon can read, service role can write
ALTER TABLE firebase_user_journey ENABLE ROW LEVEL SECURITY;
CREATE POLICY IF NOT EXISTS "anon read firebase_user_journey"
    ON firebase_user_journey FOR SELECT TO anon USING (TRUE);

CREATE TABLE IF NOT EXISTS reddit_ads_daily (
    date          DATE    NOT NULL,
    campaign_id   TEXT    NOT NULL,
    campaign_name TEXT    NOT NULL DEFAULT '',
    impressions   INTEGER NOT NULL DEFAULT 0,
    clicks        INTEGER NOT NULL DEFAULT 0,
    spend_usd     NUMERIC(10,2) NOT NULL DEFAULT 0,
    cpm           NUMERIC(10,4) NOT NULL DEFAULT 0,
    cpc           NUMERIC(10,4) NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, campaign_id)
);

-- ── Unified daily view (for overview chart) ────────────────
-- This VIEW joins all channels into one normalised row per date.
-- Use it in the dashboard for the master trend chart.
CREATE OR REPLACE VIEW daily_summary AS
SELECT
    d.date,
    COALESCE(a.impressions, 0)                                    AS store_impressions,
    COALESCE(a.page_views, 0)                                     AS store_page_views,
    COALESCE(a.downloads, 0)                                      AS store_downloads,
    COALESCE(y.views, 0)                                          AS yt_views,
    COALESCE(y.impressions, 0)                                    AS yt_impressions,
    COALESCE(y.ctr, 0)                                            AS yt_ctr,
    COALESCE(f.event_count, 0)                                    AS sessions,
    COALESCE(ma.total_spend, 0)                                   AS meta_spend,
    COALESCE(ga.total_spend, 0)                                   AS google_spend
FROM (
    SELECT DISTINCT date FROM app_store_daily
    UNION
    SELECT DISTINCT date FROM youtube_channel_daily
    UNION
    SELECT DISTINCT date FROM firebase_events WHERE event_name = 'session_start'
) d
LEFT JOIN app_store_daily a ON a.date = d.date
LEFT JOIN youtube_channel_daily y ON y.date = d.date
LEFT JOIN (
    SELECT date, SUM(event_count) AS event_count
    FROM firebase_events WHERE event_name = 'session_start'
    GROUP BY date
) f ON f.date = d.date
LEFT JOIN (
    SELECT date, SUM(spend) AS total_spend FROM meta_ads_daily GROUP BY date
) ma ON ma.date = d.date
LEFT JOIN (
    SELECT date, SUM(spend) AS total_spend FROM google_ads_daily GROUP BY date
) ga ON ga.date = d.date
ORDER BY d.date ASC;


-- ============================================================
-- Row Level Security
-- Pipeline writes with service_role key (bypasses RLS).
-- Dashboard reads with anon key — allow SELECT only.
-- ============================================================

ALTER TABLE app_store_daily      ENABLE ROW LEVEL SECURITY;
ALTER TABLE apple_ads_keywords   ENABLE ROW LEVEL SECURITY;
ALTER TABLE youtube_channel_daily ENABLE ROW LEVEL SECURITY;
ALTER TABLE youtube_video_daily  ENABLE ROW LEVEL SECURITY;
ALTER TABLE youtube_videos       ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_ads_daily     ENABLE ROW LEVEL SECURITY;
ALTER TABLE meta_post_insights   ENABLE ROW LEVEL SECURITY;
ALTER TABLE meta_ads_daily       ENABLE ROW LEVEL SECURITY;
ALTER TABLE meta_page_daily      ENABLE ROW LEVEL SECURITY;
ALTER TABLE firebase_events      ENABLE ROW LEVEL SECURITY;
ALTER TABLE firebase_user_props  ENABLE ROW LEVEL SECURITY;
ALTER TABLE reddit_ads_daily     ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_read" ON app_store_daily       FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON apple_ads_keywords    FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON youtube_channel_daily FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON youtube_video_daily   FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON youtube_videos        FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON google_ads_daily      FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON meta_post_insights    FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON meta_ads_daily        FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON meta_page_daily       FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON firebase_events       FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON firebase_user_props   FOR SELECT TO anon USING (true);
CREATE POLICY "anon_read" ON reddit_ads_daily      FOR SELECT TO anon USING (true);
