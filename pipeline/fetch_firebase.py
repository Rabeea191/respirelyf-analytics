"""
Firebase / BigQuery — daily events + session metrics.

Queries the Firebase Analytics BigQuery export tables:
  analytics_489687866.events_YYYYMMDD

Writes to:
  firebase_events      (date, event_name, event_count, unique_users)
  firebase_user_props  (date, property, value, user_count)

Run:  python -m pipeline.fetch_firebase
"""
import json
import os
import sys
from datetime import date, timedelta

_FETCH_DAYS = int(os.environ.get("FETCH_DAYS", "30"))

from google.cloud import bigquery
from google.oauth2 import service_account

from pipeline.config import (
    BIGQUERY_DATASET_ID,
    FIREBASE_PROJECT_ID,
    GCP_SERVICE_ACCOUNT_JSON,
)
from pipeline.store import upsert


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_bq_client() -> bigquery.Client:
    try:
        sa_info = json.loads(GCP_SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        # Treat as a file path (local dev)
        with open(GCP_SERVICE_ACCOUNT_JSON) as f:
            sa_info = json.load(f)

    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=creds, project=FIREBASE_PROJECT_ID)


# ── Queries ───────────────────────────────────────────────────────────────────

_EVENTS_SQL = """
SELECT
    event_date,
    event_name,
    COUNT(*)                      AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM `{project}.{dataset}.{suffix}`
GROUP BY event_date, event_name
ORDER BY event_count DESC
"""

_EVENTS_RANGE_SQL = """
SELECT
    event_date,
    event_name,
    COUNT(*)                      AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM `{project}.{dataset}.events_*`
WHERE _TABLE_SUFFIX BETWEEN '{start_sfx}' AND '{end_sfx}'
GROUP BY event_date, event_name
ORDER BY event_count DESC
"""

_USER_PROPS_SQL = """
SELECT
    event_date,
    up.key           AS property,
    up.value.string_value AS value,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM `{project}.{dataset}.{suffix}`
CROSS JOIN UNNEST(user_properties) AS up
WHERE up.value.string_value IS NOT NULL
GROUP BY event_date, up.key, up.value.string_value
ORDER BY user_count DESC
LIMIT 500
"""

_USER_PROPS_RANGE_SQL = """
SELECT
    event_date,
    up.key           AS property,
    up.value.string_value AS value,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM `{project}.{dataset}.events_*`
CROSS JOIN UNNEST(user_properties) AS up
WHERE _TABLE_SUFFIX BETWEEN '{start_sfx}' AND '{end_sfx}'
  AND up.value.string_value IS NOT NULL
GROUP BY event_date, up.key, up.value.string_value
ORDER BY user_count DESC
LIMIT 1000
"""

_USER_BEHAVIOR_SQL = """
SELECT
  user_pseudo_id,
  ANY_VALUE(geo.city)    AS city,
  ANY_VALUE(geo.region)  AS region,
  ANY_VALUE(geo.country) AS country,
  MIN(DATE(TIMESTAMP_MICROS(event_timestamp))) AS first_seen_date,
  MAX(DATE(TIMESTAMP_MICROS(event_timestamp))) AS last_seen_date,
  DATE_DIFF(
    MAX(DATE(TIMESTAMP_MICROS(event_timestamp))),
    MIN(DATE(TIMESTAMP_MICROS(event_timestamp))),
    DAY
  ) AS total_day_span,
  COUNT(DISTINCT DATE(TIMESTAMP_MICROS(event_timestamp))) AS days_actually_active,
  COUNT(*) AS total_events,
  COUNT(DISTINCT event_name) AS unique_features_used,
  COUNTIF(event_name = 'session_start') AS total_sessions,
  COUNTIF(event_name = 'rl_onboarding_ios') AS onboarding_events,
  COUNTIF(event_name = 'rl_login_ios') AS login_events,
  COUNTIF(event_name = 'rl_otp_ios') AS otp_events,
  COUNTIF(event_name = 'rl_health_profile_ios') AS health_profile_events,
  COUNTIF(event_name = 'rl_today_tab_ios') AS today_tab_visits,
  COUNTIF(event_name = 'rl_peak_flow_card_ios') AS peak_flow_logs,
  COUNTIF(event_name = 'rl_symptoms_card_ios') AS symptom_logs,
  COUNTIF(event_name = 'rl_sleep_card_ios') AS sleep_logs,
  COUNTIF(event_name = 'rl_treatment_sheet_ios') AS treatment_views,
  COUNTIF(event_name = 'rl_progress_ios') AS progress_views,
  CASE
    WHEN COUNTIF(event_name = 'rl_today_tab_ios') > 0 THEN 'Fully Activated'
    WHEN COUNTIF(event_name = 'rl_health_profile_ios') > 0 THEN 'Health Profile Done'
    WHEN COUNTIF(event_name = 'rl_otp_ios') > 0 THEN 'Reached OTP'
    WHEN COUNTIF(event_name = 'rl_login_ios') > 0 THEN 'Reached Login'
    WHEN COUNTIF(event_name = 'rl_onboarding_ios') > 0 THEN 'Onboarding Only'
    ELSE 'Bounced'
  END AS journey_stage,
  CASE
    WHEN MAX(DATE(TIMESTAMP_MICROS(event_timestamp))) >= DATE_SUB(CURRENT_DATE(), INTERVAL {fetch_days_p1} DAY)
    THEN 'Active' ELSE 'Churned'
  END AS current_status
FROM `{project}.{dataset}.events_intraday_*`
WHERE DATE(TIMESTAMP_MICROS(event_timestamp)) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {fetch_days_p1} DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND geo.city != 'Ashburn'
  AND geo.country != 'Pakistan'
  AND user_pseudo_id != '19BA69FD-ECA7-46A5-BD25-766587D2574B'
GROUP BY user_pseudo_id
ORDER BY days_actually_active DESC, total_events DESC
"""

_SESSIONS_SQL = """
SELECT
    event_date,
    event_name,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM `{project}.{dataset}.{suffix}`
WHERE event_name IN (
    'session_start', 'app_open', 'first_open',
    'user_engagement', 'screen_view'
)
GROUP BY event_date, event_name
ORDER BY event_name
"""


def _suffix(d: date) -> str:
    return d.strftime("%Y%m%d")


def _find_table(client: bigquery.Client, suffix: str) -> str | None:
    """Try events_YYYYMMDD first, then events_intraday_YYYYMMDD. Return table name or None."""
    for prefix in ("events", "events_intraday"):
        table_id = f"{FIREBASE_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{prefix}_{suffix}"
        try:
            client.get_table(table_id)
            return f"{prefix}_{suffix}"
        except Exception:
            continue
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    # Validate required creds at runtime (not import time)
    missing = [k for k, v in {
        "GCP_SERVICE_ACCOUNT_JSON": GCP_SERVICE_ACCOUNT_JSON,
        "FIREBASE_PROJECT_ID":      FIREBASE_PROJECT_ID,
        "BIGQUERY_DATASET_ID":      BIGQUERY_DATASET_ID,
    }.items() if not v]
    if missing:
        print(f"[firebase] SKIP — missing credentials: {', '.join(missing)}")
        return

    # Firebase BigQuery export is typically 1 day behind
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    start_date = target_date - timedelta(days=_FETCH_DAYS - 1)

    client = _get_bq_client()
    print(f"[firebase] authenticated as project: {FIREBASE_PROJECT_ID} | fetching {_FETCH_DAYS}d ({start_date} → {target_date})")

    # Look back up to 30 days from target_date to find the most recent available table
    table_name = None
    for days_back in range(0, 31):
        check_date = target_date - timedelta(days=days_back)
        s = _suffix(check_date)
        found = _find_table(client, s)
        if found:
            table_name = found
            target_date = check_date
            print(f"[firebase] latest table: {table_name}")
            break

    # ── User behavior (wildcard query — runs regardless of daily table) ──
    print(f"[firebase] querying user behavior (last {_FETCH_DAYS} days)...")
    beh_fmt = dict(project=FIREBASE_PROJECT_ID, dataset=BIGQUERY_DATASET_ID, fetch_days_p1=_FETCH_DAYS + 1)
    rows_beh = client.query(_USER_BEHAVIOR_SQL.format(**beh_fmt)).result()
    behavior_rows = [
        {
            "user_pseudo_id":        row.user_pseudo_id,
            "city":                  row.city,
            "region":                row.region,
            "country":               row.country,
            "first_seen_date":       str(row.first_seen_date) if row.first_seen_date else None,
            "last_seen_date":        str(row.last_seen_date) if row.last_seen_date else None,
            "total_day_span":        row.total_day_span or 0,
            "days_actually_active":  row.days_actually_active or 0,
            "total_events":          row.total_events or 0,
            "unique_features_used":  row.unique_features_used or 0,
            "total_sessions":        row.total_sessions or 0,
            "onboarding_events":     row.onboarding_events or 0,
            "login_events":          row.login_events or 0,
            "otp_events":            row.otp_events or 0,
            "health_profile_events": row.health_profile_events or 0,
            "today_tab_visits":      row.today_tab_visits or 0,
            "peak_flow_logs":        row.peak_flow_logs or 0,
            "symptom_logs":          row.symptom_logs or 0,
            "sleep_logs":            row.sleep_logs or 0,
            "treatment_views":       row.treatment_views or 0,
            "progress_views":        row.progress_views or 0,
            "journey_stage":         row.journey_stage,
            "current_status":        row.current_status,
        }
        for row in rows_beh
    ]
    upsert("firebase_user_behavior", behavior_rows)
    print(f"[firebase] user behavior done — {len(behavior_rows)} rows")

    if table_name is None:
        print("[firebase] no specific daily table found — skipping events/user_props")
        return

    base_fmt = dict(project=FIREBASE_PROJECT_ID, dataset=BIGQUERY_DATASET_ID)

    if _FETCH_DAYS > 1:
        # Multi-day range: use wildcard query (_TABLE_SUFFIX BETWEEN ...)
        start_sfx = start_date.strftime("%Y%m%d")
        end_sfx   = target_date.strftime("%Y%m%d")
        range_fmt = dict(**base_fmt, start_sfx=start_sfx, end_sfx=end_sfx)
        print(f"[firebase] querying events {start_sfx} → {end_sfx} (range)...")
        rows_events = client.query(_EVENTS_RANGE_SQL.format(**range_fmt)).result()
        event_rows = [
            {"date": row.event_date, "event_name": row.event_name,
             "event_count": row.event_count, "unique_users": row.unique_users}
            for row in rows_events
        ]
        upsert("firebase_events", event_rows)

        print("[firebase] querying user properties (range)...")
        rows_props = client.query(_USER_PROPS_RANGE_SQL.format(**range_fmt)).result()
        prop_rows = [
            {"date": row.event_date, "property": row.property,
             "value": row.value or "(none)", "user_count": row.user_count}
            for row in rows_props if row.value
        ]
        upsert("firebase_user_props", prop_rows)
    else:
        # Single-day (normal daily run)
        fmt = dict(**base_fmt, suffix=table_name)
        print("[firebase] querying events (single day)...")
        rows_events = client.query(_EVENTS_SQL.format(**fmt)).result()
        event_rows = [
            {"date": row.event_date, "event_name": row.event_name,
             "event_count": row.event_count, "unique_users": row.unique_users}
            for row in rows_events
        ]
        upsert("firebase_events", event_rows)

        print("[firebase] querying user properties...")
        rows_props = client.query(_USER_PROPS_SQL.format(**fmt)).result()
        prop_rows = [
            {"date": row.event_date, "property": row.property,
             "value": row.value or "(none)", "user_count": row.user_count}
            for row in rows_props if row.value
        ]
        upsert("firebase_user_props", prop_rows)

    print(f"[firebase] done — {len(event_rows)} events, {len(prop_rows)} user props, {len(behavior_rows)} user behavior rows")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
