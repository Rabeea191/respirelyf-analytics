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
import sys
from datetime import date, timedelta

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
FROM `{project}.{dataset}.events_{suffix}`
GROUP BY event_date, event_name
ORDER BY event_count DESC
"""

_USER_PROPS_SQL = """
SELECT
    event_date,
    up.key           AS property,
    up.value.string_value AS value,
    COUNT(DISTINCT user_pseudo_id) AS user_count
FROM `{project}.{dataset}.events_{suffix}`
CROSS JOIN UNNEST(user_properties) AS up
WHERE up.value.string_value IS NOT NULL
GROUP BY event_date, up.key, up.value.string_value
ORDER BY user_count DESC
LIMIT 500
"""

_SESSIONS_SQL = """
SELECT
    event_date,
    event_name,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM `{project}.{dataset}.events_{suffix}`
WHERE event_name IN (
    'session_start', 'app_open', 'first_open',
    'user_engagement', 'screen_view'
)
GROUP BY event_date, event_name
ORDER BY event_name
"""


def _suffix(d: date) -> str:
    return d.strftime("%Y%m%d")


def _table_exists(client: bigquery.Client, suffix: str) -> bool:
    table_id = f"{FIREBASE_PROJECT_ID}.{BIGQUERY_DATASET_ID}.events_{suffix}"
    try:
        client.get_table(table_id)
        return True
    except Exception:
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def run(target_date: date | None = None) -> None:
    # Firebase BigQuery export is typically 1 day behind
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    client = _get_bq_client()
    print(f"[firebase] authenticated as project: {FIREBASE_PROJECT_ID}")

    # Fall back up to 3 days if the table isn't ready yet
    suffix = None
    for days_back in range(0, 4):
        check_date = target_date - timedelta(days=days_back)
        s = _suffix(check_date)
        if _table_exists(client, s):
            suffix = s
            target_date = check_date
            print(f"[firebase] using table events_{suffix}")
            break

    if suffix is None:
        print("[firebase] no events table found for past 4 days — skipping")
        return

    fmt = dict(project=FIREBASE_PROJECT_ID, dataset=BIGQUERY_DATASET_ID, suffix=suffix)

    # ── Events ──────────────────────────────────────────────────────────
    print("[firebase] querying events...")
    rows_events = client.query(_EVENTS_SQL.format(**fmt)).result()
    event_rows = [
        {
            "date":        row.event_date,
            "event_name":  row.event_name,
            "event_count": row.event_count,
            "unique_users": row.unique_users,
        }
        for row in rows_events
    ]
    upsert("firebase_events", event_rows)

    # ── User properties ──────────────────────────────────────────────────
    print("[firebase] querying user properties...")
    rows_props = client.query(_USER_PROPS_SQL.format(**fmt)).result()
    prop_rows = [
        {
            "date":       row.event_date,
            "property":   row.property,
            "value":      row.value or "(none)",
            "user_count": row.user_count,
        }
        for row in rows_props
        if row.value  # skip null values
    ]
    upsert("firebase_user_props", prop_rows)

    print(f"[firebase] done — {len(event_rows)} events, {len(prop_rows)} user props")


if __name__ == "__main__":
    d = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(d)
