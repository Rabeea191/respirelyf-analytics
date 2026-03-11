"""
Supabase upsert client shared by all fetchers.
Uses the REST API with merge-duplicates preference (idempotent on re-run).
"""
import requests
from pipeline.config import SUPABASE_URL, SUPABASE_SERVICE_KEY

_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}


def upsert(table: str, rows: list[dict]) -> None:
    """Upsert a list of dicts into a Supabase table. Safe to re-run."""
    if not rows:
        print(f"[store] {table}: 0 rows — skipping")
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, json=rows, headers=_HEADERS, timeout=30)
    if not r.ok:
        print(f"[store] {table} ERROR {r.status_code}: {r.text[:400]}")
        r.raise_for_status()
    print(f"[store] {table}: upserted {len(rows)} row(s)")


def fetch_all(table: str, params: dict | None = None) -> list[dict]:
    """Fetch all rows from a table (for deduplication checks etc.)."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    hdrs = {**_HEADERS, "Prefer": ""}
    r = requests.get(url, headers=hdrs, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()
