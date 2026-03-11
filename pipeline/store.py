"""
Supabase upsert client shared by all fetchers.
Uses the REST API with merge-duplicates preference (idempotent on re-run).
"""
import requests
from pipeline.config import SUPABASE_URL, SUPABASE_SERVICE_KEY


def _get_headers() -> dict | None:
    """Return auth headers, or None if SERVICE_KEY not yet configured."""
    if not SUPABASE_SERVICE_KEY:
        print("[store] WARNING: SUPABASE_SERVICE_KEY not set — data will NOT be saved.")
        print("[store] Get it from: Supabase Dashboard → Settings → API → service_role key")
        return None
    return {
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
    headers = _get_headers()
    if not headers:
        print(f"[store] {table}: skipped (no service key) — fetched {len(rows)} row(s) locally ✓")
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    r = requests.post(url, json=rows, headers=headers, timeout=30)
    if not r.ok:
        print(f"[store] {table} ERROR {r.status_code}: {r.text[:400]}")
        r.raise_for_status()
    print(f"[store] {table}: upserted {len(rows)} row(s) ✓")


def fetch_all(table: str, params: dict | None = None) -> list[dict]:
    """Fetch all rows from a table."""
    headers = _get_headers()
    if not headers:
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    hdrs = {**headers, "Prefer": ""}
    r = requests.get(url, headers=hdrs, params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()
