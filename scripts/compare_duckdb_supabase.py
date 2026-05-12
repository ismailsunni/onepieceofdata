"""Compare row counts between local DuckDB and Supabase, and flag schema drift.

Reports per-table row counts on both sides, with a diff column. For tables that
exist on both sides, also surfaces a small sample of missing/extra primary keys
when counts differ (chapter, character).
"""

from __future__ import annotations

import os
import sys

import duckdb
from dotenv import load_dotenv
from supabase import create_client


DUCKDB_PATH = "data/onepiece.duckdb"

# Tables that intentionally exist only on one side
LOCAL_ONLY_EXPECTED = {
    "coc",
    "cov",
    "character_importance",
    "graph_extractions",
    "graph_source_text",
}
SUPABASE_ONLY_EXPECTED = {"profiles"}

# Tables where we drill into PK diffs when counts mismatch
PK_DIFF_TABLES = {
    "chapter": "title",
    "character": "id",
    "arc": "id",
    "saga": "id",
    "volume": "id",
}


def fetch_all_ids(sb, table: str, column: str) -> set:
    ids: set = set()
    page = 1000
    offset = 0
    while True:
        r = sb.table(table).select(column).range(offset, offset + page - 1).execute()
        if not r.data:
            break
        ids.update(row[column] for row in r.data)
        if len(r.data) < page:
            break
        offset += page
    return ids


def main() -> int:
    load_dotenv(".env")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("ERROR: SUPABASE_URL / SUPABASE_SERVICE_KEY missing in .env", file=sys.stderr)
        return 2

    sb = create_client(url, key)
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    local_tables = {
        t[0]
        for t in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }

    # Supabase: enumerate tables via Postgres REST OpenAPI is messy; use a known list
    # built from the union of local tables minus local-only, plus supabase-only extras.
    sb_candidate_tables = (local_tables - LOCAL_ONLY_EXPECTED) | SUPABASE_ONLY_EXPECTED

    sb_counts: dict[str, int | None] = {}
    for t in sb_candidate_tables:
        try:
            r = sb.table(t).select("*", count="exact", head=True).execute()
            sb_counts[t] = r.count
        except Exception:
            sb_counts[t] = None

    local_counts: dict[str, int] = {}
    for t in local_tables:
        local_counts[t] = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]

    all_tables = sorted(local_tables | set(sb_counts.keys()))

    print(f"{'Table':<28} {'Local':>10} {'Supabase':>10} {'Diff':>8}  Notes")
    print("-" * 80)
    drift: list[tuple[str, int, int]] = []
    for t in all_tables:
        l = local_counts.get(t)
        r = sb_counts.get(t)
        note = ""
        if l is None and t in SUPABASE_ONLY_EXPECTED:
            note = "supabase-only (expected)"
        elif r is None and t in LOCAL_ONLY_EXPECTED:
            note = "local-only (expected)"
        elif l is None:
            note = "missing locally"
        elif r is None:
            note = "missing in supabase"
        diff_str = ""
        if isinstance(l, int) and isinstance(r, int):
            d = l - r
            diff_str = f"{d:+d}" if d != 0 else "0"
            if d != 0:
                drift.append((t, l, r))
        print(
            f"{t:<28} {str(l) if l is not None else '-':>10} "
            f"{str(r) if r is not None else '-':>10} {diff_str:>8}  {note}"
        )

    # PK diff drill-down for mismatched well-known tables
    if drift:
        print()
        print("Row diffs for key tables:")
        for t, l, r in drift:
            if t not in PK_DIFF_TABLES:
                continue
            col = PK_DIFF_TABLES[t]
            local_ids = {row[0] for row in con.execute(f'SELECT "{col}" FROM "{t}"').fetchall()}
            try:
                sb_ids = fetch_all_ids(sb, t, col)
            except Exception as e:
                print(f"  {t}: could not fetch supabase ids ({e})")
                continue
            only_local = sorted(local_ids - sb_ids)
            only_sb = sorted(sb_ids - local_ids)
            print(f"  [{t}] only in local ({len(only_local)}): {only_local[:10]}")
            print(f"  [{t}] only in supabase ({len(only_sb)}): {only_sb[:10]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
