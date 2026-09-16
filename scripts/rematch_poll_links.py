"""Re-resolve character ids in the committed poll CSVs without re-scraping.

Both poll sources are offline-ish (the 2026 site is slow to scrape, the 2021 one
comes from the wiki), but the name -> character matching depends on
data/character_aliases.json, which keeps growing as unmatched entries are spotted.
This re-runs the matcher over the CSVs, rewrites them and reloads DuckDB.

    uv run python scripts/rematch_poll_links.py [--dry-run]
"""

import argparse
import csv
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from scrape_wt100_2026 import build_lookup, match  # noqa: E402

DB = ROOT / "data" / "onepiece.duckdb"
POLLS = {"wt100_2021": ROOT / "data" / "wt100_2021.csv", "wt100_2026": ROOT / "data" / "wt100_2026.csv"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report changes without writing")
    args = parser.parse_args()

    con = duckdb.connect(str(DB), read_only=args.dry_run)
    exact, loose_map = build_lookup(con)
    valid = {cid for (cid,) in con.execute("SELECT id FROM character").fetchall()}

    for poll_id, path in POLLS.items():
        with path.open() as fh:
            reader = csv.DictReader(fh)
            fields, rows = reader.fieldnames, list(reader)
        seen: set[str] = set()
        changed = 0
        for row in rows:
            title = row.get("wiki_title", "").replace(" ", "_")
            cid = title if title in valid else match(row, exact, loose_map)
            if (cid or "") != row["our_character_id"]:
                print(f"  {poll_id} #{row['rank']} {row['name']}: {row['our_character_id'] or '-'} -> {cid or '-'}")
                changed += 1
            row["our_character_id"] = cid or ""
            row["is_variant"] = str(bool(cid) and cid in seen)
            if cid:
                seen.add(cid)
        unmatched = sum(1 for r in rows if not r["our_character_id"])
        print(f"{poll_id}: {changed} changed, {len(rows) - unmatched}/{len(rows)} matched")
        if args.dry_run:
            continue
        with path.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    con.close()
    if not args.dry_run:
        print("Reload DuckDB with: make load-polls-csv")


if __name__ == "__main__":
    main()
