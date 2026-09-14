"""Load the committed poll CSVs into the DuckDB `character_poll` table.

The DuckDB file is a regenerable artifact that is not in git, so a fresh clone
(or a restored snapshot predating the polls) has no character_poll table and
`make check-poll-links` fails. The poll rankings themselves *are* committed, so
this rebuilds the table offline instead of re-scraping the live sites with
`make scrape-poll` / `make load-poll-2021`.
"""

import csv
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from scrape_wt100_2026 import SCHEMA  # noqa: E402

DB = ROOT / "data" / "onepiece.duckdb"
POLLS = {"wt100_2021": ROOT / "data" / "wt100_2021.csv", "wt100_2026": ROOT / "data" / "wt100_2026.csv"}


def main() -> None:
    con = duckdb.connect(str(DB))
    con.execute(SCHEMA)
    for poll_id, path in POLLS.items():
        with path.open() as fh:
            rows = list(csv.DictReader(fh))
        con.execute("DELETE FROM character_poll WHERE poll_id = ?", [poll_id])
        con.executemany(
            "INSERT INTO character_poll VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    poll_id,
                    int(r["rank"]),
                    r["name"],
                    int(r["points"]) if r["points"] else None,
                    r["our_character_id"] or None,
                    r["is_variant"] == "True",
                    r.get("site_face_id") or None,
                    r.get("image_url") or None,
                    r.get("image_path") or None,
                )
                for r in rows
            ],
        )
        print(f"Loaded {len(rows)} rows into character_poll (poll_id={poll_id}) from {path.name}")
    con.close()


if __name__ == "__main__":
    main()
