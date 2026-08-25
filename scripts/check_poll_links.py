"""Check that character_poll rows still resolve to characters.

There is no foreign key on character_poll.character_id: the weekly export drops
and recreates the character table, which would cascade the constraint away every
run. This check replaces it - it compares the number of poll entries that do not
resolve to a character against a stored baseline, so a jump means a character id
disappeared or changed rather than the poll data being genuinely unmatchable.

    uv run python scripts/check_poll_links.py            # warn on regression
    uv run python scripts/check_poll_links.py --update   # accept current counts
    uv run python scripts/check_poll_links.py --strict   # exit 1 on regression
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "onepiece.duckdb"
BASELINE = ROOT / "data" / "poll_link_baseline.json"

QUERY = """
SELECT poll_id,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE character_id IS NULL) AS unmatched,
       COUNT(*) FILTER (WHERE character_id IS NOT NULL AND character_id NOT IN (SELECT id FROM character)) AS orphaned
FROM character_poll
GROUP BY poll_id
ORDER BY poll_id
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update", action="store_true", help="write current counts as the new baseline")
    parser.add_argument("--strict", action="store_true", help="exit 1 when counts regress")
    args = parser.parse_args()

    con = duckdb.connect(str(DB), read_only=True)
    current = {
        poll: {"total": total, "unmatched": unmatched, "orphaned": orphaned}
        for poll, total, unmatched, orphaned in con.execute(QUERY).fetchall()
    }
    baseline = json.loads(BASELINE.read_text()) if BASELINE.exists() else {}

    regressions = []
    for poll, counts in current.items():
        was = baseline.get(poll, {})
        deltas = []
        for key in ("unmatched", "orphaned"):
            before = was.get(key)
            delta = counts[key] - before if before is not None else None
            deltas.append(f"{key} {counts[key]}" + (f" ({delta:+d})" if delta else ""))
            if delta and delta > 0:
                regressions.append(f"{poll}: {key} rose by {delta} (was {before}, now {counts[key]})")
        print(f"{poll}: {counts['total']} entries, " + ", ".join(deltas))

    if regressions:
        print("\n⚠️  poll links regressed - a character id likely changed or disappeared:")
        for line in regressions:
            print(f"   {line}")
        print("   Inspect with: SELECT * FROM character_poll WHERE character_id IS NULL;")
    elif baseline:
        print("✅ no new unmatched or orphaned poll entries")

    if args.update:
        BASELINE.write_text(json.dumps(current, indent=2) + "\n")
        print(f"Baseline written to {BASELINE}")
    if args.strict and regressions:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
