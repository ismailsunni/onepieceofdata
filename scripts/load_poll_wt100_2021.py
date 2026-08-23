"""Load the 1st Global One Piece Character Popularity Poll (WT100 2021) rankings.

The official site (onepiecewt100.com) is dead and its Wayback capture is an SPA
shell with no archived API responses, so the rankings come from the Fandom wiki
`Popularity Polls` results table instead (column 7 = "7th Poll (2021)").

Rows land in the same DuckDB `character_poll` table as the 2026 poll, under
poll_id 'wt100_2021'. Vote counts were only published for the top 100.
"""

import csv
import re
from pathlib import Path

import duckdb
import requests

from scrape_wt100_2026 import SCHEMA, build_lookup, match

POLL_ID = "wt100_2021"
POLL_COLUMN = 6  # 0-based positional param: 7th Poll (2021), overall ranking
API = "https://onepiece.fandom.com/api.php"
PAGE = "Popularity Polls"
SECTION = ("===Results Table===", "====7th Poll Top 100 Ranking by Region====")
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "onepiece.duckdb"
OUT = ROOT / "data" / "wt100_2021.csv"


def fetch_wikitext() -> str:
    resp = requests.get(
        API,
        params={"action": "parse", "page": PAGE, "prop": "wikitext", "format": "json", "formatversion": 2},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["parse"]["wikitext"]


def split_params(row: str) -> list[str]:
    """Split template params on | while keeping [[piped|links]] and {{templates}} intact."""
    out, buf, depth = [], "", 0
    for ch in row:
        if ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        if ch == "|" and depth == 0:
            out.append(buf)
            buf = ""
        else:
            buf += ch
    out.append(buf)
    return [p.strip() for p in out]


def parse_poll(wikitext: str) -> list[dict]:
    """Extract (rank, name, votes) for the target poll from the main results table."""
    start = wikitext.index(SECTION[0])
    body = wikitext[start : wikitext.index(SECTION[1], start)]
    rows = []
    for raw in re.findall(r"\{\{PopularityTable\|(.*?)\}\}\s*\n", body):
        params = split_params(raw)
        # names are [[Link]], [[Link|Display]] or {{Nihongo|Name|kanji|...}}
        cell_name = params[0]
        nihongo = re.match(r"\{\{Nihongo\|(.*)\}\}$", cell_name, re.S)
        if nihongo:
            cell_name = split_params(nihongo.group(1))[0]
        link = re.sub(r"\[\[|\]\]", "", cell_name)
        target, _, display = link.partition("|")
        cols = params[1:]
        if len(cols) <= POLL_COLUMN or not cols[POLL_COLUMN]:
            continue
        cell = re.match(r"0*(\d+)\s*(?:\(([\d,]+)\))?", cols[POLL_COLUMN])
        if not cell:
            continue
        rows.append(
            {
                "rank": int(cell.group(1)),
                "name": (display or target).strip(),
                "wiki_title": target.strip(),
                "points": int(cell.group(2).replace(",", "")) if cell.group(2) else None,
            }
        )
    rows.sort(key=lambda r: (r["rank"], r["name"]))
    return rows


def load_duckdb(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    con.execute(SCHEMA)
    con.execute("DELETE FROM character_poll WHERE poll_id = ?", [POLL_ID])
    con.executemany(
        "INSERT INTO character_poll VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (POLL_ID, r["rank"], r["name"], r["points"], r["our_character_id"], r["is_variant"], None, None, None)
            for r in rows
        ],
    )
    print(f"Loaded {len(rows)} rows into DuckDB character_poll (poll_id={POLL_ID})")


def main() -> None:
    rows = parse_poll(fetch_wikitext())
    print(f"Parsed {len(rows)} ranked characters ({sum(1 for r in rows if r['points'])} with vote counts)")

    con = duckdb.connect(str(DB))
    exact, loose_map = build_lookup(con)
    valid = {cid for (cid,) in con.execute("SELECT id FROM character").fetchall()}
    seen: set[str] = set()
    for row in rows:
        # the wiki link target IS our character id, so trust it before fuzzy matching
        cid = row["wiki_title"].replace(" ", "_")
        if cid not in valid:
            cid = match(row, exact, loose_map)
        row["our_character_id"] = cid
        row["is_variant"] = bool(cid) and cid in seen
        if cid:
            seen.add(cid)
    print(f"Matched {sum(1 for r in rows if r['our_character_id'])}/{len(rows)} to our character table")

    load_duckdb(con, rows)
    with OUT.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["rank", "name", "points", "our_character_id", "is_variant", "wiki_title"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
