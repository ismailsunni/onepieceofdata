"""Scrape the 2nd Global One Piece Character Popularity Poll (WT100 2026) final rankings.

Writes the global ranking to data/wt100_2026.csv, loads it into the DuckDB
`character_poll` table (poll_id = 'wt100_2026'), and downloads the character face
images into data/wt100_2026_images/ (upload them with scripts/upload_poll_images.py).

Points are only published for the global top 10; they are null for lower ranks.
"""

import csv
import json
import re
import unicodedata
from difflib import get_close_matches
from pathlib import Path

import duckdb
import requests

POLL_ID = "wt100_2026"
URL = "https://onepiecewt100-2026.com/en/final-rankings/"
BASE = "https://onepiecewt100-2026.com"
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "onepiece.duckdb"
ALIASES = ROOT / "data" / "character_aliases.json"
OUT_DIR = ROOT / "data"
IMG_DIR = OUT_DIR / "wt100_2026_images"
# directory prefix inside the Supabase Storage bucket, kept apart from the
# character thumbnails that live at the bucket root
STORAGE_PREFIX = f"polls/{POLL_ID}"


TITLES = ("SAINT", "MR", "MISS", "GOD")


def normalize(name: str) -> str:
    """Uppercase, strip accents/punctuation/parentheticals for fuzzy name matching."""
    name = re.sub(r"\([^)]*\)", " ", name)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = re.sub(r"[^A-Za-z0-9]+", " ", name)
    return " ".join(name.split()).upper()


def loose(name: str) -> str:
    """Normalize further: drop honorifics and collapse long-vowel romanizations."""
    words = [w for w in normalize(name).split() if w not in TITLES]
    key = " ".join(words)
    for long_vowel, short in (("OU", "O"), ("OO", "O"), ("UU", "U"), ("AA", "A"), ("EE", "E")):
        key = key.replace(long_vowel, short)
    return key


def parse_rankings(html: str) -> list[dict]:
    """Extract the global ('all' region) ranking list."""
    panel = re.split(r'data-fr-static-panel data-region="([^"]+)"', html)[2]
    chunks = re.split(r'data-rank="(\d+)"', panel)
    rows = []
    for rank, body in zip(chunks[1::2], chunks[2::2]):
        body = body[:2500]
        alt = re.search(r'alt="([^"]*)"', body)
        name = re.search(r"data-portrait-name>([^<]*)<", body)
        img = re.search(r'src="(/assets/[^"?]+)', body)
        points = re.search(r"data-points-value>([\d,]+)<", body)
        face = re.search(r"/assets/faces(?:-\d+x\d+)?/(\d+)\.png", img.group(1)) if img else None
        rows.append(
            {
                "rank": int(rank),
                "name": (name.group(1) if name and name.group(1) else alt.group(1) if alt else ""),
                "points": int(points.group(1).replace(",", "")) if points else None,
                "site_face_id": face.group(1) if face else None,
                "image_url": BASE + img.group(1) if img else None,
            }
        )
    return rows


def build_lookup(con: duckdb.DuckDBPyConnection) -> tuple[dict[str, str], dict[str, str]]:
    """Return (exact, loose) name -> character id lookups built from names, ids and aliases."""
    exact: dict[str, str] = {}
    loose_map: dict[str, str] = {}
    names = con.execute("SELECT id, name FROM character").fetchall()
    valid = {cid for cid, _ in names}
    pairs = [(cname, cid) for cid, cname in names] + [(cid.replace("_", " "), cid) for cid, _ in names]
    if ALIASES.exists():
        pairs += [
            (alias.replace("_", " "), target)
            for alias, target in json.loads(ALIASES.read_text()).items()
            if target in valid
        ]
    for key, cid in pairs:
        exact.setdefault(normalize(key), cid)
        loose_map.setdefault(loose(key), cid)
    return exact, loose_map


def match(row: dict, exact: dict[str, str], loose_map: dict[str, str]) -> str | None:
    """Try the full name, then the parenthetical alias, then the surname alone."""
    candidates = [row["name"], *re.findall(r"\(([^)]*)\)", row["name"])]
    parts = normalize(row["name"]).split()
    if len(parts) > 1:
        candidates.append(parts[-1])
    for cand in candidates:
        cid = exact.get(normalize(cand)) or loose_map.get(loose(cand))
        if cid:
            return cid
    for cand in candidates:
        close = get_close_matches(loose(cand), loose_map, n=1, cutoff=0.9)
        if close:
            return loose_map[close[0]]
    return None


def download_images(rows: list[dict], session: requests.Session) -> None:
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    for row in rows:
        if not row["image_url"]:
            continue
        # prefer the largest face variant when the listing used a thumbnail
        urls = [row["image_url"]]
        if row["site_face_id"]:
            urls.insert(0, f"{BASE}/assets/faces/{row['site_face_id']}.png")
        dest = IMG_DIR / f"{row['rank']:04d}_{re.sub(r'[^A-Za-z0-9]+', '_', row['name']).strip('_')}.png"
        row["image_path"] = f"{STORAGE_PREFIX}/{dest.name}"
        if dest.exists():
            continue
        for url in urls:
            resp = session.get(url, timeout=30)
            if resp.ok:
                dest.write_bytes(resp.content)
                break
        else:
            row["image_path"] = None


SCHEMA = """
CREATE TABLE IF NOT EXISTS character_poll (
    poll_id      TEXT NOT NULL,
    rank         INTEGER NOT NULL,
    name         TEXT NOT NULL,
    points       BIGINT,
    character_id TEXT,
    is_variant   BOOLEAN,
    site_face_id TEXT,
    image_url    TEXT,
    image_path   TEXT,
    PRIMARY KEY (poll_id, rank, name)
)
"""


def load_duckdb(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    con.execute(SCHEMA)
    con.execute("DELETE FROM character_poll WHERE poll_id = ?", [POLL_ID])
    con.executemany(
        "INSERT INTO character_poll VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                POLL_ID,
                r["rank"],
                r["name"],
                r["points"],
                r["our_character_id"],
                r["is_variant"],
                r["site_face_id"],
                r["image_url"],
                r.get("image_path"),
            )
            for r in rows
        ],
    )
    print(f"Loaded {len(rows)} rows into DuckDB character_poll (poll_id={POLL_ID})")


def main() -> None:
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (onepieceofdata)"
    rows = parse_rankings(session.get(URL, timeout=60).text)
    print(f"Parsed {len(rows)} ranked characters")

    con = duckdb.connect(str(DB))
    exact, loose_map = build_lookup(con)
    seen: set[str] = set()
    for row in rows:
        cid = match(row, exact, loose_map)
        row["our_character_id"] = cid
        # a lower-ranked entry resolving to an already-claimed character is an
        # alternate form (e.g. "ACE (Anthropomorphized form)") - flag it so
        # analytics can dedupe instead of double-counting
        row["is_variant"] = bool(cid) and cid in seen
        if cid:
            seen.add(cid)
    matched = sum(1 for r in rows if r["our_character_id"])
    print(f"Matched {matched}/{len(rows)} to our character table")

    download_images(rows, session)
    load_duckdb(con, rows)

    fields = [
        "rank",
        "name",
        "points",
        "our_character_id",
        "is_variant",
        "site_face_id",
        "image_url",
        "image_path",
    ]
    with (OUT_DIR / "wt100_2026.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {OUT_DIR / 'wt100_2026.csv'} and images to {IMG_DIR}")


if __name__ == "__main__":
    main()
