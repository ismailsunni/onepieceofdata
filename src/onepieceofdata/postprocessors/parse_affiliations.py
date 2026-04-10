"""Post-processor to parse character affiliations into structured data.

Parses the raw affiliation string from character details (semicolon-delimited,
with parenthetical status/sub-group annotations) into a character_affiliation
table in DuckDB.

Examples of raw input:
  "Straw Hat Pirates ; Marines (former) ; Baroque Works (Billions, former)"
  "Thriller Bark Pirates (Mysterious Four) (former)"
"""

import json
import re
from typing import Optional, Callable

import duckdb
from loguru import logger


# Relationship statuses that describe the character's membership,
# as opposed to sub-group labels (e.g., "SWORD", "Tobiroppo").
RELATIONSHIP_STATUSES = {
    "former", "formerly", "current",
    "disbanded", "defected", "temporary", "temporarily",
    "revoked", "resigned", "secret", "double agent", "undercover", "ruse",
    "espionage", "semi-retired", "retired",
    "dissolved", "unknown status", "post mortem", "descended",
    "illegitimate",
    # Non-canon markers (still useful to track)
    "filler", "movie", "anime only", "non-canon",
}


def _parse_single_affiliation(raw: str) -> Optional[dict]:
    """Parse one semicolon-separated affiliation entry.

    Returns:
        Dict with keys: group, sub_group (nullable), status (list of strings).
        None if the entry is empty.
    """
    raw = raw.strip().rstrip("?").rstrip("≠").strip()
    if not raw:
        return None

    # Pattern 1: Group (Sub-group) (status1, status2)
    m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*\(([^)]+)\)$", raw)
    if m:
        group = m.group(1).strip()
        sub = m.group(2).strip()
        status_parts = [s.strip().lower() for s in m.group(3).split(",")]
        statuses = [s for s in status_parts if s in RELATIONSHIP_STATUSES]
        return {"group": group, "sub_group": sub, "status": statuses or ["current"]}

    # Pattern 2: Group (mixed sub-group and statuses)
    m = re.match(r"^(.+?)\s*\(([^)]+)\)$", raw)
    if m:
        group = m.group(1).strip()
        parts = [s.strip() for s in m.group(2).split(",")]
        statuses = [p.lower() for p in parts if p.lower() in RELATIONSHIP_STATUSES]
        sub_parts = [p for p in parts if p.lower() not in RELATIONSHIP_STATUSES]
        sub = ", ".join(sub_parts).strip() if sub_parts else None
        return {"group": group, "sub_group": sub, "status": statuses or ["current"]}

    # Pattern 3: Plain group name
    return {"group": raw, "sub_group": None, "status": ["current"]}


def parse_affiliations(
    db_path: str,
    *,
    characters_json: str = "data/characters_detail.json",
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Parse character affiliations and save to character_affiliation table.

    Args:
        db_path: Path to the DuckDB database file.
        characters_json: Path to characters_detail.json.
        dry_run: If True, compute but don't write to database.
        progress_callback: Optional callback for progress updates.

    Returns:
        A dict with entry_count, group_count, character_count stats.
    """
    with open(characters_json) as f:
        characters = json.load(f)

    rows = []
    groups = set()
    characters_with_aff = set()

    for char in characters:
        aff_raw = char.get("affiliation", "")
        if not aff_raw or not aff_raw.strip():
            continue

        cid = char["id"]
        parts = aff_raw.split(";")

        for part in parts:
            result = _parse_single_affiliation(part)
            if not result:
                continue

            # Collapse "formerly" → "former"
            statuses = []
            for s in result["status"]:
                if s == "formerly":
                    statuses.append("former")
                elif s == "temporarily":
                    statuses.append("temporary")
                else:
                    statuses.append(s)

            # Pick the primary status (most significant)
            status = _pick_primary_status(statuses)

            rows.append((cid, result["group"], result["sub_group"], status))
            groups.add(result["group"])
            characters_with_aff.add(cid)

    logger.info(
        f"Parsed {len(rows)} affiliation entries "
        f"({len(characters_with_aff)} characters, {len(groups)} groups)"
    )

    if progress_callback:
        progress_callback(1, 2, "parsed")

    if not dry_run:
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute("DROP TABLE IF EXISTS character_affiliation CASCADE")
            conn.execute("""
                CREATE TABLE character_affiliation (
                    character_id TEXT NOT NULL,
                    group_name TEXT NOT NULL,
                    sub_group TEXT,
                    status TEXT NOT NULL DEFAULT 'current',
                    PRIMARY KEY (character_id, group_name)
                )
            """)

            conn.executemany(
                """INSERT INTO character_affiliation
                   (character_id, group_name, sub_group, status)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT (character_id, group_name) DO UPDATE SET
                       sub_group = EXCLUDED.sub_group,
                       status = EXCLUDED.status""",
                rows,
            )

            logger.success(f"Saved {len(rows)} affiliation entries to character_affiliation")
        finally:
            conn.close()

    if progress_callback:
        progress_callback(2, 2, "done")

    return {
        "entry_count": len(rows),
        "group_count": len(groups),
        "character_count": len(characters_with_aff),
    }


def _pick_primary_status(statuses: list[str]) -> str:
    """Pick the most significant status from a list.

    Priority: defected > former > disbanded > dissolved > revoked > resigned
              > temporary > the rest > current
    """
    priority = [
        "defected", "former", "disbanded", "dissolved", "revoked",
        "resigned", "temporary", "secret", "double agent", "undercover",
        "ruse", "espionage", "semi-retired", "retired", "post mortem",
        "descended", "illegitimate", "filler", "movie", "anime only",
        "non-canon", "unknown status",
    ]
    for p in priority:
        if p in statuses:
            return p
    return "current"
