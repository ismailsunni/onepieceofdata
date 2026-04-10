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


def _split_affiliations(raw: str) -> list[str]:
    """Split a raw affiliation string into individual entries.

    Primary delimiter is semicolon. When no semicolons are present,
    falls back to splitting by commas that are outside parentheses.
    """
    if ";" in raw:
        return [p.strip() for p in raw.split(";") if p.strip()]

    # No semicolons — split by commas outside parentheses
    parts = []
    current = []
    depth = 0
    for char in raw:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
        else:
            current.append(char)
    # Last segment
    part = "".join(current).strip()
    if part:
        parts.append(part)
    return parts


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


def _load_alias_mapping(alias_path: str = "data/character_aliases.json") -> dict:
    """Load character alias → canonical ID mapping."""
    try:
        with open(alias_path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Alias file not found: {alias_path}, skipping deduplication")
        return {}


def parse_affiliations(
    db_path: str,
    *,
    characters_json: str = "data/characters_detail.json",
    alias_file: str = "data/character_aliases.json",
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Parse character affiliations and save to character_affiliation table.

    Args:
        db_path: Path to the DuckDB database file.
        characters_json: Path to characters_detail.json.
        alias_file: Path to character alias mapping JSON file.
        dry_run: If True, compute but don't write to database.
        progress_callback: Optional callback for progress updates.

    Returns:
        A dict with entry_count, group_count, character_count stats.
    """
    with open(characters_json) as f:
        characters = json.load(f)

    alias_map = _load_alias_mapping(alias_file)

    # Load valid character IDs from the database to filter out non-characters
    conn = duckdb.connect(str(db_path), read_only=True)
    valid_ids = {r[0] for r in conn.execute("SELECT id FROM character").fetchall()}
    conn.close()

    merged_count = 0
    skipped_invalid = 0

    rows = []
    groups = set()
    characters_with_aff = set()

    for char in characters:
        aff_raw = char.get("affiliation", "")
        if not aff_raw or not aff_raw.strip():
            continue

        cid = char["id"]

        # Remap alias IDs to canonical IDs
        if cid in alias_map:
            cid = alias_map[cid]
            merged_count += 1

        # Skip entries that don't exist in the character table
        if cid not in valid_ids:
            skipped_invalid += 1
            continue

        parts = _split_affiliations(aff_raw)

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

    # Deduplicate: when aliases map to the same canonical ID, keep first occurrence
    # (the ON CONFLICT in SQL would handle this too, but cleaner to dedup here)
    seen = set()
    deduped_rows = []
    for row in rows:
        key = (row[0], row[1])  # (character_id, group_name)
        if key not in seen:
            seen.add(key)
            deduped_rows.append(row)
    duplicates_removed = len(rows) - len(deduped_rows)
    rows = deduped_rows

    if merged_count > 0:
        logger.info(f"Merged {merged_count} alias IDs to canonical IDs, removed {duplicates_removed} duplicates")
    if skipped_invalid > 0:
        logger.info(f"Skipped {skipped_invalid} non-character entries (not in character table)")

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
        "aliases_merged": merged_count,
        "duplicates_removed": duplicates_removed,
        "non_characters_skipped": skipped_invalid,
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
