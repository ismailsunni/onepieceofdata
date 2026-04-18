"""Post-processor to parse devil fruit data into a structured table.

Reads devil fruit fields from characters_detail.json and populates the
``character_devil_fruit`` table in DuckDB.

Handles edge cases:
  - Luffy's dual-identity fruit: ``Gomu Gomu no Mi(Hito Hito no Mi, Model: Nika)``
    → stores the revealed canonical identity.
  - Teach's two fruits: ``dfname`` / ``dfname2`` → two rows.
  - Reference markers like ``[24]`` are stripped.
"""

from __future__ import annotations

import json
import re
from typing import Callable, Optional

import duckdb
from loguru import logger


def _clean(value: str) -> str:
    """Strip wiki reference markers and extra whitespace."""
    return re.sub(r"\[\d+\]", "", value).strip()


def _parse_parenthetical(raw: str) -> tuple[str, Optional[str]]:
    """Split a value like ``A(B)`` into ``(A, B)``.

    Used for Luffy-style dual-identity fields where the true name/type
    is in parentheses after the cover identity.  If there are no
    parentheses, returns ``(raw, None)``.
    """
    # Match: "Cover Name(True Name)" — no space before paren is common in wiki
    m = re.match(r"^(.+?)\((.+)\)\s*$", raw)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return raw, None


def _extract_fruit(char: dict, suffix: str = "") -> Optional[dict]:
    """Extract one devil fruit entry from a character dict.

    Args:
        char: Character detail dict from characters_detail.json.
        suffix: '' for primary fruit, '_2' for the second fruit.

    Returns:
        Dict with fruit_name, english_name, meaning, fruit_type or None.
    """
    name_raw = char.get(f"devil_fruit_name{suffix}", "")
    type_raw = char.get(f"devil_fruit_type{suffix}", "")

    if not name_raw or not name_raw.strip():
        return None

    name_raw = _clean(name_raw)
    type_raw = _clean(type_raw)
    english_raw = _clean(char.get(f"devil_fruit_english_name{suffix}", "") or "")
    meaning_raw = _clean(char.get(f"devil_fruit_meaning{suffix}", "") or "")

    # Handle dual-identity fruits like Luffy's:
    #   name = "Gomu Gomu no Mi(Hito Hito no Mi, Model: Nika)"
    #   type = "Paramecia(Mythical Zoan)"
    # → use the revealed (parenthetical) identity as canonical.
    cover_name, true_name = _parse_parenthetical(name_raw)
    _cover_type, true_type = _parse_parenthetical(type_raw)
    _cover_eng, true_eng = _parse_parenthetical(english_raw)

    fruit_name = true_name or cover_name
    fruit_type = true_type or _cover_type or type_raw
    english_name = true_eng or _cover_eng or english_raw

    # Clean meaning: may have semicolons for dual (e.g. "Rubber; Human; Nika")
    # Keep the full meaning string as-is — it's descriptive, not structural.
    meaning = meaning_raw if meaning_raw else None

    if not fruit_name:
        return None

    return {
        "fruit_name": fruit_name,
        "english_name": english_name or None,
        "meaning": meaning,
        "fruit_type": fruit_type or None,
    }


def parse_devil_fruits(
    db_path: str,
    *,
    characters_json: str = "data/characters_detail.json",
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Parse devil fruit data from characters_detail.json into DuckDB.

    Args:
        db_path: Path to the DuckDB database file.
        characters_json: Path to characters_detail.json.
        dry_run: If True, compute but don't write to database.
        progress_callback: Optional callback for progress updates.

    Returns:
        Stats dict with entry_count, character_count, dual_identity, etc.
    """
    with open(characters_json) as f:
        characters = json.load(f)

    # Load valid character IDs from the database
    conn = duckdb.connect(str(db_path), read_only=True)
    valid_ids = {r[0] for r in conn.execute("SELECT id FROM character").fetchall()}
    conn.close()

    rows: list[tuple] = []
    stats = {
        "entry_count": 0,
        "character_count": 0,
        "dual_identity": 0,
        "multi_fruit": 0,
        "skipped_invalid": 0,
    }

    for char in characters:
        cid = char.get("id", "")
        if cid not in valid_ids:
            continue

        fruits_for_char = []

        # Primary fruit
        fruit1 = _extract_fruit(char)
        if fruit1:
            fruits_for_char.append(fruit1)
            # Detect dual-identity (Luffy-style)
            name_raw = _clean(char.get("devil_fruit_name", "") or "")
            _, true = _parse_parenthetical(name_raw)
            if true:
                stats["dual_identity"] += 1

        # Second fruit (Teach)
        fruit2 = _extract_fruit(char, suffix="_2")
        if fruit2:
            fruits_for_char.append(fruit2)
            stats["multi_fruit"] += 1

        for fruit in fruits_for_char:
            rows.append((
                cid,
                fruit["fruit_name"],
                fruit["english_name"],
                fruit["meaning"],
                fruit["fruit_type"],
            ))

        if fruits_for_char:
            stats["character_count"] += 1

    stats["entry_count"] = len(rows)

    logger.info(
        f"Parsed {stats['entry_count']} devil fruit entries "
        f"({stats['character_count']} characters, "
        f"{stats['dual_identity']} dual-identity, "
        f"{stats['multi_fruit']} multi-fruit)"
    )

    if progress_callback:
        progress_callback(1, 2, "parsed")

    if not dry_run:
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute("DROP TABLE IF EXISTS character_devil_fruit CASCADE")
            conn.execute("""
                CREATE TABLE character_devil_fruit (
                    character_id TEXT NOT NULL,
                    fruit_name   TEXT NOT NULL,
                    english_name TEXT,
                    meaning      TEXT,
                    fruit_type   TEXT,
                    PRIMARY KEY (character_id, fruit_name)
                )
            """)
            if rows:
                conn.executemany(
                    """INSERT INTO character_devil_fruit
                       (character_id, fruit_name, english_name, meaning, fruit_type)
                       VALUES (?, ?, ?, ?, ?)""",
                    rows,
                )
            logger.success(
                f"Saved {len(rows)} devil fruit entries to character_devil_fruit"
            )
        finally:
            conn.close()

    if progress_callback:
        progress_callback(2, 2, "done")

    return stats
