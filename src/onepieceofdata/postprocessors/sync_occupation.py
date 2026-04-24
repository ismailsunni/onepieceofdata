"""Post-processor to sync the ``occupation`` column on the character table.

The ``occupation`` field is scraped into ``data/characters_detail.json`` but
was not originally part of the ``character`` table schema. This sync adds the
column if missing and populates it from the JSON file, so existing databases
can gain the field without a full pipeline re-run.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Optional

import duckdb
from loguru import logger


def sync_occupation(
    db_path: str,
    characters_json: str = "data/characters_detail.json",
    *,
    dry_run: bool = False,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> dict:
    """Add + populate the ``occupation`` column on the character table.

    Args:
        db_path: Path to the DuckDB database file.
        characters_json: Path to ``characters_detail.json``.
        dry_run: If True, compute stats but do not write.
        progress_callback: Optional ``(current, total, msg)`` callback.

    Returns:
        Stats dict with ``total`` (characters in DB), ``with_occupation``,
        ``without_occupation``, ``missing_in_json`` counts.
    """
    detail_path = Path(characters_json)
    if not detail_path.exists():
        raise FileNotFoundError(f"{detail_path} not found")

    with open(detail_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    occupations: dict[str, Optional[str]] = {}
    for entry in data:
        cid = entry.get("id")
        if not cid:
            continue
        occ = entry.get("occupation")
        occupations[cid] = occ.strip() if isinstance(occ, str) and occ.strip() else None

    conn = duckdb.connect(str(db_path))
    try:
        # Ensure column exists. Safe on older DBs that pre-date this field.
        existing_cols = {
            r[1] for r in conn.execute("PRAGMA table_info(character)").fetchall()
        }
        if "occupation" not in existing_cols:
            if dry_run:
                logger.info("Would ALTER TABLE character ADD COLUMN occupation TEXT")
            else:
                conn.execute("ALTER TABLE character ADD COLUMN occupation TEXT")
                logger.info("Added 'occupation' column to character table")

        ids = [r[0] for r in conn.execute("SELECT id FROM character").fetchall()]
        total = len(ids)

        with_occ = 0
        without_occ = 0
        missing_in_json = 0
        updates: list[tuple[Optional[str], str]] = []

        for i, cid in enumerate(ids):
            if cid not in occupations:
                missing_in_json += 1
                updates.append((None, cid))
            else:
                occ = occupations[cid]
                if occ:
                    with_occ += 1
                else:
                    without_occ += 1
                updates.append((occ, cid))

            if progress_callback and (i + 1) % 100 == 0:
                progress_callback(i + 1, total, cid)

        if not dry_run:
            conn.executemany(
                "UPDATE character SET occupation = ? WHERE id = ?",
                updates,
            )
            logger.success(
                f"occupation synced: {with_occ} populated, "
                f"{without_occ} blank, {missing_in_json} missing in JSON"
            )

        return {
            "total": total,
            "with_occupation": with_occ,
            "without_occupation": without_occ,
            "missing_in_json": missing_in_json,
        }
    finally:
        conn.close()
