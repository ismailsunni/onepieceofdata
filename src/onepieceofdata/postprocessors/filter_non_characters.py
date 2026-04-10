"""Post-processor to filter out non-character entries from the character table.

Removes entries that:
1. Lack both `origin` and `status` fields (likely not real characters)
2. Have a non-living status like "Active" or "Destroyed" (ships, buildings,
   organizations — e.g., Baratie, Going Merry, World Government)

A whitelist is maintained for known characters that happen to have neither field.
"""

from typing import Optional, Callable

import duckdb
from loguru import logger

# Characters that have neither origin nor status but ARE real characters.
# Verified against master branch data and the One Piece wiki.
# Add entries here as needed.
CHARACTER_WHITELIST = {
    "Brownbeard",       # Chadros Higelyges - real character
    "Charlos",          # Saint Charlos - World Noble
    "Gallant_Hippo",    # Hippo Gentleman - character from Thriller Bark
    "Goa_King",         # King of Goa Kingdom
    "Lami",             # Trafalgar Lami - Law's sister
    "Octopus_Mash",     # Mash - character
    "Pandaman",         # Pandaman - recurring easter egg character
    "Rosward",          # Saint Rosward - World Noble
    "Shalria",          # Saint Shalria - World Noble
    "Sig",              # Sig - giant warrior
    "Sphinx",           # Sphinx - Whitebeard's homeland guardian
    "Ukkari",           # Ukkari - character from Hot-Spring Island
}

# Statuses that indicate non-living entities (ships, buildings, organizations).
# Real characters use "Alive", "Deceased", or "Unknown".
NON_CHARACTER_STATUSES = {"Active", "Destroyed"}


def filter_non_characters(
    db_path: str,
    *,
    dry_run: bool = False,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> dict:
    """Remove non-character rows from the character table.

    Criteria for removal (either triggers removal):
        1. ``status IS NULL`` **and** ``origin IS NULL`` (and not whitelisted)
        2. ``status`` is a non-living value like "Active" or "Destroyed"
           (ships, buildings, organizations)

    Args:
        db_path: Path to the DuckDB database file.
        dry_run: If True, report what would be removed without modifying data.
        progress_callback: Optional ``(current, total, id)`` callback.

    Returns:
        A dict with ``removed``, ``kept_whitelist``, and ``total_checked`` counts,
        plus a ``removed_entries`` list of ``(id, name)`` tuples.
    """

    conn = duckdb.connect(str(db_path))

    try:
        # Find candidates: no status AND no origin
        null_candidates = conn.execute(
            """
            SELECT id, name
            FROM character
            WHERE (status IS NULL OR TRIM(status) = '')
              AND (origin IS NULL OR TRIM(origin) = '')
            ORDER BY id
            """
        ).fetchall()

        # Find non-living entities by status (ships, buildings, organizations)
        status_placeholders = ", ".join(f"'{s}'" for s in NON_CHARACTER_STATUSES)
        non_living_candidates = conn.execute(
            f"""
            SELECT id, name
            FROM character
            WHERE status IN ({status_placeholders})
            ORDER BY id
            """
        ).fetchall()

        to_remove = []
        kept_whitelist = []

        for i, (cid, name) in enumerate(null_candidates):
            if cid in CHARACTER_WHITELIST:
                kept_whitelist.append((cid, name))
            else:
                to_remove.append((cid, name))

            if progress_callback:
                progress_callback(i + 1, len(null_candidates), cid)

        # Non-living entities are always removed (no whitelist check)
        for cid, name in non_living_candidates:
            to_remove.append((cid, name))

        candidates = null_candidates + non_living_candidates

        if not dry_run and to_remove:
            remove_ids = [r[0] for r in to_remove]
            # Delete from coc first (foreign-key-like cleanup)
            conn.execute(
                "DELETE FROM coc WHERE character = ANY(?)", [remove_ids]
            )
            # Delete from cov
            conn.execute(
                "DELETE FROM cov WHERE character = ANY(?)", [remove_ids]
            )
            # Delete from character
            conn.execute(
                "DELETE FROM character WHERE id = ANY(?)", [remove_ids]
            )

        return {
            "total_checked": len(candidates),
            "removed": len(to_remove),
            "kept_whitelist": len(kept_whitelist),
            "removed_entries": to_remove,
            "whitelisted_entries": kept_whitelist,
            "non_living_removed": len(non_living_candidates),
        }
    finally:
        conn.close()
