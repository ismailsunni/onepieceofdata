"""Post-processor to add origin_region column to the character table.

Classifies each character's ``origin`` field into a broad geographic region:
  - East Blue, North Blue, West Blue, South Blue  (the Four Seas)
  - Grand Line   (Paradise – the first half of the Grand Line)
  - New World    (the second half of the Grand Line)
  - Sky Island   (Skypiea, Birka, and other sky islands)
  - Red Line     (Mary Geoise / Marijoa and the Red Line continent)
  - Calm Belt    (islands in the Calm Belt, e.g. Amazon Lily)
  - Underwater   (Ryugu Kingdom / Fish-Man Island)

Characters whose origin cannot be mapped to any of the above regions receive
NULL in ``origin_region``.
"""

from typing import Callable, Optional

import duckdb
from loguru import logger


# Locations that are firmly in the New World (second half of Grand Line).
# These appear as substrings inside "Grand Line (…)" origin strings.
_NEW_WORLD_LOCATIONS = {
    "new world",
    "wano country",
    "totto land",
    "whole cake island",
    "dressrosa",
    "punk hazard",
    "onigashima",
    "egghead",
    "mokomo dukedom",  # Zou
    "elbaph",
    "green bit",
    "waford",
    "sphinx",          # Sphinx Island (Whitebeard's homeland, New World)
}

# Locations / keywords that indicate a Sky Island origin.
_SKY_ISLAND_KEYWORDS = {"skypiea", "birka", "sky island", "sky islands"}

# Keywords that indicate underwater origin (Fish-Man Island / Ryugu Kingdom).
_UNDERWATER_KEYWORDS = {"ryugu kingdom", "fish-man island", "fishman island"}

# Keywords that indicate Red Line origin (Mary Geoise / Marijoa).
_RED_LINE_KEYWORDS = {"red line", "mary geoise", "marijoa"}


def classify_origin_region(origin: str) -> Optional[str]:
    """Map a raw ``origin`` string to a broad One Piece region name.

    Returns one of:
        ``"East Blue"``, ``"North Blue"``, ``"West Blue"``, ``"South Blue"``,
        ``"Grand Line"``, ``"New World"``, ``"Sky Island"``, ``"Red Line"``,
        ``"Calm Belt"``, ``"Underwater"``, ``"Unknown Region"`` (origin text
        exists but doesn't match any known region), or ``None`` (no origin
        information at all).

    Args:
        origin: The raw origin string from the character table.

    Returns:
        A region string, ``"Unknown Region"``, or ``None``.
    """
    if not origin or not origin.strip():
        return None

    o = origin.strip().lower()

    # Red Line (check before Grand Line to avoid false matches)
    if any(kw in o for kw in _RED_LINE_KEYWORDS):
        return "Red Line"

    # Underwater
    if any(kw in o for kw in _UNDERWATER_KEYWORDS):
        return "Underwater"

    # Sky Islands (also catches "Grand Line ( Skypiea )" etc.)
    if any(kw in o for kw in _SKY_ISLAND_KEYWORDS):
        return "Sky Island"

    # Four Seas
    if o.startswith("east blue"):
        return "East Blue"
    if o.startswith("north blue"):
        return "North Blue"
    if o.startswith("west blue"):
        return "West Blue"
    if o.startswith("south blue"):
        return "South Blue"

    # New World (explicit prefix or known New World sub-locations)
    if o.startswith("new world") or any(kw in o for kw in _NEW_WORLD_LOCATIONS):
        return "New World"

    # Calm Belt
    if o.startswith("calm belt"):
        return "Calm Belt"

    # Grand Line catch-all (includes standalone island names like "Jaya")
    if o.startswith("grand line") or o == "jaya":
        return "Grand Line"

    # Origin text exists but doesn't match any known region
    return "Unknown Region"


def sync_origin_region(
    db_path: str,
    *,
    dry_run: bool = False,
    progress_callback: Optional[Callable[[int, int, str, dict], None]] = None,
) -> dict:
    """Add and populate the ``origin_region`` column in the character table.

    The column is created (``TEXT``) if it does not already exist, then each
    character row is classified using :func:`classify_origin_region`.

    Args:
        db_path: Path to the DuckDB database file.
        dry_run: If ``True``, compute classifications but do not write them.
        progress_callback: Optional ``(current, total, character_id, stats)``
            callback where *stats* contains ``{"region": str | None}``.

    Returns:
        A dict with counts:
        ``total``, ``classified``, ``unknown_region``, ``no_origin``, ``by_region``.
    """
    conn = duckdb.connect(str(db_path))

    try:
        rows = conn.execute(
            "SELECT id, origin FROM character ORDER BY id"
        ).fetchall()

        total = len(rows)
        classified = 0
        unknown_region = 0  # has origin text but no matching region
        no_origin = 0       # origin field is empty/NULL
        by_region: dict[str, int] = {}
        updates: list[tuple[str, str]] = []  # (region, id)

        for i, (char_id, origin) in enumerate(rows):
            region = classify_origin_region(origin or "")

            if region == "Unknown Region":
                unknown_region += 1
                by_region["Unknown Region"] = by_region.get("Unknown Region", 0) + 1
            elif region is None:
                no_origin += 1
            else:
                classified += 1
                by_region[region] = by_region.get(region, 0) + 1

            updates.append((region, char_id))

            if progress_callback:
                progress_callback(i + 1, total, char_id, {"region": region})

        if not dry_run:
            conn.executemany(
                "UPDATE character SET origin_region = ? WHERE id = ?",
                updates,
            )
            logger.info(
                f"origin_region synced: {classified} classified, {unknown_region} unknown region, {no_origin} no origin"
            )

        return {
            "total": total,
            "classified": classified,
            "unknown_region": unknown_region,
            "no_origin": no_origin,
            "by_region": by_region,
        }

    finally:
        conn.close()
