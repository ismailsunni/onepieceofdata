"""Post-processor to populate short character bios from scraped data.

Reads ``intro_text`` from ``characters_detail.json`` (populated by the
character scraper) and stores the first 2 sentences as a ``bio`` column
on the ``character`` table.  No additional API calls are needed.

Attribution: text sourced from the One Piece Wiki (onepiece.fandom.com)
under CC BY-SA 3.0 <https://creativecommons.org/licenses/by-sa/3.0/>.
"""

from __future__ import annotations

import json
import re
from typing import Callable, Optional

import duckdb
from loguru import logger

# How many sentences to keep
BIO_SENTENCES = 2


def _extract_bio(intro_text: str) -> Optional[str]:
    """Return the first BIO_SENTENCES sentences from intro text.

    Strips reference markers and collapses whitespace before splitting
    on sentence boundaries.
    """
    if not intro_text or not intro_text.strip():
        return None

    # Reject text that looks like a wiki infobox stats dump (no real bio)
    # These pages have "Statistics Japanese Name:" instead of intro prose
    if re.search(r"Statistics\s+Japanese Name:|Romanized Name:", intro_text):
        return None

    # Strip wiki reference markers like [10] or [ 10 ]
    text = re.sub(r"\[\s*\d+\s*\]", "", intro_text)
    # Strip Japanese notation parentheticals: ( Japanese, romanization? )
    # e.g. "( ニャーバン・兄弟, Nyāban Burazāzu? )" or "( 剣豪, kengō? )"
    text = re.sub(r"\(\s*[\u3000-\u9fff\uff00-\uffef・][^)]*\)", "", text)
    # Strip any remaining CJK characters
    text = re.sub(r"[\u3000-\u9fff\uff00-\uffef・]+", "", text)
    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text).strip()
    # Remove spaces before punctuation: "Luffy ," → "Luffy,"
    text = re.sub(r"\s+([,\.;:!?])", r"\1", text)
    # Remove space before possessive/contractions (straight and curly apostrophes):
    # "Family 's" → "Family's",  "Bekori \u2019s" → "Bekori\u2019s"
    text = re.sub(r"\s+(['\u2019])(\w)", r"\1\2", text)
    # Remove space before hyphen in compound words: "human -traits" → "human-traits"
    text = re.sub(r"\s+-(\w)", r"-\1", text)

    if not text:
        return None

    # Split on sentence boundaries, but protect "D." initials (One Piece names)
    # and common honorific abbreviations from being treated as sentence ends.
    # Replace them with a placeholder, split, then restore.
    protected = re.sub(r"\b([A-Z])\.\s+([A-Z])", r"\1<DOT> \2", text)
    sentences = re.split(r"(?<=[.!?])\s+", protected)
    bio = " ".join(sentences[:BIO_SENTENCES]).strip()
    bio = bio.replace("<DOT>", ".")

    return bio or None


def sync_character_bios(
    db_path: str,
    *,
    characters_json: str = "data/characters_detail.json",
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Populate the bio column on the character table from scraped intro text.

    Reads ``intro_text`` from characters_detail.json (written by the
    character scraper) and stores a short bio for each character.

    Args:
        db_path: Path to the DuckDB database file.
        characters_json: Path to characters_detail.json.
        dry_run: If True, compute but do not write to the database.
        progress_callback: Optional ``(current, total, msg)`` callback.

    Returns:
        Stats dict with total, updated, skipped counts.
    """
    with open(characters_json) as f:
        characters = json.load(f)

    conn = duckdb.connect(str(db_path))
    try:
        valid_ids = {r[0] for r in conn.execute("SELECT id FROM character").fetchall()}

        stats = {"total": 0, "updated": 0, "skipped": 0}
        updates: list[tuple] = []

        for char in characters:
            cid = char.get("id", "")
            if cid not in valid_ids:
                continue

            stats["total"] += 1
            intro_text = char.get("intro_text", "") or ""
            bio = _extract_bio(intro_text)

            if bio:
                updates.append((bio, cid))
                stats["updated"] += 1
            else:
                updates.append((None, cid))  # clear any stale bio
                stats["skipped"] += 1

        logger.info(
            f"Bios parsed: {stats['updated']} with bio, "
            f"{stats['skipped']} skipped (no intro text scraped yet)"
        )

        if progress_callback:
            progress_callback(1, 2, "parsed")

        if not dry_run:
            if updates:
                conn.executemany(
                    "UPDATE character SET bio = ? WHERE id = ?",
                    updates,
                )
            logger.success(f"Saved bios for {stats['updated']} characters")

        if progress_callback:
            progress_callback(2, 2, "done")

        return stats

    finally:
        conn.close()
