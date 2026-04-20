"""Post-processor to sync short character bios from the wiki.

Fetches the intro section of each character's wiki page via the MediaWiki
API, extracts the first 1–2 sentences, and stores it in a ``bio`` column
on the ``character`` table.

Attribution: text sourced from the One Piece Wiki (onepiece.fandom.com)
under CC BY-SA 3.0 <https://creativecommons.org/licenses/by-sa/3.0/>.
"""

from __future__ import annotations

import re
import time
from typing import Callable, Dict, List, Optional

import duckdb
from loguru import logger

from ..api.fandom_client import FandomAPIClient
from ..parsers.wikitext_cleaner import parse_wiki_page

# How many sentences to keep
BIO_SENTENCES = 2

# Delay between API requests (seconds)
REQUEST_DELAY = 0.5


def _extract_bio(intro_text: str) -> Optional[str]:
    """Return the first BIO_SENTENCES sentences from intro text.

    Strips disambiguation notes (lines starting with ':') and collapses
    whitespace before splitting on sentence boundaries.
    """
    if not intro_text or not intro_text.strip():
        return None

    # Drop lines that are wiki disambiguation notes (start with ':')
    lines = [l for l in intro_text.split("\n") if not l.startswith(":")]
    text = " ".join(lines).strip()

    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return None

    # Split on sentence-ending punctuation followed by whitespace
    sentences = re.split(r"(?<=[.!?])\s+", text)
    bio = " ".join(sentences[:BIO_SENTENCES]).strip()

    return bio or None


def sync_character_bios(
    db_path: str,
    *,
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Fetch and store short bios for all characters.

    Adds a ``bio`` TEXT column to the ``character`` table if it doesn't
    exist, then populates it from the wiki intro text.

    Args:
        db_path: Path to the DuckDB database file.
        dry_run: If True, fetch bios but do not write to the database.
        progress_callback: Optional ``(current, total, msg)`` callback.

    Returns:
        Stats dict with total, updated, skipped, failed counts.
    """
    conn = duckdb.connect(str(db_path))
    try:
        # Ensure bio column exists
        try:
            conn.execute("SELECT bio FROM character LIMIT 1")
        except (duckdb.CatalogException, duckdb.BinderException):
            conn.execute("ALTER TABLE character ADD COLUMN bio TEXT")
            logger.info("Added column character.bio")

        # Load all character IDs and their wiki URLs
        rows = conn.execute(
            "SELECT id, name FROM character ORDER BY id"
        ).fetchall()
        total = len(rows)
        logger.info(f"Fetching bios for {total} characters...")

        stats = {
            "total": total,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
        }

        client = FandomAPIClient(wiki="onepiece")
        updates: List[tuple] = []

        try:
            for i, (char_id, char_name) in enumerate(rows, 1):
                if progress_callback:
                    progress_callback(i, total, f"fetching {char_name}")

                try:
                    # Use character ID as page title (underscores = spaces)
                    page_title = char_id.replace("_", " ")
                    wikitext = client.get_page_wikitext(page_title)

                    if not wikitext:
                        logger.debug(f"No wikitext for {char_id}")
                        stats["skipped"] += 1
                        updates.append((None, char_id))
                        continue

                    intro_text, _, _ = parse_wiki_page(wikitext)
                    bio = _extract_bio(intro_text)

                    if bio:
                        updates.append((bio, char_id))
                        stats["updated"] += 1
                        logger.debug(f"Bio fetched for {char_id}: {bio[:80]}...")
                    else:
                        updates.append((None, char_id))
                        stats["skipped"] += 1
                        logger.debug(f"No bio extracted for {char_id}")

                except Exception as e:
                    logger.warning(f"Failed to fetch bio for {char_id}: {e}")
                    updates.append((None, char_id))
                    stats["failed"] += 1

                if i % 50 == 0:
                    logger.info(
                        f"Progress: {i}/{total} — "
                        f"{stats['updated']} updated, "
                        f"{stats['skipped']} skipped, "
                        f"{stats['failed']} failed"
                    )

                time.sleep(REQUEST_DELAY)

        finally:
            client.cleanup()

        logger.info(
            f"Bios fetched: {stats['updated']} updated, "
            f"{stats['skipped']} skipped, {stats['failed']} failed"
        )

        if progress_callback:
            progress_callback(total, total, "writing")

        if not dry_run:
            conn.executemany(
                "UPDATE character SET bio = ? WHERE id = ?",
                updates,
            )
            logger.success(f"Saved bios for {stats['updated']} characters")

        if progress_callback:
            progress_callback(total, total, "done")

        return stats

    finally:
        conn.close()
