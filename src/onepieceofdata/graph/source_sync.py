"""Sync wiki_text sections into graph_source_text with change detection.

For each (page_id, section) we keep an immutable snapshot of the text that
was fed to the extractor. Re-runs either:
  - do nothing (hash match or high similarity),
  - touch last_seen_at (unchanged in substance), or
  - supersede the old snapshot and insert a new one (meaningful change).

This decouples the LLM extractor from live wiki text: downstream stages
re-extract only when source snapshots actually change.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher

import duckdb
from loguru import logger

from .entity_linker import EntityLinker
from .schema import create_graph_tables


WIKI_SOURCE = "wiki_text"
MIN_SECTION_LEN = 50          # skip stubs / navigation fragments
SIMILARITY_THRESHOLD = 0.95   # above this we treat change as cosmetic


_WHITESPACE_RE = re.compile(r"\s+")
_REF_RE = re.compile(r"\[\d+\]")


def normalize_text(text: str) -> str:
    """Lowercase, strip reference markers, collapse whitespace."""
    if not text:
        return ""
    t = _REF_RE.sub("", text)
    t = _WHITESPACE_RE.sub(" ", t).strip().lower()
    return t


def normalized_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()


def text_similarity(a: str, b: str) -> float:
    """Cheap similarity ratio. Uses difflib; quick_ratio as pre-filter."""
    if not a or not b:
        return 0.0
    # Length guard: very different sizes are not "cosmetic" changes.
    la, lb = len(a), len(b)
    if min(la, lb) / max(la, lb) < 0.8:
        return 0.0
    sm = SequenceMatcher(None, a, b, autojunk=False)
    if sm.quick_ratio() < SIMILARITY_THRESHOLD:
        return sm.quick_ratio()
    return sm.ratio()


@dataclass
class SyncStats:
    scanned: int = 0
    inserted: int = 0
    unchanged: int = 0
    superseded: int = 0
    skipped_short: int = 0


def _iter_sections(raw_sections) -> list[tuple[str, str]]:
    """Yield (section_name, text) from the wiki_text.sections JSON column.

    Accepts either the raw JSON string or a parsed object. Recognizes the
    scraper's shape: {"intro": "...", "sections": [{"title": ..., "text": ...}, ...]}
    and also tolerates a plain {name: text} dict.
    """
    if raw_sections is None:
        return []
    if isinstance(raw_sections, (bytes, bytearray)):
        raw_sections = raw_sections.decode("utf-8")
    if isinstance(raw_sections, str):
        try:
            data = json.loads(raw_sections)
        except json.JSONDecodeError:
            return []
    else:
        data = raw_sections

    out: list[tuple[str, str]] = []
    if isinstance(data, dict):
        if "sections" in data and isinstance(data["sections"], list):
            intro = data.get("intro")
            if isinstance(intro, str) and intro.strip():
                out.append(("intro", intro))
            for section in data["sections"]:
                if not isinstance(section, dict):
                    continue
                title = section.get("title") or section.get("name") or ""
                text = section.get("text") or section.get("content") or ""
                if isinstance(title, str) and isinstance(text, str) and text.strip():
                    out.append((title or "(untitled)", text))
        else:
            for k, v in data.items():
                if isinstance(v, str) and v.strip():
                    out.append((str(k), v))
    return out


def sync_sources(
    db_path: str,
    similarity_threshold: float = SIMILARITY_THRESHOLD,
    limit: int | None = None,
) -> SyncStats:
    """Snapshot wiki_text.sections into graph_source_text with change detection."""
    create_graph_tables(db_path)

    linker = EntityLinker.from_db(db_path)
    logger.info(f"Entity linker loaded with {len(linker):,} aliases")

    conn = duckdb.connect(db_path)
    stats = SyncStats()
    try:
        rows = conn.execute(
            "SELECT page_id, sections FROM wiki_text WHERE sections IS NOT NULL"
            + (f" LIMIT {int(limit)}" if limit else "")
        ).fetchall()
        logger.info(f"Scanning {len(rows):,} wiki pages")

        for page_id, raw_sections in rows:
            for section_name, text in _iter_sections(raw_sections):
                stats.scanned += 1
                if len(text) < MIN_SECTION_LEN:
                    stats.skipped_short += 1
                    continue

                prev = conn.execute(
                    """
                    SELECT id, text, normalized_hash
                    FROM graph_source_text
                    WHERE source_table = ?
                      AND source_id = ?
                      AND section_name = ?
                      AND superseded_at IS NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    [WIKI_SOURCE, page_id, section_name],
                ).fetchone()

                new_hash = normalized_hash(text)
                if prev is None:
                    _insert_source(conn, page_id, section_name, text, new_hash, linker)
                    stats.inserted += 1
                    continue

                prev_id, prev_text, prev_hash = prev
                if prev_hash == new_hash or (
                    text_similarity(prev_text, text) >= similarity_threshold
                ):
                    conn.execute(
                        "UPDATE graph_source_text SET last_seen_at = CURRENT_TIMESTAMP WHERE id = ?",
                        [prev_id],
                    )
                    stats.unchanged += 1
                else:
                    conn.execute(
                        "UPDATE graph_source_text SET superseded_at = CURRENT_TIMESTAMP WHERE id = ?",
                        [prev_id],
                    )
                    _insert_source(conn, page_id, section_name, text, new_hash, linker)
                    stats.superseded += 1
                    stats.inserted += 1

        logger.success(
            f"Sync done — scanned={stats.scanned:,} inserted={stats.inserted:,} "
            f"unchanged={stats.unchanged:,} superseded={stats.superseded:,} "
            f"short={stats.skipped_short:,}"
        )
        return stats
    finally:
        conn.close()


def _insert_source(
    conn: duckdb.DuckDBPyConnection,
    page_id: str,
    section_name: str,
    text: str,
    hash_: str,
    linker: EntityLinker,
) -> None:
    entities = linker.match(text)
    conn.execute(
        """
        INSERT INTO graph_source_text
            (source_table, source_id, section_name, text, normalized_hash,
             text_length, entities_found)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [WIKI_SOURCE, page_id, section_name, text, hash_, len(text), entities],
    )
