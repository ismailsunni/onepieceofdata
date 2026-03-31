"""Split wiki pages into overlapping text chunks for embedding."""

import json
import duckdb

# Token estimation: 1 token ≈ 4 characters
_CHARS_PER_TOKEN = 4
_MAX_TOKENS = 800
_OVERLAP_TOKENS = 100
_MIN_SECTION_CHARS = 50

_MAX_CHARS = _MAX_TOKENS * _CHARS_PER_TOKEN    # 3200 chars
_OVERLAP_CHARS = _OVERLAP_TOKENS * _CHARS_PER_TOKEN  # 400 chars


def _split_text(text: str, max_chars: int, overlap_chars: int) -> list:
    """Split text into overlapping windows.

    Each window is max_chars long and advances by (max_chars - overlap_chars)
    so consecutive chunks share overlap_chars characters.
    """
    if len(text) <= max_chars:
        return [text]

    parts = []
    step = max_chars - overlap_chars
    start = 0
    while start < len(text):
        end = start + max_chars
        parts.append(text[start:end])
        if end >= len(text):
            break
        start += step
    return parts


def chunk_wiki_page(page_id: str, page_type: str, title: str, sections: dict) -> list:
    """Split a wiki page into searchable chunks.

    Each section becomes a chunk. Long sections (>800 tokens) get split
    with 100-token overlap. Short sections (<50 chars) get merged with the next.

    Each chunk: {chunk_id, page_id, page_type, title, section_name, text}
    chunk_id format: "{page_id}::{section_name}" (or "::{section_name}::part_N" for splits)
    """
    # Handle JSON string input (as stored in DuckDB)
    if isinstance(sections, str):
        try:
            sections = json.loads(sections)
        except (json.JSONDecodeError, TypeError):
            sections = {}

    if not sections or not isinstance(sections, dict):
        return []

    section_items = [(name, text) for name, text in sections.items() if text]
    if not section_items:
        return []

    # Merge short sections with the next one
    merged = []
    buffer_name = None
    buffer_text = ""

    for name, text in section_items:
        if buffer_name is None:
            buffer_name = name
            buffer_text = text
        elif len(buffer_text) < _MIN_SECTION_CHARS:
            # Current buffer is short — merge incoming section into it
            buffer_text = (buffer_text + "\n" + text).strip() if text else buffer_text
        else:
            merged.append((buffer_name, buffer_text))
            buffer_name = name
            buffer_text = text

    if buffer_name is not None:
        merged.append((buffer_name, buffer_text))

    # Split long sections and build final chunks
    chunks = []
    for section_name, text in merged:
        if len(text) <= _MAX_CHARS:
            chunks.append({
                "chunk_id": f"{page_id}::{section_name}",
                "page_id": page_id,
                "page_type": page_type,
                "title": title,
                "section_name": section_name,
                "text": text,
            })
        else:
            parts = _split_text(text, _MAX_CHARS, _OVERLAP_CHARS)
            for i, part in enumerate(parts):
                chunks.append({
                    "chunk_id": f"{page_id}::{section_name}::part_{i}",
                    "page_id": page_id,
                    "page_type": page_type,
                    "title": title,
                    "section_name": section_name,
                    "text": part,
                })

    return chunks


def chunk_all_pages(db_path: str) -> list:
    """Read all wiki_text rows from DuckDB and chunk them.

    Returns list of chunk dicts ready for embedding.
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute(
            "SELECT page_id, page_type, title, sections FROM wiki_text"
        ).fetchall()
    finally:
        conn.close()

    all_chunks = []
    for page_id, page_type, title, sections in rows:
        chunks = chunk_wiki_page(page_id, page_type, title, sections)
        all_chunks.extend(chunks)

    return all_chunks
