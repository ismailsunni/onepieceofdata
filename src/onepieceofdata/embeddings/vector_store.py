"""DuckDB vector storage and similarity search for wiki chunks."""

import duckdb


def create_chunks_table(db_path: str):
    """Create wiki_chunks table in DuckDB if it doesn't already exist."""
    conn = duckdb.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS wiki_chunks (
                chunk_id    TEXT PRIMARY KEY,
                page_id     TEXT NOT NULL,
                page_type   TEXT NOT NULL,
                title       TEXT NOT NULL,
                section_name TEXT NOT NULL,
                chunk_text  TEXT NOT NULL,
                embedding   FLOAT[384]
            )
        """)
    finally:
        conn.close()


def save_chunks(chunks: list, db_path: str):
    """Save chunks with embeddings to DuckDB wiki_chunks table.

    Uses INSERT OR REPLACE for idempotent re-runs.
    Chunks without an 'embedding' key are skipped.
    """
    if not chunks:
        return

    create_chunks_table(db_path)

    conn = duckdb.connect(db_path)
    try:
        inserted = 0
        for chunk in chunks:
            if "embedding" not in chunk:
                continue
            conn.execute(
                """
                INSERT OR REPLACE INTO wiki_chunks
                    (chunk_id, page_id, page_type, title, section_name, chunk_text, embedding)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    chunk["chunk_id"],
                    chunk["page_id"],
                    chunk["page_type"],
                    chunk["title"],
                    chunk["section_name"],
                    chunk["text"],
                    chunk["embedding"],
                ],
            )
            inserted += 1
    finally:
        conn.close()

    return inserted


def search_similar(
    query_embedding: list,
    db_path: str,
    limit: int = 5,
    page_type: str = None,
) -> list:
    """Find similar chunks using cosine similarity.

    Uses DuckDB's list_cosine_similarity (no extension needed).

    Returns list of {chunk_id, page_id, title, section_name, chunk_text, similarity}.
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        if page_type:
            rows = conn.execute(
                """
                SELECT chunk_id, page_id, title, section_name, chunk_text,
                       list_cosine_similarity(embedding, ?) AS similarity
                FROM wiki_chunks
                WHERE page_type = ?
                ORDER BY similarity DESC
                LIMIT ?
                """,
                [query_embedding, page_type, limit],
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT chunk_id, page_id, title, section_name, chunk_text,
                       list_cosine_similarity(embedding, ?) AS similarity
                FROM wiki_chunks
                ORDER BY similarity DESC
                LIMIT ?
                """,
                [query_embedding, limit],
            ).fetchall()
    finally:
        conn.close()

    return [
        {
            "chunk_id": row[0],
            "page_id": row[1],
            "title": row[2],
            "section_name": row[3],
            "chunk_text": row[4],
            "similarity": row[5],
        }
        for row in rows
    ]


def get_stats(db_path: str) -> dict:
    """Return chunk stats: total count and breakdown by page_type."""
    conn = duckdb.connect(db_path, read_only=True)
    try:
        try:
            total = conn.execute("SELECT count(*) FROM wiki_chunks").fetchone()[0]
            by_type = conn.execute(
                "SELECT page_type, count(*) FROM wiki_chunks GROUP BY page_type ORDER BY page_type"
            ).fetchall()
        except Exception:
            return {"total": 0, "by_type": {}}
    finally:
        conn.close()

    return {"total": total, "by_type": {row[0]: row[1] for row in by_type}}
