"""DuckDB schema for the story graph tables.

Idempotent: safe to call repeatedly. Existing data is preserved.
Destructive reset is only available via `drop_graph_tables`.
"""

import duckdb
from loguru import logger


GRAPH_TABLES = (
    "graph_edges",
    "graph_extractions",
    "graph_source_text",
    "graph_nodes",
)


def create_graph_tables(db_path: str) -> None:
    """Create all story-graph tables if they don't already exist."""
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS graph_nodes_seq START 1")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS graph_source_text_seq START 1")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS graph_extractions_seq START 1")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS graph_edges_seq START 1")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS graph_nodes (
                id              INTEGER PRIMARY KEY DEFAULT nextval('graph_nodes_seq'),
                type            VARCHAR NOT NULL,
                canonical_name  VARCHAR NOT NULL,
                aliases         VARCHAR[],
                source_table    VARCHAR,
                source_id       VARCHAR,
                UNIQUE (type, canonical_name)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS graph_source_text (
                id               INTEGER PRIMARY KEY DEFAULT nextval('graph_source_text_seq'),
                source_table     VARCHAR NOT NULL,
                source_id        VARCHAR NOT NULL,
                section_name     VARCHAR,
                text             TEXT NOT NULL,
                normalized_hash  VARCHAR NOT NULL,
                text_length      INTEGER,
                entities_found   INTEGER[],
                first_seen_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                superseded_at    TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS graph_extractions (
                id               INTEGER PRIMARY KEY DEFAULT nextval('graph_extractions_seq'),
                source_text_id   INTEGER NOT NULL,
                model            VARCHAR,
                prompt_version   INTEGER NOT NULL,
                extracted_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_triples      JSON,
                input_tokens     INTEGER,
                output_tokens    INTEGER,
                FOREIGN KEY (source_text_id) REFERENCES graph_source_text(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS graph_edges (
                id                    INTEGER PRIMARY KEY DEFAULT nextval('graph_edges_seq'),
                subject_id            INTEGER NOT NULL,
                relation              VARCHAR NOT NULL,
                object_id             INTEGER NOT NULL,
                evidence_chapter      INTEGER,
                evidence_text         TEXT,
                confidence            FLOAT,
                source_extraction_id  INTEGER,
                FOREIGN KEY (subject_id) REFERENCES graph_nodes(id),
                FOREIGN KEY (object_id) REFERENCES graph_nodes(id),
                FOREIGN KEY (source_extraction_id) REFERENCES graph_extractions(id)
            )
        """)

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_graph_source_lookup "
            "ON graph_source_text(source_table, source_id, section_name)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_graph_extractions_source "
            "ON graph_extractions(source_text_id, prompt_version)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_graph_edges_subject "
            "ON graph_edges(subject_id, relation)"
        )

        logger.success("Graph tables ready")
    finally:
        conn.close()


def drop_graph_tables(db_path: str) -> None:
    """Drop all story-graph tables. Destructive — extractions and edges lost."""
    conn = duckdb.connect(db_path)
    try:
        for table in GRAPH_TABLES:
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        for seq in (
            "graph_nodes_seq",
            "graph_source_text_seq",
            "graph_extractions_seq",
            "graph_edges_seq",
        ):
            conn.execute(f"DROP SEQUENCE IF EXISTS {seq}")
        logger.warning("Dropped all graph tables")
    finally:
        conn.close()


def graph_table_counts(db_path: str) -> dict[str, int]:
    """Return row counts for each graph table. Missing tables report 0."""
    conn = duckdb.connect(db_path)
    try:
        counts: dict[str, int] = {}
        for table in GRAPH_TABLES:
            try:
                (count,) = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
            except duckdb.CatalogException:
                count = 0
            counts[table] = count
        return counts
    finally:
        conn.close()
