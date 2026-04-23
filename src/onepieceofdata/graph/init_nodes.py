"""Populate graph_nodes from existing tables (characters, affiliations, fruits).

Idempotent: uses ON CONFLICT DO NOTHING against the (type, canonical_name)
unique constraint, so re-running only inserts new rows.
"""

import duckdb
from loguru import logger

from .schema import create_graph_tables


CREW_KEYWORDS = ("pirates", "crew", "fleet")


def init_nodes(db_path: str) -> dict[str, int]:
    """Populate graph_nodes from character, character_affiliation, character_devil_fruit.

    Returns a dict of per-type insert counts (new rows added this run).
    """
    create_graph_tables(db_path)
    conn = duckdb.connect(db_path)
    try:
        before = _counts_by_type(conn)

        _insert_characters(conn)
        _insert_groups(conn)
        _insert_devil_fruits(conn)

        after = _counts_by_type(conn)

        diffs = {t: after.get(t, 0) - before.get(t, 0) for t in after}
        totals = after

        logger.success(
            f"Nodes: {totals.get('character', 0)} characters, "
            f"{totals.get('crew', 0)} crews, "
            f"{totals.get('organization', 0)} organizations, "
            f"{totals.get('devil_fruit', 0)} devil fruits"
        )
        return {"added": diffs, "totals": totals}
    finally:
        conn.close()


def _counts_by_type(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT type, count(*) FROM graph_nodes GROUP BY 1"
    ).fetchall()
    return {t: n for t, n in rows}


def _insert_characters(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        INSERT INTO graph_nodes (type, canonical_name, aliases, source_table, source_id)
        SELECT 'character', name, [name], 'character', id
        FROM character
        WHERE is_likely_character = TRUE
          AND name IS NOT NULL
          AND trim(name) <> ''
        ON CONFLICT (type, canonical_name) DO NOTHING
        """
    )


def _insert_groups(conn: duckdb.DuckDBPyConnection) -> None:
    keyword_expr = " OR ".join(
        f"lower(group_name) LIKE '%{kw}%'" for kw in CREW_KEYWORDS
    )
    conn.execute(
        f"""
        INSERT INTO graph_nodes (type, canonical_name, aliases, source_table, source_id)
        SELECT
            CASE WHEN {keyword_expr} THEN 'crew' ELSE 'organization' END AS type,
            group_name,
            [group_name],
            'character_affiliation',
            NULL
        FROM (
            SELECT DISTINCT group_name
            FROM character_affiliation
            WHERE group_name IS NOT NULL AND trim(group_name) <> ''
        )
        ON CONFLICT (type, canonical_name) DO NOTHING
        """
    )


def _insert_devil_fruits(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        INSERT INTO graph_nodes (type, canonical_name, aliases, source_table, source_id)
        SELECT 'devil_fruit', fruit_name,
               CASE
                   WHEN english_name IS NOT NULL AND english_name <> fruit_name
                       THEN [fruit_name, english_name]
                   ELSE [fruit_name]
               END,
               'character_devil_fruit',
               NULL
        FROM (
            SELECT fruit_name, any_value(english_name) AS english_name
            FROM character_devil_fruit
            WHERE fruit_name IS NOT NULL AND trim(fruit_name) <> ''
            GROUP BY fruit_name
        )
        ON CONFLICT (type, canonical_name) DO NOTHING
        """
    )
