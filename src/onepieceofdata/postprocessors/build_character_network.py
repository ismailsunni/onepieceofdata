"""Post-processor to build a character co-appearance network from chapter data.

Creates two tables in DuckDB:
  - character_network_node: one row per character with appearance stats and community
  - character_network_edge: one row per character pair with co-appearance weight

Characters are connected when they appear in the same chapter. The edge weight
is the number of shared chapters. Community detection uses the Louvain algorithm
via networkx.
"""

from typing import Optional, Callable

import duckdb
import networkx as nx
from loguru import logger


# Only include characters with at least this many chapter appearances
MIN_APPEARANCES = 5
# Only include edges with at least this many co-appearances
MIN_EDGE_WEIGHT = 3


def build_character_network(
    db_path: str,
    *,
    min_appearances: int = MIN_APPEARANCES,
    min_edge_weight: int = MIN_EDGE_WEIGHT,
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Build character co-appearance network and save to DuckDB.

    Args:
        db_path: Path to the DuckDB database file.
        min_appearances: Minimum chapter appearances to include a character.
        min_edge_weight: Minimum co-appearances to include an edge.
        dry_run: If True, compute but don't write to database.
        progress_callback: Optional callback for progress updates.

    Returns:
        A dict with node_count, edge_count, and community_count.
    """
    conn = duckdb.connect(str(db_path))

    try:
        # Step 1: Get characters that meet the minimum appearance threshold
        logger.info(f"Loading characters with >= {min_appearances} appearances...")
        characters = conn.execute(
            """
            SELECT c.id, c.name, c.appearance_count, c.first_appearance,
                   c.last_appearance, c.status, c.bounty
            FROM character c
            WHERE c.appearance_count IS NOT NULL
              AND c.appearance_count >= ?
            ORDER BY c.appearance_count DESC
            """,
            [min_appearances],
        ).fetchall()

        char_ids = {r[0] for r in characters}
        char_info = {r[0]: r for r in characters}
        logger.info(f"Found {len(characters)} characters with >= {min_appearances} appearances")

        # Step 2: Build co-appearance edges from CoC table
        logger.info("Computing co-appearance edges from chapter data...")
        edges_raw = conn.execute(
            """
            SELECT a.character AS source, b.character AS target, COUNT(*) AS weight
            FROM coc a
            JOIN coc b ON a.chapter = b.chapter AND a.character < b.character
            GROUP BY a.character, b.character
            HAVING COUNT(*) >= ?
            """,
            [min_edge_weight],
        ).fetchall()

        # Filter to only include edges between characters that meet the threshold
        edges = [
            (src, tgt, w)
            for src, tgt, w in edges_raw
            if src in char_ids and tgt in char_ids
        ]
        logger.info(f"Found {len(edges)} edges with weight >= {min_edge_weight}")

        # Step 3: Build networkx graph for community detection
        logger.info("Running community detection (Louvain)...")
        G = nx.Graph()
        for cid in char_ids:
            G.add_node(cid)
        for src, tgt, w in edges:
            G.add_edge(src, tgt, weight=w)

        communities = nx.community.louvain_communities(G, weight="weight", seed=42)
        community_map = {}
        for i, community in enumerate(communities):
            for cid in community:
                community_map[cid] = i
        logger.info(f"Detected {len(communities)} communities")

        # Step 4: Compute degree and weighted degree per node
        degree_map = dict(G.degree())
        weighted_degree_map = dict(G.degree(weight="weight"))

        if progress_callback:
            progress_callback(1, 2, "network computed")

        # Step 5: Write to database
        if not dry_run:
            # Drop and recreate tables
            conn.execute("DROP TABLE IF EXISTS character_network_edge CASCADE")
            conn.execute("DROP TABLE IF EXISTS character_network_node CASCADE")

            conn.execute("""
                CREATE TABLE character_network_node (
                    character_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    appearance_count INTEGER,
                    first_appearance INTEGER,
                    last_appearance INTEGER,
                    status TEXT,
                    bounty BIGINT,
                    community INTEGER NOT NULL,
                    degree INTEGER NOT NULL,
                    weighted_degree INTEGER NOT NULL
                )
            """)

            conn.execute("""
                CREATE TABLE character_network_edge (
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    weight INTEGER NOT NULL,
                    PRIMARY KEY (source, target)
                )
            """)

            # Insert nodes
            node_rows = []
            for cid, name, app_count, first_app, last_app, status, bounty in characters:
                node_rows.append((
                    cid, name, app_count, first_app, last_app, status, bounty,
                    community_map.get(cid, -1),
                    degree_map.get(cid, 0),
                    weighted_degree_map.get(cid, 0),
                ))

            conn.executemany(
                """INSERT INTO character_network_node
                   (character_id, name, appearance_count, first_appearance,
                    last_appearance, status, bounty, community, degree, weighted_degree)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                node_rows,
            )

            # Insert edges
            conn.executemany(
                "INSERT INTO character_network_edge (source, target, weight) VALUES (?, ?, ?)",
                edges,
            )

            logger.success(
                f"Saved {len(node_rows)} nodes and {len(edges)} edges "
                f"({len(communities)} communities)"
            )

        if progress_callback:
            progress_callback(2, 2, "done")

        return {
            "node_count": len(characters),
            "edge_count": len(edges),
            "community_count": len(communities),
            "min_appearances": min_appearances,
            "min_edge_weight": min_edge_weight,
        }

    finally:
        conn.close()
