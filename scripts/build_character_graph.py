#!/usr/bin/env python3
"""
Build a character co-appearance graph from One Piece data.

Weight types:
  arc       - number of arcs both characters appear in together
  saga      - number of sagas both characters appear in together
  chapter   - number of chapters both characters appear in together
  consec-N  - number of windows of N consecutive chapters both characters appear in
               (N can be 2, 3, 4, 5, or 7)

Output JSON format:
  {
    "nodes": [{"id": "luffy", "label": "Monkey D. Luffy", "appearance_count": 1100}, ...],
    "edges": [{"source": "luffy", "target": "zoro", "weight": 120}, ...]
  }
"""

import argparse
import json
import sys
from pathlib import Path

import duckdb


DB_PATH = Path(__file__).parent.parent / "data" / "onepiece.duckdb"


def get_nodes(con: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = con.execute(
        """
        SELECT id, name, appearance_count
        FROM character
        WHERE chapter_list IS NOT NULL AND appearance_count > 0
        ORDER BY appearance_count DESC
        """
    ).fetchall()
    return [{"id": r[0], "label": r[1], "appearance_count": r[2]} for r in rows]


def build_arc_edges(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Weight = number of arcs both characters appear in together."""
    rows = con.execute(
        """
        SELECT a.character AS source, b.character AS target, COUNT(*) AS weight
        FROM (
            SELECT DISTINCT c.id AS character, arc.arc_id
            FROM character c
            JOIN arc ON c.first_appearance <= arc.end_chapter
                    AND c.last_appearance  >= arc.start_chapter
            WHERE c.chapter_list IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM coc
                  WHERE coc.character = c.id
                    AND coc.chapter BETWEEN arc.start_chapter AND arc.end_chapter
              )
        ) a
        JOIN (
            SELECT DISTINCT c.id AS character, arc.arc_id
            FROM character c
            JOIN arc ON c.first_appearance <= arc.end_chapter
                    AND c.last_appearance  >= arc.start_chapter
            WHERE c.chapter_list IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM coc
                  WHERE coc.character = c.id
                    AND coc.chapter BETWEEN arc.start_chapter AND arc.end_chapter
              )
        ) b ON a.arc_id = b.arc_id AND a.character < b.character
        GROUP BY a.character, b.character
        HAVING COUNT(*) > 0
        ORDER BY weight DESC
        """
    ).fetchall()
    return [{"source": r[0], "target": r[1], "weight": r[2]} for r in rows]


def build_saga_edges(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Weight = number of sagas both characters appear in together."""
    rows = con.execute(
        """
        SELECT a.character AS source, b.character AS target, COUNT(*) AS weight
        FROM (
            SELECT DISTINCT c.id AS character, saga.saga_id
            FROM character c
            JOIN saga ON c.first_appearance <= saga.end_chapter
                     AND c.last_appearance  >= saga.start_chapter
            WHERE c.chapter_list IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM coc
                  WHERE coc.character = c.id
                    AND coc.chapter BETWEEN saga.start_chapter AND saga.end_chapter
              )
        ) a
        JOIN (
            SELECT DISTINCT c.id AS character, saga.saga_id
            FROM character c
            JOIN saga ON c.first_appearance <= saga.end_chapter
                     AND c.last_appearance  >= saga.start_chapter
            WHERE c.chapter_list IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM coc
                  WHERE coc.character = c.id
                    AND coc.chapter BETWEEN saga.start_chapter AND saga.end_chapter
              )
        ) b ON a.saga_id = b.saga_id AND a.character < b.character
        GROUP BY a.character, b.character
        HAVING COUNT(*) > 0
        ORDER BY weight DESC
        """
    ).fetchall()
    return [{"source": r[0], "target": r[1], "weight": r[2]} for r in rows]


def build_chapter_edges(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Weight = number of chapters both characters appear in together."""
    rows = con.execute(
        """
        SELECT a.character AS source, b.character AS target, COUNT(*) AS weight
        FROM coc a
        JOIN coc b ON a.chapter = b.chapter AND a.character < b.character
        GROUP BY a.character, b.character
        HAVING COUNT(*) > 0
        ORDER BY weight DESC
        """
    ).fetchall()
    return [{"source": r[0], "target": r[1], "weight": r[2]} for r in rows]


def build_consecutive_edges(con: duckdb.DuckDBPyConnection, window: int) -> list[dict]:
    """
    Weight = number of windows of `window` consecutive chapters where both characters appear.
    A window starting at chapter C covers chapters [C, C + window - 1].
    We count distinct window starts where both characters have at least one appearance.
    """
    rows = con.execute(
        f"""
        WITH chapter_windows AS (
            -- For each coc entry, emit all window starts that contain this chapter.
            -- Window start W covers [W, W + {window} - 1], so W = chapter - {window} + 1 .. chapter
            SELECT character, chapter - gs AS window_start
            FROM coc
            JOIN generate_series(0, {window - 1}) gs(gs) ON TRUE
            WHERE chapter - gs >= 1
        )
        SELECT a.character AS source, b.character AS target, COUNT(*) AS weight
        FROM (SELECT DISTINCT character, window_start FROM chapter_windows) a
        JOIN (SELECT DISTINCT character, window_start FROM chapter_windows) b
          ON a.window_start = b.window_start AND a.character < b.character
        GROUP BY a.character, b.character
        HAVING COUNT(*) > 0
        ORDER BY weight DESC
        """
    ).fetchall()
    return [{"source": r[0], "target": r[1], "weight": r[2]} for r in rows]


def main():
    parser = argparse.ArgumentParser(description="Build One Piece character co-appearance graph")
    parser.add_argument(
        "--weight",
        required=True,
        choices=["arc", "saga", "chapter", "consec-2", "consec-3", "consec-4", "consec-5", "consec-7"],
        help="Edge weight type",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON file path (default: data/network_<weight>.json)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help=f"Path to DuckDB database (default: {DB_PATH})",
    )
    parser.add_argument(
        "--min-weight",
        type=int,
        default=1,
        help="Minimum edge weight to include (default: 1)",
    )
    args = parser.parse_args()

    output_path = args.output or Path(__file__).parent.parent / "data" / f"network_{args.weight}.json"

    print(f"Connecting to {args.db} ...", file=sys.stderr)
    con = duckdb.connect(str(args.db), read_only=True)

    print("Loading nodes ...", file=sys.stderr)
    nodes = get_nodes(con)
    print(f"  {len(nodes)} character nodes", file=sys.stderr)

    print(f"Computing edges (weight={args.weight}) ...", file=sys.stderr)
    if args.weight == "arc":
        edges = build_arc_edges(con)
    elif args.weight == "saga":
        edges = build_saga_edges(con)
    elif args.weight == "chapter":
        edges = build_chapter_edges(con)
    else:
        window = int(args.weight.split("-")[1])
        edges = build_consecutive_edges(con, window)

    if args.min_weight > 1:
        edges = [e for e in edges if e["weight"] >= args.min_weight]

    print(f"  {len(edges)} edges (min_weight={args.min_weight})", file=sys.stderr)

    graph = {"nodes": nodes, "edges": edges}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(graph, f, separators=(",", ":"))

    print(f"Written to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
