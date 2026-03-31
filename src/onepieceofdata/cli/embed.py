"""CLI for the embedding pipeline.

Chunks wiki text from DuckDB, generates embeddings, and stores them back in DuckDB.

Usage:
    python -m onepieceofdata.cli.embed --run        # Full pipeline: chunk + embed + save
    python -m onepieceofdata.cli.embed --status     # Show stats
    python -m onepieceofdata.cli.embed --search "gear 5"  # Test search
"""

import argparse
import sys
from loguru import logger

from ..utils.logging import setup_logging


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Chunk and embed One Piece wiki text into DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--run", action="store_true", help="Full pipeline: chunk + embed + save to DuckDB.")
    mode.add_argument("--status", action="store_true", help="Show embedding stats from DuckDB.")
    mode.add_argument("--search", metavar="QUERY", help="Search for similar chunks using a text query.")
    parser.add_argument("--db", metavar="PATH", default=None, help="Override DuckDB database path.")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results for --search (default: 5).")
    parser.add_argument("--page-type", default=None, choices=["character", "arc", "saga"], help="Filter search by page type.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def _resolve_db_path(db_path=None) -> str:
    if db_path:
        return db_path
    from ..config.settings import get_settings
    return str(get_settings().database_path)


def cmd_run(db_path=None):
    """Full pipeline: chunk all wiki pages, embed them, save to DuckDB."""
    from ..embeddings.chunker import chunk_all_pages
    from ..embeddings.embedder import load_model, embed_chunks
    from ..embeddings.vector_store import save_chunks

    db_path = _resolve_db_path(db_path)

    logger.info("Step 1/3 — Chunking wiki pages...")
    chunks = chunk_all_pages(db_path)
    logger.info(f"Produced {len(chunks)} chunks from wiki_text")

    logger.info("Step 2/3 — Loading embedding model...")
    model = load_model()

    logger.info("Step 3/3 — Generating embeddings and saving to DuckDB...")
    chunks = embed_chunks(chunks, model=model)
    saved = save_chunks(chunks, db_path)

    print(f"Done: {saved} chunks embedded and saved to {db_path}")


def cmd_status(db_path=None):
    """Print stats from the wiki_chunks table in DuckDB."""
    from ..embeddings.vector_store import get_stats

    db_path = _resolve_db_path(db_path)
    stats = get_stats(db_path)
    total = stats.get("total", 0)
    by_type = stats.get("by_type", {})

    if total == 0:
        print("wiki_chunks table: empty or not yet created")
        print("Run: make embed-wiki")
        return

    print(f"wiki_chunks table: {total} total chunks")
    for page_type, count in sorted(by_type.items()):
        print(f"  {page_type:12s}: {count}")


def cmd_search(query: str, db_path=None, top_k: int = 5, page_type: str = None):
    """Search for chunks similar to a text query."""
    from ..embeddings.embedder import load_model
    from ..embeddings.vector_store import search_similar

    db_path = _resolve_db_path(db_path)

    logger.info("Loading embedding model...")
    model = load_model()

    query_embedding = model.encode(query).tolist()
    results = search_similar(query_embedding, db_path, limit=top_k, page_type=page_type)

    if not results:
        print("No results found.")
        return

    print(f"\nTop {len(results)} results for: \"{query}\"\n")
    for i, r in enumerate(results, 1):
        print(f"[{i}] {r['title']} — {r['section_name']}  (score: {r['similarity']:.4f})")
        print(f"    chunk_id : {r['chunk_id']}")
        snippet = r["chunk_text"][:200].replace("\n", " ")
        print(f"    snippet  : {snippet}...")
        print()


def main(argv=None):
    args = _parse_args(argv)
    setup_logging(log_level=args.log_level)
    try:
        if args.run:
            cmd_run(db_path=args.db)
        elif args.status:
            cmd_status(db_path=args.db)
        elif args.search:
            cmd_search(args.search, db_path=args.db, top_k=args.top_k, page_type=args.page_type)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
