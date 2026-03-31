"""CLI entry point for wiki text scraping.

Scrapes wiki pages and saves to DuckDB (same as other scrapers).
Use export-postgres to sync DuckDB → Supabase.

Usage:
    python -m onepieceofdata.cli.wiki_scrape --all
    python -m onepieceofdata.cli.wiki_scrape --characters
    python -m onepieceofdata.cli.wiki_scrape --arcs
    python -m onepieceofdata.cli.wiki_scrape --status
"""

import argparse
import sys
from loguru import logger

from ..utils.logging import setup_logging


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Scrape One Piece wiki pages and save to DuckDB.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--all", action="store_true", help="Scrape all pages: characters, arcs, and sagas.")
    mode.add_argument("--characters", action="store_true", help="Scrape character wiki pages only.")
    mode.add_argument("--arcs", action="store_true", help="Scrape arc and saga wiki pages only.")
    mode.add_argument("--status", action="store_true", help="Show current scraping stats from DuckDB.")
    parser.add_argument("--db", metavar="PATH", default=None, help="Override DuckDB database path.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def cmd_status(db_path=None):
    """Print stats from the wiki_text table in DuckDB."""
    from ..scrapers.wiki_text_scraper import WikiTextScraper
    scraper = WikiTextScraper(database_path=db_path)
    stats = scraper.get_duckdb_stats()
    total = stats.get("total", 0)
    by_type = stats.get("by_type", {})
    if total == 0:
        print("wiki_text table: empty or not yet created")
        print("Run: make wiki-scrape")
        return
    print(f"wiki_text table: {total} total rows")
    for page_type, count in sorted(by_type.items()):
        print(f"  {page_type:12s}: {count}")


def cmd_scrape_characters(db_path=None):
    from ..scrapers.wiki_text_scraper import WikiTextScraper
    scraper = WikiTextScraper(database_path=db_path)
    try:
        pages = scraper.scrape_characters()
        scraper.save_to_duckdb(pages)
        print(f"Characters: scraped and saved {len(pages)} pages to DuckDB")
    finally:
        scraper.cleanup()


def cmd_scrape_arcs(db_path=None):
    from ..scrapers.wiki_text_scraper import WikiTextScraper
    scraper = WikiTextScraper(database_path=db_path)
    try:
        arc_pages = scraper.scrape_arcs()
        saga_pages = scraper.scrape_sagas()
        all_pages = arc_pages + saga_pages
        scraper.save_to_duckdb(all_pages)
        print(f"Arcs: {len(arc_pages)}, Sagas: {len(saga_pages)} — saved to DuckDB")
    finally:
        scraper.cleanup()


def cmd_scrape_all(db_path=None):
    from ..scrapers.wiki_text_scraper import WikiTextScraper
    scraper = WikiTextScraper(database_path=db_path)
    try:
        pages = scraper.scrape_all()
        scraper.save_to_duckdb(pages)
        print(f"Total: {len(pages)} pages saved to DuckDB")
    finally:
        scraper.cleanup()


def main(argv=None):
    args = _parse_args(argv)
    setup_logging(log_level=args.log_level)
    try:
        if args.status:
            cmd_status(db_path=args.db)
        elif args.characters:
            cmd_scrape_characters(db_path=args.db)
        elif args.arcs:
            cmd_scrape_arcs(db_path=args.db)
        elif args.all:
            cmd_scrape_all(db_path=args.db)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
