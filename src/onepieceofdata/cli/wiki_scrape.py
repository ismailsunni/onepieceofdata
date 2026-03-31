"""CLI entry point for wiki text scraping.

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
        description="Scrape One Piece wiki pages and upload to Supabase.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--all",
        action="store_true",
        help="Scrape all pages: characters, arcs, and sagas.",
    )
    mode.add_argument(
        "--characters",
        action="store_true",
        help="Scrape character wiki pages only.",
    )
    mode.add_argument(
        "--arcs",
        action="store_true",
        help="Scrape arc and saga wiki pages only.",
    )
    mode.add_argument(
        "--status",
        action="store_true",
        help="Show current scraping stats from Supabase.",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Override DuckDB database path (default: value from OP_DATABASE_PATH env).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args(argv)


def _get_uploader():
    """Import and instantiate WikiTextUploader, with a friendly error on failure."""
    from ..scrapers.wiki_text_uploader import WikiTextUploader
    return WikiTextUploader()


def cmd_status():
    """Print stats from the wiki_text table."""
    uploader = _get_uploader()
    try:
        stats = uploader.get_stats()
        total = stats.get("total", 0)
        by_type = stats.get("by_type", {})
        print(f"wiki_text table: {total} total rows")
        for page_type, count in sorted(by_type.items()):
            print(f"  {page_type:12s}: {count}")
    finally:
        uploader.close()


def cmd_scrape_characters(db_path=None):
    """Scrape all character pages and upload."""
    from ..scrapers.wiki_text_scraper import WikiTextScraper

    scraper = WikiTextScraper(database_path=db_path)
    uploader = _get_uploader()
    try:
        uploader.create_table()
        pages = scraper.scrape_characters()
        uploaded = uploader.upload_pages(pages, page_type="character")
        print(f"Characters: scraped {len(pages)}, uploaded {uploaded}")
    finally:
        scraper.cleanup()
        uploader.close()


def cmd_scrape_arcs(db_path=None):
    """Scrape arc and saga pages and upload."""
    from ..scrapers.wiki_text_scraper import WikiTextScraper

    scraper = WikiTextScraper(database_path=db_path)
    uploader = _get_uploader()
    try:
        uploader.create_table()

        arc_pages = scraper.scrape_arcs()
        arc_uploaded = uploader.upload_pages(arc_pages, page_type="arc")

        saga_pages = scraper.scrape_sagas()
        saga_uploaded = uploader.upload_pages(saga_pages, page_type="saga")

        print(f"Arcs:  scraped {len(arc_pages)}, uploaded {arc_uploaded}")
        print(f"Sagas: scraped {len(saga_pages)}, uploaded {saga_uploaded}")
    finally:
        scraper.cleanup()
        uploader.close()


def cmd_scrape_all(db_path=None):
    """Scrape everything and upload."""
    from ..scrapers.wiki_text_scraper import WikiTextScraper

    scraper = WikiTextScraper(database_path=db_path)
    uploader = _get_uploader()
    try:
        uploader.create_table()

        pages = scraper.scrape_all()

        # Group by page_type for upload
        by_type: dict = {}
        for page in pages:
            pt = page.get("page_type", "unknown")
            by_type.setdefault(pt, []).append(page)

        total_uploaded = 0
        for page_type, type_pages in by_type.items():
            uploaded = uploader.upload_pages(type_pages, page_type=page_type)
            total_uploaded += uploaded
            print(f"{page_type:12s}: scraped {len(type_pages)}, uploaded {uploaded}")

        print(f"Total: {len(pages)} scraped, {total_uploaded} uploaded")
    finally:
        scraper.cleanup()
        uploader.close()


def main(argv=None):
    args = _parse_args(argv)
    setup_logging(log_level=args.log_level)

    try:
        if args.status:
            cmd_status()
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
