#!/usr/bin/env python3
"""
Proof of Concept: API-based scraping for One Piece data

This script demonstrates how to use the Fandom MediaWiki API
to scrape chapter data without Cloudflare blocking.

Run with: uv run python poc_api_scraper.py
"""

import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from onepieceofdata.api import FandomAPIClient, WikitextParser
from loguru import logger


def scrape_chapter_via_api(chapter_number: int):
    """Scrape a single chapter using the API approach."""
    logger.info(f"=== Scraping Chapter {chapter_number} via API ===")

    # Initialize API client
    client = FandomAPIClient(wiki="onepiece")

    # Get wikitext for the chapter page
    page_title = f"Chapter {chapter_number}"
    logger.info(f"Fetching wikitext for: {page_title}")

    wikitext = client.get_page_wikitext(page_title)

    if not wikitext:
        logger.error(f"Failed to retrieve wikitext for {page_title}")
        return None

    logger.success(f"Retrieved {len(wikitext)} characters of wikitext")

    # Parse Chapter Box template
    logger.info("Parsing Chapter Box template...")
    chapter_info = WikitextParser.parse_chapter_box(wikitext)

    if chapter_info:
        logger.success(f"Extracted chapter info: {chapter_info}")
    else:
        logger.warning("Could not parse Chapter Box")

    # Parse character table
    logger.info("Parsing character table...")
    characters = WikitextParser.parse_character_table(wikitext)

    logger.success(f"Found {len(characters)} characters")
    if characters:
        logger.info(f"First few characters: {characters[:5]}")

    # Combine results
    result = {
        "chapter_number": chapter_number,
        "chapter_info": chapter_info,
        "characters": characters,
    }

    return result


def batch_scrape_chapters(start: int, end: int):
    """Demonstrate batch scraping multiple chapters."""
    logger.info(f"=== Batch Scraping Chapters {start}-{end} ===")

    client = FandomAPIClient(wiki="onepiece")

    # Build list of page titles
    titles = [f"Chapter {i}" for i in range(start, end + 1)]

    logger.info(f"Fetching {len(titles)} chapters in batch...")

    # Get all wikitexts in one or more batched requests
    wikitext_map = client.get_multiple_pages_wikitext(titles)

    logger.success(f"Retrieved {len(wikitext_map)} pages")

    # Parse each chapter
    results = []
    for title, wikitext in wikitext_map.items():
        if not wikitext:
            logger.warning(f"No wikitext for {title}")
            continue

        chapter_number = WikitextParser.extract_chapter_number_from_title(title)
        if not chapter_number:
            continue

        chapter_info = WikitextParser.parse_chapter_box(wikitext)
        characters = WikitextParser.parse_character_table(wikitext)

        results.append({
            "chapter_number": chapter_number,
            "title": chapter_info.get('title') if chapter_info else None,
            "character_count": len(characters),
        })

    logger.success(f"Parsed {len(results)} chapters successfully")
    for r in results:
        logger.info(f"  Chapter {r['chapter_number']}: {r['title']} ({r['character_count']} chars)")

    return results


def compare_with_existing_data(chapter_number: int):
    """Compare API scraping with existing JSON data."""
    logger.info(f"=== Comparing Chapter {chapter_number} with existing data ===")

    # Load existing data
    existing_file = Path("data/chapters.json")
    if not existing_file.exists():
        logger.warning("No existing chapters.json found for comparison")
        return

    with open(existing_file) as f:
        existing_data = json.load(f)

    # Find the chapter in existing data
    existing_chapter = None
    for chapter in existing_data:
        if chapter.get('chapter_number') == chapter_number:
            existing_chapter = chapter
            break

    if not existing_chapter:
        logger.warning(f"Chapter {chapter_number} not found in existing data")
        return

    logger.info(f"Existing data: {json.dumps(existing_chapter, indent=2)[:200]}...")

    # Scrape via API
    api_result = scrape_chapter_via_api(chapter_number)

    if not api_result:
        logger.error("API scraping failed")
        return

    logger.info(f"API result: {json.dumps(api_result, indent=2)[:200]}...")

    # Compare
    logger.info("\n=== Comparison ===")
    logger.info(f"Existing character count: {len(existing_chapter.get('characters', []))}")
    logger.info(f"API character count: {len(api_result['characters'])}")

    # Compare titles
    existing_title = existing_chapter.get('title', '')
    api_title = api_result['chapter_info'].get('title', '') if api_result['chapter_info'] else ''

    logger.info(f"Existing title: {existing_title}")
    logger.info(f"API title: {api_title}")


def main():
    """Main entry point for POC."""
    logger.info("=" * 60)
    logger.info("Proof of Concept: Fandom API-based Scraping")
    logger.info("=" * 60)

    # Test 1: Scrape a single chapter
    logger.info("\n" + "=" * 60)
    logger.info("TEST 1: Single Chapter Scraping")
    logger.info("=" * 60)
    result1 = scrape_chapter_via_api(1)

    if result1:
        logger.success("✓ Single chapter scraping successful")
        print("\nChapter 1 Data:")
        print(json.dumps(result1, indent=2)[:500] + "...\n")
    else:
        logger.error("✗ Single chapter scraping failed")

    # Test 2: Batch scrape multiple chapters
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Batch Chapter Scraping")
    logger.info("=" * 60)
    results2 = batch_scrape_chapters(1, 5)

    if results2:
        logger.success(f"✓ Batch scraping successful ({len(results2)} chapters)")
    else:
        logger.error("✗ Batch scraping failed")

    # Test 3: Compare with existing data
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Compare with Existing Data")
    logger.info("=" * 60)
    compare_with_existing_data(1)

    logger.info("\n" + "=" * 60)
    logger.info("Proof of Concept Complete!")
    logger.info("=" * 60)
    logger.info("\nNext steps:")
    logger.info("1. Review docs/API_MIGRATION_ANALYSIS.md for full migration plan")
    logger.info("2. Decide whether to use regex or mwparserfromhell for parsing")
    logger.info("3. Migrate chapter scraper to use API client")
    logger.info("4. Migrate character and volume scrapers")
    logger.info("5. Update tests and remove cloudscraper dependency")


if __name__ == "__main__":
    main()
