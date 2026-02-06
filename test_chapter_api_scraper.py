#!/usr/bin/env python3
"""Test script for the new API-based chapter scraper."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from onepieceofdata.scrapers.chapter_api import ChapterScraperAPI
from loguru import logger


def test_single_chapter():
    """Test scraping a single chapter."""
    logger.info("=" * 60)
    logger.info("TEST 1: Single Chapter Scraping")
    logger.info("=" * 60)

    scraper = ChapterScraperAPI()
    result = scraper.scrape_chapter(1)

    if result.success:
        logger.success(f"✓ Successfully scraped Chapter 1")
        logger.info(f"  Title: {result.data.get('title', 'N/A')}")
        logger.info(f"  Characters: {len(result.data.get('characters', []))}")
        logger.info(f"  Volume: {result.data.get('volume', 'N/A')}")
        return True
    else:
        logger.error(f"✗ Failed to scrape Chapter 1: {result.error}")
        return False


def test_batch_scraping():
    """Test batch scraping multiple chapters."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Batch Scraping (Chapters 1-5)")
    logger.info("=" * 60)

    scraper = ChapterScraperAPI()
    chapters = scraper.scrape_chapters(
        start_chapter=1,
        end_chapter=5,
        use_batch=True
    )

    if chapters:
        logger.success(f"✓ Successfully scraped {len(chapters)} chapters")
        for chapter in chapters:
            logger.info(f"  Chapter {chapter['chapter_number']}: {chapter.get('title', 'N/A')}")
        return True
    else:
        logger.error("✗ Failed to scrape chapters")
        return False


def test_sequential_scraping():
    """Test sequential scraping."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Sequential Scraping (Chapters 1-3)")
    logger.info("=" * 60)

    scraper = ChapterScraperAPI()
    chapters = scraper.scrape_chapters(
        start_chapter=1,
        end_chapter=3,
        use_batch=False
    )

    if chapters:
        logger.success(f"✓ Successfully scraped {len(chapters)} chapters sequentially")
        return True
    else:
        logger.error("✗ Failed to scrape chapters")
        return False


def main():
    """Run all tests."""
    logger.info("Testing API-based Chapter Scraper")
    logger.info("=" * 60)

    results = []

    # Test 1: Single chapter
    results.append(("Single chapter", test_single_chapter()))

    # Test 2: Batch scraping
    results.append(("Batch scraping", test_batch_scraping()))

    # Test 3: Sequential scraping
    results.append(("Sequential scraping", test_sequential_scraping()))

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)

    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"{status}: {test_name}")

    all_passed = all(result[1] for result in results)

    if all_passed:
        logger.success("\n✓ All tests passed! API scraper is working correctly.")
        return 0
    else:
        logger.error("\n✗ Some tests failed. Check the logs above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
