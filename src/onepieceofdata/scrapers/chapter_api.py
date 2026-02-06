"""API-based chapter scraper using Fandom MediaWiki API.

This is a replacement for chapter.py that uses the official MediaWiki API
instead of HTML scraping with cloudscraper. This bypasses Cloudflare blocking.
"""

import json
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Any

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..api import FandomAPIClient, WikitextParser
from ..config import settings
from ..models import ChapterModel, ScrapingResult
from ..utils.logging import get_logger


logger = get_logger(__name__)


def scrape_chapter_worker_api(chapter_num: int) -> ScrapingResult:
    """
    Worker function for parallel chapter scraping using API.
    This function creates its own API client to avoid sharing state.

    Args:
        chapter_num: Chapter number to scrape

    Returns:
        ScrapingResult with chapter data
    """
    try:
        scraper = ChapterScraperAPI()
        result = scraper.scrape_chapter(chapter_num)
        return result
    except Exception as e:
        logger.error(f"Worker failed to scrape chapter {chapter_num}: {str(e)}")
        return ScrapingResult(
            success=False,
            data=None,
            error=f"Worker error: {str(e)}",
            url=f"https://onepiece.fandom.com/wiki/Chapter_{chapter_num}"
        )


class ChapterScraperAPI:
    """API-based chapter scraper using Fandom MediaWiki API."""

    def __init__(self):
        """Initialize the API-based chapter scraper."""
        self.api_client = FandomAPIClient(wiki="onepiece")
        self.parser = WikitextParser()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception)
    )
    def _fetch_chapter_wikitext(self, chapter_number: int) -> Optional[str]:
        """
        Fetch chapter wikitext from API with retry logic.

        Args:
            chapter_number: Chapter number to fetch

        Returns:
            Raw wikitext content or None if failed

        Raises:
            Exception: If all retry attempts fail
        """
        try:
            page_title = f"Chapter {chapter_number}"
            logger.debug(f"Fetching wikitext for: {page_title}")

            wikitext = self.api_client.get_page_wikitext(page_title)

            if not wikitext:
                raise Exception(f"No wikitext returned for {page_title}")

            return wikitext

        except Exception as e:
            logger.warning(f"Failed to fetch chapter {chapter_number}: {e}")
            raise

    def _parse_chapter_info(self, wikitext: str, chapter_number: int) -> Dict[str, Any]:
        """
        Parse chapter information from wikitext.

        Args:
            wikitext: Raw wikitext content
            chapter_number: Chapter number being parsed

        Returns:
            Dictionary with chapter information
        """
        chapter_info = {"chapter_number": chapter_number}

        try:
            # Parse Chapter Box template
            chapter_box = self.parser.parse_chapter_box(wikitext)

            if chapter_box:
                # Map template fields to our schema
                if "title" in chapter_box:
                    chapter_info["title"] = chapter_box["title"]

                if "english_title" in chapter_box:
                    chapter_info["english_name"] = chapter_box["english_title"]

                if "japanese_title" in chapter_box:
                    chapter_info["japanese_title"] = chapter_box["japanese_title"]

                # Extract additional fields that might be in the template
                for key in ["vol", "chapter", "page", "date2", "jump"]:
                    if key in chapter_box:
                        # Map to our field names
                        field_mapping = {
                            "vol": "volume",
                            "chapter": "chapter_display",
                            "page": "pages",
                            "date2": "release_date",
                            "jump": "jump_info"
                        }
                        field_name = field_mapping.get(key, key)
                        chapter_info[field_name] = chapter_box[key]
            else:
                logger.warning(f"No Chapter Box found for chapter {chapter_number}")

            # Parse character table
            chapter_info["characters"] = self._parse_characters(wikitext, chapter_number)

        except Exception as e:
            logger.error(f"Error parsing chapter {chapter_number}: {e}")

        return chapter_info

    def _parse_characters(self, wikitext: str, chapter_number: int) -> List[Dict[str, str]]:
        """
        Parse character information from wikitext.

        Args:
            wikitext: Raw wikitext content
            chapter_number: Chapter number being parsed

        Returns:
            List of character dictionaries
        """
        try:
            characters = self.parser.parse_character_table(wikitext)

            if not characters:
                logger.debug(f"No characters found for chapter {chapter_number}")
                return []

            # Transform to match existing format
            formatted_characters = []
            for char in characters:
                char_data = {
                    "name": char.get("name", ""),
                    "url": char.get("url", ""),
                    "note": char.get("note", ""),
                    "full_text": char.get("name", "")  # Use name as full_text for compatibility
                }
                if char_data["name"]:  # Only add if we have a name
                    formatted_characters.append(char_data)

            logger.debug(f"Found {len(formatted_characters)} characters in chapter {chapter_number}")
            return formatted_characters

        except Exception as e:
            logger.error(f"Error parsing characters for chapter {chapter_number}: {e}")
            return []

    def scrape_chapter(self, chapter_number: int) -> ScrapingResult:
        """
        Scrape a single chapter using the API.

        Args:
            chapter_number: Chapter number to scrape

        Returns:
            ScrapingResult with chapter data or error information
        """
        chapter_url = f"https://onepiece.fandom.com/wiki/Chapter_{chapter_number}"

        try:
            logger.info(f"Scraping chapter {chapter_number} via API")

            # Add delay to be respectful to the server
            time.sleep(settings.scraping_delay)

            # Fetch the wikitext
            wikitext = self._fetch_chapter_wikitext(chapter_number)

            if not wikitext:
                raise Exception(f"Failed to fetch wikitext for chapter {chapter_number}")

            # Parse the content
            chapter_info = self._parse_chapter_info(wikitext, chapter_number)

            logger.debug(f"Successfully scraped chapter {chapter_number}")

            return ScrapingResult(
                success=True,
                data=chapter_info,
                url=chapter_url
            )

        except Exception as e:
            error_msg = f"Failed to scrape chapter {chapter_number}: {e}"
            logger.error(error_msg)

            return ScrapingResult(
                success=False,
                error=error_msg,
                url=chapter_url
            )

    def scrape_chapters(self, start_chapter: int = 1, end_chapter: Optional[int] = None,
                       use_parallel: bool = False, max_workers: Optional[int] = None,
                       use_batch: bool = True) -> List[Dict[str, Any]]:
        """
        Scrape multiple chapters with optional parallel processing and batch queries.

        Args:
            start_chapter: Starting chapter number
            end_chapter: Ending chapter number (uses settings.last_chapter if None)
            use_parallel: Whether to use parallel processing
            max_workers: Maximum number of worker processes (defaults to config)
            use_batch: Whether to use batch API queries (faster but less granular error handling)

        Returns:
            List of chapter data dictionaries
        """
        if end_chapter is None:
            end_chapter = settings.last_chapter

        if max_workers is None:
            max_workers = settings.max_workers

        chapter_range = list(range(start_chapter, end_chapter + 1))
        total_chapters = len(chapter_range)

        logger.info(f"Starting to scrape chapters {start_chapter} to {end_chapter}")
        logger.info(f"Total chapters to scrape: {total_chapters}")

        # Use batch API queries if enabled and not using parallel processing
        if use_batch and not use_parallel:
            return self._scrape_chapters_batch(chapter_range)
        elif use_parallel and total_chapters > 1:
            return self._scrape_chapters_parallel(chapter_range, max_workers)
        else:
            return self._scrape_chapters_sequential(chapter_range)

    def _scrape_chapters_batch(self, chapter_range: List[int]) -> List[Dict[str, Any]]:
        """
        Scrape chapters using batch API queries.
        This is faster than sequential but processes in groups.
        """
        logger.info(f"Using batch API queries (up to 50 chapters per request)")

        successful_chapters = []
        failed_chapters = []

        # Batch size of 50 (API limit)
        batch_size = 50

        for i in range(0, len(chapter_range), batch_size):
            batch = chapter_range[i:i+batch_size]
            logger.info(f"Fetching batch of {len(batch)} chapters...")

            try:
                # Build page titles
                titles = [f"Chapter {num}" for num in batch]

                # Fetch all wikitexts in one API call
                wikitext_map = self.api_client.get_multiple_pages_wikitext(titles)

                # Parse each chapter
                for title, wikitext in wikitext_map.items():
                    chapter_num = self.parser.extract_chapter_number_from_title(title)

                    if not chapter_num:
                        logger.warning(f"Could not extract chapter number from: {title}")
                        continue

                    if not wikitext:
                        logger.warning(f"No wikitext for chapter {chapter_num}")
                        failed_chapters.append(chapter_num)
                        continue

                    try:
                        chapter_info = self._parse_chapter_info(wikitext, chapter_num)
                        successful_chapters.append(chapter_info)
                    except Exception as e:
                        logger.error(f"Failed to parse chapter {chapter_num}: {e}")
                        failed_chapters.append(chapter_num)

                # Add delay between batches
                if settings.scraping_delay > 0:
                    time.sleep(settings.scraping_delay)

            except Exception as e:
                logger.error(f"Batch query failed: {e}")
                # Add entire batch to failed
                failed_chapters.extend(batch)

        # Sort by chapter number
        successful_chapters.sort(key=lambda x: x.get('chapter_number', 0))

        logger.info(f"Successfully scraped {len(successful_chapters)} chapters using batch queries")
        if failed_chapters:
            logger.warning(f"Failed to scrape {len(failed_chapters)} chapters: {sorted(failed_chapters)}")

        return successful_chapters

    def _scrape_chapters_sequential(self, chapter_range: List[int]) -> List[Dict[str, Any]]:
        """Scrape chapters sequentially (one at a time)."""
        logger.info("Using sequential processing")

        successful_chapters = []
        failed_chapters = []

        for chapter_num in chapter_range:
            result = self.scrape_chapter(chapter_num)
            if result.success:
                successful_chapters.append(result.data)
            else:
                failed_chapters.append(chapter_num)

            # Add delay between requests
            if settings.scraping_delay > 0:
                time.sleep(settings.scraping_delay)

        logger.info(f"Successfully scraped {len(successful_chapters)} chapters")
        if failed_chapters:
            logger.warning(f"Failed to scrape {len(failed_chapters)} chapters: {failed_chapters}")

        return successful_chapters

    def _scrape_chapters_parallel(self, chapter_range: List[int], max_workers: int) -> List[Dict[str, Any]]:
        """Scrape chapters in parallel using ProcessPoolExecutor."""
        # Cap workers at reasonable limits
        actual_workers = min(max_workers, multiprocessing.cpu_count() - 1, 8)
        logger.info(f"Using parallel processing with {actual_workers} workers")

        successful_chapters = []
        failed_chapters = []

        try:
            with ProcessPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all tasks
                future_to_chapter = {
                    executor.submit(scrape_chapter_worker_api, chapter_num): chapter_num
                    for chapter_num in chapter_range
                }

                # Process completed tasks
                for future in as_completed(future_to_chapter):
                    chapter_num = future_to_chapter[future]
                    try:
                        result = future.result()
                        if result.success:
                            successful_chapters.append(result.data)
                            logger.debug(f"Successfully scraped chapter {chapter_num}")
                        else:
                            failed_chapters.append(chapter_num)
                            logger.error(f"Failed to scrape chapter {chapter_num}: {result.error}")
                    except Exception as e:
                        failed_chapters.append(chapter_num)
                        logger.error(f"Exception while processing chapter {chapter_num}: {str(e)}")

        except Exception as e:
            logger.error(f"Parallel processing failed: {str(e)}")
            # Fallback to sequential processing
            logger.info("Falling back to sequential processing")
            return self._scrape_chapters_sequential(chapter_range)

        # Sort successful chapters by chapter number
        successful_chapters.sort(key=lambda x: x.get('chapter_number', 0))

        logger.info(f"Successfully scraped {len(successful_chapters)} chapters using parallel processing")
        if failed_chapters:
            logger.warning(f"Failed to scrape {len(failed_chapters)} chapters: {sorted(failed_chapters)}")

        return successful_chapters

    def save_chapters(self, chapters: List[Dict[str, Any]], output_path: Path) -> None:
        """
        Save chapters to JSON file.

        Args:
            chapters: List of chapter data
            output_path: Path to save the JSON file
        """
        try:
            # Ensure directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save to JSON
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(chapters, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved {len(chapters)} chapters to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save chapters to {output_path}: {e}")
            raise


# Convenience function for backwards compatibility
def scrape_chapters_api(start_chapter: int, end_chapter: int, output_path: str,
                       use_batch: bool = True) -> None:
    """
    Legacy function for scraping chapters using API.

    Args:
        start_chapter: Starting chapter number
        end_chapter: Last chapter number to scrape
        output_path: Path to save the JSON file
        use_batch: Whether to use batch API queries
    """
    scraper = ChapterScraperAPI()
    chapters = scraper.scrape_chapters(
        start_chapter=start_chapter,
        end_chapter=end_chapter,
        use_batch=use_batch
    )
    scraper.save_chapters(chapters, Path(output_path))
