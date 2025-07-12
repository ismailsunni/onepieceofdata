"""Modern chapter scraper with error handling and retry logic."""

import asyncio
import json
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

import urllib3
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..config import settings
from ..models import ChapterModel, ScrapingResult
from ..utils.logging import get_logger


logger = get_logger(__name__)


def scrape_chapter_worker(chapter_num: int) -> ScrapingResult:
    """
    Worker function for parallel chapter scraping.
    This function creates its own scraper instance to avoid sharing state.
    
    Args:
        chapter_num: Chapter number to scrape
        
    Returns:
        ScrapingResult with chapter data
    """
    try:
        scraper = ChapterScraper()
        result = scraper.scrape_chapter(chapter_num)
        return result
    except Exception as e:
        logger.error(f"Worker failed to scrape chapter {chapter_num}: {str(e)}")
        return ScrapingResult(
            success=False,
            data=None,
            error=f"Worker error: {str(e)}",
            url=f"{settings.base_chapter_url}{chapter_num}"
        )


class ChapterScraper:
    """Modern chapter scraper with robust error handling."""
    
    def __init__(self):
        """Initialize the chapter scraper."""
        self.http_pool = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=10.0, read=settings.request_timeout)
        )
        self.base_url = settings.base_chapter_url
        
    def __del__(self):
        """Clean up HTTP pool."""
        if hasattr(self, 'http_pool'):
            self.http_pool.clear()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((urllib3.exceptions.HTTPError, Exception))
    )
    def _fetch_page(self, url: str) -> bytes:
        """
        Fetch a web page with retry logic.
        
        Args:
            url: URL to fetch
            
        Returns:
            Raw HTML content
            
        Raises:
            Exception: If all retry attempts fail
        """
        try:
            logger.debug(f"Fetching URL: {url}")
            response = self.http_pool.urlopen("GET", url)
            
            if response.status != 200:
                raise Exception(f"HTTP {response.status}: {url}")
                
            return response.data
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            raise
    
    def _parse_chapter_info(self, soup: BeautifulSoup, chapter_number: int) -> Dict[str, Any]:
        """
        Parse chapter information from BeautifulSoup object.
        
        Args:
            soup: BeautifulSoup object of the page
            chapter_number: Chapter number being parsed
            
        Returns:
            Dictionary with chapter information
        """
        chapter_info = {"chapter_number": chapter_number}
        
        try:
            # Find the main info section
            chapter_sections = soup.findAll("section", {"class": "pi-item pi-group pi-border-color"})
            if not chapter_sections:
                logger.warning(f"No chapter info section found for chapter {chapter_number}")
                return chapter_info
                
            chapter_section = chapter_sections[0]
            
            # Extract basic information
            info_mapping = {
                "vol": "volume",
                "chapter": "chapter_display",
                "ename": "title",
                "page": "pages",
                "date2": "release_date",
                "jump": "jump_info"
            }
            
            for data_source, field_name in info_mapping.items():
                try:
                    elements = chapter_section.findAll("div", {"data-source": data_source})
                    if elements:
                        value_div = elements[0].findAll("div", {"class": "pi-data-value"})
                        if value_div:
                            value = value_div[0].text.strip()
                            # Clean up reference markers
                            if "[ref]" in value:
                                value = value.replace("[ref]", "").strip()
                            chapter_info[field_name] = value
                except (IndexError, AttributeError) as e:
                    logger.debug(f"Could not extract {field_name} for chapter {chapter_number}: {e}")
            
            # Parse characters
            chapter_info["characters"] = self._parse_characters(soup, chapter_number)
            
        except Exception as e:
            logger.error(f"Error parsing chapter {chapter_number}: {e}")
            
        return chapter_info
    
    def _parse_characters(self, soup: BeautifulSoup, chapter_number: int) -> List[Dict[str, str]]:
        """
        Parse character information from chapter page.
        
        Args:
            soup: BeautifulSoup object of the page
            chapter_number: Chapter number being parsed
            
        Returns:
            List of character dictionaries
        """
        characters = []
        
        try:
            character_tables = soup.findAll("table", {"class": "CharTable"})
            if not character_tables:
                logger.debug(f"No character table found for chapter {chapter_number}")
                return characters
                
            character_table = character_tables[0]
            char_items = character_table.findAll("li")
            
            for char_item in char_items:
                try:
                    full_text = char_item.text.rstrip("\n").strip()
                    if not full_text:
                        continue
                        
                    # Extract note from parentheses
                    note = ""
                    if "(" in full_text and ")" in full_text:
                        start = full_text.find("(")
                        end = full_text.find(")")
                        if start < end:
                            note = full_text[start + 1:end]
                    
                    # Extract character name and URL
                    char_links = char_item.findAll("a")
                    if char_links:
                        char_name = char_links[0].text.strip()
                        char_url = char_links[0].get("href", "")
                    else:
                        char_name = full_text
                        char_url = ""
                        logger.debug(f"No URL found for character in chapter {chapter_number}: {full_text}")
                    
                    if char_name:  # Only add if we have a name
                        character_data = {
                            "name": char_name,
                            "url": char_url,
                            "note": note,
                            "full_text": full_text
                        }
                        characters.append(character_data)
                        
                        if note:
                            logger.debug(f"Character note in chapter {chapter_number} - {char_name}: {note}")
                            
                except Exception as e:
                    logger.warning(f"Error parsing character in chapter {chapter_number}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error parsing characters for chapter {chapter_number}: {e}")
            
        return characters
    
    def scrape_chapter(self, chapter_number: int) -> ScrapingResult:
        """
        Scrape a single chapter.
        
        Args:
            chapter_number: Chapter number to scrape
            
        Returns:
            ScrapingResult with chapter data or error information
        """
        chapter_url = f"{self.base_url}{chapter_number}"
        
        try:
            logger.info(f"Scraping chapter {chapter_number}")
            
            # Add delay to be respectful to the server
            time.sleep(settings.scraping_delay)
            
            # Fetch the page
            html_content = self._fetch_page(chapter_url)
            
            # Parse the content
            soup = BeautifulSoup(html_content, "html.parser")
            chapter_info = self._parse_chapter_info(soup, chapter_number)
            
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
                       use_parallel: bool = False, max_workers: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Scrape multiple chapters with optional parallel processing.
        
        Args:
            start_chapter: Starting chapter number
            end_chapter: Ending chapter number (uses settings.last_chapter if None)
            use_parallel: Whether to use parallel processing
            max_workers: Maximum number of worker processes (defaults to config)
            
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
        
        if use_parallel and total_chapters > 1:
            return self._scrape_chapters_parallel(chapter_range, max_workers)
        else:
            return self._scrape_chapters_sequential(chapter_range)
    
    def _scrape_chapters_sequential(self, chapter_range: List[int]) -> List[Dict[str, Any]]:
        """Scrape chapters sequentially (original behavior)."""
        logger.info("Using sequential processing")
        
        successful_chapters = []
        failed_chapters = []
        
        for chapter_num in chapter_range:
            result = self.scrape_chapter(chapter_num)
            if result.success:
                successful_chapters.append(result.data)
            else:
                failed_chapters.append(chapter_num)
                
            # Add delay between requests when sequential
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
                    executor.submit(scrape_chapter_worker, chapter_num): chapter_num 
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
def scrap_chapters(last_chapter: int, output_path: str) -> None:
    """
    Legacy function for scraping chapters.
    
    Args:
        last_chapter: Last chapter number to scrape
        output_path: Path to save the JSON file
    """
    scraper = ChapterScraper()
    chapters = scraper.scrape_chapters(end_chapter=last_chapter)
    scraper.save_chapters(chapters, Path(output_path))
