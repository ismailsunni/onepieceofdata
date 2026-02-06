"""API-based saga scraper using Fandom MediaWiki API.

This uses the MediaWiki API to bypass Cloudflare protection.
"""

from typing import List, Optional, Dict, Any
import re
from bs4 import BeautifulSoup, Tag
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..api import FandomAPIClient
from ..models.data import SagaModel, ScrapingResult
from ..config.settings import get_settings
from ..utils.logging import get_logger


logger = get_logger(__name__)


class SagaScraper:
    """API-based saga scraper using Fandom MediaWiki API."""

    def __init__(self):
        """Initialize the API-based saga scraper."""
        self.settings = get_settings()
        self.api_client = FandomAPIClient(wiki="onepiece")
        self.base_url = "https://onepiece.fandom.com"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception)
    )
    def _fetch_story_arcs_page(self) -> BeautifulSoup:
        """Fetch the Story Arcs page with retry logic.

        Returns:
            BeautifulSoup object of the Story Arcs page

        Raises:
            Exception: If all retry attempts fail
        """
        logger.info(f"Fetching Story Arcs page via API")

        try:
            # Use API to get rendered HTML
            html = self.api_client.get_page_html("Story_Arcs")

            if not html:
                raise Exception("Failed to fetch Story Arcs page HTML")

            soup = BeautifulSoup(html, "html.parser")
            logger.debug("Successfully fetched and parsed Story Arcs page")

            return soup

        except Exception as e:
            logger.error(f"Error fetching Story Arcs page: {str(e)}")
            raise

    def _extract_chapter_range(self, text: str) -> tuple[Optional[int], Optional[int]]:
        """Extract chapter range from text.

        Args:
            text: Text containing chapter information

        Returns:
            Tuple of (start_chapter, end_chapter)
        """
        match = re.search(r'chapters:\s*\d+\s*\((\d+)(?:-(\d*))?\)', text, re.IGNORECASE)
        if match:
            start = int(match.group(1))
            end_str = match.group(2)
            if end_str is None:
                end = start
            elif end_str == '':
                end = self.settings.last_chapter
            else:
                end = int(end_str)
            return start, end

        match = re.search(r'\((\d+)-(\d+)\)', text)
        if match:
            return int(match.group(1)), int(match.group(2))

        match = re.search(r'\((\d+)\)', text)
        if match:
            start = int(match.group(1))
            return start, start

        logger.warning(f"Could not extract chapter range from text: {text}")
        return None, None

    def _clean_title(self, title: str) -> str:
        """Clean saga title by removing extra whitespace and formatting.

        Args:
            title: Raw title string

        Returns:
            Cleaned title
        """
        if not title:
            return ""
        title = re.sub(r'\s+', ' ', title.strip())
        title = re.sub(r'\s+(Saga)\s*$', '', title, flags=re.IGNORECASE)
        return title.strip()

    def scrape_sagas_from_list_page(self) -> List[ScrapingResult]:
        """Scrape sagas from the One Piece story arcs list page.

        Returns:
            List of ScrapingResult objects containing saga data
        """
        url = f"{self.base_url}/wiki/Story_Arcs"
        results = []

        try:
            logger.info(f"Scraping sagas from: {url}")
            soup = self._fetch_story_arcs_page()

            content = soup.find('div', {'class': 'mw-parser-output'})
            if not content:
                raise Exception("Could not find main content area")

            # Sagas are under h3 tags
            saga_headers = content.find_all('h3')

            for header in saga_headers:
                span = header.find('span', {'class': 'mw-headline'})
                if not span:
                    continue

                title = span.get_text(strip=True)
                cleaned_title = self._clean_title(title)
                if not cleaned_title:
                    continue

                min_start_chapter, max_end_chapter = float('inf'), float('-inf')

                # Find all arcs (h4) under this saga (h3)
                next_element = header.find_next_sibling()
                while next_element and next_element.name != 'h3':
                    if next_element.name == 'h4':
                        # This is an arc header, find its chapter info
                        arc_ul = next_element.find_next_sibling('ul')
                        if arc_ul:
                            lis = arc_ul.find_all('li')
                            for li in lis:
                                li_text = li.get_text()
                                if 'Chapters' in li_text:
                                    start_ch, end_ch = self._extract_chapter_range(li_text)
                                    if start_ch is not None:
                                        min_start_chapter = min(min_start_chapter, start_ch)
                                    if end_ch is not None:
                                        max_end_chapter = max(max_end_chapter, end_ch)
                                    break # Assume first chapter info is correct
                    next_element = next_element.find_next_sibling()

                if min_start_chapter != float('inf') and max_end_chapter != float('-inf'):
                    saga_id = re.sub(r'[^a-zA-Z0-9]+', '_', cleaned_title.lower()).strip('_')
                    saga_data = SagaModel(
                        saga_id=saga_id,
                        title=cleaned_title,
                        start_chapter=int(min_start_chapter),
                        end_chapter=int(max_end_chapter)
                    )
                    results.append(ScrapingResult(
                        success=True,
                        data=saga_data.model_dump(),
                        url=url
                    ))
                else:
                    logger.warning(f"Could not determine chapter range for saga: {cleaned_title}")

            if not results:
                logger.warning("No sagas were scraped. The page structure might have changed.")

            logger.success(f"Successfully scraped {len(results)} sagas")
            return results

        except Exception as e:
            logger.error(f"Failed to scrape sagas: {str(e)}")
            return [ScrapingResult(
                success=False,
                data=None,
                error=str(e),
                url=url
            )]

    def scrape_all_sagas(self) -> List[ScrapingResult]:
        """Scrape all sagas from various sources.

        Returns:
            List of ScrapingResult objects containing all saga data
        """
        logger.info("Starting comprehensive saga scraping")
        all_results = self.scrape_sagas_from_list_page()
        logger.success(f"Completed saga scraping. Total sagas: {len([r for r in all_results if r.success])}")
        return all_results

    def export_to_json(self, results: List[ScrapingResult], output_path: str) -> bool:
        """Export scraping results to JSON file.

        Args:
            results: List of scraping results
            output_path: Path to output JSON file

        Returns:
            True if export successful, False otherwise
        """
        try:
            import json

            successful_results = [r.data for r in results if r.success and r.data]

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(successful_results, f, indent=2, ensure_ascii=False)

            logger.success(f"Exported {len(successful_results)} sagas to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export sagas to {output_path}: {str(e)}")
            return False

    def cleanup(self):
        """Clean up resources."""
        if hasattr(self.api_client, 'http_pool') and hasattr(self.api_client.http_pool, 'clear'):
            self.api_client.http_pool.clear()
