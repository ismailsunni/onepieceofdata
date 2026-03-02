"""API-based arc scraper using Fandom MediaWiki API.

This uses the MediaWiki API to bypass Cloudflare protection.
"""

from typing import List, Optional, Dict, Any
import re
from bs4 import BeautifulSoup, Tag
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..api import FandomAPIClient
from ..models.data import ArcModel, ScrapingResult
from ..config.settings import get_settings
from ..utils.logging import get_logger


logger = get_logger(__name__)


class ArcScraper:
    """API-based arc scraper using Fandom MediaWiki API."""

    def __init__(self):
        """Initialize the API-based arc scraper."""
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
        """Clean arc title by removing extra whitespace and formatting.

        Args:
            title: Raw title string

        Returns:
            Cleaned title
        """
        if not title:
            return ""

        # Remove extra whitespace
        title = re.sub(r'\s+', ' ', title.strip())

        # Remove common suffixes like "Arc", "Saga"
        title = re.sub(r'\s+(Arc|Saga)\s*$', '', title, flags=re.IGNORECASE)

        return title.strip()

    def scrape_arcs_from_list_page(self) -> List[ScrapingResult]:
        """Scrape arcs from the One Piece story arcs list page.

        Returns:
            List of ScrapingResult objects containing arc data
        """
        url = f"{self.base_url}/wiki/Story_Arcs"
        results = []

        try:
            logger.info(f"Scraping arcs from: {url}")
            soup = self._fetch_story_arcs_page()

            content = soup.find('div', {'class': 'mw-parser-output'})
            if not content:
                raise Exception("Could not find main content area")

            # Arcs are under h4 tags
            arc_headers = content.find_all('h4')

            for header in arc_headers:
                span = header.find('span', {'class': 'mw-headline'})
                if not span:
                    continue

                title_link = span.find('a')
                title = title_link.get_text(strip=True) if title_link else span.get_text(strip=True)

                cleaned_title = self._clean_title(title)
                if not cleaned_title:
                    continue

                # Find chapter info in the next elements
                start_chapter, end_chapter = None, None
                next_element = header.find_next_sibling()

                # Search within a reasonable number of next siblings
                for _ in range(5):  # Look at next 5 siblings
                    if next_element is None:
                        break

                    if next_element.name == 'ul':
                        lis = next_element.find_all('li')
                        for li in lis:
                            li_text = li.get_text()
                            if 'Chapters' in li_text:
                                start_chapter, end_chapter = self._extract_chapter_range(li_text)
                                if start_chapter is not None:
                                    break
                        if start_chapter is not None:
                            break

                    if next_element.name in ['h2', 'h3', 'h4']:
                        break

                    next_element = next_element.find_next_sibling()

                if start_chapter is not None and end_chapter is not None:
                    arc_id = re.sub(r'[^a-zA-Z0-9]+', '_', cleaned_title.lower()).strip('_')
                    arc_data = ArcModel(
                        arc_id=arc_id,
                        title=cleaned_title,
                        start_chapter=start_chapter,
                        end_chapter=end_chapter
                    )
                    results.append(ScrapingResult(
                        success=True,
                        data=arc_data.model_dump(),
                        url=url
                    ))
                else:
                    # This is expected for some h4s that are not arcs, so no warning
                    pass

            if not results:
                logger.warning("No arcs were scraped. The page structure might have changed.")

            logger.success(f"Successfully scraped {len(results)} arcs")
            return results

        except Exception as e:
            logger.error(f"Failed to scrape arcs: {str(e)}")
            return [ScrapingResult(
                success=False,
                data=None,
                error=str(e),
                url=url
            )]

    def scrape_all_arcs(self) -> List[ScrapingResult]:
        """Scrape all arcs from various sources.

        Returns:
            List of ScrapingResult objects containing all arc data
        """
        logger.info("Starting comprehensive arc scraping")

        all_results = []

        # Scrape from main story arcs page
        list_results = self.scrape_arcs_from_list_page()
        all_results.extend(list_results)

        logger.success(f"Completed arc scraping. Total arcs: {len([r for r in all_results if r.success])}")

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

            logger.success(f"Exported {len(successful_results)} arcs to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export arcs to {output_path}: {str(e)}")
            return False

    def cleanup(self):
        """Clean up resources."""
        if hasattr(self.api_client, 'http_pool') and hasattr(self.api_client.http_pool, 'clear'):
            self.api_client.http_pool.clear()
