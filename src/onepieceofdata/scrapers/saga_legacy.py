"""Modern saga scraper for One Piece story sagas."""

from typing import List, Optional, Dict, Any
import urllib3
import re
from bs4 import BeautifulSoup, Tag
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..models.data import SagaModel, ScrapingResult
from ..config.settings import get_settings


class SagaScraper:
    """Modern saga scraper with error handling and retry mechanisms."""
    
    def __init__(self):
        """Initialize the saga scraper."""
        self.settings = get_settings()
        self.http_pool = urllib3.PoolManager()
        self.base_url = "https://onepiece.fandom.com"
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((urllib3.exceptions.HTTPError, Exception))
    )
    def _fetch_page(self, url: str) -> BeautifulSoup:
        """Fetch and parse a web page with retry logic.

        Args:
            url: URL to fetch

        Returns:
            BeautifulSoup parsed page
        """
        logger.debug(f"Fetching page: {url}")

        response = self.http_pool.request(
            'GET',
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            },
            decode_content=True
        )

        if response.status != 200:
            raise Exception(f"HTTP {response.status} error for {url}")

        soup = BeautifulSoup(response.data, 'html.parser')
        logger.debug(f"Successfully parsed page: {url}")

        return soup
        
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
            soup = self._fetch_page(url)
            
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
        
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self.http_pool, 'clear'):
            self.http_pool.clear()
