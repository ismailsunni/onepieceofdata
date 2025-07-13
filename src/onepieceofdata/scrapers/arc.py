"""Modern arc scraper for One Piece story arcs."""

from typing import List, Optional, Dict, Any
import urllib3
import re
from bs4 import BeautifulSoup, Tag
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..models.data import ArcModel, ScrapingResult
from ..config.settings import get_settings


class ArcScraper:
    """Modern arc scraper with error handling and retry mechanisms."""
    
    def __init__(self):
        """Initialize the arc scraper."""
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
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
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
        # Look for patterns like "Chapter 1-3", "Chapters 4-7", "Chapter 8", etc.
        patterns = [
            r'Chapter(?:s)?\s+(\d+)(?:-(\d+))?',
            r'Ch\.?\s*(\d+)(?:-(\d+))?',
            r'(\d+)(?:-(\d+))?\s*(?:chapters?|ch\.?)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                start = int(match.group(1))
                end = int(match.group(2)) if match.group(2) else start
                return start, end
                
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
            soup = self._fetch_page(url)
            
            # Find the main content area
            content = soup.find('div', {'class': 'mw-parser-output'})
            if not content:
                raise Exception("Could not find main content area")
                
            # Look for tables containing arc information
            tables = content.find_all('table', {'class': 'wikitable'})
            
            for table in tables:
                rows = table.find_all('tr')[1:]  # Skip header row
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 2:
                        continue
                        
                    try:
                        arc_data = self._extract_arc_from_row(cells)
                        if arc_data:
                            results.append(ScrapingResult(
                                success=True,
                                data=arc_data.model_dump(),
                                url=url
                            ))
                    except Exception as e:
                        logger.warning(f"Failed to extract arc from row: {str(e)}")
                        continue
                        
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
            
    def _extract_arc_from_row(self, cells: List[Tag]) -> Optional[ArcModel]:
        """Extract arc data from a table row.
        
        Args:
            cells: Table cells from a row
            
        Returns:
            ArcModel if extraction successful, None otherwise
        """
        try:
            # Typically: Arc Name | Chapters | Description
            title_cell = cells[0]
            chapter_cell = cells[1] if len(cells) > 1 else None
            
            # Extract title
            title_link = title_cell.find('a')
            if title_link:
                title = title_link.get_text(strip=True)
            else:
                title = title_cell.get_text(strip=True)
                
            title = self._clean_title(title)
            if not title:
                return None
                
            # Extract chapter range
            chapter_text = chapter_cell.get_text(strip=True) if chapter_cell else ""
            start_chapter, end_chapter = self._extract_chapter_range(chapter_text)
            
            if start_chapter is None or end_chapter is None:
                logger.warning(f"Could not extract chapter range for arc: {title}")
                return None
                
            # Create arc ID from title
            arc_id = re.sub(r'[^a-zA-Z0-9]+', '_', title.lower()).strip('_')
            
            return ArcModel(
                arc_id=arc_id,
                title=title,
                start_chapter=start_chapter,
                end_chapter=end_chapter
            )
            
        except Exception as e:
            logger.warning(f"Failed to extract arc from row: {str(e)}")
            return None
            
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
        
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self.http_pool, 'clear'):
            self.http_pool.clear()
