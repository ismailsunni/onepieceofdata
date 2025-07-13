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
        # Look for patterns like "Chapter 1-100", "Chapters 101-200", etc.
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
        """Clean saga title by removing extra whitespace and formatting.
        
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
        
    def scrape_sagas_from_list_page(self) -> List[ScrapingResult]:
        """Scrape sagas from the One Piece story arcs/sagas list page.
        
        Returns:
            List of ScrapingResult objects containing saga data
        """
        url = f"{self.base_url}/wiki/Story_Arcs"
        results = []
        
        try:
            logger.info(f"Scraping sagas from: {url}")
            soup = self._fetch_page(url)
            
            # Find the main content area
            content = soup.find('div', {'class': 'mw-parser-output'})
            if not content:
                raise Exception("Could not find main content area")
                
            # Look for section headers that indicate sagas
            headers = content.find_all(['h2', 'h3'], string=re.compile(r'Saga|saga', re.IGNORECASE))
            
            for header in headers:
                try:
                    saga_data = self._extract_saga_from_section(header)
                    if saga_data:
                        results.append(ScrapingResult(
                            success=True,
                            data=saga_data.model_dump(),
                            url=url
                        ))
                except Exception as e:
                    logger.warning(f"Failed to extract saga from section: {str(e)}")
                    continue
                    
            # Also look for saga information in navboxes or infoboxes
            navboxes = content.find_all('table', {'class': re.compile(r'navbox|infobox')})
            for navbox in navboxes:
                try:
                    saga_results = self._extract_sagas_from_navbox(navbox)
                    results.extend(saga_results)
                except Exception as e:
                    logger.warning(f"Failed to extract sagas from navbox: {str(e)}")
                    continue
                        
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
            
    def _extract_saga_from_section(self, header: Tag) -> Optional[SagaModel]:
        """Extract saga data from a section header and its content.
        
        Args:
            header: Section header tag
            
        Returns:
            SagaModel if extraction successful, None otherwise
        """
        try:
            # Extract title from header
            title = header.get_text(strip=True)
            title = self._clean_title(title)
            
            if not title or len(title) < 3:
                return None
                
            # Find the next sibling elements to get chapter information
            current = header.next_sibling
            chapter_text = ""
            
            while current and current.name not in ['h2', 'h3']:
                if hasattr(current, 'get_text'):
                    chapter_text += current.get_text()
                current = current.next_sibling
                
            # Extract chapter range
            start_chapter, end_chapter = self._extract_chapter_range(chapter_text)
            
            if start_chapter is None or end_chapter is None:
                logger.debug(f"Could not extract chapter range for saga: {title}")
                return None
                
            # Create saga ID from title
            saga_id = re.sub(r'[^a-zA-Z0-9]+', '_', title.lower()).strip('_')
            
            return SagaModel(
                saga_id=saga_id,
                title=title,
                start_chapter=start_chapter,
                end_chapter=end_chapter
            )
            
        except Exception as e:
            logger.warning(f"Failed to extract saga from section: {str(e)}")
            return None
            
    def _extract_sagas_from_navbox(self, navbox: Tag) -> List[ScrapingResult]:
        """Extract saga data from a navigation box.
        
        Args:
            navbox: Navigation box table
            
        Returns:
            List of ScrapingResult objects
        """
        results = []
        
        try:
            # Look for rows that contain saga information
            rows = navbox.find_all('tr')
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                
                for cell in cells:
                    cell_text = cell.get_text(strip=True)
                    
                    # Check if this cell contains saga information
                    if re.search(r'saga', cell_text, re.IGNORECASE):
                        try:
                            saga_data = self._extract_saga_from_cell(cell)
                            if saga_data:
                                results.append(ScrapingResult(
                                    success=True,
                                    data=saga_data.model_dump(),
                                    url=self.base_url + "/wiki/Story_Arcs"
                                ))
                        except Exception as e:
                            logger.debug(f"Failed to extract saga from cell: {str(e)}")
                            continue
                            
        except Exception as e:
            logger.warning(f"Failed to extract sagas from navbox: {str(e)}")
            
        return results
        
    def _extract_saga_from_cell(self, cell: Tag) -> Optional[SagaModel]:
        """Extract saga data from a table cell.
        
        Args:
            cell: Table cell tag
            
        Returns:
            SagaModel if extraction successful, None otherwise
        """
        try:
            # Extract title
            links = cell.find_all('a')
            title = None
            
            for link in links:
                link_text = link.get_text(strip=True)
                if re.search(r'saga', link_text, re.IGNORECASE):
                    title = self._clean_title(link_text)
                    break
                    
            if not title:
                cell_text = cell.get_text(strip=True)
                if re.search(r'saga', cell_text, re.IGNORECASE):
                    title = self._clean_title(cell_text)
                    
            if not title or len(title) < 3:
                return None
                
            # Try to extract chapter information from the cell or nearby cells
            all_text = cell.get_text()
            start_chapter, end_chapter = self._extract_chapter_range(all_text)
            
            # If we can't find chapters in this cell, use placeholder values
            # These should be updated manually or through additional scraping
            if start_chapter is None or end_chapter is None:
                logger.debug(f"Could not extract chapter range for saga: {title}")
                return None
                
            # Create saga ID from title
            saga_id = re.sub(r'[^a-zA-Z0-9]+', '_', title.lower()).strip('_')
            
            return SagaModel(
                saga_id=saga_id,
                title=title,
                start_chapter=start_chapter,
                end_chapter=end_chapter
            )
            
        except Exception as e:
            logger.debug(f"Failed to extract saga from cell: {str(e)}")
            return None
            
    def scrape_all_sagas(self) -> List[ScrapingResult]:
        """Scrape all sagas from various sources.
        
        Returns:
            List of ScrapingResult objects containing all saga data
        """
        logger.info("Starting comprehensive saga scraping")
        
        all_results = []
        
        # Scrape from main story arcs page
        list_results = self.scrape_sagas_from_list_page()
        all_results.extend(list_results)
        
        logger.success(f"Completed saga scraping. Total sagas: {len([r for r in all_results if r.success])}")
        
        return all_results
        
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self.http_pool, 'clear'):
            self.http_pool.clear()
