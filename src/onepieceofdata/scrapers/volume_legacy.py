"""Modern volume scraper for One Piece volumes."""

from typing import List, Optional, Dict, Any
import cloudscraper
from bs4 import BeautifulSoup, Tag
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..models.data import VolumeModel, ScrapingResult
from ..config.settings import get_settings


class VolumeScraper:
    """Modern volume scraper with error handling and retry mechanisms."""
    
    def __init__(self):
        """Initialize the volume scraper."""
        self.settings = get_settings()
        self.base_url = "https://onepiece.fandom.com/wiki/Chapters_and_Volumes/Volumes"

    def _create_scraper(self):
        """Create a new cloudscraper instance for each request."""
        return cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
    def _parse_volume_table(self, soup: BeautifulSoup, volume_number: int) -> Optional[Dict[str, Any]]:
        """Parse volume information from the HTML table.
        
        Args:
            soup: BeautifulSoup object of the page
            volume_number: Volume number to parse
            
        Returns:
            Dictionary with volume data or None if parsing fails
        """
        logger.debug(f"Parsing volume {volume_number}")
        
        try:
            volume_table = soup.find("table", id=f"Volume_{volume_number}")
            if not volume_table:
                logger.warning(f"Volume table not found for volume {volume_number}")
                return None
                
            rows = volume_table.findAll("tr")
            if len(rows) < 4:
                logger.warning(f"Insufficient rows in volume table for volume {volume_number}")
                return None
                
            # Parse English title
            english_title = ""
            japanese_title = ""
            if len(rows) >= 4:
                row = rows[3]
                cells = row.findAll(["td", "th"])
                if len(cells) > 1:
                    title = cells[1].get_text()
                    english_title = title.strip()
                    
            # Parse Japanese title if available
            if len(rows) >= 3:
                row = rows[2]
                cells = row.findAll(["td", "th"])
                if len(cells) > 1:
                    japanese_title = cells[1].get_text().strip()
                    
            # Parse cover characters
            cover_characters = []
            if len(rows) >= 5:
                row = rows[4]
                cells = row.findAll(["td", "th"])
                if len(cells) > 1:
                    characters_cells = cells[1]
                    chars_href = characters_cells.findAll("li")
                    for c in chars_href:
                        a_tags = c.findAll("a")
                        character_name = c.get_text().strip()
                        
                        if len(a_tags) == 0:
                            character_slug = character_name
                            logger.debug(f"No a tag for character: {character_name}")
                        else:
                            href = a_tags[0].get("href", "")
                            character_slug = href.split("/")[-1] if href else character_name
                            
                        if character_name:  # Only add if name exists
                            cover_characters.append({
                                "name": character_name,
                                "slug": character_slug
                            })
                            
            return {
                "volume_number": volume_number,
                "english_title": english_title,
                "japanese_title": japanese_title,
                "cover_characters": cover_characters,
            }
            
        except Exception as e:
            logger.error(f"Failed to parse volume {volume_number}: {str(e)}")
            return None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception),
    )
    def _fetch_volumes_page(self) -> BeautifulSoup:
        """Fetch the volumes page with retry logic.
        
        Returns:
            BeautifulSoup object of the volumes page
            
        Raises:
            Exception: If all retry attempts fail
        """
        logger.info(f"Fetching volumes page: {self.base_url}")

        try:
            # Create a new scraper for each request to bypass Cloudflare session blocking
            scraper = self._create_scraper()
            response = scraper.get(self.base_url, timeout=self.settings.request_timeout)
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: Failed to fetch volumes page")

            html_page = response.content
            soup = BeautifulSoup(html_page, "html.parser")

            logger.debug("Successfully fetched and parsed volumes page")
            return soup

        except Exception as e:
            logger.error(f"HTTP error fetching volumes page: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching volumes page: {str(e)}")
            raise
    
    def scrape_volume(self, volume_number: int) -> ScrapingResult:
        """Scrape a single volume.
        
        Args:
            volume_number: Volume number to scrape
            
        Returns:
            ScrapingResult with volume data
        """
        logger.info(f"Scraping volume {volume_number}")
        
        try:
            soup = self._fetch_volumes_page()
            volume_data = self._parse_volume_table(soup, volume_number)
            
            if volume_data is None:
                return ScrapingResult(
                    success=False,
                    error=f"Failed to parse volume {volume_number}",
                    url=self.base_url
                )
            
            # Validate with Pydantic model
            volume_model = VolumeModel(
                volume_number=volume_data["volume_number"],
                title=volume_data["english_title"],
                japanese_title=volume_data.get("japanese_title"),
            )
            
            logger.success(f"Successfully scraped volume {volume_number}: {volume_model.title}")
            
            return ScrapingResult(
                success=True,
                data=volume_data,
                url=self.base_url
            )
            
        except Exception as e:
            logger.error(f"Failed to scrape volume {volume_number}: {str(e)}")
            return ScrapingResult(
                success=False,
                error=str(e),
                url=self.base_url
            )
    
    def scrape_volumes(self, start_volume: int = 1, end_volume: Optional[int] = None) -> List[ScrapingResult]:
        """Scrape multiple volumes.
        
        Args:
            start_volume: First volume to scrape
            end_volume: Last volume to scrape (defaults to last volume in settings)
            
        Returns:
            List of ScrapingResult objects
        """
        if end_volume is None:
            end_volume = self.settings.last_volume
            
        logger.info(f"Scraping volumes {start_volume} to {end_volume}")
        
        results = []
        soup = None
        
        try:
            # Fetch page once for all volumes
            soup = self._fetch_volumes_page()
        except Exception as e:
            logger.error(f"Failed to fetch volumes page: {str(e)}")
            return [ScrapingResult(
                success=False,
                error=f"Failed to fetch volumes page: {str(e)}",
                url=self.base_url
            )]
        
        for volume_number in range(start_volume, end_volume + 1):
            try:
                volume_data = self._parse_volume_table(soup, volume_number)
                
                if volume_data is None:
                    result = ScrapingResult(
                        success=False,
                        error=f"Failed to parse volume {volume_number}",
                        url=self.base_url
                    )
                else:
                    # Validate with Pydantic model
                    volume_model = VolumeModel(
                        volume_number=volume_data["volume_number"],
                        title=volume_data["english_title"],
                        japanese_title=volume_data.get("japanese_title"),
                    )
                    
                    result = ScrapingResult(
                        success=True,
                        data=volume_data,
                        url=self.base_url
                    )
                    
                    logger.success(f"Volume {volume_number}: {volume_model.title}")
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Failed to scrape volume {volume_number}: {str(e)}")
                results.append(ScrapingResult(
                    success=False,
                    error=str(e),
                    url=self.base_url
                ))
        
        successful_count = sum(1 for r in results if r.success)
        logger.info(f"Scraping completed: {successful_count}/{len(results)} volumes successful")
        
        return results
    
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
            
            logger.success(f"Exported {len(successful_results)} volumes to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export volumes to {output_path}: {str(e)}")
            return False
