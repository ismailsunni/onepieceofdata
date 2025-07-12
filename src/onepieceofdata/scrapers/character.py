"""Modern character scraper for One Piece characters."""

from typing import List, Optional, Dict, Any
import urllib3
import pandas as pd
import re
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from bs4 import BeautifulSoup, Tag
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..models.data import CharacterModel, ScrapingResult
from ..config.settings import get_settings


def scrape_character_worker(character_data: Dict[str, Any]) -> ScrapingResult:
    """
    Worker function for parallel character scraping.
    This function creates its own scraper instance to avoid sharing state.
    
    Args:
        character_data: Character data dictionary from CSV
        
    Returns:
        ScrapingResult with character data
    """
    try:
        scraper = CharacterScraper()
        result = scraper.scrape_character(character_data)
        return result
    except Exception as e:
        logger.error(f"Worker failed to scrape character {character_data.get('name', 'unknown')}: {str(e)}")
        return ScrapingResult(
            success=False,
            data=None,
            error=f"Worker error: {str(e)}",
            url=f"https://onepiece.fandom.com/wiki/{character_data.get('name', '')}"
        )


class CharacterScraper:
    """Modern character scraper with error handling and retry mechanisms."""
    
    def __init__(self):
        """Initialize the character scraper."""
        self.settings = get_settings()
        self.http_pool = urllib3.PoolManager()
        self.base_url = "https://onepiece.fandom.com"
        
    def _remove_footnote(self, text: str) -> str:
        """Remove footnote references from text.
        
        Args:
            text: Text to clean
            
        Returns:
            Cleaned text without footnotes
        """
        return re.sub(r"\[[0-9]+\]", "", text)
    
    def _remove_note(self, text: str) -> str:
        """Remove notes in parentheses from text.
        
        Args:
            text: Text to clean
            
        Returns:
            Text without notes
        """
        return text.split("(")[0].strip()
    
    def _parse_affiliation(self, div_element: Tag) -> List[str]:
        """Parse affiliation information from div element.
        
        Args:
            div_element: BeautifulSoup div element
            
        Returns:
            List of affiliations
        """
        a_elements = div_element.find_all("a")
        affiliations = []
        for a_element in a_elements:
            title = a_element.get("title")
            if title:
                affiliations.append(title)
        return affiliations
    
    def _parse_generic(self, div_element: Tag) -> List[str]:
        """Parse generic content from div element.
        
        Args:
            div_element: BeautifulSoup div element
            
        Returns:
            List of parsed values
        """
        text = self._remove_footnote(div_element.text)
        if ";" in text:
            return [x.strip() for x in text.split(";")]
        else:
            # Handle <br/> tags as separators
            new_div = BeautifulSoup(str(div_element).replace("<br/>", ";"), "html.parser")
            text = self._remove_footnote(new_div.text)
            return [x.strip() for x in text.split(";")]
    
    def _parse_list(self, div_element: Tag) -> List[str]:
        """Parse list content from div element.
        
        Args:
            div_element: BeautifulSoup div element
            
        Returns:
            List of values from li tags
        """
        li_tags = div_element.find_all("li")
        values = [x.text for x in li_tags]
        # Remove footnotes
        values = [self._remove_footnote(x) for x in values]
        return values
    
    def _parse_content(self, div_element: Tag) -> List[str]:
        """Parse content from div element (handles both lists and generic content).
        
        Args:
            div_element: BeautifulSoup div element
            
        Returns:
            List of parsed values
        """
        has_ul = bool(div_element.find("ul"))
        if has_ul:
            return self._parse_list(div_element)
        else:
            return self._parse_generic(div_element)
    
    def _parse_age(self, ages: List[str]) -> List[str]:
        """Parse age values removing notes.
        
        Args:
            ages: List of age strings
            
        Returns:
            Cleaned age values
        """
        return [self._remove_note(a) for a in ages]
    
    def _parse_height(self, heights: List[str]) -> List[str]:
        """Parse height values removing notes.
        
        Args:
            heights: List of height strings
            
        Returns:
            Cleaned height values
        """
        return [self._remove_note(h) for h in heights]
    
    def _parse_bounty(self, bounties: List[str]) -> List[str]:
        """Parse bounty values removing commas.
        
        Args:
            bounties: List of bounty strings
            
        Returns:
            Cleaned bounty values
        """
        return [b.replace(",", "") for b in bounties]
    
    def _parse_alias(self, aliases: List[str]) -> List[str]:
        """Parse alias values removing notes.
        
        Args:
            aliases: List of alias strings
            
        Returns:
            Cleaned alias values
        """
        return [self._remove_note(a) for a in aliases]
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((urllib3.exceptions.HTTPError, Exception)),
    )
    def _scrape_character_from_url(self, character_url: str) -> Dict[str, Any]:
        """Scrape character information from URL.
        
        Args:
            character_url: Full URL to character page
            
        Returns:
            Dictionary with character information
            
        Raises:
            Exception: If scraping fails after retries
        """
        logger.debug(f"Scraping character from: {character_url}")
        
        try:
            response = self.http_pool.urlopen("GET", character_url)
            if response.status != 200:
                raise Exception(f"HTTP {response.status}: Failed to fetch character page")
                
            html_page = response.data
            soup = BeautifulSoup(html_page, "html.parser")
            
            character_info = {}
            
            # Parse character name
            name_h2 = soup.find("h2", {"data-source": "name"})
            character_name = None
            if name_h2:
                character_name = name_h2.text.strip()
            
            character_info["name"] = character_name
            
            # Parse character sections
            character_sections = soup.findAll(
                "section",
                {"class": "pi-item pi-group pi-border-color pi-collapse pi-collapse-open"},
            )
            
            for character_section in character_sections:
                div_elements = character_section.find_all("div", {"data-source": True})
                
                for div_element in div_elements:
                    data_source = div_element.get("data-source")
                    div_content_elements = div_element.find_all(
                        "div", {"class": "pi-data-value pi-font"}
                    )
                    
                    if not div_content_elements:
                        continue
                        
                    div_content = div_content_elements[0]
                    
                    # Parse different data types
                    if data_source == "bounty":
                        value = self._parse_content(div_content)
                        value = self._parse_bounty(value)
                    elif data_source in ["alias", "epithet"]:
                        value = self._parse_content(div_content)
                        value = self._parse_alias(value)
                    elif data_source == "age":
                        value = self._parse_content(div_content)
                        value = self._parse_age(value)
                    elif data_source == "height":
                        value = self._parse_content(div_content)
                        value = self._parse_height(value)
                    elif data_source == "affiliation":
                        value = self._parse_affiliation(div_content)
                    else:
                        value = self._parse_content(div_content)
                    
                    character_info[data_source] = value
            
            logger.debug(f"Successfully scraped character: {character_name}")
            return character_info
            
        except urllib3.exceptions.HTTPError as e:
            logger.error(f"HTTP error scraping character from {character_url}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error scraping character from {character_url}: {str(e)}")
            raise
    
    def scrape_character(self, character_data: Dict[str, Any]) -> ScrapingResult:
        """Scrape a single character.
        
        Args:
            character_data: Dictionary with character info (id, name, url)
            
        Returns:
            ScrapingResult with character data
        """
        char_id = character_data.get("id")
        name = character_data.get("name", "Unknown")
        url = character_data.get("url")
        
        logger.info(f"Scraping character: {name} (ID: {char_id})")
        
        # Basic result structure
        result_data = {"id": char_id, "name": name}
        
        # Validate URL
        if not url or pd.isna(url):
            logger.warning(f"URL is null for {name}")
            return ScrapingResult(
                success=False,
                error=f"No URL provided for character {name}",
                data=result_data
            )
            
        if "http" in url or "Video_Games" in url:
            logger.warning(f"URL is outside onepiece wikia or a video game specific for {name}")
            return ScrapingResult(
                success=False,
                error=f"Invalid URL for character {name}: {url}",
                data=result_data
            )
        
        try:
            full_url = self.base_url + url
            character_info = self._scrape_character_from_url(full_url)
            result_data.update(character_info)
            
            # Try to create CharacterModel for validation
            try:
                character_model = CharacterModel(
                    name=character_info.get("name", name),
                    japanese_name=character_info.get("japanese_name"),
                    epithet=character_info.get("epithet", [None])[0] if character_info.get("epithet") else None,
                    affiliation=", ".join(character_info.get("affiliation", [])) if character_info.get("affiliation") else None,
                    occupation=character_info.get("occupation", [None])[0] if character_info.get("occupation") else None,
                    first_appearance=character_info.get("debut", [None])[0] if character_info.get("debut") else None,
                )
                logger.success(f"Successfully scraped and validated character: {character_model.name}")
            except Exception as validation_error:
                logger.warning(f"Character data validation failed for {name}: {validation_error}")
                # Continue anyway, validation is not critical
            
            return ScrapingResult(
                success=True,
                data=result_data,
                url=full_url
            )
            
        except Exception as e:
            logger.error(f"Failed to scrape character {name}: {str(e)}")
            return ScrapingResult(
                success=False,
                error=str(e),
                data=result_data,
                url=full_url if 'full_url' in locals() else None
            )
    
    def scrape_characters(self, characters_data: List[Dict[str, Any]], max_workers: int = 4) -> List[ScrapingResult]:
        """Scrape multiple characters.
        
        Args:
            characters_data: List of character dictionaries
            max_workers: Maximum number of concurrent workers (not used yet, keeping for future)
            
        Returns:
            List of ScrapingResult objects
        """
        logger.info(f"Scraping {len(characters_data)} characters")
        
        results = []
        
        for i, character_data in enumerate(characters_data, 1):
            result = self.scrape_character(character_data)
            results.append(result)
            
            # Log progress
            if i % 10 == 0:
                successful_count = sum(1 for r in results if r.success)
                logger.info(f"Progress: {i}/{len(characters_data)} characters processed ({successful_count} successful)")
        
        successful_count = sum(1 for r in results if r.success)
        logger.info(f"Character scraping completed: {successful_count}/{len(results)} characters successful")
        
        return results
    
    def load_characters_from_csv(self, csv_path: str) -> List[Dict[str, Any]]:
        """Load character data from CSV file.
        
        Args:
            csv_path: Path to characters CSV file
            
        Returns:
            List of character dictionaries
        """
        try:
            df = pd.read_csv(csv_path)
            characters_data = df.to_dict('records')
            logger.info(f"Loaded {len(characters_data)} characters from {csv_path}")
            return characters_data
        except Exception as e:
            logger.error(f"Failed to load characters from {csv_path}: {str(e)}")
            return []
    
    def scrape_characters_parallel(self, characters_data: List[Dict[str, Any]], 
                                 max_workers: Optional[int] = None) -> List[ScrapingResult]:
        """
        Scrape multiple characters using parallel processing.
        
        Args:
            characters_data: List of character data dictionaries
            max_workers: Maximum number of worker processes
            
        Returns:
            List of ScrapingResult objects
        """
        if max_workers is None:
            max_workers = self.settings.max_workers
            
        # Cap workers at reasonable limits
        actual_workers = min(max_workers, multiprocessing.cpu_count() - 1, 8)
        total_characters = len(characters_data)
        
        logger.info(f"Starting parallel character scraping with {actual_workers} workers")
        logger.info(f"Total characters to scrape: {total_characters}")
        
        results = []
        
        try:
            with ProcessPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all tasks
                future_to_character = {
                    executor.submit(scrape_character_worker, char_data): char_data 
                    for char_data in characters_data
                }
                
                # Process completed tasks
                for future in as_completed(future_to_character):
                    char_data = future_to_character[future]
                    try:
                        result = future.result()
                        results.append(result)
                        char_name = char_data.get('name', 'unknown')
                        if result.success:
                            logger.debug(f"Successfully scraped character: {char_name}")
                        else:
                            logger.error(f"Failed to scrape character: {char_name} - {result.error}")
                    except Exception as e:
                        char_name = char_data.get('name', 'unknown')
                        logger.error(f"Exception while processing character {char_name}: {str(e)}")
                        results.append(ScrapingResult(
                            success=False,
                            data=None,
                            error=f"Processing error: {str(e)}",
                            url=f"https://onepiece.fandom.com/wiki/{char_name}"
                        ))
                        
        except Exception as e:
            logger.error(f"Parallel processing failed: {str(e)}")
            # Fallback to sequential processing
            logger.info("Falling back to sequential processing")
            results = []
            for char_data in characters_data:
                result = self.scrape_character(char_data)
                results.append(result)
                time.sleep(0.5)  # Add delay for respectful scraping
        
        successful_count = sum(1 for r in results if r.success)
        logger.info(f"Successfully scraped {successful_count}/{len(results)} characters using parallel processing")
        
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
            
            logger.success(f"Exported {len(successful_results)} characters to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export characters to {output_path}: {str(e)}")
            return False
