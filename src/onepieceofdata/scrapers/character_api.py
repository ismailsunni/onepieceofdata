"""API-based character scraper using Fandom MediaWiki API.

This replaces the cloudscraper-based character.py with API access.
"""

import json
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..api import FandomAPIClient, WikitextParser
from ..config.settings import get_settings
from ..models.data import CharacterModel, ScrapingResult
from ..utils.logging import get_logger


logger = get_logger(__name__)


def scrape_character_worker_api(character_data: Dict[str, Any]) -> ScrapingResult:
    """
    Worker function for parallel character scraping using API.

    Args:
        character_data: Character data dictionary

    Returns:
        ScrapingResult with character data
    """
    try:
        scraper = CharacterScraperAPI()
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


class CharacterScraperAPI:
    """API-based character scraper using Fandom MediaWiki API."""

    def __init__(self):
        """Initialize the API-based character scraper."""
        self.settings = get_settings()
        self.api_client = FandomAPIClient(wiki="onepiece")
        self.parser = WikitextParser()
        self.base_url = "https://onepiece.fandom.com"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(Exception)
    )
    def _fetch_character_wikitext(self, character_name: str) -> Optional[str]:
        """
        Fetch character wikitext from API with retry logic.

        Args:
            character_name: Character name (used in page title)

        Returns:
            Raw wikitext content or None if failed
        """
        try:
            wikitext = self.api_client.get_page_wikitext(character_name)
            if not wikitext:
                raise Exception(f"No wikitext returned for {character_name}")
            return wikitext
        except Exception as e:
            logger.debug(f"Failed to fetch wikitext for {character_name}: {e}")
            raise

    def _parse_character_infobox_from_html(self, html: str, character_name: str) -> Dict[str, Any]:
        """
        Parse character infobox from rendered HTML.

        Character pages use portable infoboxes that are easier to parse from HTML.

        Args:
            html: Rendered HTML content
            character_name: Character name for logging

        Returns:
            Dictionary with character information
        """
        from bs4 import BeautifulSoup

        character_info = {}

        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Find portable infobox
            infobox = soup.find('aside', {'class': 'portable-infobox'})
            if not infobox:
                logger.debug(f"No portable infobox found for {character_name}")
                return character_info

            # Find all data fields
            fields = infobox.find_all('div', {'data-source': True})

            if not fields:
                logger.debug(f"No fields found in infobox for {character_name}")
                return character_info

            logger.debug(f"Found {len(fields)} infobox fields for {character_name}")

            # Map HTML data-source fields to our schema
            field_mapping = {
                # Names
                "jname": "japanese_name",
                "rname": "romanized_name",
                "ename": "english_name",

                # Basic info
                "alias": "alias",
                "epithet": "epithet",
                "age": "age",
                "birth": "birthday",
                "height": "height",
                "bounty": "bounty",

                # Affiliations and roles
                "affiliation": "affiliation",
                "occupation": "occupation",
                "residence": "residence",
                "origin": "origin",

                # Debut
                "first": "first_appearance",
                "debut": "debut",

                # Additional fields
                "status": "status",
                "blood": "blood_type",

                # Devil fruit fields
                "dfname": "devil_fruit_name",
                "dfename": "devil_fruit_english_name",
                "dfmeaning": "devil_fruit_meaning",
                "dftype": "devil_fruit_type",
                # Second devil fruit (e.g. Marshall D. Teach)
                "dfname2": "devil_fruit_name_2",
                "dfename2": "devil_fruit_english_name_2",
                "dfmeaning2": "devil_fruit_meaning_2",
                "dftype2": "devil_fruit_type_2",
            }

            for field in fields:
                source = field.get('data-source')

                if source in field_mapping:
                    value_div = field.find('div', {'class': 'pi-data-value'})
                    if value_div:
                        value = value_div.get_text(strip=True)

                        # Clean up reference markers
                        if '[' in value:
                            # Remove [1], [2], etc
                            import re
                            value = re.sub(r'\[\d+\]', '', value)

                        output_key = field_mapping[source]
                        character_info[output_key] = value

            # Extract intro text: first <p> tags before any section headings.
            try:
                content_div = soup.find('div', {'class': 'mw-parser-output'})
                if content_div:
                    intro_paragraphs = []
                    for tag in content_div.children:
                        if hasattr(tag, 'name'):
                            if tag.name in ('h2', 'h3'):
                                break
                            if tag.name == 'p':
                                text = tag.get_text(separator=' ', strip=True)
                                if text and not text.startswith(':'):
                                    intro_paragraphs.append(text)
                    if intro_paragraphs:
                        character_info['intro_text'] = ' '.join(intro_paragraphs)
            except Exception as intro_err:
                logger.debug(f"Could not extract intro text for {character_name}: {intro_err}")

        except Exception as e:
            logger.error(f"Error parsing HTML infobox for {character_name}: {e}")

        return character_info

    def scrape_character(self, character_data: Dict[str, Any]) -> ScrapingResult:
        """
        Scrape a single character using the API.

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
                success=True,  # Include in final data
                error=f"No URL provided for character {name}",
                data=result_data,
                url=""
            )

        if "http" in url or "Video_Games" in url:
            logger.warning(f"URL is outside onepiece wikia or a video game specific for {name}")
            return ScrapingResult(
                success=True,  # Include in final data
                error=f"Invalid URL for character {name}: {url}",
                data=result_data,
                url=url
            )

        try:
            # Extract page title from URL
            if url.startswith("/wiki/"):
                page_title = url.replace("/wiki/", "").replace("_", " ")
            else:
                page_title = name

            # Add delay
            time.sleep(self.settings.scraping_delay)

            # Fetch HTML (for character pages, HTML portable infobox is easier to parse)
            html = self.api_client.get_page_html(page_title)

            if not html:
                raise Exception(f"Failed to fetch HTML for {name}")

            # Parse character infobox from HTML
            character_info = self._parse_character_infobox_from_html(html, name)
            result_data.update(character_info)

            # Ensure name is set
            if "name" not in result_data or not result_data["name"]:
                result_data["name"] = name

            logger.debug(f"Successfully scraped character: {name}")

            return ScrapingResult(
                success=True,
                data=result_data,
                url=self.base_url + url
            )

        except Exception as e:
            logger.error(f"Failed to scrape character {name}: {str(e)}")
            return ScrapingResult(
                success=False,
                error=str(e),
                data=result_data,
                url=self.base_url + url if url else None
            )

    def scrape_characters(self, characters_data: List[Dict[str, Any]],
                         max_workers: int = 4) -> List[ScrapingResult]:
        """
        Scrape multiple characters sequentially.

        Args:
            characters_data: List of character dictionaries
            max_workers: Not used in sequential mode (kept for compatibility)

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
                    executor.submit(scrape_character_worker_api, char_data): char_data
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

    def export_to_json(self, results: List[ScrapingResult], output_path: str) -> bool:
        """Export scraping results to JSON file.

        Args:
            results: List of scraping results
            output_path: Path to output JSON file

        Returns:
            True if export successful, False otherwise
        """
        try:
            successful_results = [r.data for r in results if r.success and r.data]

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(successful_results, f, indent=2, ensure_ascii=False)

            logger.success(f"Exported {len(successful_results)} characters to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export characters to {output_path}: {str(e)}")
            return False
