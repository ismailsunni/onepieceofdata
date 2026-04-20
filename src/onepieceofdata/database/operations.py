"""Modern database operations for One Piece of Data."""

import json
from typing import List, Dict, Any, Optional, Callable
from pathlib import Path
from datetime import datetime
import duckdb
import pandas as pd
from loguru import logger
import re

from ..models.data import ChapterModel, VolumeModel, CharacterModel, ArcModel, SagaModel, ScrapingResult
from ..config.settings import get_settings
from ..utils.birth_parser import BirthDateParser


class DatabaseManager:
    """Modern database manager with DuckDB backend."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize the database manager.
        
        Args:
            db_path: Path to DuckDB database file. If None, uses settings default.
        """
        self.settings = get_settings()
        self.db_path = db_path or self.settings.database_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date from 'Month Day, Year' format to 'YYYY-MM-DD'."""
        if not date_str:
            return None
        
        try:
            # Parse dates like "July 19, 1997"
            parsed_date = datetime.strptime(date_str, "%B %d, %Y")
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            try:
                # Try alternative format like "July 19,1997" (no space after comma)
                parsed_date = datetime.strptime(date_str, "%B %d,%Y")
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                logger.warning(f"Could not parse date: {date_str}")
                return None
    
    def _parse_page_number(self, pages_str: str) -> Optional[int]:
        """Parse page number from complex strings like '19 (Excluding cover page)*'."""
        if not pages_str:
            return None
        
        # Handle special cases
        if "Volume" in pages_str:
            # Chapter 999 has "1917 (Volume)" but should probably be "19"
            if pages_str.startswith("1917"):
                return 19
        
        # Extract the first number from the string
        match = re.match(r'^(\d+)', pages_str.strip())
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                logger.warning(f"Could not parse page number: {pages_str}")
                return None
        
        logger.warning(f"Could not parse page number: {pages_str}")
        return None
    
    def connect(self) -> duckdb.DuckDBPyConnection:
        """Connect to the database.
        
        Returns:
            DuckDB connection object
        """
        if self.conn is None:
            logger.info(f"Connecting to database: {self.db_path}")
            self.conn = duckdb.connect(str(self.db_path))
            logger.success("Database connection established")
        return self.conn
    
    def disconnect(self):
        """Disconnect from the database."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    def create_tables(self):
        """Create all necessary tables in the database."""
        logger.info("Creating database tables")
        conn = self.connect()
        
        # Drop existing tables in correct order (foreign key dependencies)
        conn.execute("DROP TABLE IF EXISTS coc CASCADE")
        conn.execute("DROP TABLE IF EXISTS cov CASCADE")
        conn.execute("DROP TABLE IF EXISTS arc CASCADE") 
        conn.execute("DROP TABLE IF EXISTS saga CASCADE")
        conn.execute("DROP TABLE IF EXISTS chapter CASCADE")
        conn.execute("DROP TABLE IF EXISTS volume CASCADE")
        conn.execute("DROP TABLE IF EXISTS character CASCADE")
        
        # Create volume table (simplified, matching original)
        volume_table_query = """
        CREATE TABLE volume (
            number INTEGER PRIMARY KEY,
            title TEXT
        )
        """
        conn.execute(volume_table_query)
        logger.debug("Created volume table")
        
        # Create chapter table (matching original structure)
        chapter_table_query = """
        CREATE TABLE chapter (
            number INTEGER PRIMARY KEY,
            volume INTEGER,
            title TEXT,
            num_page INTEGER,
            date DATE,
            jump TEXT,
            FOREIGN KEY(volume) REFERENCES volume(number)
        )
        """
        conn.execute(chapter_table_query)
        logger.debug("Created chapter table")
        
        # Create character table (enhanced with scraping status column)
        character_table_query = """
        CREATE TABLE character (
            id TEXT PRIMARY KEY,
            name TEXT,
            origin TEXT,
            status TEXT,
            birth TEXT,
            blood_type TEXT,
            blood_type_group TEXT,
            bounties TEXT,
            bounty BIGINT,
            age INT,
            is_likely_character BOOLEAN,
            scraping_status TEXT,
            scraping_note TEXT,
            chapter_list INTEGER[],
            volume_list INTEGER[],
            arc_list TEXT[],
            saga_list TEXT[],
            appearance_count INTEGER,
            volume_appearance_count INTEGER,
            first_appearance INTEGER,
            last_appearance INTEGER,
            birth_date TEXT,
            cover_volume_list INTEGER[],
            cover_appearance_count INTEGER,
            origin_region TEXT,
            haki_observation BOOLEAN DEFAULT FALSE,
            haki_armament BOOLEAN DEFAULT FALSE,
            haki_conqueror BOOLEAN DEFAULT FALSE,
            bio TEXT
        )
        """
        conn.execute(character_table_query)
        logger.debug("Created character table")
        
        # Create character-of-chapter (CoC) table (matching original structure but without FK to character)
        coc_table_query = """
        CREATE TABLE coc (
            chapter INTEGER,
            character TEXT,
            note TEXT NULL,
            FOREIGN KEY(chapter) REFERENCES chapter(number)
        )
        """
        conn.execute(coc_table_query)
        logger.debug("Created coc (character-of-chapter) table")
        
        # Create character-of-volume (CoV) table for volume cover characters
        cov_table_query = """
        CREATE TABLE cov (
            volume INTEGER,
            character TEXT,
            FOREIGN KEY(volume) REFERENCES volume(number)
        )
        """
        conn.execute(cov_table_query)
        logger.debug("Created cov (character-of-volume) table")
        
        # Create saga table first (referenced by arc table)
        saga_table_query = """
        CREATE TABLE saga (
            saga_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            japanese_title TEXT,
            romanized_title TEXT,
            start_chapter INTEGER NOT NULL,
            end_chapter INTEGER NOT NULL,
            description TEXT,
            FOREIGN KEY(start_chapter) REFERENCES chapter(number),
            FOREIGN KEY(end_chapter) REFERENCES chapter(number)
        )
        """
        conn.execute(saga_table_query)
        logger.debug("Created saga table")
        
        # Create arc table
        arc_table_query = """
        CREATE TABLE arc (
            arc_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            japanese_title TEXT,
            romanized_title TEXT,
            start_chapter INTEGER NOT NULL,
            end_chapter INTEGER NOT NULL,
            saga_id TEXT,
            description TEXT,
            FOREIGN KEY(saga_id) REFERENCES saga(saga_id),
            FOREIGN KEY(start_chapter) REFERENCES chapter(number),
            FOREIGN KEY(end_chapter) REFERENCES chapter(number)
        )
        """
        conn.execute(arc_table_query)
        logger.debug("Created arc table")
        
        logger.success("All database tables created successfully")
    
    def load_chapters_from_json(self, json_path: str) -> bool:
        """Load chapter data from JSON file into database.
        
        Args:
            json_path: Path to chapters JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading chapters from {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                chapters_data = json.load(f)
            
            conn = self.connect()
            
            # Clear existing data first (like original parser)
            conn.execute("DELETE FROM coc")
            conn.execute("DELETE FROM chapter")
            logger.info("Cleared existing chapter and CoC data")
            
            for chapter_data in chapters_data:
                try:
                    # Extract and clean data
                    chapter_number = chapter_data.get('chapter_number')
                    volume = chapter_data.get('volume')
                    title = chapter_data.get('title', '')  # Chapter title
                    num_page = chapter_data.get('pages')
                    release_date = self._parse_date(chapter_data.get('release_date'))
                    jump_info = chapter_data.get('jump_info', '')
                    
                    # Convert chapter number to int, skip if invalid
                    if chapter_number and chapter_number != 'unknown':
                        try:
                            chapter_number = int(chapter_number)
                        except ValueError:
                            logger.warning(f"Skipping chapter with invalid number: {chapter_number}")
                            continue
                    else:
                        logger.warning(f"Skipping chapter with missing/unknown number: {chapter_number}")
                        continue
                    
                    # Convert volume to int
                    if volume:
                        try:
                            volume = int(volume)
                        except ValueError:
                            volume = None
                    
                    # Convert pages to int using helper function
                    num_page = self._parse_page_number(num_page)
                    
                    # Insert chapter (simple INSERT since table is cleared)
                    conn.execute("""
                        INSERT INTO chapter 
                        (number, volume, title, num_page, date, jump)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [chapter_number, volume, title, num_page, release_date, jump_info])
                    
                    # Insert character-of-chapter relationships (using original structure)
                    for char in chapter_data.get('characters', []):
                        if isinstance(char, dict):
                            character_name = char.get('name', '').replace(' ', '_')  # Match original format
                            note = char.get('note', '')
                        else:
                            character_name = str(char).replace(' ', '_')
                            note = ''
                        
                        if character_name:  # Only insert non-empty names
                            conn.execute("""
                                INSERT INTO coc (chapter, character, note)
                                VALUES (?, ?, ?)
                            """, [chapter_number, character_name, note])
                            
                            # Also ensure this character exists in the character table
                            char_exists = conn.execute("""
                                SELECT 1 FROM character WHERE id = ?
                            """, [character_name]).fetchone()
                            
                            if not char_exists:
                                conn.execute("""
                                    INSERT INTO character (id, name)
                                    VALUES (?, ?)
                                    ON CONFLICT(id) DO NOTHING
                                """, [character_name, character_name.replace('_', ' ')])
                        
                except Exception as e:
                    logger.error(f"Failed to insert chapter {chapter_data.get('chapter_number', 'unknown')}: {str(e)}")
                    continue
            
            conn.commit()
            logger.success(f"Successfully loaded {len(chapters_data)} chapters")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load chapters from {json_path}: {str(e)}")
            return False
    
    def load_volumes_from_json(self, json_path: str) -> bool:
        """Load volume data from JSON file into database.
        
        Args:
            json_path: Path to volumes JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading volumes from {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                volumes_data = json.load(f)
            
            conn = self.connect()
            
            # Clear existing volume data first (like original parser)
            conn.execute("DELETE FROM volume")
            logger.info("Cleared existing volume data")
            
            for volume_data in volumes_data:
                try:
                    volume_number = volume_data.get('volume_number')
                    title = volume_data.get('english_title', '')
                    
                    conn.execute("""
                        INSERT INTO volume 
                        (number, title)
                        VALUES (?, ?)
                    """, [volume_number, title])
                    
                except Exception as e:
                    logger.error(f"Failed to insert volume {volume_data.get('volume_number', 'unknown')}: {str(e)}")
                    continue
            
            conn.commit()
            logger.success(f"Successfully loaded {len(volumes_data)} volumes")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load volumes from {json_path}: {str(e)}")
            return False
    
    def load_cov_from_json(self, json_path: str) -> bool:
        """Load character-on-volume (COV) data from JSON file into database.
        
        Args:
            json_path: Path to volumes JSON file containing cover_characters data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading COV data from {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                volumes_data = json.load(f)
            
            conn = self.connect()
            
            # Clear existing COV data first
            conn.execute("DELETE FROM cov")
            logger.info("Cleared existing COV data")
            
            total_cov_entries = 0
            
            for volume_data in volumes_data:
                try:
                    volume_number = volume_data.get('volume_number')
                    cover_characters = volume_data.get('cover_characters', [])
                    
                    if not volume_number:
                        logger.warning(f"Volume missing volume_number: {volume_data}")
                        continue
                    
                    for char_data in cover_characters:
                        try:
                            # Use slug as character identifier (same as character table id)
                            character_id = char_data.get('slug', char_data.get('name', ''))
                            
                            if character_id:
                                conn.execute("""
                                    INSERT INTO cov 
                                    (volume, character)
                                    VALUES (?, ?)
                                """, [volume_number, character_id])
                                total_cov_entries += 1
                            else:
                                logger.warning(f"Character missing identifier in volume {volume_number}: {char_data}")
                                
                        except Exception as e:
                            logger.error(f"Failed to insert COV entry for volume {volume_number}, character {char_data}: {str(e)}")
                            continue
                    
                except Exception as e:
                    logger.error(f"Failed to process volume {volume_data.get('volume_number', 'unknown')}: {str(e)}")
                    continue
            
            conn.commit()
            logger.success(f"Successfully loaded {total_cov_entries} COV entries from {len(volumes_data)} volumes")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load COV data from {json_path}: {str(e)}")
            return False
    
    def load_characters_from_json(self, json_path: str) -> bool:
        """Load character data from JSON file into database.
        
        Args:
            json_path: Path to characters JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading characters from {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                characters_data = json.load(f)
            
            conn = self.connect()
            
            # Clear existing character data first (like original parser)
            conn.execute("DELETE FROM character")
            logger.info("Cleared existing character data")
            
            for character_data in characters_data:
                try:
                    # Initialize all variables to None (matching original parser)
                    char_id = character_data.get('id')
                    name = self._get_name(character_data)
                    origin = self._get_string(character_data, 'origin')
                    status = self._get_string(character_data, 'status')
                    birth = self._get_string(character_data, 'birth')
                    blood_type, blood_type_group = self._parse_blood_type(character_data)
                    bounties, bounty = self._parse_bounty(character_data)
                    age = self._parse_age(character_data)
                    is_likely_character = character_data.get('is_likely_character', True)  # Default True for backward compat

                    # Determine scraping status and note
                    scraping_status, scraping_note = self._determine_scraping_status(character_data)

                    # Insert character (simple INSERT since table is cleared)
                    conn.execute("""
                        INSERT INTO character
                        (id, name, origin, status, birth, blood_type, blood_type_group, bounties, bounty, age, is_likely_character, scraping_status, scraping_note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [char_id, name, origin, status, birth, blood_type, blood_type_group, bounties, bounty, age, is_likely_character, scraping_status, scraping_note])
                    
                except Exception as e:
                    logger.error(f"Failed to insert character {character_data.get('name', 'unknown')}: {str(e)}")
                    continue
            
            conn.commit()
            logger.success(f"Successfully loaded {len(characters_data)} characters")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load characters from {json_path}: {str(e)}")
            return False
    
    def load_arcs_from_scraped_data(self, arcs_results: List[ScrapingResult]) -> bool:
        """Load arc data from scraped results into database.
        
        Args:
            arcs_results: List of ScrapingResult objects containing arc data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading {len(arcs_results)} arc results into database")
            
            conn = self.connect()
            
            # Clear existing arc data
            conn.execute("DELETE FROM arc")
            logger.info("Cleared existing arc data")
            
            successful_count = 0
            
            for result in arcs_results:
                if not result.success or not result.data:
                    logger.warning(f"Skipping failed result: {result.error}")
                    continue
                    
                try:
                    arc_data = result.data
                    
                    # Validate that start and end chapters exist in the database
                    start_chapter = arc_data.get('start_chapter')
                    end_chapter = arc_data.get('end_chapter')
                    
                    if start_chapter:
                        chapter_exists = conn.execute(
                            "SELECT 1 FROM chapter WHERE number = ?", (start_chapter,)
                        ).fetchone()
                        if not chapter_exists:
                            logger.warning(f"Skipping arc {arc_data.get('title', 'unknown')}: start chapter {start_chapter} does not exist in database")
                            continue
                    
                    if end_chapter:
                        chapter_exists = conn.execute(
                            "SELECT 1 FROM chapter WHERE number = ?", (end_chapter,)
                        ).fetchone()
                        if not chapter_exists:
                            logger.warning(f"Skipping arc {arc_data.get('title', 'unknown')}: end chapter {end_chapter} does not exist in database")
                            continue
                    
                    # Insert arc data
                    conn.execute("""
                        INSERT INTO arc (
                            arc_id, title, japanese_title, romanized_title,
                            start_chapter, end_chapter, saga_id, description
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        arc_data.get('arc_id'),
                        arc_data.get('title'),
                        arc_data.get('japanese_title'),
                        arc_data.get('romanized_title'),
                        start_chapter,
                        end_chapter,
                        arc_data.get('saga_id'),
                        arc_data.get('description')
                    ))
                    
                    successful_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to insert arc {arc_data.get('title', 'unknown')}: {str(e)}")
                    continue
            
            logger.success(f"Successfully loaded {successful_count} arcs into database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load arcs: {str(e)}")
            return False
    
    def load_sagas_from_scraped_data(self, sagas_results: List[ScrapingResult]) -> bool:
        """Load saga data from scraped results into database.
        
        Args:
            sagas_results: List of ScrapingResult objects containing saga data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading {len(sagas_results)} saga results into database")
            
            conn = self.connect()
            
            # Clear existing saga data
            conn.execute("DELETE FROM saga")
            logger.info("Cleared existing saga data")
            
            successful_count = 0
            
            for result in sagas_results:
                if not result.success or not result.data:
                    logger.warning(f"Skipping failed result: {result.error}")
                    continue
                    
                try:
                    saga_data = result.data
                    
                    # Validate that start and end chapters exist in the database
                    start_chapter = saga_data.get('start_chapter')
                    end_chapter = saga_data.get('end_chapter')
                    
                    if start_chapter:
                        chapter_exists = conn.execute(
                            "SELECT 1 FROM chapter WHERE number = ?", (start_chapter,)
                        ).fetchone()
                        if not chapter_exists:
                            logger.warning(f"Skipping saga {saga_data.get('title', 'unknown')}: start chapter {start_chapter} does not exist in database")
                            continue
                    
                    if end_chapter:
                        chapter_exists = conn.execute(
                            "SELECT 1 FROM chapter WHERE number = ?", (end_chapter,)
                        ).fetchone()
                        if not chapter_exists:
                            logger.warning(f"Skipping saga {saga_data.get('title', 'unknown')}: end chapter {end_chapter} does not exist in database")
                            continue
                    
                    # Insert saga data
                    conn.execute("""
                        INSERT INTO saga (
                            saga_id, title, japanese_title, romanized_title,
                            start_chapter, end_chapter, description
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        saga_data.get('saga_id'),
                        saga_data.get('title'),
                        saga_data.get('japanese_title'),
                        saga_data.get('romanized_title'),
                        start_chapter,
                        end_chapter,
                        saga_data.get('description')
                    ))
                    
                    successful_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to insert saga {saga_data.get('title', 'unknown')}: {str(e)}")
                    continue
            
            logger.success(f"Successfully loaded {successful_count} sagas into database")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load sagas: {str(e)}")
            return False

    def _list_to_string(self, value: Any) -> Optional[str]:
        """Convert list values to string representation.
        
        Args:
            value: Value to convert
            
        Returns:
            String representation or None
        """
        if value is None:
            return None
        if isinstance(value, list):
            return "; ".join(str(v) for v in value if v)
        return str(value) if value else None
    
    def _get_string(self, attributes: Dict[str, Any], key: str) -> Optional[str]:
        """Return the first item from a list or the item itself if it's not a list."""
        if key not in attributes:
            return None
        try:
            value = attributes.get(key)
            if isinstance(value, list):
                return value[0] if value else None
            return value
        except Exception as e:
            logger.warning(f"get_string error for key '{key}': {e}")
            return None
    
    def _get_name(self, attributes: Dict[str, Any]) -> str:
        """Get character name from various possible fields."""
        name = self._get_string(attributes, "name")
        if not name:
            name = self._get_string(attributes, "ename")
        if not name:
            name = self._get_string(attributes, "id")
        return name or ""
    
    def _get_blood_type_group(self, blood_type: str) -> str:
        """Extract blood type group from full blood type string."""
        return blood_type.split(" ")[0] if blood_type else ""
    
    def _parse_blood_type(self, attributes: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
        """Parse blood type information."""
        # Check both "blood type" (old) and "blood_type" (new API scraper)
        blood_types_raw = attributes.get("blood type") or attributes.get("blood_type")
        if not blood_types_raw:
            return None, None

        # Handle both string (from new API scraper) and list (from old scraper)
        if isinstance(blood_types_raw, str):
            # Split by newlines and keep only non-empty, non-reference lines
            blood_types = []
            for line in blood_types_raw.split('\n'):
                line = line.strip()
                if line and not line.isdigit() and line not in ['[', ']']:
                    blood_types.append(line)
        else:
            blood_types = blood_types_raw

        if not blood_types:
            return None, None

        if len(blood_types) > 1:
            blood_type = ", ".join(blood_types)
            blood_type_group = "mixed"
        else:
            blood_type = blood_types[0]
            blood_type_group = self._get_blood_type_group(blood_type)

        return blood_type, blood_type_group
    
    def _parse_bounty(self, attributes: Dict[str, Any]) -> tuple[Optional[str], Optional[int]]:
        """Parse bounty information with improved parsing for new API format."""
        if "bounty" not in attributes:
            return None, None

        bounties_raw = attributes["bounty"]
        if not bounties_raw:
            return None, None

        # Handle both string (from new API scraper) and list (from old scraper)
        if isinstance(bounties_raw, str):
            # Handle both newline and semicolon separated formats
            if '\n' in bounties_raw:
                # Old format: split by newlines
                bounties_list = []
                for line in bounties_raw.split('\n'):
                    line = line.strip()
                    # Skip empty lines, brackets, single numbers (reference markers)
                    if not line or line in ['[', ']'] or line.isdigit():
                        continue
                    bounties_list.append(line)
            else:
                # New format: may contain semicolons
                bounties_list = [bounties_raw.strip()]
        else:
            bounties_list = bounties_raw

        if not bounties_list:
            return None, None

        bounties = ";".join(bounties_list)

        # Improved bounty parsing that handles semicolon-separated format
        bounty = None
        import re
        
        # Look through all parts (split by semicolon and newline)
        all_parts = []
        for entry in bounties_list:
            # Split by semicolons to handle new API format
            parts = [part.strip() for part in entry.split(';')]
            all_parts.extend(parts)
        
        # Find numeric bounty value from all parts
        for part in all_parts:
            part = part.replace("¥", "").strip()
            
            # Skip obvious non-bounty parts
            if part in ["At least", "Unknown", "Over", "★", "(", ")"] or part.startswith("bounty"):
                continue
            if part.replace("★", "").replace(" ", "") == "":
                continue
                
            # Look for numbers with commas
            match = re.search(r'[\d,]+', part)
            if match:
                try:
                    potential_bounty = int(match.group().replace(",", ""))
                    # Sanity check - bounties should be reasonable
                    if potential_bounty > 0 and potential_bounty <= 10_000_000_000:
                        bounty = potential_bounty
                        break
                except ValueError:
                    continue
        
        # Special case for Buggy (from original parser)
        if attributes.get("id") == "Buggy":
            bounty = 3189000000
        
        return bounties, bounty
    
    def _parse_age(self, attributes: Dict[str, Any]) -> Optional[int]:
        """Parse age information."""
        if "age" not in attributes:
            return None

        ages_raw = attributes["age"]
        if not ages_raw:
            return None

        # Handle both string (from new API scraper) and list (from old scraper)
        if isinstance(ages_raw, str):
            # Split by newlines and filter out empty lines and reference markers
            ages = [line.strip() for line in ages_raw.split('\n')
                   if line.strip() and not line.strip().startswith('[')]
        else:
            ages = ages_raw

        if not ages:
            return None

        def parse_raw_age(raw_age: str) -> Optional[int]:
            age_string = raw_age.split(" ")
            if age_string[0] in ["Over", "Under", "Roughly", "Bas:", "And:", "Kerville:"]:
                return int(age_string[1])
            elif age_string[0] == "At" and age_string[1] == "least":
                return int(age_string[2])
            elif "–" in age_string[0]:  # Makino's Child, get the max age
                return int(age_string[0].split("–")[1])
            elif "-" in age_string[0]:  # Bonney's age, get the max age
                return int(age_string[0].split("-")[1])
            elif "biologically" in age_string[0]:  # Momonosuke
                return int(age_string[1].split(")")[0])
            else:
                try:
                    return int(age_string[0])
                except ValueError:
                    logger.warning(f"parse_raw_age error: {age_string}")
                    return None
        
        try:
            parsed_ages = [parse_raw_age(age) for age in ages if parse_raw_age(age) is not None]
            return max(parsed_ages) if parsed_ages else None
        except Exception as e:
            logger.warning(f"parse_age error: {e}")
            return None
    
    def _determine_scraping_status(self, character_data: Dict[str, Any]) -> tuple[str, str]:
        """Determine scraping status and note for a character.
        
        Args:
            character_data: Character data dictionary
            
        Returns:
            Tuple of (status, note)
        """
        # Check if character has meaningful data beyond just ID
        meaningful_fields = ['name', 'origin', 'status', 'birth', 'blood type', 'bounty', 'age']
        has_data = any(character_data.get(field) for field in meaningful_fields)
        
        if has_data:
            return 'success', 'Successfully scraped character details'
        
        # Character failed to scrape - determine why
        char_id = character_data.get('id', 'unknown')
        
        # Common failure patterns from our analysis
        if char_id in ['Ichika', 'Nika', 'Sanka', 'Yonka', 'Yonka_2', 'Yonka_Two']:
            return 'failed_no_url', 'Character has no URL in character list'
        elif char_id == 'Arashi':
            return 'failed_external_url', 'Character URL points to external site (Wikipedia)'
        elif char_id in ['Hidélie_Penguin', 'Kaio_Dragon', 'Ogre_Sazae_Kancho', 'Silver_Bat', 'Smiley_Risu', 'Wandering_Kung_Fu_Dugong']:
            return 'failed_video_game', 'Character is from video games section'
        else:
            return 'failed_unknown', 'Unknown scraping failure'
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get statistics about the database contents.
        
        Returns:
            Dictionary with table row counts
        """
        try:
            conn = self.connect()
            stats = {}
            
            # Get row counts for each table
            tables = ['chapter', 'volume', 'character', 'coc', 'cov', 'arc', 'saga']
            for table in tables:
                try:
                    result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    stats[table] = result[0] if result else 0
                except Exception:
                    stats[table] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {str(e)}")
            return {}

    def merge_characters(self, alias_mapping: Dict[str, str], dry_run: bool = False, progress_callback: Optional[Callable] = None) -> Dict[str, int]:
        """Merge duplicate characters using an alias mapping.

        Args:
            alias_mapping: Dictionary mapping alias IDs to canonical IDs
            dry_run: If True, only report what would be changed without modifying database
            progress_callback: Optional callback function(current, total, alias_id, canonical_id) for progress updates

        Returns:
            Dictionary with merge statistics
        """
        try:
            conn = self.connect()
            stats = {
                'characters_merged': 0,
                'coc_entries_updated': 0,
                'coc_duplicates_collapsed': 0,
                'errors': 0
            }

            logger.info(f"Starting character merge operation (dry_run={dry_run})")
            logger.info(f"Processing {len(alias_mapping)} character aliases")

            # Start transaction
            if not dry_run:
                conn.execute("BEGIN TRANSACTION")

            total = len(alias_mapping)
            current = 0

            for alias_id, canonical_id in alias_mapping.items():
                current += 1

                # Call progress callback if provided
                if progress_callback:
                    progress_callback(current, total, alias_id, canonical_id)
                try:
                    # Check if both characters exist
                    alias_exists = conn.execute(
                        "SELECT COUNT(*) FROM character WHERE id = ?",
                        [alias_id]
                    ).fetchone()[0] > 0

                    canonical_exists = conn.execute(
                        "SELECT COUNT(*) FROM character WHERE id = ?",
                        [canonical_id]
                    ).fetchone()[0] > 0

                    if not alias_exists:
                        logger.warning(f"Alias character not found: {alias_id}")
                        continue

                    if not canonical_exists:
                        logger.warning(f"Canonical character not found: {canonical_id}, skipping {alias_id}")
                        stats['errors'] += 1
                        continue

                    # Count CoC entries that will be updated
                    coc_count = conn.execute(
                        "SELECT COUNT(*) FROM coc WHERE character = ?",
                        [alias_id]
                    ).fetchone()[0]

                    if dry_run:
                        logger.info(f"Would merge {alias_id} -> {canonical_id} ({coc_count} CoC entries)")
                    else:
                        # Update CoC entries
                        conn.execute(
                            "UPDATE coc SET character = ? WHERE character = ?",
                            [canonical_id, alias_id]
                        )

                        # Delete the alias character
                        conn.execute(
                            "DELETE FROM character WHERE id = ?",
                            [alias_id]
                        )

                        logger.info(f"Merged {alias_id} -> {canonical_id} ({coc_count} CoC entries)")

                    stats['characters_merged'] += 1
                    stats['coc_entries_updated'] += coc_count

                except Exception as e:
                    logger.error(f"Error merging {alias_id} -> {canonical_id}: {str(e)}")
                    stats['errors'] += 1
                    continue

            # Collapse fully-identical CoC rows produced by the merge
            # (same chapter + character + note). Semantic duplicates with
            # differing notes like 'cover'/'doll' are preserved.
            if not dry_run:
                before = conn.execute("SELECT COUNT(*) FROM coc").fetchone()[0]
                conn.execute("""
                    CREATE OR REPLACE TEMPORARY TABLE _coc_dedup AS
                    SELECT DISTINCT chapter, character, note FROM coc
                """)
                conn.execute("DELETE FROM coc")
                conn.execute("INSERT INTO coc SELECT chapter, character, note FROM _coc_dedup")
                conn.execute("DROP TABLE _coc_dedup")
                after = conn.execute("SELECT COUNT(*) FROM coc").fetchone()[0]
                stats['coc_duplicates_collapsed'] = before - after
                if stats['coc_duplicates_collapsed']:
                    logger.info(
                        f"Collapsed {stats['coc_duplicates_collapsed']} identical CoC rows "
                        f"({before} → {after})"
                    )

            # Commit transaction
            if not dry_run:
                conn.execute("COMMIT")
                logger.success(f"Merge complete: {stats['characters_merged']} characters merged, "
                             f"{stats['coc_entries_updated']} CoC entries updated, "
                             f"{stats['coc_duplicates_collapsed']} identical rows collapsed")
            else:
                logger.info(f"Dry run complete: would merge {stats['characters_merged']} characters, "
                          f"update {stats['coc_entries_updated']} CoC entries")

            return stats

        except Exception as e:
            if not dry_run:
                try:
                    conn.execute("ROLLBACK")
                    logger.error("Transaction rolled back due to error")
                except:
                    pass
            logger.error(f"Failed to merge characters: {str(e)}")
            return {'characters_merged': 0, 'coc_entries_updated': 0, 'errors': 1}

    def sync_character_appearances(self, progress_callback: Optional[Callable] = None) -> Dict[str, int]:
        """Sync character appearance analytics from CoC/CoV tables.

        Populates the following columns in character table:
        - chapter_list: sorted list of chapter numbers
        - volume_list: sorted list of volume numbers
        - arc_list: sorted list of arc IDs (computed from chapter_list)
        - saga_list: sorted list of saga IDs (computed from chapter_list)
        - appearance_count: total chapter appearances
        - volume_appearance_count: total volume appearances
        - first_appearance: first chapter number
        - last_appearance: last chapter number

        Args:
            progress_callback: Optional callback function(current, total, character_id, stats)

        Returns:
            Dictionary with sync statistics
        """
        try:
            conn = self.connect()

            logger.info("Starting character appearance sync...")

            # Add columns if they don't exist (for existing databases)
            try:
                logger.info("Checking for appearance analytics columns...")
                # Try to query the columns - if it fails, they don't exist
                conn.execute("SELECT chapter_list FROM character LIMIT 1")
                logger.info("Basic columns already exist")
            except:
                logger.info("Adding basic appearance analytics columns to character table...")
                conn.execute("ALTER TABLE character ADD COLUMN chapter_list INTEGER[]")
                conn.execute("ALTER TABLE character ADD COLUMN volume_list INTEGER[]")
                conn.execute("ALTER TABLE character ADD COLUMN appearance_count INTEGER")
                conn.execute("ALTER TABLE character ADD COLUMN volume_appearance_count INTEGER")
                conn.execute("ALTER TABLE character ADD COLUMN first_appearance INTEGER")
                conn.execute("ALTER TABLE character ADD COLUMN last_appearance INTEGER")
                conn.commit()
                logger.success("Basic appearance analytics columns added successfully")

            # Check for arc_list and saga_list columns (added later)
            try:
                conn.execute("SELECT arc_list FROM character LIMIT 1")
                logger.info("Arc/saga columns already exist")
            except:
                logger.info("Adding arc_list and saga_list columns to character table...")
                conn.execute("ALTER TABLE character ADD COLUMN arc_list TEXT[]")
                conn.execute("ALTER TABLE character ADD COLUMN saga_list TEXT[]")
                conn.commit()
                logger.success("Arc/saga columns added successfully")

            # Get all characters
            characters = conn.execute("SELECT id FROM character ORDER BY id").fetchall()
            total_characters = len(characters)

            if total_characters == 0:
                logger.warning("No characters found in database")
                return {'characters_updated': 0, 'total_appearances': 0, 'errors': 0}

            stats = {
                'characters_updated': 0,
                'total_appearances': 0,
                'characters_with_no_appearances': 0,
                'errors': 0
            }

            logger.info(f"Processing {total_characters} characters...")

            for idx, (character_id,) in enumerate(characters, 1):
                try:
                    # Get chapter appearances from CoC.
                    # DISTINCT because CoC can legitimately hold multiple rows
                    # per (chapter, character) with differing notes (e.g. a
                    # character appearing both as 'cover' and 'doll' in one
                    # chapter); chapter_list must still count each chapter once.
                    chapter_data = conn.execute("""
                        SELECT DISTINCT chapter
                        FROM coc
                        WHERE character = ?
                        ORDER BY chapter
                    """, [character_id]).fetchall()

                    chapter_list = [row[0] for row in chapter_data]
                    appearance_count = len(chapter_list)

                    # Get volume appearances by joining with chapter table
                    volume_data = conn.execute("""
                        SELECT DISTINCT c.volume
                        FROM coc coc_table
                        JOIN chapter c ON coc_table.chapter = c.number
                        WHERE coc_table.character = ? AND c.volume IS NOT NULL
                        ORDER BY c.volume
                    """, [character_id]).fetchall()

                    volume_list = [row[0] for row in volume_data]
                    volume_appearance_count = len(volume_list)

                    # Calculate first and last appearances
                    first_appearance = chapter_list[0] if chapter_list else None
                    last_appearance = chapter_list[-1] if chapter_list else None

                    # Compute arc appearances based on chapter_list
                    arc_list = []
                    saga_list = []

                    if chapter_list:
                        # Get arcs where character appears
                        # Check if any chapter in chapter_list falls within arc range
                        arc_data = conn.execute("""
                            SELECT DISTINCT arc_id
                            FROM arc
                            WHERE (
                                SELECT COUNT(*)
                                FROM (SELECT UNNEST(?) AS ch) chapters
                                WHERE ch BETWEEN start_chapter AND end_chapter
                            ) > 0
                            ORDER BY start_chapter
                        """, [chapter_list]).fetchall()

                        arc_list = [row[0] for row in arc_data]

                        # Get sagas where character appears
                        # Check if any chapter in chapter_list falls within saga range
                        saga_data = conn.execute("""
                            SELECT DISTINCT saga_id
                            FROM saga
                            WHERE (
                                SELECT COUNT(*)
                                FROM (SELECT UNNEST(?) AS ch) chapters
                                WHERE ch BETWEEN start_chapter AND end_chapter
                            ) > 0
                            ORDER BY start_chapter
                        """, [chapter_list]).fetchall()

                        saga_list = [row[0] for row in saga_data]

                    # Update character table
                    conn.execute("""
                        UPDATE character
                        SET chapter_list = ?,
                            volume_list = ?,
                            arc_list = ?,
                            saga_list = ?,
                            appearance_count = ?,
                            volume_appearance_count = ?,
                            first_appearance = ?,
                            last_appearance = ?
                        WHERE id = ?
                    """, [
                        chapter_list,
                        volume_list,
                        arc_list,
                        saga_list,
                        appearance_count,
                        volume_appearance_count,
                        first_appearance,
                        last_appearance,
                        character_id
                    ])

                    stats['characters_updated'] += 1
                    stats['total_appearances'] += appearance_count

                    if appearance_count == 0:
                        stats['characters_with_no_appearances'] += 1

                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(
                            idx,
                            total_characters,
                            character_id,
                            {
                                'chapters': appearance_count,
                                'volumes': volume_appearance_count,
                                'arcs': len(arc_list),
                                'sagas': len(saga_list),
                                'first': first_appearance,
                                'last': last_appearance
                            }
                        )

                    logger.debug(f"Synced {character_id}: {appearance_count} chapters, {volume_appearance_count} volumes, {len(arc_list)} arcs, {len(saga_list)} sagas")

                except Exception as e:
                    logger.error(f"Error syncing character {character_id}: {str(e)}")
                    stats['errors'] += 1
                    continue

            conn.commit()

            logger.success(f"Sync complete: {stats['characters_updated']} characters updated, "
                         f"{stats['total_appearances']} total appearances")

            if stats['characters_with_no_appearances'] > 0:
                logger.info(f"Note: {stats['characters_with_no_appearances']} characters have no appearances")

            return stats

        except Exception as e:
            logger.error(f"Failed to sync character appearances: {str(e)}")
            return {'characters_updated': 0, 'total_appearances': 0, 'errors': 1}

    def sync_cover_appearances(self, progress_callback: Optional[Callable] = None) -> Dict[str, int]:
        """Sync character volume cover appearance analytics from CoV table.

        Populates the following columns in character table:
        - cover_volume_list: sorted list of volume numbers where character appears on cover
        - cover_appearance_count: total volume cover appearances

        Args:
            progress_callback: Optional callback function(current, total, character_id, stats)

        Returns:
            Dictionary with sync statistics
        """
        try:
            conn = self.connect()

            logger.info("Starting character cover appearance sync...")

            # Add columns if they don't exist (for existing databases)
            try:
                logger.info("Checking for cover appearance analytics columns...")
                # Try to query the columns - if it fails, they don't exist
                conn.execute("SELECT cover_volume_list FROM character LIMIT 1")
                logger.info("Cover columns already exist")
            except:
                logger.info("Adding cover appearance analytics columns to character table...")
                conn.execute("ALTER TABLE character ADD COLUMN cover_volume_list INTEGER[]")
                conn.execute("ALTER TABLE character ADD COLUMN cover_appearance_count INTEGER")
                conn.commit()
                logger.success("Cover appearance analytics columns added successfully")

            # Get all characters
            characters = conn.execute("SELECT id FROM character ORDER BY id").fetchall()
            total_characters = len(characters)

            if total_characters == 0:
                logger.warning("No characters found in database")
                return {
                    'total_characters': 0,
                    'characters_with_covers': 0,
                    'total_cover_appearances': 0,
                    'characters_without_covers': 0,
                    'errors': 0
                }

            stats = {
                'total_characters': total_characters,
                'characters_with_covers': 0,
                'total_cover_appearances': 0,
                'characters_without_covers': 0,
                'errors': 0
            }

            logger.info(f"Processing {total_characters} characters...")

            for idx, (character_id,) in enumerate(characters, 1):
                try:
                    # Get volume cover appearances from CoV
                    cover_data = conn.execute("""
                        SELECT volume
                        FROM cov
                        WHERE character = ?
                        ORDER BY volume
                    """, [character_id]).fetchall()

                    cover_volume_list = [row[0] for row in cover_data]
                    cover_appearance_count = len(cover_volume_list)

                    # Update character table
                    conn.execute("""
                        UPDATE character
                        SET cover_volume_list = ?,
                            cover_appearance_count = ?
                        WHERE id = ?
                    """, [
                        cover_volume_list,
                        cover_appearance_count,
                        character_id
                    ])

                    if cover_appearance_count > 0:
                        stats['characters_with_covers'] += 1
                        stats['total_cover_appearances'] += cover_appearance_count
                    else:
                        stats['characters_without_covers'] += 1

                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback(
                            idx,
                            total_characters,
                            character_id,
                            {
                                'covers': cover_appearance_count
                            }
                        )

                    logger.debug(f"Synced {character_id}: {cover_appearance_count} cover appearances")

                except Exception as e:
                    logger.error(f"Error syncing character {character_id}: {str(e)}")
                    stats['errors'] += 1
                    continue

            conn.commit()

            logger.success(f"Sync complete: {stats['characters_with_covers']} characters with covers, "
                         f"{stats['total_cover_appearances']} total cover appearances")

            if stats['characters_without_covers'] > 0:
                logger.info(f"Note: {stats['characters_without_covers']} characters have no cover appearances")

            return stats

        except Exception as e:
            logger.error(f"Failed to sync cover appearances: {str(e)}")
            return {
                'total_characters': 0,
                'characters_with_covers': 0,
                'total_cover_appearances': 0,
                'characters_without_covers': 0,
                'errors': 1
            }

    def export_to_csv(self, output_dir: str) -> bool:
        """Export all tables to CSV files.
        
        Args:
            output_dir: Directory to save CSV files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            conn = self.connect()
            tables = ['chapter', 'volume', 'character', 'coc', 'cov', 'arc', 'saga']
            
            for table in tables:
                try:
                    df = conn.execute(f"SELECT * FROM {table}").df()
                    csv_path = output_path / f"{table}.csv"
                    df.to_csv(csv_path, index=False)
                    logger.info(f"Exported {table} to {csv_path}")
                except Exception as e:
                    logger.error(f"Failed to export {table}: {str(e)}")
                    return False
            
            logger.success(f"Successfully exported all tables to {output_dir}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export to CSV: {str(e)}")
            return False
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            DataFrame with query results
        """
        try:
            conn = self.connect()
            return conn.execute(sql).df()
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            return pd.DataFrame()
    
    def add_birth_date_column(self, date_format: str = "mm_dd") -> bool:
        """Add a parsed birth_date column to the character table.
        
        Args:
            date_format: Format for the birth_date column:
                - "mm_dd": VARCHAR with MM-DD format (e.g., "03-09")
                - "full_date": DATE with year 2000 (e.g., "2000-03-09")
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.connect()
            
            # Check if column already exists
            existing_columns = conn.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'character' AND column_name = 'birth_date'
            """).fetchall()
            
            if existing_columns:
                logger.info("birth_date column already exists")
                return True
            
            # Add the column based on format
            if date_format == "mm_dd":
                alter_query = "ALTER TABLE character ADD COLUMN birth_date VARCHAR(5)"
                logger.info("Adding birth_date column as VARCHAR(5) for MM-DD format")
            elif date_format == "full_date":
                alter_query = "ALTER TABLE character ADD COLUMN birth_date DATE"
                logger.info("Adding birth_date column as DATE for full date format")
            else:
                logger.error(f"Invalid date_format: {date_format}. Use 'mm_dd' or 'full_date'")
                return False
            
            conn.execute(alter_query)
            logger.success(f"Added birth_date column with format: {date_format}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add birth_date column: {str(e)}")
            return False
    
    def populate_birth_date_column(self, date_format: str = "mm_dd") -> bool:
        """Parse birth strings and populate the birth_date column.
        
        Args:
            date_format: Format to use for parsing:
                - "mm_dd": MM-DD format (e.g., "03-09")
                - "full_date": Full date with year 2000 (e.g., "2000-03-09")
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.connect()
            
            # Get all characters with birth data
            characters = conn.execute("""
                SELECT id, name, birth 
                FROM character 
                WHERE birth IS NOT NULL AND birth != ''
            """).fetchall()
            
            logger.info(f"Processing {len(characters)} characters with birth data")
            
            successful_updates = 0
            failed_updates = 0
            
            for char_id, name, birth in characters:
                if date_format == "mm_dd":
                    parsed_date = BirthDateParser.parse_to_mm_dd(birth)
                elif date_format == "full_date":
                    parsed_date = BirthDateParser.parse_to_date_with_year(birth)
                else:
                    logger.error(f"Invalid date_format: {date_format}")
                    return False
                
                if parsed_date:
                    try:
                        conn.execute("""
                            UPDATE character 
                            SET birth_date = ? 
                            WHERE id = ?
                        """, (parsed_date, char_id))
                        successful_updates += 1
                    except Exception as e:
                        logger.warning(f"Failed to update {name} ({char_id}): {str(e)}")
                        failed_updates += 1
                else:
                    logger.warning(f"Could not parse birth date for {name}: '{birth}'")
                    failed_updates += 1
            
            logger.success(f"Birth date parsing completed: {successful_updates} successful, {failed_updates} failed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to populate birth_date column: {str(e)}")
            return False
    
    def migrate_birth_dates(self, date_format: str = "mm_dd") -> bool:
        """Complete migration: add birth_date column and populate it.
        
        Args:
            date_format: Format for the birth_date column:
                - "mm_dd": VARCHAR with MM-DD format (e.g., "03-09") 
                - "full_date": DATE with year 2000 (e.g., "2000-03-09")
        
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Starting birth date migration with format: {date_format}")
        
        # Add column
        if not self.add_birth_date_column(date_format):
            return False
        
        # Populate column
        if not self.populate_birth_date_column(date_format):
            return False
        
        logger.success("Birth date migration completed successfully")
        return True
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def extract_characters_from_chapters(self, json_path: str, output_path: str) -> bool:
        """Extract character list from chapters JSON file and save to CSV.
        
        Args:
            json_path: Path to chapters JSON file
            output_path: Path to output CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Extracting character list from {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                chapters_data = json.load(f)
            
            # Dictionary to store unique characters with their URLs
            unique_characters = {}
            
            # Extract all characters from chapters
            for chapter_data in chapters_data:
                for char in chapter_data.get('characters', []):
                    if isinstance(char, dict):
                        character_name = char.get('name', '')
                        character_id = character_name.replace(' ', '_')
                        character_url = char.get('url', '')
                    else:
                        character_name = str(char)
                        character_id = character_name.replace(' ', '_')
                        character_url = f"/wiki/{character_id}"
                    
                    if character_name:
                        # Store both name and URL
                        unique_characters[character_id] = {'name': character_name, 'url': character_url}
            
            # Convert to DataFrame
            df = pd.DataFrame([
                {
                    'id': character_id,
                    'name': character_data['name'],
                    'url': character_data['url']
                }
                for character_id, character_data in unique_characters.items()
            ])
            
            # Sort by name for consistency
            df = df.sort_values('name')
            
            # Save to CSV
            df.to_csv(output_path, index=False)
            logger.success(f"Saved {len(unique_characters)} characters to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to extract characters from {json_path}: {str(e)}")
            return False
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
