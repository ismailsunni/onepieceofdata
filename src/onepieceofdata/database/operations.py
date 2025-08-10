"""Modern database operations for One Piece of Data."""

import json
from typing import List, Dict, Any, Optional
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
            scraping_status TEXT,
            scraping_note TEXT
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
                    
                    # Determine scraping status and note
                    scraping_status, scraping_note = self._determine_scraping_status(character_data)
                    
                    # Insert character (simple INSERT since table is cleared)
                    conn.execute("""
                        INSERT INTO character 
                        (id, name, origin, status, birth, blood_type, blood_type_group, bounties, bounty, age, scraping_status, scraping_note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [char_id, name, origin, status, birth, blood_type, blood_type_group, bounties, bounty, age, scraping_status, scraping_note])
                    
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
        if "blood type" not in attributes:
            return None, None
        
        blood_types = attributes["blood type"]
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
        """Parse bounty information."""
        if "bounty" not in attributes:
            return None, None
        
        bounties_list = attributes["bounty"]
        if not bounties_list:
            return None, None
        
        bounties = ";".join(bounties_list)
        first_entry = bounties_list[0].replace("¥", "")
        
        if "★" in first_entry or first_entry == "Unknown" or first_entry == "":
            if len(bounties_list) > 1:
                first_entry = bounties_list[1]
            else:
                first_entry = first_entry.replace("★", "")
        
        if first_entry in ["Unknown", ""] or "Unknown" in first_entry:
            bounty = None
        else:
            try:
                bounty = int(
                    first_entry.split(" ")[-1]
                    .replace(";", "")
                    .replace("(", "")
                    .replace(")", "")
                )
            except ValueError:
                try:
                    bounty = int(
                        first_entry.split(" ")[0]
                        .replace(";", "")
                        .replace("(", "")
                        .replace(")", "")
                    )
                except ValueError:
                    bounty = None
        
        # Special case for Buggy (from original parser)
        if attributes.get("id") == "Buggy":
            bounty = 3189000000
        
        return bounties, bounty
    
    def _parse_age(self, attributes: Dict[str, Any]) -> Optional[int]:
        """Parse age information."""
        if "age" not in attributes:
            return None
        
        ages = attributes["age"]
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
            tables = ['chapter', 'volume', 'character', 'coc', 'arc', 'saga']
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
            tables = ['chapter', 'volume', 'character', 'coc', 'arc', 'saga']
            
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
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
