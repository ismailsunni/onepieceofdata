"""Modern database operations for One Piece of Data."""

import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import duckdb
import pandas as pd
from loguru import logger

from ..models.data import ChapterModel, VolumeModel, CharacterModel, ScrapingResult
from ..config.settings import get_settings


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
        
        # Drop existing tables if they exist
        conn.execute("DROP TABLE IF EXISTS coc CASCADE")
        conn.execute("DROP TABLE IF EXISTS chapter CASCADE")
        conn.execute("DROP TABLE IF EXISTS volume CASCADE")
        conn.execute("DROP TABLE IF EXISTS character CASCADE")
        
        # Create volume table
        volume_table_query = """
        CREATE TABLE volume (
            number INTEGER PRIMARY KEY,
            title TEXT,
            japanese_title TEXT,
            release_date DATE,
            cover_characters TEXT  -- JSON string of characters
        )
        """
        conn.execute(volume_table_query)
        logger.debug("Created volume table")
        
        # Create chapter table
        chapter_table_query = """
        CREATE TABLE chapter (
            number INTEGER PRIMARY KEY,
            volume INTEGER,
            title TEXT,
            japanese_title TEXT,
            romanized_title TEXT,
            num_page INTEGER,
            release_date DATE,
            characters TEXT  -- JSON string of characters
        )
        """
        conn.execute(chapter_table_query)
        logger.debug("Created chapter table")
        
        # Create character table
        character_table_query = """
        CREATE TABLE character (
            id TEXT PRIMARY KEY,
            name TEXT,
            japanese_name TEXT,
            romanized_name TEXT,
            epithet TEXT,
            affiliation TEXT,
            occupation TEXT,
            first_appearance TEXT,
            age TEXT,
            height TEXT,
            bounty TEXT,
            status TEXT,
            origin TEXT,
            blood_type TEXT,
            additional_data TEXT  -- JSON string for extra fields
        )
        """
        conn.execute(character_table_query)
        logger.debug("Created character table")
        
        # Create character-of-chapter (CoC) table
        coc_table_query = """
        CREATE TABLE coc (
            chapter_number INTEGER,
            character_name TEXT,
            character_id TEXT,
            PRIMARY KEY (chapter_number, character_name),
            FOREIGN KEY(chapter_number) REFERENCES chapter(number)
        )
        """
        conn.execute(coc_table_query)
        logger.debug("Created coc (character-of-chapter) table")
        
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
            
            for chapter_data in chapters_data:
                try:
                    # Extract and clean data
                    chapter_number = chapter_data.get('chapter_number')
                    volume = chapter_data.get('volume')
                    title = chapter_data.get('title', '')  # Chapter title
                    japanese_title = None  # Not in current data structure
                    romanized_title = None  # Not in current data structure
                    num_page = chapter_data.get('pages')
                    release_date = self._parse_date(chapter_data.get('release_date'))
                    
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
                    
                    # Convert pages to int
                    if num_page:
                        try:
                            num_page = int(num_page)
                        except ValueError:
                            num_page = None
                    
                    # Extract character names from the characters list
                    character_names = []
                    for char in chapter_data.get('characters', []):
                        if isinstance(char, dict):
                            character_names.append(char.get('name', ''))
                        else:
                            character_names.append(str(char))
                    
                    characters = json.dumps(character_names)
                    
                    # Insert chapter
                    conn.execute("""
                        INSERT OR REPLACE INTO chapter 
                        (number, volume, title, japanese_title, romanized_title, num_page, release_date, characters)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, [chapter_number, volume, title, japanese_title, romanized_title, num_page, release_date, characters])
                    
                    # Insert character-of-chapter relationships
                    for character_name in character_names:
                        if character_name:  # Only insert non-empty names
                            conn.execute("""
                                INSERT OR IGNORE INTO coc (chapter_number, character_name, character_id)
                                VALUES (?, ?, ?)
                            """, [chapter_number, character_name, None])  # character_id can be filled later
                        
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
            
            for volume_data in volumes_data:
                try:
                    volume_number = volume_data.get('volume_number')
                    title = volume_data.get('english_title', '')
                    japanese_title = volume_data.get('japanese_title')
                    cover_characters = json.dumps(volume_data.get('cover_characters', []))
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO volume 
                        (number, title, japanese_title, cover_characters)
                        VALUES (?, ?, ?, ?)
                    """, [volume_number, title, japanese_title, cover_characters])
                    
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
            
            for character_data in characters_data:
                try:
                    char_id = character_data.get('id')
                    name = character_data.get('name', '')
                    japanese_name = character_data.get('japanese_name')
                    romanized_name = character_data.get('romanized_name')
                    epithet = self._list_to_string(character_data.get('epithet'))
                    affiliation = self._list_to_string(character_data.get('affiliation'))
                    occupation = self._list_to_string(character_data.get('occupation'))
                    first_appearance = self._list_to_string(character_data.get('debut'))
                    age = self._list_to_string(character_data.get('age'))
                    height = self._list_to_string(character_data.get('height'))
                    bounty = self._list_to_string(character_data.get('bounty'))
                    status = self._list_to_string(character_data.get('status'))
                    origin = self._list_to_string(character_data.get('origin'))
                    blood_type = self._list_to_string(character_data.get('blood_type'))
                    
                    # Store additional fields as JSON
                    additional_data = {k: v for k, v in character_data.items() 
                                     if k not in ['id', 'name', 'japanese_name', 'romanized_name', 
                                                'epithet', 'affiliation', 'occupation', 'debut', 
                                                'age', 'height', 'bounty', 'status', 'origin', 'blood_type']}
                    additional_data_json = json.dumps(additional_data) if additional_data else None
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO character 
                        (id, name, japanese_name, romanized_name, epithet, affiliation, occupation, 
                         first_appearance, age, height, bounty, status, origin, blood_type, additional_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [char_id, name, japanese_name, romanized_name, epithet, affiliation, 
                         occupation, first_appearance, age, height, bounty, status, origin, 
                         blood_type, additional_data_json])
                    
                    # Update character IDs in CoC table if possible
                    if char_id and name:
                        conn.execute("""
                            UPDATE coc SET character_id = ? WHERE character_name = ?
                        """, [char_id, name])
                    
                except Exception as e:
                    logger.error(f"Failed to insert character {character_data.get('name', 'unknown')}: {str(e)}")
                    continue
            
            conn.commit()
            logger.success(f"Successfully loaded {len(characters_data)} characters")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load characters from {json_path}: {str(e)}")
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
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get statistics about the database contents.
        
        Returns:
            Dictionary with table row counts
        """
        try:
            conn = self.connect()
            stats = {}
            
            # Get row counts for each table
            tables = ['chapter', 'volume', 'character', 'coc']
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
            tables = ['chapter', 'volume', 'character', 'coc']
            
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
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
