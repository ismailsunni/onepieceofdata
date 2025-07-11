"""Tests for database operations."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import json
import pandas as pd

from onepieceofdata.database.operations import DatabaseManager


class TestDatabaseManager:
    """Test cases for DatabaseManager."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path for testing."""
        with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as f:
            db_path = f.name
        # File is deleted, so we have a clean path
        yield db_path
        # Cleanup
        Path(db_path).unlink(missing_ok=True)
    
    @pytest.fixture
    def db_manager(self, temp_db_path):
        """Create a DatabaseManager instance for testing."""
        return DatabaseManager(temp_db_path)
    
    @pytest.fixture
    def sample_chapters_data(self):
        """Sample chapters data for testing."""
        return [
            {
                'chapter_number': 1,
                'title': 'Romance Dawn',
                'japanese_title': 'ロマンス ドーン',
                'volume': 1,
                'pages': 54,
                'release_date': '1997-07-22',
                'characters': ['Monkey D. Luffy', 'Red-Haired Shanks']
            },
            {
                'chapter_number': 2,
                'title': 'They Call Him "Straw Hat Luffy"',
                'volume': 1,
                'pages': 20,
                'characters': ['Monkey D. Luffy', 'Koby']
            }
        ]
    
    @pytest.fixture
    def sample_volumes_data(self):
        """Sample volumes data for testing."""
        return [
            {
                'volume_number': 1,
                'english_title': 'Romance Dawn',
                'japanese_title': 'ロマンス ドーン',
                'cover_characters': [
                    {'name': 'Monkey D. Luffy', 'slug': 'Luffy'},
                    {'name': 'Red-Haired Shanks', 'slug': 'Shanks'}
                ]
            },
            {
                'volume_number': 2,
                'english_title': 'Buggy the Clown',
                'japanese_title': 'バギー',
                'cover_characters': [
                    {'name': 'Monkey D. Luffy', 'slug': 'Luffy'},
                    {'name': 'Buggy', 'slug': 'Buggy'}
                ]
            }
        ]
    
    @pytest.fixture
    def sample_characters_data(self):
        """Sample characters data for testing."""
        return [
            {
                'id': 'luffy',
                'name': 'Monkey D. Luffy',
                'epithet': ['Straw Hat'],
                'affiliation': ['Straw Hat Pirates'],
                'age': ['19'],
                'bounty': ['3000000000'],
                'debut': ['Chapter 1']
            },
            {
                'id': 'zoro',
                'name': 'Roronoa Zoro',
                'epithet': ['Pirate Hunter'],
                'affiliation': ['Straw Hat Pirates'],
                'age': ['21'],
                'bounty': ['1111000000']
            }
        ]
    
    def test_initialization(self, temp_db_path):
        """Test DatabaseManager initialization."""
        db = DatabaseManager(temp_db_path)
        assert str(db.db_path) == temp_db_path
        assert db.conn is None
    
    def test_initialization_with_default_path(self):
        """Test DatabaseManager initialization with default path."""
        db = DatabaseManager()
        assert db.db_path is not None
        assert db.conn is None
    
    def test_connect_and_disconnect(self, db_manager):
        """Test database connection and disconnection."""
        # Test connection
        conn = db_manager.connect()
        assert conn is not None
        assert db_manager.conn is conn
        
        # Test that subsequent calls return the same connection
        conn2 = db_manager.connect()
        assert conn2 is conn
        
        # Test disconnection
        db_manager.disconnect()
        assert db_manager.conn is None
    
    def test_create_tables(self, db_manager):
        """Test database table creation."""
        db_manager.create_tables()
        
        conn = db_manager.connect()
        
        # Check that all tables exist
        tables = ['volume', 'chapter', 'character', 'coc']
        for table in tables:
            result = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'").fetchone()
            assert result is not None, f"Table {table} was not created"
    
    def test_load_chapters_from_json(self, db_manager, sample_chapters_data):
        """Test loading chapters from JSON data."""
        # Create tables first
        db_manager.create_tables()
        
        # Create temporary JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_chapters_data, f)
            json_path = f.name
        
        try:
            # Test loading
            success = db_manager.load_chapters_from_json(json_path)
            assert success is True
            
            # Verify data was loaded
            conn = db_manager.connect()
            chapters = conn.execute("SELECT * FROM chapter ORDER BY number").fetchall()
            assert len(chapters) == 2
            assert chapters[0][0] == 1  # chapter number
            assert chapters[0][2] == 'Romance Dawn'  # title
            
            # Verify CoC data was loaded
            coc_entries = conn.execute("SELECT * FROM coc").fetchall()
            assert len(coc_entries) >= 2  # At least 2 character-chapter relationships
            
        finally:
            Path(json_path).unlink(missing_ok=True)
    
    def test_load_chapters_from_json_file_not_found(self, db_manager):
        """Test loading chapters from non-existent file."""
        success = db_manager.load_chapters_from_json("/non/existent/file.json")
        assert success is False
    
    def test_load_volumes_from_json(self, db_manager, sample_volumes_data):
        """Test loading volumes from JSON data."""
        db_manager.create_tables()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_volumes_data, f)
            json_path = f.name
        
        try:
            success = db_manager.load_volumes_from_json(json_path)
            assert success is True
            
            # Verify data was loaded
            conn = db_manager.connect()
            volumes = conn.execute("SELECT * FROM volume ORDER BY number").fetchall()
            assert len(volumes) == 2
            assert volumes[0][0] == 1  # volume number
            assert volumes[0][1] == 'Romance Dawn'  # title
            
        finally:
            Path(json_path).unlink(missing_ok=True)
    
    def test_load_characters_from_json(self, db_manager, sample_characters_data):
        """Test loading characters from JSON data."""
        db_manager.create_tables()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_characters_data, f)
            json_path = f.name
        
        try:
            success = db_manager.load_characters_from_json(json_path)
            assert success is True
            
            # Verify data was loaded
            conn = db_manager.connect()
            characters = conn.execute("SELECT * FROM character ORDER BY id").fetchall()
            assert len(characters) == 2
            assert characters[0][0] == 'luffy'  # id
            assert characters[0][1] == 'Monkey D. Luffy'  # name
            
        finally:
            Path(json_path).unlink(missing_ok=True)
    
    def test_list_to_string_conversion(self, db_manager):
        """Test list to string conversion utility method."""
        # Test with list
        result = db_manager._list_to_string(['item1', 'item2', 'item3'])
        assert result == 'item1; item2; item3'
        
        # Test with single value
        result = db_manager._list_to_string('single_value')
        assert result == 'single_value'
        
        # Test with None
        result = db_manager._list_to_string(None)
        assert result is None
        
        # Test with empty list
        result = db_manager._list_to_string([])
        assert result == ''
        
        # Test with list containing empty values
        result = db_manager._list_to_string(['item1', '', 'item3', None])
        assert result == 'item1; item3'
    
    def test_get_database_stats(self, db_manager, sample_chapters_data, sample_volumes_data):
        """Test getting database statistics."""
        db_manager.create_tables()
        
        # Initially should have zero counts
        stats = db_manager.get_database_stats()
        assert stats['chapter'] == 0
        assert stats['volume'] == 0
        assert stats['character'] == 0
        assert stats['coc'] == 0
        
        # Load some data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_chapters_data, f)
            chapters_path = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_volumes_data, f)
            volumes_path = f.name
        
        try:
            db_manager.load_chapters_from_json(chapters_path)
            db_manager.load_volumes_from_json(volumes_path)
            
            # Check stats again
            stats = db_manager.get_database_stats()
            assert stats['chapter'] == 2
            assert stats['volume'] == 2
            assert stats['coc'] > 0  # Should have some character-chapter relationships
            
        finally:
            Path(chapters_path).unlink(missing_ok=True)
            Path(volumes_path).unlink(missing_ok=True)
    
    def test_export_to_csv(self, db_manager, sample_chapters_data):
        """Test exporting database to CSV files."""
        db_manager.create_tables()
        
        # Load some test data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_chapters_data, f)
            json_path = f.name
        
        try:
            db_manager.load_chapters_from_json(json_path)
            
            # Test export
            with tempfile.TemporaryDirectory() as temp_dir:
                success = db_manager.export_to_csv(temp_dir)
                assert success is True
                
                # Check that CSV files were created
                expected_files = ['chapter.csv', 'volume.csv', 'character.csv', 'coc.csv']
                for filename in expected_files:
                    file_path = Path(temp_dir) / filename
                    assert file_path.exists(), f"{filename} was not created"
                
                # Verify chapter CSV content
                chapter_df = pd.read_csv(Path(temp_dir) / 'chapter.csv')
                assert len(chapter_df) == 2
                assert 'Romance Dawn' in chapter_df['title'].values
                
        finally:
            Path(json_path).unlink(missing_ok=True)
    
    def test_query(self, db_manager, sample_chapters_data):
        """Test SQL query execution."""
        db_manager.create_tables()
        
        # Load test data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_chapters_data, f)
            json_path = f.name
        
        try:
            db_manager.load_chapters_from_json(json_path)
            
            # Test query
            result_df = db_manager.query("SELECT title FROM chapter WHERE number = 1")
            assert len(result_df) == 1
            assert result_df.iloc[0]['title'] == 'Romance Dawn'
            
            # Test invalid query
            result_df = db_manager.query("INVALID SQL")
            assert len(result_df) == 0  # Should return empty DataFrame on error
            
        finally:
            Path(json_path).unlink(missing_ok=True)
    
    def test_context_manager(self, temp_db_path):
        """Test using DatabaseManager as context manager."""
        with DatabaseManager(temp_db_path) as db:
            assert db.conn is not None
            
            # Test basic operation
            db.create_tables()
            stats = db.get_database_stats()
            assert isinstance(stats, dict)
        
        # Connection should be closed after exiting context
        assert db.conn is None
    
    def test_load_with_malformed_data(self, db_manager):
        """Test loading malformed JSON data."""
        db_manager.create_tables()
        
        # Create malformed JSON data
        malformed_data = [
            {'chapter_number': 'not_a_number'},  # Invalid chapter number
            {},  # Missing required fields
            {'chapter_number': 1, 'title': 'Valid Chapter'}  # Valid data
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(malformed_data, f)
            json_path = f.name
        
        try:
            # Should handle errors gracefully and continue with valid data
            success = db_manager.load_chapters_from_json(json_path)
            assert success is True  # Should still succeed overall
            
            # Check that only valid data was loaded
            stats = db_manager.get_database_stats()
            assert stats['chapter'] <= 1  # Only valid chapters should be loaded
            
        finally:
            Path(json_path).unlink(missing_ok=True)
