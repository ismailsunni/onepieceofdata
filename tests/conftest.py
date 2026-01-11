"""Test configuration and fixtures for One Piece of Data tests."""

import pytest
from pathlib import Path
import tempfile
import shutil

from onepieceofdata.config import Settings


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def test_settings(temp_dir):
    """Create test settings with temporary paths."""
    return Settings(
        data_dir=temp_dir / "data",
        database_path=temp_dir / "data" / "test.duckdb",
        log_file=temp_dir / "logs" / "test.log",
        last_chapter=10,  # Small number for testing
        last_volume=5,
        scraping_delay=0.1,  # Faster for tests
        max_retries=1,  # Fewer retries for tests
    )


@pytest.fixture
def sample_chapter_data():
    """Sample chapter data for testing."""
    return {
        "chapter_number": 1,
        "title": "Romance Dawn",
        "volume": 1,
        "pages": 20,
        "release_date": "July 22, 1997",
        "characters": [
            {
                "name": "Monkey D. Luffy",
                "url": "/wiki/Monkey_D._Luffy",
                "note": "",
                "full_text": "Monkey D. Luffy"
            }
        ]
    }
