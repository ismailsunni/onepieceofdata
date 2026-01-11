"""
One Piece of Data - A Python pipeline for scraping and processing One Piece manga data.

This package provides tools to:
- Scrape chapter, volume, and character data from One Piece Fandom Wiki
- Process and validate the scraped data
- Store data in a DuckDB database
- Export data in various formats

Usage:
    # Command line interface
    uv run onepieceofdata --help
    
    # Programmatic usage
    from onepieceofdata import ChapterScraper, settings
    
    scraper = ChapterScraper()
    chapters = scraper.scrape_chapters(start_chapter=1, end_chapter=10)
"""

__version__ = "2.0.0"
__author__ = "Ismail Sunni"
__email__ = "ismailsunni@example.com"

# Import main components for easy access
from .config import settings, Settings
from .models import (
    ChapterModel,
    VolumeModel,
    CharacterModel,
    CharacterOfChapterModel,
    ScrapingResult,
)
from .scrapers.chapter import ChapterScraper
from .utils import setup_logging, get_logger

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",
    
    # Configuration
    "settings",
    "Settings",
    
    # Models
    "ChapterModel",
    "VolumeModel",
    "CharacterModel", 
    "CharacterOfChapterModel",
    "ScrapingResult",
    
    # Scrapers
    "ChapterScraper",
    
    # Utilities
    "setup_logging",
    "get_logger",
]
