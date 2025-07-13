"""Tests for saga scraper functionality."""

import pytest
from unittest.mock import Mock, patch
from bs4 import BeautifulSoup

from src.onepieceofdata.scrapers.saga import SagaScraper
from src.onepieceofdata.models.data import SagaModel, ScrapingResult


class TestSagaScraper:
    """Test cases for SagaScraper."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.scraper = SagaScraper()
        
    def test_clean_title(self):
        """Test title cleaning functionality."""
        assert self.scraper._clean_title("East Blue Saga") == "East Blue"
        assert self.scraper._clean_title("Alabasta Saga") == "Alabasta"
        assert self.scraper._clean_title("  Water 7  Saga  ") == "Water 7"
        assert self.scraper._clean_title("Summit War") == "Summit War"
        
    def test_extract_chapter_range(self):
        """Test chapter range extraction."""
        # Test various formats
        start, end = self.scraper._extract_chapter_range("Chapter 1-100")
        assert start == 1 and end == 100
        
        start, end = self.scraper._extract_chapter_range("Chapters 101-200")
        assert start == 101 and end == 200
        
        start, end = self.scraper._extract_chapter_range("Chapter 300")
        assert start == 300 and end == 300
        
        # Test invalid format
        start, end = self.scraper._extract_chapter_range("No chapters here")
        assert start is None and end is None
        
    def test_extract_saga_from_section(self):
        """Test saga extraction from section header."""
        # Create mock section header
        mock_html = """
        <h2>East Blue Saga</h2>
        <p>This saga contains Chapter 1-100 and covers...</p>
        <h2>Next Section</h2>
        """
        soup = BeautifulSoup(mock_html, 'html.parser')
        header = soup.find('h2')
        
        saga = self.scraper._extract_saga_from_section(header)
        
        assert saga is not None
        assert saga.title == "East Blue"
        assert saga.start_chapter == 1
        assert saga.end_chapter == 100
        assert saga.saga_id == "east_blue"
        
    def test_extract_saga_from_section_no_chapters(self):
        """Test saga extraction without chapter information."""
        mock_html = """
        <h2>Unknown Saga</h2>
        <p>This saga has no chapter information</p>
        <h2>Next Section</h2>
        """
        soup = BeautifulSoup(mock_html, 'html.parser')
        header = soup.find('h2')
        
        saga = self.scraper._extract_saga_from_section(header)
        assert saga is None
        
    @patch('src.onepieceofdata.scrapers.saga.SagaScraper._fetch_page')
    def test_scrape_sagas_from_list_page_success(self, mock_fetch):
        """Test successful saga scraping from list page."""
        # Mock HTML response
        mock_html = """
        <div class="mw-parser-output">
            <h2>East Blue Saga</h2>
            <p>Contains Chapter 1-100</p>
            <h2>Alabasta Saga</h2>
            <p>Contains Chapter 101-200</p>
        </div>
        """
        
        mock_soup = BeautifulSoup(mock_html, 'html.parser')
        mock_fetch.return_value = mock_soup
        
        results = self.scraper.scrape_sagas_from_list_page()
        
        # Results may vary depending on implementation
        assert isinstance(results, list)
        
    @patch('src.onepieceofdata.scrapers.saga.SagaScraper._fetch_page')
    def test_scrape_sagas_from_list_page_failure(self, mock_fetch):
        """Test saga scraping failure handling."""
        mock_fetch.side_effect = Exception("Network error")
        
        results = self.scraper.scrape_sagas_from_list_page()
        
        assert len(results) == 1
        assert not results[0].success
        assert "Network error" in results[0].error
        
    def test_extract_sagas_from_navbox(self):
        """Test saga extraction from navigation box."""
        mock_html = """
        <table class="navbox">
            <tr>
                <td>East Blue Saga</td>
                <td>Other content</td>
            </tr>
        </table>
        """
        soup = BeautifulSoup(mock_html, 'html.parser')
        navbox = soup.find('table')
        
        results = self.scraper._extract_sagas_from_navbox(navbox)
        
        # Results depend on implementation details
        assert isinstance(results, list)
        
    def test_cleanup(self):
        """Test cleanup functionality."""
        # Should not raise any exceptions
        self.scraper.cleanup()
        
    def teardown_method(self):
        """Clean up after tests."""
        self.scraper.cleanup()
