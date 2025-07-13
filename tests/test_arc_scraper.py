"""Tests for arc scraper functionality."""

import pytest
from unittest.mock import Mock, patch
from bs4 import BeautifulSoup

from src.onepieceofdata.scrapers.arc import ArcScraper
from src.onepieceofdata.models.data import ArcModel, ScrapingResult


class TestArcScraper:
    """Test cases for ArcScraper."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.scraper = ArcScraper()
        
    def test_clean_title(self):
        """Test title cleaning functionality."""
        assert self.scraper._clean_title("East Blue Saga") == "East Blue"
        assert self.scraper._clean_title("Romance Dawn Arc") == "Romance Dawn"
        assert self.scraper._clean_title("  Alabasta   Arc  ") == "Alabasta"
        assert self.scraper._clean_title("Water 7") == "Water 7"
        
    def test_extract_chapter_range(self):
        """Test chapter range extraction."""
        # Test various formats
        start, end = self.scraper._extract_chapter_range("Chapter 1-7")
        assert start == 1 and end == 7
        
        start, end = self.scraper._extract_chapter_range("Chapters 8-19")
        assert start == 8 and end == 19
        
        start, end = self.scraper._extract_chapter_range("Chapter 20")
        assert start == 20 and end == 20
        
        start, end = self.scraper._extract_chapter_range("Ch. 100-150")
        assert start == 100 and end == 150
        
        # Test invalid format
        start, end = self.scraper._extract_chapter_range("No chapters here")
        assert start is None and end is None
        
    def test_extract_arc_from_row(self):
        """Test arc extraction from table row."""
        # Create mock table cells
        mock_html = """
        <tr>
            <td><a href="/wiki/Romance_Dawn_Arc">Romance Dawn Arc</a></td>
            <td>Chapter 1-7</td>
            <td>The beginning of Luffy's adventure</td>
        </tr>
        """
        soup = BeautifulSoup(mock_html, 'html.parser')
        cells = soup.find('tr').find_all(['td', 'th'])
        
        arc = self.scraper._extract_arc_from_row(cells)
        
        assert arc is not None
        assert arc.title == "Romance Dawn"
        assert arc.start_chapter == 1
        assert arc.end_chapter == 7
        assert arc.arc_id == "romance_dawn"
        
    def test_extract_arc_from_row_invalid(self):
        """Test arc extraction with invalid data."""
        # Empty cells
        mock_html = "<tr><td></td><td></td></tr>"
        soup = BeautifulSoup(mock_html, 'html.parser')
        cells = soup.find('tr').find_all(['td', 'th'])
        
        arc = self.scraper._extract_arc_from_row(cells)
        assert arc is None
        
    @patch('src.onepieceofdata.scrapers.arc.ArcScraper._fetch_page')
    def test_scrape_arcs_from_list_page_success(self, mock_fetch):
        """Test successful arc scraping from list page."""
        # Mock HTML response
        mock_html = """
        <div class="mw-parser-output">
            <table class="wikitable">
                <tr><th>Arc</th><th>Chapters</th><th>Description</th></tr>
                <tr>
                    <td><a href="/wiki/Romance_Dawn_Arc">Romance Dawn Arc</a></td>
                    <td>Chapter 1-7</td>
                    <td>Beginning</td>
                </tr>
                <tr>
                    <td><a href="/wiki/Orange_Town_Arc">Orange Town Arc</a></td>
                    <td>Chapter 8-21</td>
                    <td>Buggy arc</td>
                </tr>
            </table>
        </div>
        """
        
        mock_soup = BeautifulSoup(mock_html, 'html.parser')
        mock_fetch.return_value = mock_soup
        
        results = self.scraper.scrape_arcs_from_list_page()
        
        assert len(results) == 2
        assert all(result.success for result in results)
        
        # Check first arc
        first_arc_data = results[0].data
        assert first_arc_data['title'] == "Romance Dawn"
        assert first_arc_data['start_chapter'] == 1
        assert first_arc_data['end_chapter'] == 7
        
    @patch('src.onepieceofdata.scrapers.arc.ArcScraper._fetch_page')
    def test_scrape_arcs_from_list_page_failure(self, mock_fetch):
        """Test arc scraping failure handling."""
        mock_fetch.side_effect = Exception("Network error")
        
        results = self.scraper.scrape_arcs_from_list_page()
        
        assert len(results) == 1
        assert not results[0].success
        assert "Network error" in results[0].error
        
    def test_cleanup(self):
        """Test cleanup functionality."""
        # Should not raise any exceptions
        self.scraper.cleanup()
        
    def teardown_method(self):
        """Clean up after tests."""
        self.scraper.cleanup()
