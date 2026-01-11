"""Tests for volume scraper."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import json

from onepieceofdata.scrapers.volume import VolumeScraper
from onepieceofdata.models.data import ScrapingResult


class TestVolumeScraper:
    """Test cases for VolumeScraper."""
    
    @pytest.fixture
    def scraper(self):
        """Create a VolumeScraper instance for testing."""
        return VolumeScraper()
    
    @pytest.fixture
    def mock_html(self):
        """Mock HTML content for testing."""
        return '''
        <html>
            <body>
                <table id="Volume_1">
                    <tr><td>Header 1</td><td>Header 2</td></tr>
                    <tr><td>Japanese</td><td>日本語タイトル</td></tr>
                    <tr><td>English</td><td>Test Volume Title</td></tr>
                    <tr><td>Title</td><td>Test Volume Title</td></tr>
                    <tr><td>Characters</td>
                        <td>
                            <li><a href="/wiki/Luffy">Monkey D. Luffy</a></li>
                            <li><a href="/wiki/Zoro">Roronoa Zoro</a></li>
                        </td>
                    </tr>
                </table>
            </body>
        </html>
        '''
    
    def test_scraper_initialization(self, scraper):
        """Test scraper initialization."""
        assert scraper.base_url == "https://onepiece.fandom.com/wiki/Chapters_and_Volumes/Volumes"
        assert scraper.http_pool is not None
        assert scraper.settings is not None
    
    @patch('urllib3.PoolManager')
    def test_fetch_volumes_page_success(self, mock_pool_manager, scraper, mock_html):
        """Test successful fetching of volumes page."""
        # Setup mock
        mock_pool = Mock()
        mock_pool_manager.return_value = mock_pool
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = mock_html.encode('utf-8')
        mock_pool.urlopen.return_value = mock_response
        
        # Create new scraper to use mocked pool
        scraper.http_pool = mock_pool
        
        # Test
        soup = scraper._fetch_volumes_page()
        assert soup is not None
        assert soup.find('table', id='Volume_1') is not None
    
    @patch('urllib3.PoolManager')
    def test_fetch_volumes_page_http_error(self, mock_pool_manager, scraper):
        """Test HTTP error when fetching volumes page."""
        # Setup mock
        mock_pool = Mock()
        mock_pool_manager.return_value = mock_pool
        mock_response = Mock()
        mock_response.status = 404
        mock_pool.urlopen.return_value = mock_response
        
        scraper.http_pool = mock_pool
        
        # Test
        with pytest.raises(Exception, match="RetryError"):
            scraper._fetch_volumes_page()
    
    def test_parse_volume_table_success(self, scraper, mock_html):
        """Test successful parsing of volume table."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(mock_html, 'html.parser')
        
        result = scraper._parse_volume_table(soup, 1)
        
        assert result is not None
        assert result['volume_number'] == 1
        assert result['english_title'] == 'Test Volume Title'
        assert len(result['cover_characters']) == 2
        assert result['cover_characters'][0]['name'] == 'Monkey D. Luffy'
        assert result['cover_characters'][0]['slug'] == 'Luffy'
    
    def test_parse_volume_table_missing_table(self, scraper, mock_html):
        """Test parsing when volume table is missing."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(mock_html, 'html.parser')
        
        result = scraper._parse_volume_table(soup, 999)  # Non-existent volume
        
        assert result is None
    
    @patch.object(VolumeScraper, '_fetch_volumes_page')
    @patch.object(VolumeScraper, '_parse_volume_table')
    def test_scrape_volume_success(self, mock_parse, mock_fetch, scraper):
        """Test successful volume scraping."""
        # Setup mocks
        mock_fetch.return_value = Mock()  # Mock BeautifulSoup object
        mock_parse.return_value = {
            'volume_number': 1,
            'english_title': 'Test Volume',
            'japanese_title': 'テストボリューム',
            'cover_characters': []
        }
        
        # Test
        result = scraper.scrape_volume(1)
        
        assert isinstance(result, ScrapingResult)
        assert result.success is True
        assert result.data is not None
        assert result.data['volume_number'] == 1
    
    @patch.object(VolumeScraper, '_fetch_volumes_page')
    def test_scrape_volume_fetch_failure(self, mock_fetch, scraper):
        """Test volume scraping when page fetch fails."""
        mock_fetch.side_effect = Exception("Network error")
        
        result = scraper.scrape_volume(1)
        
        assert isinstance(result, ScrapingResult)
        assert result.success is False
        assert "Network error" in result.error
    
    @patch.object(VolumeScraper, '_fetch_volumes_page')
    @patch.object(VolumeScraper, '_parse_volume_table')
    def test_scrape_volumes_multiple(self, mock_parse, mock_fetch, scraper):
        """Test scraping multiple volumes."""
        # Setup mocks
        mock_fetch.return_value = Mock()
        mock_parse.side_effect = [
            {'volume_number': 1, 'english_title': 'Volume 1', 'cover_characters': []},
            {'volume_number': 2, 'english_title': 'Volume 2', 'cover_characters': []},
            None  # Simulate failure for volume 3
        ]
        
        # Test
        results = scraper.scrape_volumes(1, 3)
        
        assert len(results) == 3
        assert results[0].success is True
        assert results[1].success is True
        assert results[2].success is False
    
    def test_export_to_json(self, scraper):
        """Test exporting results to JSON."""
        # Create test data
        results = [
            ScrapingResult(
                success=True,
                data={'volume_number': 1, 'english_title': 'Test Volume 1'},
                url="test_url"
            ),
            ScrapingResult(
                success=False,
                error="Test error",
                url="test_url"
            ),
            ScrapingResult(
                success=True,
                data={'volume_number': 2, 'english_title': 'Test Volume 2'},
                url="test_url"
            )
        ]
        
        # Test export
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            success = scraper.export_to_json(results, temp_path)
            assert success is True
            
            # Verify file contents
            with open(temp_path, 'r') as f:
                data = json.load(f)
            
            assert len(data) == 2  # Only successful results
            assert data[0]['volume_number'] == 1
            assert data[1]['volume_number'] == 2
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_export_to_json_failure(self, scraper):
        """Test export failure handling."""
        results = [ScrapingResult(success=True, data={'test': 'data'}, url="test")]
        
        # Try to export to invalid path
        success = scraper.export_to_json(results, "/invalid/path/file.json")
        assert success is False
