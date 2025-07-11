"""Tests for character scraper."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import json
import pandas as pd

from onepieceofdata.scrapers.character import CharacterScraper
from onepieceofdata.models.data import ScrapingResult


class TestCharacterScraper:
    """Test cases for CharacterScraper."""
    
    @pytest.fixture
    def scraper(self):
        """Create a CharacterScraper instance for testing."""
        return CharacterScraper()
    
    @pytest.fixture
    def mock_character_html(self):
        """Mock HTML content for character page."""
        return '''
        <html>
            <body>
                <h2 data-source="name">Monkey D. Luffy</h2>
                <section class="pi-item pi-group pi-border-color pi-collapse pi-collapse-open">
                    <div data-source="epithet">
                        <div class="pi-data-value pi-font">Straw Hat</div>
                    </div>
                    <div data-source="affiliation">
                        <div class="pi-data-value pi-font">
                            <a title="Straw Hat Pirates">Straw Hat Pirates</a>
                        </div>
                    </div>
                    <div data-source="age">
                        <div class="pi-data-value pi-font">17 (debut); 19 (post-timeskip)</div>
                    </div>
                    <div data-source="bounty">
                        <div class="pi-data-value pi-font">3,000,000,000</div>
                    </div>
                </section>
            </body>
        </html>
        '''
    
    @pytest.fixture
    def sample_character_data(self):
        """Sample character data for testing."""
        return {
            'id': 'luffy',
            'name': 'Monkey D. Luffy',
            'url': '/wiki/Monkey_D._Luffy'
        }
    
    def test_scraper_initialization(self, scraper):
        """Test scraper initialization."""
        assert scraper.base_url == "https://onepiece.fandom.com"
        assert scraper.http_pool is not None
        assert scraper.settings is not None
    
    def test_remove_footnote(self, scraper):
        """Test footnote removal."""
        text_with_footnote = "This is text[1] with footnotes[23]"
        result = scraper._remove_footnote(text_with_footnote)
        assert result == "This is text with footnotes"
    
    def test_remove_note(self, scraper):
        """Test note removal."""
        text_with_note = "Age 19 (post-timeskip)"
        result = scraper._remove_note(text_with_note)
        assert result == "Age 19"
    
    def test_parse_generic_with_semicolon(self, scraper):
        """Test parsing generic content with semicolons."""
        from bs4 import BeautifulSoup
        html = '<div>Value 1; Value 2; Value 3</div>'
        div = BeautifulSoup(html, 'html.parser').find('div')
        
        result = scraper._parse_generic(div)
        assert result == ['Value 1', 'Value 2', 'Value 3']
    
    def test_parse_generic_with_br_tags(self, scraper):
        """Test parsing generic content with br tags."""
        from bs4 import BeautifulSoup
        html = '<div>Value 1<br/>Value 2<br/>Value 3</div>'
        div = BeautifulSoup(html, 'html.parser').find('div')
        
        result = scraper._parse_generic(div)
        assert result == ['Value 1', 'Value 2', 'Value 3']
    
    def test_parse_list(self, scraper):
        """Test parsing list content."""
        from bs4 import BeautifulSoup
        html = '''
        <div>
            <ul>
                <li>Item 1</li>
                <li>Item 2[1]</li>
                <li>Item 3</li>
            </ul>
        </div>
        '''
        div = BeautifulSoup(html, 'html.parser').find('div')
        
        result = scraper._parse_list(div)
        assert result == ['Item 1', 'Item 2', 'Item 3']
    
    def test_parse_affiliation(self, scraper):
        """Test parsing affiliation content."""
        from bs4 import BeautifulSoup
        html = '''
        <div>
            <a title="Straw Hat Pirates">Pirates</a>
            <a title="Worst Generation">Generation</a>
        </div>
        '''
        div = BeautifulSoup(html, 'html.parser').find('div')
        
        result = scraper._parse_affiliation(div)
        assert result == ['Straw Hat Pirates', 'Worst Generation']
    
    def test_parse_bounty(self, scraper):
        """Test parsing bounty values."""
        bounties = ['3,000,000,000', '1,500,000,000', '500,000,000']
        result = scraper._parse_bounty(bounties)
        assert result == ['3000000000', '1500000000', '500000000']
    
    def test_parse_alias(self, scraper):
        """Test parsing alias values."""
        aliases = ['Straw Hat (麦わら)', 'Captain (キャプテン)', 'Luffy']
        result = scraper._parse_alias(aliases)
        assert result == ['Straw Hat', 'Captain', 'Luffy']
    
    @patch('urllib3.PoolManager')
    def test_scrape_character_from_url_success(self, mock_pool_manager, scraper, mock_character_html):
        """Test successful character scraping from URL."""
        # Setup mock
        mock_pool = Mock()
        mock_pool_manager.return_value = mock_pool
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = mock_character_html.encode('utf-8')
        mock_pool.urlopen.return_value = mock_response
        
        scraper.http_pool = mock_pool
        
        # Test
        result = scraper._scrape_character_from_url("https://test.com/character")
        
        assert result['name'] == 'Monkey D. Luffy'
        assert 'epithet' in result
        assert 'affiliation' in result
        assert 'age' in result
        assert 'bounty' in result
    
    @patch('urllib3.PoolManager')
    def test_scrape_character_from_url_http_error(self, mock_pool_manager, scraper):
        """Test HTTP error when scraping character."""
        mock_pool = Mock()
        mock_pool_manager.return_value = mock_pool
        mock_response = Mock()
        mock_response.status = 404
        mock_pool.urlopen.return_value = mock_response
        
        scraper.http_pool = mock_pool
        
        with pytest.raises(Exception, match="RetryError"):
            scraper._scrape_character_from_url("https://test.com/character")
    
    def test_scrape_character_invalid_url(self, scraper):
        """Test scraping character with invalid URL."""
        character_data = {
            'id': 'test',
            'name': 'Test Character',
            'url': None
        }
        
        result = scraper.scrape_character(character_data)
        
        assert isinstance(result, ScrapingResult)
        assert result.success is False
        assert "No URL provided" in result.error
    
    def test_scrape_character_external_url(self, scraper):
        """Test scraping character with external URL."""
        character_data = {
            'id': 'test',
            'name': 'Test Character',
            'url': 'http://external-site.com/character'
        }
        
        result = scraper.scrape_character(character_data)
        
        assert isinstance(result, ScrapingResult)
        assert result.success is False
        assert "Invalid URL" in result.error
    
    def test_scrape_character_video_game_url(self, scraper):
        """Test scraping character with video game URL."""
        character_data = {
            'id': 'test',
            'name': 'Test Character',
            'url': '/wiki/Video_Games/Character'
        }
        
        result = scraper.scrape_character(character_data)
        
        assert isinstance(result, ScrapingResult)
        assert result.success is False
        assert "Invalid URL" in result.error
    
    @patch.object(CharacterScraper, '_scrape_character_from_url')
    def test_scrape_character_success(self, mock_scrape_url, scraper, sample_character_data):
        """Test successful character scraping."""
        mock_scrape_url.return_value = {
            'name': 'Monkey D. Luffy',
            'epithet': ['Straw Hat'],
            'affiliation': ['Straw Hat Pirates'],
            'age': ['19']
        }
        
        result = scraper.scrape_character(sample_character_data)
        
        assert isinstance(result, ScrapingResult)
        assert result.success is True
        assert result.data['name'] == 'Monkey D. Luffy'
    
    @patch.object(CharacterScraper, '_scrape_character_from_url')
    def test_scrape_character_scraping_failure(self, mock_scrape_url, scraper, sample_character_data):
        """Test character scraping failure."""
        mock_scrape_url.side_effect = Exception("Network error")
        
        result = scraper.scrape_character(sample_character_data)
        
        assert isinstance(result, ScrapingResult)
        assert result.success is False
        assert "Network error" in result.error
    
    def test_scrape_characters_multiple(self, scraper):
        """Test scraping multiple characters."""
        characters_data = [
            {'id': '1', 'name': 'Character 1', 'url': '/wiki/Character_1'},
            {'id': '2', 'name': 'Character 2', 'url': None},  # This should fail
            {'id': '3', 'name': 'Character 3', 'url': '/wiki/Character_3'}
        ]
        
        with patch.object(scraper, 'scrape_character') as mock_scrape:
            mock_scrape.side_effect = [
                ScrapingResult(success=True, data={'name': 'Character 1'}, url="test"),
                ScrapingResult(success=False, error="No URL", url=None),
                ScrapingResult(success=True, data={'name': 'Character 3'}, url="test")
            ]
            
            results = scraper.scrape_characters(characters_data)
            
            assert len(results) == 3
            assert results[0].success is True
            assert results[1].success is False
            assert results[2].success is True
    
    def test_load_characters_from_csv(self, scraper):
        """Test loading characters from CSV."""
        # Create temporary CSV file
        csv_data = pd.DataFrame({
            'id': ['1', '2', '3'],
            'name': ['Luffy', 'Zoro', 'Nami'],
            'url': ['/wiki/Luffy', '/wiki/Zoro', '/wiki/Nami']
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            csv_data.to_csv(f.name, index=False)
            temp_path = f.name
        
        try:
            characters = scraper.load_characters_from_csv(temp_path)
            assert len(characters) == 3
            assert characters[0]['name'] == 'Luffy'
            assert characters[1]['name'] == 'Zoro'
            assert characters[2]['name'] == 'Nami'
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_load_characters_from_csv_failure(self, scraper):
        """Test loading characters from non-existent CSV."""
        characters = scraper.load_characters_from_csv("/non/existent/file.csv")
        assert characters == []
    
    def test_export_to_json(self, scraper):
        """Test exporting character results to JSON."""
        results = [
            ScrapingResult(
                success=True,
                data={'id': '1', 'name': 'Character 1'},
                url="test_url"
            ),
            ScrapingResult(
                success=False,
                error="Test error",
                url="test_url"
            ),
            ScrapingResult(
                success=True,
                data={'id': '2', 'name': 'Character 2'},
                url="test_url"
            )
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            success = scraper.export_to_json(results, temp_path)
            assert success is True
            
            # Verify file contents
            with open(temp_path, 'r') as f:
                data = json.load(f)
            
            assert len(data) == 2  # Only successful results
            assert data[0]['name'] == 'Character 1'
            assert data[1]['name'] == 'Character 2'
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_export_to_json_failure(self, scraper):
        """Test export failure handling."""
        results = [ScrapingResult(success=True, data={'test': 'data'}, url="test")]
        
        success = scraper.export_to_json(results, "/invalid/path/file.json")
        assert success is False
