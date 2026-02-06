"""Tests for FandomAPIClient."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from onepieceofdata.api import FandomAPIClient


class TestFandomAPIClient:
    """Test suite for FandomAPIClient."""

    def test_init(self):
        """Test client initialization."""
        client = FandomAPIClient(wiki="onepiece")
        assert client.base_url == "https://onepiece.fandom.com/api.php"

    def test_init_custom_wiki(self):
        """Test client initialization with custom wiki."""
        client = FandomAPIClient(wiki="naruto")
        assert client.base_url == "https://naruto.fandom.com/api.php"

    @patch('onepieceofdata.api.fandom_client.urllib3.PoolManager')
    def test_get_page_wikitext_success(self, mock_pool_manager):
        """Test successful page retrieval."""
        # Mock HTTP response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = b'''{
            "query": {
                "pages": {
                    "123": {
                        "pageid": 123,
                        "title": "Chapter 1",
                        "revisions": [{
                            "slots": {
                                "main": {
                                    "*": "{{Chapter Box\\n| title = Test}}"
                                }
                            }
                        }]
                    }
                }
            }
        }'''

        mock_pool = Mock()
        mock_pool.request.return_value = mock_response
        mock_pool_manager.return_value = mock_pool

        client = FandomAPIClient()
        result = client.get_page_wikitext("Chapter 1")

        assert result is not None
        assert "Chapter Box" in result

    @patch('onepieceofdata.api.fandom_client.urllib3.PoolManager')
    def test_get_page_wikitext_not_found(self, mock_pool_manager):
        """Test page not found scenario."""
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = b'''{
            "query": {
                "pages": {
                    "-1": {
                        "missing": ""
                    }
                }
            }
        }'''

        mock_pool = Mock()
        mock_pool.request.return_value = mock_response
        mock_pool_manager.return_value = mock_pool

        client = FandomAPIClient()
        result = client.get_page_wikitext("NonExistent Page")

        assert result is None

    @patch('onepieceofdata.api.fandom_client.urllib3.PoolManager')
    def test_get_multiple_pages_wikitext(self, mock_pool_manager):
        """Test batch page retrieval."""
        mock_response = Mock()
        mock_response.status = 200
        mock_response.data = b'''{
            "query": {
                "pages": {
                    "123": {
                        "title": "Chapter 1",
                        "revisions": [{
                            "slots": {
                                "main": {
                                    "*": "{{Chapter Box | title = Chapter 1}}"
                                }
                            }
                        }]
                    },
                    "124": {
                        "title": "Chapter 2",
                        "revisions": [{
                            "slots": {
                                "main": {
                                    "*": "{{Chapter Box | title = Chapter 2}}"
                                }
                            }
                        }]
                    }
                }
            }
        }'''

        mock_pool = Mock()
        mock_pool.request.return_value = mock_response
        mock_pool_manager.return_value = mock_pool

        client = FandomAPIClient()
        results = client.get_multiple_pages_wikitext(["Chapter 1", "Chapter 2"])

        assert len(results) == 2
        assert "Chapter 1" in results
        assert "Chapter 2" in results


class TestFandomAPIClientIntegration:
    """Integration tests that make real API calls."""

    @pytest.mark.integration
    def test_get_page_wikitext_real(self):
        """Test real API call to get Chapter 1."""
        client = FandomAPIClient(wiki="onepiece")
        result = client.get_page_wikitext("Chapter 1")

        assert result is not None
        assert len(result) > 0
        assert "Chapter Box" in result or "chapter" in result.lower()

    @pytest.mark.integration
    def test_batch_query_real(self):
        """Test real batch query."""
        client = FandomAPIClient(wiki="onepiece")
        results = client.get_multiple_pages_wikitext(["Chapter 1", "Chapter 2", "Chapter 3"])

        assert len(results) >= 2
        # At least some should have content
        assert any(v is not None and len(v) > 0 for v in results.values())

    @pytest.mark.integration
    def test_page_not_found_real(self):
        """Test real API call for non-existent page."""
        client = FandomAPIClient(wiki="onepiece")
        result = client.get_page_wikitext("ThisPageDefinitelyDoesNotExist12345")

        assert result is None
