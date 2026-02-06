"""Fandom MediaWiki API client for scraping wiki data."""

import time
from typing import Dict, List, Optional, Any
import urllib3
from urllib.parse import urlencode
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..config.settings import get_settings


class FandomAPIClient:
    """Client for interacting with Fandom's MediaWiki API.

    This client bypasses Cloudflare by using the official MediaWiki API
    instead of scraping HTML pages directly.
    """

    def __init__(self, wiki: str = "onepiece"):
        """Initialize the Fandom API client.

        Args:
            wiki: The Fandom wiki name (default: "onepiece")
        """
        self.settings = get_settings()
        self.base_url = f"https://{wiki}.fandom.com/api.php"
        self.http_pool = urllib3.PoolManager()
        self.last_request_time = 0

        logger.info(f"Initialized Fandom API client for {wiki} wiki")

    def _rate_limit(self):
        """Apply rate limiting between requests."""
        if self.settings.scraping_delay > 0:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.settings.scraping_delay:
                time.sleep(self.settings.scraping_delay - elapsed)
        self.last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((urllib3.exceptions.HTTPError, Exception))
    )
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make an API request with retry logic.

        Args:
            params: Query parameters for the API request

        Returns:
            Parsed JSON response

        Raises:
            Exception: If the request fails after retries
        """
        # Always add format=json
        params['format'] = 'json'

        # Build URL
        url = f"{self.base_url}?{urlencode(params)}"

        logger.debug(f"API request: {params.get('action', 'unknown')}")

        self._rate_limit()

        try:
            response = self.http_pool.request(
                'GET',
                url,
                headers={
                    'User-Agent': 'OnePieceOfData/2.0 (https://github.com/ismailsunni/onepieceofdata; bot)',
                    'Accept': 'application/json',
                },
                timeout=self.settings.request_timeout
            )

            if response.status != 200:
                raise Exception(f"HTTP {response.status} error for API request")

            # Parse JSON
            import json
            data = json.loads(response.data.decode('utf-8'))

            # Check for API errors
            if 'error' in data:
                error_info = data['error']
                raise Exception(f"API error: {error_info.get('code', 'unknown')} - {error_info.get('info', 'no details')}")

            return data

        except Exception as e:
            logger.warning(f"API request failed: {str(e)}")
            raise

    def get_page_wikitext(self, title: str) -> Optional[str]:
        """Get the raw wikitext content of a page.

        Args:
            title: Page title (e.g., "Chapter 1", "Monkey D. Luffy")

        Returns:
            Raw wikitext content or None if page not found
        """
        params = {
            'action': 'query',
            'titles': title,
            'prop': 'revisions',
            'rvprop': 'content',
            'rvslots': 'main',
        }

        try:
            data = self._make_request(params)

            # Extract page content from response
            pages = data.get('query', {}).get('pages', {})

            # Get the first (and should be only) page
            page_id = list(pages.keys())[0]

            if page_id == '-1':
                logger.warning(f"Page not found: {title}")
                return None

            page = pages[page_id]
            revisions = page.get('revisions', [])

            if not revisions:
                logger.warning(f"No revisions found for page: {title}")
                return None

            content = revisions[0].get('slots', {}).get('main', {}).get('*', '')

            logger.debug(f"Retrieved wikitext for: {title} ({len(content)} chars)")
            return content

        except Exception as e:
            logger.error(f"Failed to get wikitext for {title}: {str(e)}")
            return None

    def get_multiple_pages_wikitext(self, titles: List[str]) -> Dict[str, Optional[str]]:
        """Get wikitext for multiple pages in a single request (batch query).

        Args:
            titles: List of page titles

        Returns:
            Dictionary mapping title to wikitext content
        """
        if not titles:
            return {}

        # MediaWiki API supports up to 50 titles per request for anonymous users
        batch_size = 50
        results = {}

        for i in range(0, len(titles), batch_size):
            batch = titles[i:i+batch_size]
            titles_param = '|'.join(batch)

            params = {
                'action': 'query',
                'titles': titles_param,
                'prop': 'revisions',
                'rvprop': 'content',
                'rvslots': 'main',
            }

            try:
                data = self._make_request(params)
                pages = data.get('query', {}).get('pages', {})

                for page_id, page_data in pages.items():
                    if page_id == '-1':
                        continue

                    page_title = page_data.get('title', '')
                    revisions = page_data.get('revisions', [])

                    if revisions:
                        content = revisions[0].get('slots', {}).get('main', {}).get('*', '')
                        results[page_title] = content
                    else:
                        results[page_title] = None

                logger.info(f"Retrieved {len(results)} pages in batch query")

            except Exception as e:
                logger.error(f"Failed to batch query pages: {str(e)}")
                # Add None for failed pages
                for title in batch:
                    if title not in results:
                        results[title] = None

        return results

    def get_page_html(self, title: str) -> Optional[str]:
        """Get the rendered HTML content of a page.

        Args:
            title: Page title

        Returns:
            Rendered HTML or None if page not found
        """
        params = {
            'action': 'parse',
            'page': title,
            'prop': 'text',
        }

        try:
            data = self._make_request(params)
            html = data.get('parse', {}).get('text', {}).get('*', '')

            logger.debug(f"Retrieved HTML for: {title} ({len(html)} chars)")
            return html

        except Exception as e:
            logger.error(f"Failed to get HTML for {title}: {str(e)}")
            return None

    def get_page_info(self, title: str) -> Optional[Dict[str, Any]]:
        """Get page metadata (ID, last revision, etc.).

        Args:
            title: Page title

        Returns:
            Dictionary with page info or None if not found
        """
        params = {
            'action': 'query',
            'titles': title,
            'prop': 'info',
        }

        try:
            data = self._make_request(params)
            pages = data.get('query', {}).get('pages', {})
            page_id = list(pages.keys())[0]

            if page_id == '-1':
                return None

            return pages[page_id]

        except Exception as e:
            logger.error(f"Failed to get page info for {title}: {str(e)}")
            return None

    def search_pages(self, search_term: str, limit: int = 10) -> List[str]:
        """Search for pages matching a term.

        Args:
            search_term: Search query
            limit: Maximum number of results

        Returns:
            List of matching page titles
        """
        params = {
            'action': 'query',
            'list': 'search',
            'srsearch': search_term,
            'srlimit': limit,
        }

        try:
            data = self._make_request(params)
            search_results = data.get('query', {}).get('search', [])

            titles = [result['title'] for result in search_results]
            logger.info(f"Found {len(titles)} pages matching: {search_term}")

            return titles

        except Exception as e:
            logger.error(f"Search failed for {search_term}: {str(e)}")
            return []

    def cleanup(self):
        """Clean up resources."""
        if hasattr(self.http_pool, 'clear'):
            self.http_pool.clear()
