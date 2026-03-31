"""Scrape full wiki page text for RAG pipeline."""

from typing import Optional
from loguru import logger

from ..api import FandomAPIClient
from ..parsers.wikitext_cleaner import parse_wiki_page


class WikiTextScraper:
    """Scrapes full wiki page wikitext for use in the RAG pipeline."""

    def __init__(self, database_path: Optional[str] = None):
        """Initialize the scraper.

        Args:
            database_path: Path to DuckDB database. Defaults to settings value.
        """
        self.client = FandomAPIClient()
        self._database_path = database_path

    def _get_db_path(self) -> str:
        """Resolve the DuckDB database path."""
        if self._database_path:
            return self._database_path

        from ..config.settings import get_settings
        return str(get_settings().database_path)

    def scrape_page(self, page_slug: str) -> Optional[dict]:
        """Fetch and parse a single wiki page.

        Uses FandomAPIClient.get_page_wikitext() to fetch raw wikitext,
        then parse_wiki_page() to clean it.

        Args:
            page_slug: Wiki page title/slug (spaces or underscores both work).

        Returns:
            Dict with keys: page_id, title, intro_text, full_text, sections.
            None if the page was not found or fetch failed.
        """
        # Normalise: replace underscores with spaces for the API title
        title = page_slug.replace('_', ' ')

        logger.debug(f"Fetching wikitext for: {title}")
        raw = self.client.get_page_wikitext(title)

        if raw is None:
            logger.warning(f"Page not found or fetch failed: {title}")
            return None

        intro_text, full_text, sections = parse_wiki_page(raw)

        return {
            "page_id": page_slug,
            "title": title,
            "intro_text": intro_text,
            "full_text": full_text,
            "sections": sections,
        }

    def scrape_characters(self, character_ids: Optional[list] = None) -> list:
        """Scrape all character pages.

        If character_ids is None, reads character ids from DuckDB.
        Logs progress every 50 pages.

        Args:
            character_ids: List of character id strings (wiki page slugs).
                           If None, reads from the character table.

        Returns:
            List of parsed page dicts (pages that failed are skipped).
        """
        if character_ids is None:
            character_ids = self._load_ids_from_db("SELECT id FROM character")
            logger.info(f"Loaded {len(character_ids)} character ids from database")

        results = []
        total = len(character_ids)

        for i, char_id in enumerate(character_ids, 1):
            page = self.scrape_page(char_id)
            if page is not None:
                page["page_type"] = "character"
                results.append(page)

            if i % 50 == 0 or i == total:
                logger.info(f"Characters: {i}/{total} scraped, {len(results)} successful")

        return results

    def scrape_arcs(self) -> list:
        """Scrape arc pages, reading arc titles from DuckDB.

        Arc titles are used as wiki page names (spaces → underscores).

        Returns:
            List of parsed page dicts.
        """
        rows = self._load_rows_from_db("SELECT arc_id, title FROM arc")
        logger.info(f"Loaded {len(rows)} arcs from database")

        results = []
        for arc_id, title in rows:
            # Use title as the wiki page name
            page_slug = title.replace(' ', '_') if title else arc_id
            page = self.scrape_page(page_slug)
            if page is not None:
                page["page_id"] = arc_id
                page["page_type"] = "arc"
                results.append(page)

        logger.info(f"Scraped {len(results)}/{len(rows)} arc pages successfully")
        return results

    def scrape_sagas(self) -> list:
        """Scrape saga pages, reading saga titles from DuckDB.

        Returns:
            List of parsed page dicts.
        """
        rows = self._load_rows_from_db("SELECT saga_id, title FROM saga")
        logger.info(f"Loaded {len(rows)} sagas from database")

        results = []
        for saga_id, title in rows:
            page_slug = title.replace(' ', '_') if title else saga_id
            page = self.scrape_page(page_slug)
            if page is not None:
                page["page_id"] = saga_id
                page["page_type"] = "saga"
                results.append(page)

        logger.info(f"Scraped {len(results)}/{len(rows)} saga pages successfully")
        return results

    def scrape_all(self) -> list:
        """Scrape characters, arcs, and sagas.

        Returns:
            Combined list of all parsed page dicts.
        """
        logger.info("Starting full wiki text scrape (characters + arcs + sagas)")

        characters = self.scrape_characters()
        arcs = self.scrape_arcs()
        sagas = self.scrape_sagas()

        all_pages = characters + arcs + sagas
        logger.success(
            f"Scrape complete: {len(characters)} characters, "
            f"{len(arcs)} arcs, {len(sagas)} sagas — "
            f"{len(all_pages)} total"
        )
        return all_pages

    def save_to_duckdb(self, pages: list, db_path: str = None):
        """Save scraped wiki text pages to DuckDB.

        Creates the wiki_text table if it doesn't exist.
        Uses INSERT OR REPLACE for upsert behavior.

        Args:
            pages: List of page dicts from scrape_*() methods.
            db_path: DuckDB path. Defaults to the configured database.
        """
        import duckdb
        import json
        from datetime import datetime

        db_path = db_path or self._get_db_path()
        conn = duckdb.connect(db_path)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS wiki_text (
                page_id VARCHAR PRIMARY KEY,
                page_type VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                intro_text TEXT,
                full_text TEXT,
                sections JSON,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        now = datetime.now()
        inserted = 0
        for page in pages:
            conn.execute("""
                INSERT OR REPLACE INTO wiki_text
                (page_id, page_type, title, intro_text, full_text, sections, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                page['page_id'],
                page.get('page_type', 'character'),
                page['title'],
                page.get('intro_text', ''),
                page.get('full_text', ''),
                json.dumps(page.get('sections', {})),
                now,
            ])
            inserted += 1

        conn.close()
        logger.success(f"Saved {inserted} wiki text pages to DuckDB ({db_path})")

    def get_duckdb_stats(self, db_path: str = None) -> dict:
        """Get wiki_text table stats from DuckDB.

        Returns:
            Dict with total count and counts by page_type.
        """
        import duckdb

        db_path = db_path or self._get_db_path()
        conn = duckdb.connect(db_path, read_only=True)

        try:
            total = conn.execute("SELECT count(*) FROM wiki_text").fetchone()[0]
            by_type = conn.execute(
                "SELECT page_type, count(*) FROM wiki_text GROUP BY page_type ORDER BY page_type"
            ).fetchall()
            conn.close()
            return {"total": total, "by_type": {r[0]: r[1] for r in by_type}}
        except Exception:
            conn.close()
            return {"total": 0, "by_type": {}}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_ids_from_db(self, query: str) -> list:
        """Execute a single-column SELECT and return the values as a list."""
        import duckdb
        db_path = self._get_db_path()
        try:
            conn = duckdb.connect(db_path, read_only=True)
            rows = conn.execute(query).fetchall()
            conn.close()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Failed to load ids from database ({db_path}): {e}")
            return []

    def _load_rows_from_db(self, query: str) -> list:
        """Execute a multi-column SELECT and return rows as a list of tuples."""
        import duckdb
        db_path = self._get_db_path()
        try:
            conn = duckdb.connect(db_path, read_only=True)
            rows = conn.execute(query).fetchall()
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"Failed to load rows from database ({db_path}): {e}")
            return []

    def cleanup(self):
        """Release HTTP resources."""
        self.client.cleanup()
