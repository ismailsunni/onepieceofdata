"""Upload scraped wiki text to Supabase / PostgreSQL."""

import json
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from loguru import logger

from ..config.settings import get_settings


class WikiTextUploader:
    """Uploads parsed wiki page text to the wiki_text table in PostgreSQL."""

    def __init__(self, connection_url: Optional[str] = None):
        """Connect using existing env vars or an explicit URL.

        Args:
            connection_url: Full PostgreSQL connection URL. If None, uses
                            settings.postgres_connection_url (built from env vars).
        """
        settings = get_settings()
        url = connection_url or settings.postgres_connection_url
        logger.info("Connecting to PostgreSQL for wiki text upload...")
        self.conn = psycopg2.connect(url)
        self.conn.autocommit = False
        logger.success("PostgreSQL connection established")

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def create_table(self) -> None:
        """Create the wiki_text table if it does not already exist."""
        ddl = """
        CREATE TABLE IF NOT EXISTS wiki_text (
            page_id     TEXT PRIMARY KEY,
            page_type   TEXT NOT NULL,
            title       TEXT,
            intro_text  TEXT,
            full_text   TEXT,
            sections    JSONB,
            scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(ddl)
        self.conn.commit()
        logger.info("wiki_text table ready")

    # ------------------------------------------------------------------
    # Upload
    # ------------------------------------------------------------------

    def upload_pages(self, pages: list, page_type: str) -> int:
        """Upsert a list of parsed page dicts into the wiki_text table.

        Args:
            pages: List of dicts as returned by WikiTextScraper.scrape_page()
                   (keys: page_id, title, intro_text, full_text, sections).
            page_type: One of "character", "arc", "saga" (or any label).

        Returns:
            Number of rows successfully upserted.
        """
        if not pages:
            logger.warning(f"No {page_type} pages to upload")
            return 0

        upsert_sql = """
        INSERT INTO wiki_text
            (page_id, page_type, title, intro_text, full_text, sections, scraped_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (page_id)
        DO UPDATE SET
            page_type  = EXCLUDED.page_type,
            title      = EXCLUDED.title,
            intro_text = EXCLUDED.intro_text,
            full_text  = EXCLUDED.full_text,
            sections   = EXCLUDED.sections,
            scraped_at = EXCLUDED.scraped_at;
        """

        now = datetime.now(timezone.utc)
        count = 0

        with self.conn.cursor() as cur:
            for page in pages:
                try:
                    cur.execute(
                        upsert_sql,
                        (
                            page["page_id"],
                            page.get("page_type", page_type),
                            page.get("title"),
                            page.get("intro_text"),
                            page.get("full_text"),
                            json.dumps(page.get("sections", {})),
                            now,
                        ),
                    )
                    count += 1
                except Exception as e:
                    logger.error(f"Failed to upsert page {page.get('page_id')}: {e}")
                    self.conn.rollback()
                    raise

        self.conn.commit()
        logger.success(f"Upserted {count} {page_type} pages into wiki_text")
        return count

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """Return stats about rows in the wiki_text table.

        Returns:
            Dict with keys: total (int), by_type (dict[str, int]).
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wiki_text;")
            total = cur.fetchone()[0]

            cur.execute(
                "SELECT page_type, COUNT(*) FROM wiki_text GROUP BY page_type ORDER BY page_type;"
            )
            by_type = {row[0]: row[1] for row in cur.fetchall()}

        return {"total": total, "by_type": by_type}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            logger.debug("PostgreSQL connection closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
