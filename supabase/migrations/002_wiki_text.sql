-- Migration 002: wiki_text table for RAG pipeline
-- Stores cleaned wikitext for characters, arcs, and sagas.

CREATE TABLE IF NOT EXISTS wiki_text (
    page_id     TEXT        PRIMARY KEY,
    page_type   TEXT        NOT NULL,           -- 'character' | 'arc' | 'saga'
    title       TEXT,
    intro_text  TEXT,                           -- cleaned intro section
    full_text   TEXT,                           -- all sections concatenated
    sections    JSONB,                          -- {"intro": "...", "Appearance": "...", ...}
    scraped_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for filtering by page type
CREATE INDEX IF NOT EXISTS idx_wiki_text_page_type ON wiki_text (page_type);

-- Index to support full-text search (optional, used by pgvector stage)
CREATE INDEX IF NOT EXISTS idx_wiki_text_full_text_gin
    ON wiki_text USING gin(to_tsvector('english', COALESCE(full_text, '')));
