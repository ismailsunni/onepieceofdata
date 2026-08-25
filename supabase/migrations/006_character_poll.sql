-- Migration 006: character_poll table for character popularity poll rankings
-- Source: the Global ONE PIECE Character Popularity Poll (WT100).
-- Populated by `make export-postgres-poll` from the DuckDB character_poll table;
-- this migration documents the schema and the indexes the exporter does not create.

CREATE TABLE IF NOT EXISTS character_poll (
    poll_id      TEXT    NOT NULL,   -- e.g. 'wt100_2026'
    rank         INTEGER NOT NULL,   -- ties share a rank
    name         TEXT    NOT NULL,   -- name as published by the poll site
    points       BIGINT,             -- only published for the global top 10
    character_id TEXT,               -- our character.id, NULL when unmatched; deliberately
                                     -- no FK: the weekly full export drops and recreates
                                     -- character, which would cascade the constraint away.
                                     -- scripts/check_poll_links.py guards the link instead.
    is_variant   BOOLEAN,            -- alternate form of an already-ranked character
    site_face_id TEXT,               -- face asset id on the poll site
    image_url    TEXT,               -- source image on the poll site
    image_path   TEXT,               -- object path inside the character-images Storage bucket
    PRIMARY KEY (poll_id, rank, name)
);

CREATE INDEX IF NOT EXISTS idx_character_poll_character_id ON character_poll (character_id);
CREATE INDEX IF NOT EXISTS idx_character_poll_poll_rank ON character_poll (poll_id, rank);
