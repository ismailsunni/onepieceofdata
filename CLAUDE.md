# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**One Piece of Data** is a modern Python data pipeline that scrapes, processes, and stores One Piece manga information from the One Piece Fandom Wiki. Version 2.0 uses modern Python tooling (uv, Pydantic, DuckDB) with parallel processing, robust error handling, and PostgreSQL export capabilities.

## Core Architecture

### Three-Stage Pipeline

1. **Scraping**: Web scrapers extract data from Fandom Wiki
2. **Parsing**: Parsers validate and transform scraped data
3. **Storage**: Database operations handle DuckDB and PostgreSQL persistence

### Key Components

**Scrapers** (`src/onepieceofdata/scrapers/`)
- Each scraper (ChapterScraper, CharacterScraper, VolumeScraper, ArcScraper, SagaScraper) handles a specific data type
- All use urllib3 for HTTP with retry logic via tenacity
- Return `ScrapingResult` objects with success/failure tracking
- Support parallel processing through worker pools

**Parsers** (`src/onepieceofdata/parsers/`)
- Process scraped data before database insertion
- ArcParser includes `auto_link_arcs_to_sagas()` that links arcs to sagas based on chapter range containment
- Validate data using Pydantic models

**Database** (`src/onepieceofdata/database/`)
- `DatabaseManager` (operations.py): Main DuckDB interface
- `PostgresExporter` (postgres_export.py): Handles PostgreSQL export with full/incremental modes
- `SchemaMapper` (schema_mapper.py): Maps DuckDB types to PostgreSQL (including INTEGER[] arrays)

**Models** (`src/onepieceofdata/models/data.py`)
- Pydantic models validate all data: ChapterModel, VolumeModel, CharacterModel, ArcModel, SagaModel
- ScrapingResult wraps scraper outputs with success/error tracking

## Database Schema

### Core Tables
- `saga`: Story sagas with chapter ranges
- `arc`: Story arcs with saga_id (auto-linked via chapter ranges)
- `volume`: Manga volumes
- `chapter`: Individual chapters with metadata
- `character`: Character details with appearance analytics
- `coc`: Character-of-chapter (many-to-many relationship)
- `cov`: Character-on-volume (cover characters)

### Critical Schema Details
- Arc-to-saga linking is **automatic** based on chapter range containment (not scraped)
- Character table includes appearance analytics: `chapter_list`, `volume_list`, `appearance_count`, `first_appearance`, `last_appearance` (all INTEGER[] or INTEGER)
- PostgreSQL export uses `SchemaMapper` for type conversion (DuckDB INTEGER[] → PostgreSQL INTEGER[])

## Common Development Commands

### Build & Test
```bash
make setup              # Initial setup with uv
make test               # Run pytest
make lint               # Run ruff linting
make format             # Format with black
make check              # Run lint + test
```

### Pipeline Operations
```bash
# Full pipeline with parallel processing
make run-full-pipeline-parallel

# Individual steps (in order)
make run-scrape-parallel              # Scrape chapters
make run-scrape-volumes               # Scrape volumes
make extract-characters               # Extract character list from chapters
make run-scrape-characters-parallel   # Scrape character details
make run-parse                        # Load basic data to DuckDB
make run-scrape-story-structure       # Scrape arcs and sagas
make run-parse-story-structure        # Load story structure (auto-links arcs→sagas)
make migrate-birth-dates              # Parse birth strings
make load-cov                         # Load volume cover characters
make sync-character-appearances       # Sync appearance analytics

# PostgreSQL export
make export-postgres-full             # Full sync (drops/recreates tables)
make export-postgres                  # Incremental sync
make postgres-status                  # Check sync status
```

### Testing Commands
```bash
make test-scrape-parallel             # Test with chapters 1-10
make test-scrape-volumes              # Test with volumes 1-5
uv run pytest tests/test_config.py -v # Run specific test
```

## Critical Implementation Details

### Story Structure Auto-Linking
Arcs are automatically linked to sagas during `parse-story-structure`:
1. Sagas are loaded first (define chapter ranges)
2. Arcs are parsed but not saved
3. `ArcParser.auto_link_arcs_to_sagas()` assigns saga_id where arc's chapter range falls within saga's range
4. Linked arcs are then saved to database

This is NOT scraped - it's computed from chapter ranges.

### PostgreSQL Export Schema Sync
Full export mode (`--mode full`) **always** drops and recreates tables to ensure schema stays in sync with DuckDB. This is critical when new columns are added. Incremental mode only exports changed data without schema updates.

The `SchemaMapper` handles type conversions:
- Checks for exact type matches first (handles `INTEGER[]`)
- Falls back to base type extraction for parameterized types (`VARCHAR(255)`)

### Character Appearance Analytics
The `sync_character_appearances` command populates character analytics by:
1. Querying CoC table for chapter appearances
2. Joining with chapter table to get volume info
3. Computing aggregates: lists, counts, first/last appearances
4. Storing as DuckDB arrays (INTEGER[])

### Parallel Processing
Parallel scraping uses Python's `concurrent.futures.ProcessPoolExecutor`:
- Default 4 workers (configurable via `OP_MAX_WORKERS`)
- Chunks processed in batches for better progress tracking
- Each worker has independent retry logic
- Results aggregated with success/failure tracking

## Configuration

Environment variables (`.env`):
- `OP_LAST_CHAPTER`: Current last chapter (default: 1165)
- `OP_LAST_VOLUME`: Current last volume (default: 113)
- `OP_MAX_WORKERS`: Parallel workers (default: 4)
- `OP_SCRAPING_DELAY`: Delay between requests (default: 1.0s)
- `POSTGRES_HOST`, `POSTGRES_PORT`, etc.: PostgreSQL connection
- `POSTGRES_URL`: Alternative connection string format

All managed via `src/onepieceofdata/config/settings.py` using pydantic-settings.

## CLI Entry Point

`src/onepieceofdata/cli.py` contains all Click commands. Commands use:
- Scrapers for data extraction
- Parsers for data processing
- DatabaseManager for DuckDB operations
- PostgresExporter for PostgreSQL sync

## Git Commit Guidelines

When committing changes, do not mention AI tools or Claude in commit messages. Write clear, technical commit messages describing what was changed and why.

## Testing Notes

Tests use pytest with markers:
- `@pytest.mark.slow`: Slow tests (skip with `-m "not slow"`)
- `@pytest.mark.integration`: Integration tests

Run with coverage: `uv run pytest --cov=src/onepieceofdata`
