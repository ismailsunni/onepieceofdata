# One Piece of Data

![alt text](./onepieceofdata-header.png)

**A modern Python pipeline for scraping and processing One Piece manga data**

One Piece of Data is a comprehensive data pipeline that scrapes, processes, and stores information about One Piece manga chapters, characters, and volumes from the One Piece Fandom Wiki. **Version 2.0** brings significant improvements in reliability, performance, and data quality.

## ✨ Features

- 🚀 **Modern Python Development**: Built with `uv` for lightning-fast dependency management
- 🔄 **Robust Scraping**: Advanced error handling, retry logic, and parallel processing
- 📊 **Data Validation**: Pydantic models ensure data integrity
- 🗄️ **DuckDB Storage**: Efficient analytical database with enhanced schema
- 🖥️ **CLI Interface**: Comprehensive command-line tools for all operations
- 📝 **Comprehensive Logging**: Structured logging with Loguru
- 🧪 **Testing**: Full test suite with pytest
- ⚙️ **Configurable**: Environment-based configuration management
- 🎯 **Data Quality**: Built-in scraping status tracking and validation
- ✨ **Complete Coverage**: 100% data compatibility with enhanced quality metrics
- 🏴‍☠️ **Story Structure**: Comprehensive arc and saga data extraction and organization

## � Database Schema & Usage

The database contains **6 main tables** that store comprehensive One Piece manga data:

### Quick Schema Overview

```mermaid
erDiagram
    SAGA ||--o{ ARC : contains
    ARC ||--o{ CHAPTER : spans
    VOLUME ||--o{ CHAPTER : contains
    CHAPTER ||--o{ COC : appears_in
    CHARACTER ||--o{ COC : appears_in
```

| Table | Description | Key Fields |
|-------|-------------|------------|
| `saga` | Major story sagas | `saga_id`, `title`, `start_chapter`, `end_chapter` |
| `arc` | Individual story arcs | `arc_id`, `title`, `saga_id`, `start_chapter`, `end_chapter` |
| `volume` | Manga volumes | `number`, `title` |
| `chapter` | Individual chapters | `number`, `title`, `volume`, `num_page`, `date` |
| `character` | Character details | `id`, `name`, `bounty`, `status`, `scraping_status` |
| `coc` | Character appearances | `chapter`, `character`, `note` |

### Common Usage Examples

```sql
-- Get all arcs in East Blue saga
SELECT title, start_chapter, end_chapter 
FROM arc WHERE saga_id = 'east_blue';

-- Find highest bounty characters
SELECT name, bounty FROM character 
WHERE bounty IS NOT NULL ORDER BY bounty DESC LIMIT 10;

-- Get characters in a specific chapter
SELECT character FROM coc WHERE chapter = 1;

-- Count chapters per saga
SELECT s.title, COUNT(c.number) as chapters
FROM saga s
JOIN chapter c ON c.number BETWEEN s.start_chapter AND s.end_chapter
GROUP BY s.title;
```

📖 **For complete schema documentation, see [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)**

## �🚀 Quick Start

### Prerequisites

Install [uv](https://github.com/astral-sh/uv) (the modern Python package manager):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Installation (30 seconds setup!)

1. **Clone and setup**:
   ```bash
   git clone https://github.com/ismailsunni/onepieceofdata.git
   cd onepieceofdata
   make setup
   ```

That's it! The setup will:
- Install all dependencies using `uv`
- Create required directories
- Set up the development environment

### Next Steps

After setup, use the recommended two-command workflow:

```bash
# 1. Prepare all data (scrape, parse, post-process)
make run-data-pipeline

# 2. Export when ready (CSV + PostgreSQL)
make run-all-exports
```

Or use the all-in-one command:

```bash
make run-full-pipeline-parallel
```

### Basic Usage

**Recommended Two-Command Workflow** (most users should use this):

1. **Prepare all data** (scrape → parse → post-process):
   ```bash
   make run-data-pipeline
   ```

2. **Export when ready** (CSV + PostgreSQL):
   ```bash
   make run-all-exports
   ```

**Alternative: All-in-One Command** (includes export):
```bash
make run-full-pipeline-parallel
```

**Quick Testing** (before running full pipeline):

1. **Check pipeline status**:
   ```bash
   make status
   ```

2. **Test scraping** (first 10 chapters):
   ```bash
   make test-scrape-parallel
   ```

3. **View configuration**:
   ```bash
   make config
   ```

## 🎮 Command Line Interface

The CLI provides comprehensive commands for managing the complete data pipeline:

### Core Scraping Commands

```bash
# Scrape all chapters (recommended)
uv run onepieceofdata scrape-chapters --parallel

# Scrape specific chapter range
uv run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 100

# Scrape all volumes
uv run onepieceofdata scrape-volumes

# Scrape character details (from chapter data)
uv run onepieceofdata scrape-characters --parallel

# NEW: Scrape story structure data
uv run onepieceofdata scrape-arcs           # Scrape story arcs
uv run onepieceofdata scrape-sagas          # Scrape story sagas
uv run onepieceofdata scrape-story-structure # Scrape both arcs and sagas
```

### Database Operations

```bash
# Parse all data into database
uv run onepieceofdata parse --create-tables

# Parse specific data types
uv run onepieceofdata parse --chapters-file data/chapters.json
uv run onepieceofdata parse --volumes-file data/volumes.json
uv run onepieceofdata parse --characters-file data/characters_detail.json

# Export data to CSV
uv run onepieceofdata export --format csv --output-dir exports/
```

### Character Management

```bash
# Complete character workflow (extract → scrape → parse → merge → sync)
make run-character-workflow

# Individual character commands:

# Extract character list from chapters
uv run onepieceofdata extract-characters

# Merge duplicate characters (run once after character scraping)
# Preview changes first
uv run onepieceofdata merge-characters --dry-run

# Apply character merges
uv run onepieceofdata merge-characters

# Sync character appearance analytics (chapter_list, appearance_count, etc.)
uv run onepieceofdata sync-character-appearances
uv run onepieceofdata sync-character-appearances --verbose  # Detailed output

# Show characters in a specific chapter (useful to check for duplicates)
uv run onepieceofdata show-chapter-characters --chapter 1000
uv run onepieceofdata show-chapter-characters  # Shows latest chapter
```

**Note on Character Merging**: Characters may appear with multiple IDs due to:
- Code names (Mr. 1 / Daz Bonez)
- Epithets (Akainu / Sakazuki)
- Disguises (Lucy / Sabo)

The merge command consolidates these duplicates using `data/character_aliases.json`.
Run this once after scraping characters, or when you notice duplicates.

**Note on Character Appearance Syncing**: The `sync-character-appearances` command computes:
- `chapter_list`: Array of chapter numbers where character appears
- `volume_list`: Array of volume numbers where character appears
- `appearance_count`: Total number of chapter appearances
- `first_appearance`: First chapter number
- `last_appearance`: Last chapter number

### PostgreSQL Export

Export your One Piece data to PostgreSQL (works with local PostgreSQL or Supabase):

```bash
# Full export (complete sync)
uv run onepieceofdata export-postgres --mode full

# Incremental export (only changed tables)
uv run onepieceofdata export-postgres --mode incremental

# Export specific tables only
uv run onepieceofdata export-postgres --tables chapter,character

# Check sync status
uv run onepieceofdata sync-status

# Preview without making changes
uv run onepieceofdata export-postgres --dry-run
```

#### Local PostgreSQL Testing with Docker

Start a local PostgreSQL instance for testing:

```bash
# Quick start: Initialize PostgreSQL and export data
make postgres-init

# Or run commands individually:
make postgres-start           # Start PostgreSQL and pgAdmin
make export-postgres-full     # Full export to PostgreSQL
make export-postgres          # Incremental export (only changes)
make postgres-status          # Check sync status
make postgres-logs            # View PostgreSQL logs
make postgres-stop            # Stop services
```

**Access:**
- PostgreSQL: `localhost:5432` (user: `postgres`, password: `onepiece`)
- pgAdmin: `http://localhost:5050` (email: `admin@onepiece.com`, password: `admin`)

#### Configuration

Set PostgreSQL connection details in `.env`:

```bash
# Local PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=onepiece
POSTGRES_USER=postgres
POSTGRES_PASSWORD=onepiece

# Or use connection URL (for Supabase)
POSTGRES_URL=postgresql://postgres:password@host:port/database
```

See `.env.example` for full configuration options.

### Modular Pipeline Commands

The pipeline is organized into modular stages that can be run independently or combined:

```bash
# RECOMMENDED: Two-command workflow for better control
make run-data-pipeline    # 1. Scrape → Parse → Post-process (all data in DuckDB)
make run-all-exports      # 2. Export to CSV + PostgreSQL (when ready)

# Individual pipeline stages:

# Stage 1: Run ALL scrapers (chapters, volumes, characters, arcs, sagas)
make run-all-scrapers

# Stage 2: Run ALL parsers (load scraped JSON data into DuckDB)
make run-all-parsers

# Stage 3: Run ALL post-processors (birth dates, COV, merge, appearances)
make run-all-postprocessors

# Stage 4: Run ALL exports (CSV + PostgreSQL)
make run-all-exports

# Alternative: All-in-one command (runs all stages including export)
make run-full-pipeline-parallel
```

**Why use the modular approach?**
- **Better control**: Separate data preparation from export
- **Iterative development**: Re-run only the stages you need
- **Error handling**: If one stage fails, you don't lose previous work
- **Flexibility**: Mix and match stages as needed

**Pipeline Stages Breakdown**:
1. **`run-all-scrapers`**: Scrapes chapters (parallel), volumes, characters (parallel), and story structure
2. **`run-all-parsers`**: Loads all JSON files into DuckDB (chapters, volumes, characters, arcs, sagas)
3. **`run-all-postprocessors`**: Enriches data (birth dates, COV, merge duplicates, appearance analytics)
4. **`run-all-exports`**: Exports to CSV files and PostgreSQL

### Pipeline Management

```bash
# Show pipeline status and configuration
uv run onepieceofdata status

# View current configuration
uv run onepieceofdata config

# Get comprehensive help
uv run onepieceofdata --help
```

## ⚡ Parallel Processing

One Piece of Data supports parallel processing for faster chapter scraping:

### Using CLI Commands

```bash
# Enable parallel processing with default workers (4)
uv run onepieceofdata scrape-chapters --parallel

# Specify number of workers
uv run onepieceofdata scrape-chapters --parallel --workers 8

# Test parallel processing with a small sample
uv run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 10 --parallel
```

### Using Make Commands

```bash
# Test parallel scraping
make test-scrape-parallel

# Run full pipeline with parallel processing
make run-full-pipeline-parallel

# Regular vs parallel scraping
make run-scrape          # Sequential (safe, slower)
make run-scrape-parallel # Parallel (faster, uses more resources)
```

### Performance Comparison

- **Sequential**: Safer, better error handling, respects rate limits
- **Parallel**: 3-4x faster, uses multiple CPU cores, higher server load

### Configuration

You can configure parallel processing via environment variables:

```bash
# .env file
OP_ENABLE_PARALLEL=true
OP_MAX_WORKERS=4
OP_PARALLEL_CHUNK_SIZE=10
```

## 📁 Project Structure

```
onepieceofdata/
├── src/onepieceofdata/         # Main package
│   ├── cli.py                  # Command-line interface
│   ├── config/                 # Configuration management
│   ├── models/                 # Pydantic data models
│   ├── scrapers/               # Web scraping modules
│   │   ├── chapter.py         # Chapter scraper
│   │   ├── character.py       # Character scraper
│   │   ├── volume.py          # Volume scraper
│   │   ├── arc.py             # Story arc scraper
│   │   └── saga.py            # Story saga scraper
│   ├── parsers/                # Data processing modules
│   │   ├── arc.py             # Arc data parser
│   │   └── saga.py            # Saga data parser
│   ├── database/               # Database operations
│   └── utils/                  # Utility functions
├── tests/                      # Test suite
├── data/                       # Data storage
├── logs/                       # Log files
├── docs/                       # 📚 Documentation (schema, features, plans)
│   ├── DATABASE_SCHEMA.md      # 📖 Complete database schema
│   ├── FEATURES.md             # 🎯 Feature overview & CLI reference
│   ├── SCHEMA_VISUAL.md        # 📊 Visual database schema
│   └── IMPLEMENTATION_SUMMARY.md # 📋 Technical implementation
├── pyproject.toml             # Modern Python project config
├── Makefile                   # Development commands
└── .env                       # Environment configuration
```

## ⚙️ Configuration

Configuration is managed through environment variables and `.env` file:

```bash
# Chapter and Volume Settings
OP_LAST_CHAPTER=1153
OP_LAST_VOLUME=112

# Data Directories
OP_DATA_DIR=./data
OP_DATABASE_PATH=./data/onepiece.duckdb

# Scraping Configuration
OP_SCRAPING_DELAY=1.0
OP_MAX_RETRIES=3
OP_REQUEST_TIMEOUT=30

# Logging
OP_LOG_LEVEL=INFO
OP_LOG_FILE=./logs/onepieceofdata.log
```

## 🛠️ Development

### Development Commands

```bash
# Setup development environment
make setup

# Run tests
make test

# Format code
make format

# Run linting
make lint

# Run all checks (lint + test)
make check

# Clean up generated files
make clean
```

### Testing Commands

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/onepieceofdata

# Run specific test file
uv run pytest tests/test_config.py -v

# Test scraping with limited data
make test-scrape                   # Sequential (chapters 1-10)
make test-scrape-parallel          # Parallel (chapters 1-10, 4 workers)
make test-scrape-workers WORKERS=2 # Custom workers (chapters 1-10)

# Test volume scraping
make test-scrape-volumes           # Volumes 1-5

# Test story structure scraping
make test-scrape-story-structure   # Arcs and sagas
```

## 📊 Data Pipeline

The **v2.0 pipeline** is a complete, production-ready system organized into **four modular stages**:

### Recommended Workflow

**Two-Command Approach** (best for most users):
```bash
make run-data-pipeline    # Stages 1-3: Prepare all data in DuckDB
make run-all-exports      # Stage 4: Export when ready
```

**Alternative All-in-One**:
```bash
make run-full-pipeline-parallel    # All stages including export
```

### Pipeline Stages

### 1. **Scraping Stage** (`make run-all-scrapers`)

- **Chapters**: Extract 1,153+ chapters with complete metadata
- **Volumes**: Extract 112+ volumes with titles and details
- **Characters**: Extract 1,533+ characters with full biographical data
- **Story Arcs**: Extract detailed arc information with chapter ranges
- **Story Sagas**: Extract saga data and arc relationships
- **Parallel Processing**: 4x faster scraping with intelligent error handling
- **Retry Logic**: Automatic retry with exponential backoff
- **Data Validation**: Pydantic models ensure data integrity

### 2. **Parsing Stage** (`make run-all-parsers`)

- **Complex Parsing**: Handle annotated page numbers, special characters
- **Data Cleaning**: Normalize character names, parse dates and numbers
- **Quality Tracking**: Built-in scraping status and error categorization
- **Relationship Mapping**: Character-to-chapter associations
- **Story Structure**: Arc-to-saga relationships and chapter mappings
- **Database Loading**: Load all scraped JSON files into DuckDB

### 3. **Post-Processing Stage** (`make run-all-postprocessors`)

- **Birth Date Migration**: Parse birth strings into structured dates
- **COV Loading**: Load character-on-volume (cover) data
- **Character Merging**: Deduplicate characters with multiple IDs
- **Appearance Analytics**: Compute chapter/volume appearance statistics

### 4. **Export Stage** (`make run-all-exports`)

- **CSV Export**: Export all tables to CSV files
- **PostgreSQL Export**: Full sync to PostgreSQL database
- **Schema Mapping**: Automatic type conversion (DuckDB → PostgreSQL)
- **Incremental Updates**: Support for incremental exports

### Data Quality Metrics

- ✅ **Chapters**: 1,153/1,153 (100%) with complete page numbers
- ✅ **Volumes**: 112/112 (100%) with English titles  
- ✅ **Characters**: 1,533/1,533 (100%) with quality status tracking
- ✅ **Character Relations**: 25,708+ chapter-character relationships
- ✅ **Story Arcs**: Comprehensive arc data with chapter ranges
- ✅ **Story Sagas**: Complete saga structure with arc relationships
- 📊 **Success Rate**: 97.7% full data extraction, 2.3% with status flags

### Legacy vs Modern Comparison

| Aspect | v1.x (Legacy) | **v2.0 (Modern)** |
|--------|---------------|-------------------|
| Setup Time | ~5 minutes | **30 seconds** |
| Error Handling | Basic | **Advanced retry + validation** |
| Performance | Sequential only | **Parallel processing (4x faster)** |
| Data Quality | No tracking | **Built-in quality metrics** |
| Missing Data | Silent failures | **Tracked with status codes** |
| Architecture | Monolithic scripts | **Modular pipeline stages** |
| Testing | None | **Comprehensive test suite** |
| Maintenance | Manual | **Automated with make commands** |
| Export | CSV only | **CSV + PostgreSQL sync** |

## 🎉 Completed Features (v2.0)

- [x] **UV Migration**: Lightning-fast dependency management
- [x] **Complete Scrapers**: All chapters, volumes, and characters
- [x] **Story Structure Scrapers**: Comprehensive arc and saga extraction
- [x] **Modern Database**: Enhanced DuckDB schema with quality tracking
- [x] **Parallel Processing**: 4x faster scraping with robust error handling
- [x] **Data Quality**: 100% coverage with built-in status tracking
- [x] **Modular Pipeline**: Four-stage pipeline with independent execution
- [x] **CLI Interface**: Comprehensive command-line tools
- [x] **Export System**: CSV and PostgreSQL export capabilities
- [x] **Character Analytics**: Appearance tracking and deduplication
- [x] **Testing Suite**: Full test coverage with pytest
- [x] **Production Ready**: Robust error handling and validation

## � Documentation

- 📖 **[docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)** - Complete database schema with examples
- 🎯 **[docs/FEATURES.md](docs/FEATURES.md)** - Feature overview and CLI command reference
- 📋 **[docs/IMPLEMENTATION_SUMMARY.md](docs/IMPLEMENTATION_SUMMARY.md)** - Technical implementation details

## 🌐 Character Network Explorer

Explore the chapter co-appearance network in a local interactive web app.

### Prerequisites

Make sure these files exist:

- `exports/network_analysis/character_network_nodes_gt10.csv`
- `exports/network_analysis/character_coappearance_edges_gt10.csv`

Start the app:

```bash
python scripts/run_network_explorer.py
```

Or via Makefile:

```bash
make run-network-explorer
```

Then open:

- http://127.0.0.1:8765/web/network_explorer/index.html

### What you can do

- Filter nodes by minimum chapter appearances
- Filter edges by minimum co-appearance weight
- Limit max rendered edges for smoother performance
- Search and focus a character node
- Zoom, pan, drag, and inspect edge/node tooltips

## 🚀 Future Enhancements

- [ ] **Incremental Updates**: Smart re-scraping of only new content
- [ ] **Data Analytics**: Built-in analysis and visualization tools
- [ ] **API Server**: REST API for data access
- [ ] **Real-time Monitoring**: Live scraping status dashboard
- [ ] **Additional Database Connectors**: MySQL and cloud database support
- [ ] **Enhanced Character Analytics**: Devil fruit categorization, affiliations tracking
- [ ] **Visual Data Explorer**: Interactive web interface for browsing data

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes
4. Run tests: `make check`
5. Submit a pull request

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Data sourced from [One Piece Fandom Wiki](https://onepiece.fandom.com/)
- Built with modern Python tools: [uv](https://github.com/astral-sh/uv), [Pydantic](https://pydantic.dev/), [DuckDB](https://duckdb.org/)

---

### 🏴‍☠️ Sail into the world of One Piece data

There will be 3 parts of the project:

- Scraping data
- Clean the data
- Store the data to duckdb

## Credits

- Header is generated by using: [Font Generator](https://www.textstudio.com/)
