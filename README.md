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

📖 **For complete schema documentation, see [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)**

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

### Basic Usage

1. **Check pipeline status**:
   ```bash
   make status
   # or
   uv run onepieceofdata status
   ```

2. **Scrape chapter data** (test with first 10 chapters):
   ```bash
   make run-scrape
   # or
   uv run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 10
   ```

3. **View configuration**:
   ```bash
   uv run onepieceofdata config
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
# Merge duplicate characters (run once after character scraping)
# Preview changes first
uv run onepieceofdata merge-characters --dry-run

# Apply character merges
uv run onepieceofdata merge-characters

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
├── DATABASE_SCHEMA.md          # 📖 Complete database schema
├── FEATURES.md                 # 🎯 Feature overview & CLI reference
├── SCHEMA_VISUAL.md            # 📊 Visual database schema
├── IMPLEMENTATION_SUMMARY.md   # 📋 Technical implementation
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

# Run all checks
make check

# Clean up generated files
make clean
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/onepieceofdata

# Run specific test file
uv run pytest tests/test_config.py -v
```

## 📊 Data Pipeline

The **v2.0 pipeline** is a complete, production-ready system with three main stages:

### 1. **Scraping Stage**

- **Chapters**: Extract 1,153+ chapters with complete metadata
- **Volumes**: Extract 112+ volumes with titles and details  
- **Characters**: Extract 1,533+ characters with full biographical data
- **Story Arcs**: Extract detailed arc information with chapter ranges
- **Story Sagas**: Extract saga data and arc relationships
- **Parallel Processing**: 4x faster scraping with intelligent error handling
- **Retry Logic**: Automatic retry with exponential backoff
- **Data Validation**: Pydantic models ensure data integrity

### 2. **Processing Stage**

- **Complex Parsing**: Handle annotated page numbers, special characters
- **Data Cleaning**: Normalize character names, parse dates and numbers
- **Quality Tracking**: Built-in scraping status and error categorization
- **Relationship Mapping**: Character-to-chapter associations
- **Story Structure**: Arc-to-saga relationships and chapter mappings

### 3. **Storage Stage**

- **Enhanced Schema**: New columns for data quality tracking
- **Complete Coverage**: 100% data compatibility with original database
- **Story Organization**: Dedicated tables for arcs and sagas
- **Performance**: Optimized DuckDB operations with proper indexing
- **Export Options**: CSV, JSON, and direct SQL query support

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
| Architecture | Monolithic scripts | **Modern CLI + modules** |
| Testing | None | **Comprehensive test suite** |
| Maintenance | Manual | **Automated with make commands** |

## 🎉 Completed Features (v2.0)

- [x] **UV Migration**: Lightning-fast dependency management
- [x] **Complete Scrapers**: All chapters, volumes, and characters  
- [x] **Story Structure Scrapers**: Comprehensive arc and saga extraction
- [x] **Modern Database**: Enhanced DuckDB schema with quality tracking
- [x] **Parallel Processing**: 4x faster scraping with robust error handling
- [x] **Data Quality**: 100% coverage with built-in status tracking
- [x] **CLI Interface**: Comprehensive command-line tools
- [x] **Export System**: CSV and JSON export capabilities
- [x] **Testing Suite**: Full test coverage with pytest
- [x] **Production Ready**: Robust error handling and validation

## � Documentation

- 📖 **[DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)** - Complete database schema with examples
- 🎯 **[FEATURES.md](FEATURES.md)** - Feature overview and CLI command reference
- 📋 **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Technical implementation details

## �🚀 Future Enhancements

- [ ] **Incremental Updates**: Smart re-scraping of only new content
- [ ] **Data Analytics**: Built-in analysis and visualization tools  
- [ ] **API Server**: REST API for data access
- [ ] **Real-time Monitoring**: Live scraping status dashboard
- [ ] **Advanced Exports**: PostgreSQL, MySQL, and cloud database connectors

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
