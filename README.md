# One Piece of Data

![alt text](./onepieceofdata-header.png)

**A modern Python pipeline for scraping and processing One Piece manga data**

One Piece of Data is a comprehensive data pipeline that scrapes, processes, and stores information about One Piece manga chapters, characters, and volumes from the One Piece Fandom Wiki.

## ✨ Features

- 🚀 **Modern Python Development**: Built with `uv` for lightning-fast dependency management
- 🔄 **Robust Scraping**: Advanced error handling, retry logic, and rate limiting
- 📊 **Data Validation**: Pydantic models ensure data integrity
- 🗄️ **DuckDB Storage**: Efficient analytical database for fast queries
- 🖥️ **CLI Interface**: Easy-to-use command-line tools
- 📝 **Comprehensive Logging**: Structured logging with Loguru
- 🧪 **Testing**: Full test suite with pytest
- ⚙️ **Configurable**: Environment-based configuration management

## 🚀 Quick Start

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

The CLI provides several commands for managing the data pipeline:

```bash
# Scrape chapters
uv run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 100

# Show pipeline status
uv run onepieceofdata status

# View configuration
uv run onepieceofdata config

# Get help
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
│   ├── parsers/                # Data processing modules
│   └── utils/                  # Utility functions
├── tests/                      # Test suite
├── data/                       # Data storage
├── logs/                       # Log files
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

The pipeline consists of three main stages:

1. **Scraping**: Extract data from One Piece Fandom Wiki
2. **Processing**: Clean and validate data using Pydantic models  
3. **Storage**: Store in DuckDB for efficient querying

### Legacy vs Modern

**Before (v1.x)**:
```bash
# Old way - slow setup, manual management
pip install virtualenv
virtualenv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/main_scrapper.py
```

**Now (v2.x)**:
```bash
# New way - 30 second setup, robust pipeline
make setup
make run-scrape
```

## 🚧 Roadmap

- [x] **Phase 1**: UV migration and foundation
- [ ] **Phase 2**: Complete scraper implementations
- [ ] **Phase 3**: Modern parser and database operations
- [ ] **Phase 4**: Advanced features (incremental scraping, exports)

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

*Sail into the world of One Piece data! 🏴‍☠️*

There will be 3 parts of the project:
- Scraping data
- Clean the data
- Store the data to duckdb

## Credits

- Header is generated by using: [Font Generator](https://www.textstudio.com/)
