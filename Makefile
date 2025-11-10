# Makefile for One Piece of Data development
UV := uv

.PHONY: help install install-dev test lint format clean setup check extract-characters # Run character scraping with parallel processing
run-scrape-characters-parallel:
	@echo "🚀 Running character scraping with parallel processing..."
	@if [ ! -f "data/characters.csv" ]; then \
		echo "❌ characters.csv not found. Run 'make run-scrape' first."; \
		exit 1; \
	fi
	@echo "📊 Input: $$(( $$(wc -l < data/characters.csv) - 1 )) characters in characters.csv (excluding header)"
	@echo "⚙️  Using parallel processing with 4 workers and better error handling..."
	$(UV) run onepieceofdata scrape-characters --parallel --workers 4 --retry-count 3 --delay 2 --verbose
	@if [ -f "data/characters_detail.json" ]; then \
		echo "📊 Results Summary:"; \
		echo "  - Input (characters.csv): $$(( $$(wc -l < data/characters.csv) - 1 )) characters"; \
		echo "  - Output (characters_detail.json): $$(( $$(jq length data/characters_detail.json) )) characters"; \
		echo "  - Success Rate: $$(( $$(jq length data/characters_detail.json) * 100 / ( $$(wc -l < data/characters.csv) - 1 ) ))%"; \
	firape run-scrape-parallel run-scrape-workers run-scrape-characters run-scrape-characters-parallel run-scrape-characters-workers run-scrape-volumes run-scrape-arcs run-scrape-sagas run-scrape-story-structure run-parse run-full-pipeline run-full-pipeline-parallel run-full-pipeline-workers status db-status migrate-birth-dates migrate-birth-dates-full load-cov config export test-scrape test-scrape-parallel test-scrape-workers test-scrape-volumes test-scrape-characters test-scrape-characters-parallel test-scrape-story-structure

# Default target
help:
	@echo "One Piece of Data - Development Commands"
	@echo "========================================"
	@echo ""
	@echo "Setup Commands:"
	@echo "  setup          - Initial project setup (install uv, dependencies, etc.)"
	@echo "  install        - Install dependencies"
	@echo "  install-dev    - Install development dependencies"
	@echo ""
	@echo "Development Commands:"
	@echo "  test           - Run tests"
	@echo "  lint           - Run linting (ruff)"
	@echo "  format         - Format code (black)"
	@echo "  check          - Run all checks (lint + test)"
	@echo ""
	@echo "Pipeline Commands:"
	@echo "  run-scrape     - Run chapter scraping (uses config: all chapters)"
	@echo "  run-scrape-parallel - Run chapter scraping with parallel processing"
	@echo "  run-scrape-workers WORKERS=N - Run chapter scraping with N workers"
	@echo "  run-scrape-characters - Run character scraping (all characters from CSV)"
	@echo "  run-scrape-characters-parallel - Run character scraping with parallel processing"
	@echo "  run-scrape-characters-workers WORKERS=N - Run character scraping with N workers"
	@echo "  run-scrape-volumes - Run volume scraping (uses config: all volumes)"
	@echo "  run-scrape-arcs - Run arc scraping (story arcs)"
	@echo "  run-scrape-sagas - Run saga scraping (story sagas)"
	@echo "  run-scrape-story-structure - Scrape story structure (arcs and sagas) to JSON"
	@echo "  run-parse-story-structure - Parse story structure JSON files into database"
	@echo "  run-parse      - Run data parsing and database loading"
	@echo "  run-full-pipeline - Run complete pipeline (scrape + parse)"
	@echo "  run-full-pipeline-parallel - Run complete pipeline with parallel processing"
	@echo "  run-full-pipeline-workers WORKERS=N - Run complete pipeline with N workers"
	@echo "  status         - Show pipeline status"
	@echo "  db-status      - Show database content status (quick test after parsing)"
	@echo "  migrate-birth-dates - Parse birth strings and add birth_date column (MM-DD format)"
	@echo "  migrate-birth-dates-full - Parse birth strings and add birth_date column (full date format)"
	@echo "  load-cov       - Load character-on-volume (COV) data from volumes.json"
	@echo "  config         - Show current configuration"
	@echo "  export         - Export database to CSV files"
	@echo ""
	@echo "Test/Development Commands:"
	@echo "  test-scrape    - Test chapter scraping (chapters 1-10)"
	@echo "  test-scrape-parallel - Test chapter scraping with parallel processing"
	@echo "  test-scrape-workers WORKERS=N - Test chapter scraping with N workers"
	@echo "  test-scrape-volumes - Test volume scraping (volumes 1-5)"
	@echo "  test-scrape-characters - Test character scraping (all from CSV)"
	@echo "  test-scrape-characters-parallel - Test character scraping with parallel processing"
	@echo "  test-scrape-story-structure - Test story structure scraping (arcs and sagas)"
	@echo ""
	@echo "Utility Commands:"
	@echo "  clean          - Clean up generated files"

# Initial setup
setup:
	@echo "🚀 Setting up One Piece of Data development environment..."
	$(UV) sync --all-extras
	@echo "📁 Creating required directories..."
	mkdir -p data logs exports
	@echo "✅ Setup complete!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run 'make test-scrape' to test scraping with limited data"
	@echo "  2. Run 'make status' to check pipeline status"
	@echo "  3. Run 'make run-full-pipeline' for complete workflow (scrapes ALL data)"

# Install dependencies
install:
	$(UV) sync

# Install development dependencies
install-dev:
	$(UV) sync --all-extras

# Run tests
test:
	@echo "🧪 Running tests..."
	$(UV) run pytest tests/ -v

# Run linting
lint:
	@echo "🔍 Running linting..."
	$(UV) run ruff check src/ tests/

# Format code
format:
	@echo "🎨 Formatting code..."
	$(UV) run black src/ tests/
	$(UV) run ruff check --fix src/ tests/

# Run all checks
check: lint test
	@echo "✅ All checks passed!"

# Run chapter scraping (uses config defaults: all chapters)
run-scrape:
	@echo "📖 Running chapter scraping (using config defaults)..."
	$(UV) run onepieceofdata scrape-chapters

# Run chapter scraping with parallel processing
run-scrape-parallel:
	@echo "🚀 Running chapter scraping with parallel processing..."
	$(UV) run onepieceofdata scrape-chapters --parallel

# Run chapter scraping with custom number of workers
# Usage: make run-scrape-workers WORKERS=8
run-scrape-workers:
	@echo "🚀 Running chapter scraping with $(WORKERS) workers..."
	$(UV) run onepieceofdata scrape-chapters --parallel --workers $(WORKERS)

# Run character scraping (uses all characters from characters.csv)
run-scrape-characters:
	@echo "👥 Running character scraping (using all characters from CSV)..."
	@if [ ! -f "data/characters.csv" ]; then \
		echo "❌ characters.csv not found. Run 'make run-scrape' first."; \
		exit 1; \
	fi
	$(UV) run onepieceofdata scrape-characters

# Run character scraping with parallel processing
run-scrape-characters-parallel:
	@echo "🚀 Running character scraping with parallel processing..."
	@if [ ! -f "data/characters.csv" ]; then \
		echo "❌ characters.csv not found. Run 'make run-scrape' first."; \
		exit 1; \
	fi
	@echo "📊 Total characters to scrape: $$(( $$(wc -l < data/characters.csv) - 1 )) (excluding header)"
	@echo "⚙️  Using parallel processing with 4 workers and better error handling..."
	$(UV) run onepieceofdata scrape-characters --parallel 

# Run character scraping with custom number of workers
# Usage: make run-scrape-characters-workers WORKERS=4
run-scrape-characters-workers:
	@echo "🚀 Running character scraping with $(WORKERS) workers..."
	@if [ ! -f "data/characters.csv" ]; then \
		echo "❌ characters.csv not found. Run 'make run-scrape' first."; \
		exit 1; \
	fi
	$(UV) run onepieceofdata scrape-characters --parallel --workers $(WORKERS)

# Run volume scraping (uses config defaults: all volumes)
run-scrape-volumes:
	@echo "📚 Running volume scraping (using config defaults)..."
	$(UV) run onepieceofdata scrape-volumes

# Extract character list from chapters
extract-characters:
	@echo "👥 Extracting character list from chapters..."
	@if [ ! -f "data/chapters.json" ]; then \
		echo "❌ chapters.json not found. Run 'make run-scrape' first."; \
		exit 1; \
	fi
	$(UV) run onepieceofdata extract-characters

# Run arc scraping (story arcs)
run-scrape-arcs:
	@echo "🏴‍☠️ Running arc scraping (story arcs)..."
	$(UV) run onepieceofdata scrape-arcs

# Run saga scraping (story sagas)
run-scrape-sagas:
	@echo "📖 Running saga scraping (story sagas)..."
	$(UV) run onepieceofdata scrape-sagas

# Run story structure scraping (arcs and sagas)
run-scrape-story-structure:
	@echo "⚓ Scraping story structure data (arcs and sagas)..."
	$(UV) run onepieceofdata scrape-story-structure

# Run story structure parsing (process JSON files and load into database)
run-parse-story-structure:
	@echo "📊 Parsing story structure data (arcs and sagas)..."
	$(UV) run onepieceofdata parse-story-structure

# Run data parsing and database loading
run-parse:
	@echo "�️  Running data parsing and database loading..."
	$(UV) run onepieceofdata parse --create-tables

# Run complete pipeline
run-full-pipeline:
	@echo "🚀 Running complete One Piece data pipeline..."
	@echo ""
	@echo "Step 1: Scraping chapters..."
	$(MAKE) run-scrape
	@echo ""
	@echo "Step 2: Scraping volumes..."
	$(MAKE) run-scrape-volumes
	@echo ""
	@echo "Step 3: Scraping characters..."
	$(MAKE) run-scrape-characters
	@echo ""
	@echo "Step 4: Loading basic data into database (volumes, chapters, characters)..."
	$(MAKE) run-parse
	@echo ""
	@echo "Step 5: Scraping story structure (arcs and sagas)..."
	$(MAKE) run-scrape-story-structure
	@echo ""
	@echo "Step 6: Loading story structure into database (arcs and sagas)..."
	$(MAKE) run-parse-story-structure
	@echo ""
	@echo "Step 7: Parsing birth dates and adding birth_date column..."
	$(MAKE) migrate-birth-dates
	@echo ""
	@echo "Step 8: Loading character-on-volume (COV) data..."
	$(MAKE) load-cov
	@echo ""
	@echo "✅ Pipeline completed! Check status with 'make db-status'"

# Run complete pipeline with parallel processing
run-full-pipeline-parallel:
	@echo "🚀 Running complete One Piece data pipeline with parallel processing..."
	@echo ""
	@echo "Step 1: Scraping chapters (parallel)..."
	$(MAKE) run-scrape-parallel
	@echo ""
	@echo "Step 2: Scraping volumes..."
	$(MAKE) run-scrape-volumes
	@echo ""
	@echo "Step 3: Extract initial character list from chapters..."
	$(MAKE) extract-characters
	@echo ""
	@echo "Step 4: Scraping characters (parallel)..."
	$(MAKE) run-scrape-characters-parallel
	@echo ""
	@echo "Step 5: Scraping story structure (arcs and sagas)..."
	$(MAKE) run-scrape-story-structure
	@echo ""
	@echo "Step 6: Loading story structure into database (arcs and sagas)..."
	$(MAKE) run-parse-story-structure
	@echo ""
	@echo "Step 7: Loading all data into database..."
	$(MAKE) run-parse
	@echo ""
	@echo "Step 8: Parsing birth dates and adding birth_date column..."
	$(MAKE) migrate-birth-dates
	@echo ""
	@echo "Step 9: Loading character-on-volume (COV) data..."
	$(MAKE) load-cov
	@echo ""
	@echo "✅ Parallel pipeline completed!"
	$(MAKE) db-status

# Run complete pipeline with custom number of workers
# Usage: make run-full-pipeline-workers WORKERS=8
run-full-pipeline-workers:
	@echo "🚀 Running complete One Piece data pipeline with $(WORKERS) workers..."
	@echo ""
	@echo "Step 1: Scraping chapters ($(WORKERS) workers)..."
	$(MAKE) run-scrape-workers WORKERS=$(WORKERS)
	@echo ""
	@echo "Step 2: Scraping volumes..."
	$(MAKE) run-scrape-volumes
	@echo ""
	@echo "Step 3: Scraping characters ($(WORKERS) workers)..."
	$(MAKE) run-scrape-characters-workers WORKERS=$(WORKERS)
	@echo ""
	@echo "Step 4: Loading basic data into database (volumes, chapters, characters)..."
	$(MAKE) run-parse
	@echo ""
	@echo "Step 5: Scraping story structure (arcs and sagas)..."
	$(MAKE) run-scrape-story-structure
	@echo ""
	@echo "Step 6: Loading story structure into database (arcs and sagas)..."
	$(MAKE) run-parse-story-structure
	@echo ""
	@echo "Step 7: Parsing birth dates and adding birth_date column..."
	$(MAKE) migrate-birth-dates
	@echo ""
	@echo "Step 8: Loading character-on-volume (COV) data..."
	$(MAKE) load-cov
	@echo ""
	@echo "✅ Pipeline with $(WORKERS) workers completed! Check status with 'make db-status'"

# Export database to CSV files
export:
	@echo "📤 Exporting database to CSV files..."
	$(UV) run onepieceofdata export --output-dir exports

# PostgreSQL Export Commands
postgres-start:
	@echo "🐘 Starting PostgreSQL with Docker..."
	docker compose up -d
	@echo ""
	@echo "✅ PostgreSQL is running!"
	@echo "   PostgreSQL: localhost:5432 (user: postgres, password: onepiece)"
	@echo "   pgAdmin:    http://localhost:5050 (email: admin@onepiece.com, password: admin)"

postgres-stop:
	@echo "🛑 Stopping PostgreSQL..."
	docker compose down

postgres-logs:
	@echo "📋 PostgreSQL logs (Ctrl+C to exit)..."
	docker compose logs -f postgres

export-postgres-full:
	@echo "🚀 Exporting to PostgreSQL (full sync)..."
	$(UV) run onepieceofdata export-postgres --mode full

export-postgres:
	@echo "🚀 Exporting to PostgreSQL (incremental sync)..."
	$(UV) run onepieceofdata export-postgres --mode incremental

postgres-status:
	@echo "🔍 Checking PostgreSQL sync status..."
	$(UV) run onepieceofdata sync-status

# Combined: start PostgreSQL and run full export
postgres-init:
	@echo "🚀 Initializing local PostgreSQL and exporting data..."
	@$(MAKE) postgres-start
	@echo ""
	@echo "⏳ Waiting 5 seconds for PostgreSQL to be ready..."
	@sleep 5
	@echo ""
	@$(MAKE) export-postgres-full
	@echo ""
	@echo "✅ Done! Check status with 'make postgres-status'"

# Show pipeline status
status:
	$(UV) run onepieceofdata status

# Show database content status (quick test after parsing)
db-status:
	@echo "🗄️  Checking database content status..."
	$(UV) run onepieceofdata db-status

# Migrate birth dates (parse birth strings and add birth_date column)
migrate-birth-dates:
	@echo "📅 Migrating birth dates (MM-DD format)..."
	$(UV) run onepieceofdata migrate-birth-dates

# Migrate birth dates with full date format (YYYY-MM-DD)
migrate-birth-dates-full:
	@echo "📅 Migrating birth dates (full date format with year 2000)..."
	$(UV) run onepieceofdata migrate-birth-dates --format full_date

# Load character-on-volume (COV) data
load-cov:
	@echo "🎨 Loading Character-on-Volume (COV) data..."
	$(UV) run onepieceofdata load-cov

# Show current configuration including parallel settings
config:
	@echo "📋 Current Configuration:"
	@echo "========================"
	@$(UV) run python -c "from src.onepieceofdata.config.settings import settings; print(f'Last Chapter: {settings.last_chapter}'); print(f'Last Volume: {settings.last_volume}'); print(f'Enable Parallel: {settings.enable_parallel}'); print(f'Max Workers: {settings.max_workers}'); print(f'Scraping Delay: {settings.scraping_delay}s')"

# Test commands for development (limited scope)
test-scrape:
	@echo "📖 Testing chapter scraping (chapters 1-10)..."
	$(UV) run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 10

test-scrape-parallel:
	@echo "🚀 Testing chapter scraping with parallel processing (chapters 1-10)..."
	$(UV) run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 10 --parallel

# Test parallel scraping with custom workers
# Usage: make test-scrape-workers WORKERS=2
test-scrape-workers:
	@echo "🚀 Testing chapter scraping with $(WORKERS) workers (chapters 1-10)..."
	$(UV) run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 10 --parallel --workers $(WORKERS)

test-scrape-volumes:
	@echo "📚 Testing volume scraping (volumes 1-5)..."
	$(UV) run onepieceofdata scrape-volumes --start-volume 1 --end-volume 5

test-scrape-characters:
	@echo "👥 Testing character scraping..."
	@if [ ! -f "data/characters.csv" ]; then \
		echo "❌ characters.csv not found. Run 'make test-scrape' first."; \
		exit 1; \
	fi
	@echo "ℹ️  Note: This will scrape ALL characters from characters.csv"
	@echo "💡 For faster testing, consider creating a smaller test CSV file"
	$(UV) run onepieceofdata scrape-characters

test-scrape-characters-parallel:
	@echo "🚀 Testing character scraping with parallel processing..."
	@if [ ! -f "data/characters.csv" ]; then \
		echo "❌ characters.csv not found. Run 'make test-scrape' first."; \
		exit 1; \
	fi
	@echo "ℹ️  Note: This will scrape ALL characters from characters.csv with parallel processing"
	$(UV) run onepieceofdata scrape-characters --parallel

test-scrape-story-structure:
	@echo "⚓ Testing story structure scraping (arcs and sagas)..."
	$(UV) run onepieceofdata scrape-story-structure

# Clean up generated files
clean:
	@echo "🧹 Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/
	@echo "✅ Cleanup complete!"
