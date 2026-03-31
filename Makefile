# Makefile for One Piece of Data development
UV := uv

.PHONY: help install install-dev test lint format clean setup check \
	filter-non-characters filter-non-characters-dry-run \
	extract-characters merge-characters merge-characters-dry-run sync-character-appearances sync-character-appearances-verbose sync-cover-appearances sync-cover-appearances-verbose sync-origin-region sync-origin-region-dry-run run-character-workflow \
	run-scrape run-scrape-parallel run-scrape-workers run-scrape-characters run-scrape-characters-parallel run-scrape-characters-workers run-scrape-volumes \
	run-scrape-arcs run-scrape-sagas run-scrape-story-structure run-parse run-parse-story-structure \
	run-full-pipeline run-full-pipeline-parallel run-full-pipeline-workers \
	run-all-scrapers run-all-parsers run-all-postprocessors run-all-exports run-data-pipeline \
	status db-status migrate-birth-dates migrate-birth-dates-full load-cov config export \
	postgres-start postgres-stop postgres-logs postgres-init export-postgres export-postgres-full postgres-status \
	test-scrape test-scrape-parallel test-scrape-workers test-scrape-volumes test-scrape-characters test-scrape-characters-parallel test-scrape-story-structure \
	run-network-explorer \
	wiki-scrape wiki-scrape-characters wiki-scrape-arcs wiki-status \
	embed-wiki embed-status search

# Default target
help:
	@echo "┌─────────────────────────────────────────────────────────────────────┐"
	@echo "│         One Piece of Data - Development Commands                   │"
	@echo "└─────────────────────────────────────────────────────────────────────┘"
	@echo ""
	@echo "📊 PIPELINE FLOW:"
	@echo "═══════════════════════════════════════════════════════════════════════"
	@echo "  ┌─ SCRAPING ──────────────────────────────────────────────────┐"
	@echo "  │  1. run-scrape-parallel       → chapters.json               │"
	@echo "  │  2. run-scrape-volumes        → volumes.json                │"
	@echo "  │  3. extract-characters        → characters.csv              │"
	@echo "  │  4. run-scrape-characters-parallel → characters_detail.json │"
	@echo "  │  5. run-scrape-story-structure → arcs.json, sagas.json      │"
	@echo "  └─────────────────────────────────────────────────────────────┘"
	@echo "           ↓"
	@echo "  ┌─ PARSING & DATABASE ────────────────────────────────────────┐"
	@echo "  │  6. run-parse                 → DuckDB (chapters, volumes)  │"
	@echo "  │  7. run-parse-story-structure → DuckDB (arcs, sagas)        │"
	@echo "  └─────────────────────────────────────────────────────────────┘"
	@echo "           ↓"
	@echo "  ┌─ POST-PROCESSING ───────────────────────────────────────────┐"
	@echo "  │  8. migrate-birth-dates       → Add birth_date column       │"
	@echo "  │  9. load-cov                  → Load cover characters       │"
	@echo "  │ 10. merge-characters          → Merge duplicate characters  │"
	@echo "  │ 11. sync-character-appearances→ Compute chapter stats       │"
	@echo "  │ 12. sync-cover-appearances    → Compute cover stats         │"
	@echo "  └─────────────────────────────────────────────────────────────┘"
	@echo "           ↓"
	@echo "  ┌─ EXPORT ────────────────────────────────────────────────────┐"
	@echo "  │ 12. export-postgres-full      → PostgreSQL (full sync)      │"
	@echo "  │     OR export                 → CSV files                   │"
	@echo "  └─────────────────────────────────────────────────────────────┘"
	@echo ""
	@echo "🚀 QUICK START:"
	@echo "  make setup                      # First-time setup"
	@echo ""
	@echo "  ⭐ RECOMMENDED TWO-COMMAND WORKFLOW:"
	@echo "  make run-data-pipeline          # 1. Scrape → Parse → Post-process (all data in DuckDB)"
	@echo "  make run-all-exports            # 2. Export to CSV + PostgreSQL"
	@echo ""
	@echo "  OR use the all-in-one command:"
	@echo "  make run-full-pipeline-parallel # Run entire pipeline with parallel processing"
	@echo ""
	@echo "════════════════════════════════════════════════════════════════════════"
	@echo ""
	@echo "📦 SETUP & DEVELOPMENT"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  setup                - Initial project setup (uv, dependencies, directories)"
	@echo "  install              - Install production dependencies only"
	@echo "  install-dev          - Install all dependencies (dev + extras)"
	@echo "  test                 - Run pytest test suite"
	@echo "  lint                 - Run ruff linting"
	@echo "  format               - Format code with black + ruff"
	@echo "  check                - Run all checks (lint + test)"
	@echo "  clean                - Clean up generated files (__pycache__, etc.)"
	@echo ""
	@echo "📖 SCRAPING - CHAPTERS"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  run-scrape                    - Scrape all chapters (sequential)"
	@echo "  run-scrape-parallel           - Scrape all chapters (parallel, 4 workers)"
	@echo "  run-scrape-workers WORKERS=N  - Scrape all chapters (N workers)"
	@echo "  test-scrape                   - Test scraping (chapters 1-10)"
	@echo "  test-scrape-parallel          - Test scraping with parallel (chapters 1-10)"
	@echo "  test-scrape-workers WORKERS=N - Test scraping with N workers"
	@echo ""
	@echo "📚 SCRAPING - VOLUMES"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  run-scrape-volumes   - Scrape all volumes"
	@echo "  test-scrape-volumes  - Test scraping (volumes 1-5)"
	@echo ""
	@echo "👥 SCRAPING - CHARACTERS"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  extract-characters                     - Extract character list from chapters"
	@echo "  run-scrape-characters                  - Scrape all characters (sequential)"
	@echo "  run-scrape-characters-parallel         - Scrape all characters (parallel, 4 workers)"
	@echo "  run-scrape-characters-workers WORKERS=N- Scrape all characters (N workers)"
	@echo "  test-scrape-characters                 - Test character scraping (all from CSV)"
	@echo "  test-scrape-characters-parallel        - Test character scraping (parallel)"
	@echo ""
	@echo "⚓ SCRAPING - STORY STRUCTURE (ARCS & SAGAS)"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  run-scrape-arcs            - Scrape story arcs only"
	@echo "  run-scrape-sagas           - Scrape story sagas only"
	@echo "  run-scrape-story-structure - Scrape both arcs and sagas to JSON"
	@echo "  test-scrape-story-structure- Test story structure scraping"
	@echo ""
	@echo "🗄️  PARSING & DATABASE"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  run-parse                - Parse JSON files → DuckDB (chapters, volumes, characters)"
	@echo "  run-parse-story-structure- Parse story structure → DuckDB (arcs, sagas)"
	@echo "  db-status                - Show database table row counts and sample data"
	@echo ""
	@echo "🔧 POST-PROCESSING"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  filter-non-characters             - Remove non-character entries (crews, locations, etc.)"
	@echo "  filter-non-characters-dry-run    - Preview non-character removal"
	@echo "  migrate-birth-dates              - Parse birth strings → birth_date (MM-DD)"
	@echo "  migrate-birth-dates-full         - Parse birth strings → birth_date (YYYY-MM-DD)"
	@echo "  load-cov                         - Load character-on-volume (cover characters)"
	@echo "  merge-characters-dry-run         - Preview character deduplication (recommended first)"
	@echo "  merge-characters                 - Merge duplicate characters"
	@echo "  sync-character-appearances       - Compute chapter appearance analytics"
	@echo "  sync-character-appearances-verbose - Compute chapter appearances (verbose output)"
	@echo "  sync-cover-appearances           - Compute cover appearance analytics"
	@echo "  sync-cover-appearances-verbose   - Compute cover appearances (verbose output)"
	@echo ""
	@echo "🔄 FULL PIPELINES"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  run-full-pipeline                - Complete pipeline (sequential)"
	@echo "  run-full-pipeline-parallel       - Complete pipeline (parallel, 4 workers) ⭐"
	@echo "  run-full-pipeline-workers WORKERS=N - Complete pipeline (N workers)"
	@echo "  run-character-workflow           - Character workflow (scrape→parse→merge→sync)"
	@echo ""
	@echo "📦 CONTAINER COMMANDS (Modular Pipeline Stages)"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  run-all-scrapers         - Run ALL scrapers in parallel (chapters, volumes, characters, arcs/sagas)"
	@echo "  run-all-parsers          - Run ALL parsers (load scraped data into DuckDB)"
	@echo "  run-all-postprocessors   - Run ALL post-processors (birth dates, COV, merge, chapter/cover appearances)"
	@echo "  run-all-exports          - Run ALL exports (CSV + PostgreSQL full sync)"
	@echo ""
	@echo "  🌟 TWO-COMMAND WORKFLOW:"
	@echo "     make run-data-pipeline    - Scraping → Parsing → Post-Processing ⭐⭐"
	@echo "     make run-all-exports      - Export to CSV + PostgreSQL"
	@echo ""
	@echo "🐘 POSTGRESQL EXPORT"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  postgres-start       - Start PostgreSQL + pgAdmin (Docker Compose)"
	@echo "  postgres-stop        - Stop PostgreSQL containers"
	@echo "  postgres-logs        - View PostgreSQL logs (follow mode)"
	@echo "  postgres-init        - Start PostgreSQL + run full export ⭐"
	@echo "  export-postgres-full - Export DuckDB → PostgreSQL (full sync, drops tables)"
	@echo "  export-postgres      - Export DuckDB → PostgreSQL (incremental sync)"
	@echo "  postgres-status      - Check PostgreSQL sync status and row counts"
	@echo ""
	@echo "📤 OTHER EXPORTS"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  export               - Export DuckDB → CSV files (exports/ directory)"
	@echo ""
	@echo "🛠️  UTILITY COMMANDS"
	@echo "─────────────────────────────────────────────────────────────────────"
	@echo "  status               - Show pipeline status (files, sizes, record counts)"
	@echo "  config               - Show current configuration (workers, delays, etc.)"
	@echo "  run-network-explorer - Launch local web app to explore character network"
	@echo ""
	@echo "💡 TIPS:"
	@echo "  • RECOMMENDED: Use the two-command workflow for better control:"
	@echo "    1. make run-data-pipeline  (prepare all data)"
	@echo "    2. make run-all-exports    (export when ready)"
	@echo "  • For quick all-in-one: make setup && make run-full-pipeline-parallel"
	@echo "  • Parallel commands use 4 workers by default (faster)"
	@echo "  • Use WORKERS=N to customize: make run-scrape-workers WORKERS=8"
	@echo "  • Story arcs auto-link to sagas based on chapter range containment"
	@echo "  • PostgreSQL export requires Docker (use postgres-init for easy setup)"
	@echo ""

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

# Merge duplicate characters (dry-run mode)
merge-characters-dry-run:
	@echo "🔍 Previewing character merge (dry-run mode)..."
	$(UV) run onepieceofdata merge-characters --dry-run

# Merge duplicate characters (actual merge)
merge-characters:
	@echo "🔀 Merging duplicate characters..."
	$(UV) run onepieceofdata merge-characters

# Sync character appearance analytics from CoC/CoV tables
sync-character-appearances:
	@echo "🔄 Syncing character appearance analytics..."
	$(UV) run onepieceofdata sync-character-appearances

# Sync character appearances with verbose output
sync-character-appearances-verbose:
	@echo "🔄 Syncing character appearance analytics (verbose mode)..."
	$(UV) run onepieceofdata sync-character-appearances --verbose

# Sync character volume cover appearance analytics from CoV table
sync-cover-appearances:
	@echo "🔄 Syncing character cover appearance analytics..."
	$(UV) run onepieceofdata sync-cover-appearances

# Sync cover appearances with verbose output
sync-cover-appearances-verbose:
	@echo "🔄 Syncing character cover appearance analytics (verbose mode)..."
	$(UV) run onepieceofdata sync-cover-appearances --verbose

# Complete character workflow (scrape → parse → merge → sync)
run-character-workflow:
	@echo "👥 Running complete character workflow..."
	@echo ""
	@$(MAKE) run-scrape-characters
	@echo ""
	@$(MAKE) run-parse
	@echo ""
	@echo "📋 Step 1/3: Merge Characters"
	@$(MAKE) merge-characters-dry-run
	@echo ""
	@echo "📋 Step 2/3: Proceeding with merge..."
	@$(MAKE) merge-characters
	@echo ""
	@echo "📋 Step 3/3: Sync Appearances"
	@$(MAKE) sync-character-appearances
	@echo ""
	@echo "✅ Character workflow complete!"

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
	@echo "Step 9: Syncing character chapter appearance analytics..."
	$(MAKE) sync-character-appearances
	@echo ""
	@echo "Step 10: Syncing character cover appearance analytics..."
	$(MAKE) sync-cover-appearances
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
	@echo "Step 5: Loading basic data into database (volumes, chapters, characters)..."
	$(MAKE) run-parse
	@echo ""
	@echo "Step 6: Scraping story structure (arcs and sagas)..."
	$(MAKE) run-scrape-story-structure
	@echo ""
	@echo "Step 7: Loading story structure into database (arcs and sagas)..."
	$(MAKE) run-parse-story-structure
	@echo ""
	@echo "Step 8: Parsing birth dates and adding birth_date column..."
	$(MAKE) migrate-birth-dates
	@echo ""
	@echo "Step 9: Loading character-on-volume (COV) data..."
	$(MAKE) load-cov
	@echo ""
	@echo "Step 10: Syncing character chapter appearance analytics..."
	$(MAKE) sync-character-appearances
	@echo ""
	@echo "Step 11: Syncing character cover appearance analytics..."
	$(MAKE) sync-cover-appearances
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

# ============================================================================
# CONTAINER COMMANDS - Grouped pipeline stages for modular execution
# ============================================================================

# Run ALL scrapers in parallel mode
run-all-scrapers:
	@echo "🕷️  Running ALL scrapers in parallel mode..."
	@echo ""
	@echo "📖 Step 1/5: Scraping chapters (parallel)..."
	$(MAKE) run-scrape-parallel
	@echo ""
	@echo "📚 Step 2/5: Scraping volumes..."
	$(MAKE) run-scrape-volumes
	@echo ""
	@echo "👥 Step 3/5: Extracting character list from chapters..."
	$(MAKE) extract-characters
	@echo ""
	@echo "👤 Step 4/5: Scraping character details (parallel)..."
	$(MAKE) run-scrape-characters-parallel
	@echo ""
	@echo "⚓ Step 5/5: Scraping story structure (arcs and sagas)..."
	$(MAKE) run-scrape-story-structure
	@echo ""
	@echo "✅ All scraping completed!"

# Run ALL parsers (load data into DuckDB)
run-all-parsers:
	@echo "📊 Running ALL parsers (loading data into DuckDB)..."
	@echo ""
	@echo "🗄️  Step 1/2: Parsing basic data (chapters, volumes, characters)..."
	$(MAKE) run-parse
	@echo ""
	@echo "⚓ Step 2/2: Parsing story structure (arcs and sagas)..."
	$(MAKE) run-parse-story-structure
	@echo ""
	@echo "✅ All parsing completed!"

# Run ALL post-processors (enrich and transform data)
run-all-postprocessors:
	@echo "🔧 Running ALL post-processors..."
	@echo ""
	@echo "🧹 Step 1/7: Filtering non-character entries..."
	$(MAKE) filter-non-characters
	@echo ""
	@echo "📅 Step 2/7: Migrating birth dates..."
	$(MAKE) migrate-birth-dates
	@echo ""
	@echo "🎨 Step 3/7: Loading character-on-volume (COV) data..."
	$(MAKE) load-cov
	@echo ""
	@echo "🔀 Step 4/7: Merging duplicate characters..."
	$(MAKE) merge-characters
	@echo ""
	@echo "🔄 Step 5/7: Syncing character chapter appearance analytics..."
	$(MAKE) sync-character-appearances
	@echo ""
	@echo "🎨 Step 6/7: Syncing character cover appearance analytics..."
	$(MAKE) sync-cover-appearances
	@echo ""
	@echo "🗺️  Step 7/7: Syncing character origin regions..."
	$(MAKE) sync-origin-region
	@echo ""
	@echo "✅ All post-processing completed!"

# Run ALL exports (CSV + PostgreSQL)
run-all-exports:
	@echo "📤 Running ALL exports..."
	@echo ""
	@echo "📄 Step 1/2: Exporting to CSV files..."
	$(MAKE) export
	@echo ""
	@echo "🐘 Step 2/2: Exporting to PostgreSQL (full sync)..."
	$(MAKE) export-postgres-full
	@echo ""
	@echo "✅ All exports completed!"

# MAIN WORKFLOW: Run scraping + parsing + post-processing (NO export)
# This is the command to prepare all data in DuckDB
run-data-pipeline:
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "🚀 RUNNING COMPLETE DATA PIPELINE (Scraping → Parsing → Post-Processing)"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo ""
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║  STAGE 1/3: SCRAPING                                          ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	$(MAKE) run-all-scrapers
	@echo ""
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║  STAGE 2/3: PARSING                                           ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	$(MAKE) run-all-parsers
	@echo ""
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║  STAGE 3/3: POST-PROCESSING                                   ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	$(MAKE) run-all-postprocessors
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "✅ DATA PIPELINE COMPLETED!"
	@echo "═══════════════════════════════════════════════════════════════"
	@$(MAKE) db-status

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

# Filter non-character entries (crews, locations, titles, etc.)
filter-non-characters:
	@echo "🧹 Filtering non-character entries..."
	$(UV) run onepieceofdata filter-non-characters

filter-non-characters-dry-run:
	@echo "🔍 Previewing non-character filter (dry run)..."
	$(UV) run onepieceofdata filter-non-characters --dry-run --verbose

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

# Populate origin_region column based on character origin field
sync-origin-region:
	@echo "🗺️  Syncing character origin regions..."
	$(UV) run onepieceofdata sync-origin-region

sync-origin-region-dry-run:
	@echo "🔍 Previewing origin region classification (dry run)..."
	$(UV) run onepieceofdata sync-origin-region --dry-run --verbose

# Show current configuration including parallel settings
config:
	@echo "📋 Current Configuration:"
	@echo "========================"
	@$(UV) run python -c "from src.onepieceofdata.config.settings import settings; print(f'Last Chapter: {settings.last_chapter}'); print(f'Last Volume: {settings.last_volume}'); print(f'Enable Parallel: {settings.enable_parallel}'); print(f'Max Workers: {settings.max_workers}'); print(f'Scraping Delay: {settings.scraping_delay}s')"

# Build character co-appearance graph JSON files for all weight types
build-character-graphs:
	@echo "Building character graph: arc..."
	@$(UV) run python scripts/build_character_graph.py --weight arc --output data/network_arc.json
	@echo "Building character graph: saga..."
	@$(UV) run python scripts/build_character_graph.py --weight saga --output data/network_saga.json
	@echo "Building character graph: chapter..."
	@$(UV) run python scripts/build_character_graph.py --weight chapter --output data/network_chapter.json
	@echo "Building character graph: consec-2..."
	@$(UV) run python scripts/build_character_graph.py --weight consec-2 --output data/network_consec-2.json
	@echo "Building character graph: consec-3..."
	@$(UV) run python scripts/build_character_graph.py --weight consec-3 --output data/network_consec-3.json
	@echo "Building character graph: consec-4..."
	@$(UV) run python scripts/build_character_graph.py --weight consec-4 --output data/network_consec-4.json
	@echo "Building character graph: consec-5..."
	@$(UV) run python scripts/build_character_graph.py --weight consec-5 --output data/network_consec-5.json
	@echo "Building character graph: consec-7..."
	@$(UV) run python scripts/build_character_graph.py --weight consec-7 --output data/network_consec-7.json
	@echo "Done. Output files in data/network_*.json"

# Launch interactive web app for character co-appearance network exploration
run-network-explorer:
	@echo "🌐 Starting network explorer web app..."
	$(UV) run python scripts/run_network_explorer.py

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

# RAG Pipeline
wiki-scrape: ## Scrape all wiki pages (characters + arcs + sagas)
	uv run python -m onepieceofdata.cli.wiki_scrape --all

wiki-scrape-characters: ## Scrape character wiki pages only
	uv run python -m onepieceofdata.cli.wiki_scrape --characters

wiki-scrape-arcs: ## Scrape arc and saga wiki pages only
	uv run python -m onepieceofdata.cli.wiki_scrape --arcs

wiki-status: ## Show wiki scraping progress
	uv run python -m onepieceofdata.cli.wiki_scrape --status

embed-wiki: ## Chunk + embed wiki text + save to DuckDB
	uv run python -m onepieceofdata.cli.embed --run --db ./onepiece-master.duckdb

embed-status: ## Show embedding stats
	uv run python -m onepieceofdata.cli.embed --status --db ./onepiece-master.duckdb

search: ## Search wiki (usage: make search Q="gear 5")
	uv run python -m onepieceofdata.cli.embed --search "$(Q)" --db ./onepiece-master.duckdb

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
