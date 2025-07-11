# Makefile for One Piece of Data development
UV := uv

.PHONY: help install install-dev test lint format clean setup check-uv run-scrape run-scrape-characters run-scrape-volumes run-parse run-full-pipeline status export

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
	@echo "  run-scrape     - Run chapter scraping (1-10 for testing)"
	@echo "  run-scrape-characters - Run character scraping (requires characters.csv)"
	@echo "  run-scrape-volumes - Run volume scraping (1-5 for testing)"
	@echo "  run-parse      - Run data parsing and database loading"
	@echo "  run-full-pipeline - Run complete pipeline (scrape + parse)"
	@echo "  status         - Show pipeline status"
	@echo "  export         - Export database to CSV files"
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
	@echo "  1. Run 'make run-scrape' to test scraping"
	@echo "  2. Run 'make status' to check pipeline status"
	@echo "  3. Run 'make run-full-pipeline' for complete workflow"

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

# Run chapter scraping (test with chapters 1-10)
run-scrape:
	@echo "📖 Running chapter scraping (chapters 1-10 for testing)..."
	$(UV) run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 10

# Run character scraping (requires characters.csv from chapters)
run-scrape-characters:
	@echo "👥 Running character scraping..."
	@if [ ! -f "data/characters.csv" ]; then \
		echo "❌ characters.csv not found. Run 'make run-scrape' first."; \
		exit 1; \
	fi
	$(UV) run onepieceofdata scrape-characters --max-characters 10

# Run volume scraping (test with volumes 1-5)
run-scrape-volumes:
	@echo "📚 Running volume scraping (volumes 1-5 for testing)..."
	$(UV) run onepieceofdata scrape-volumes --start-volume 1 --end-volume 5

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
	@echo "Step 4: Loading data into database..."
	$(MAKE) run-parse
	@echo ""
	@echo "✅ Pipeline completed! Check status with 'make status'"

# Export database to CSV files
export:
	@echo "📤 Exporting database to CSV files..."
	$(UV) run onepieceofdata export --output-dir exports

# Show pipeline status
status:
	$(UV) run onepieceofdata status

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
