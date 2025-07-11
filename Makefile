# Makefile for One Piece of Data development
UV := uv

.PHONY: help install install-dev test lint format clean setup check-uv run-scrape run-parse status

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
	@echo "  run-parse      - Run data parsing"
	@echo "  status         - Show pipeline status"
	@echo ""
	@echo "Utility Commands:"
	@echo "  clean          - Clean up generated files"

# Initial setup
setup:
	@echo "🚀 Setting up One Piece of Data development environment..."
	$(UV) sync --all-extras
	@echo "📁 Creating required directories..."
	mkdir -p data logs
	@echo "✅ Setup complete!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run 'make run-scrape' to test scraping"
	@echo "  2. Run 'make status' to check pipeline status"

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

# Run data parsing
run-parse:
	@echo "🔄 Running data parsing..."
	$(UV) run onepieceofdata parse

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
