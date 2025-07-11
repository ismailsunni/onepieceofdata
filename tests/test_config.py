"""Tests for configuration management."""

import os
from pathlib import Path
import pytest

from onepieceofdata.config import Settings, settings


def test_default_settings():
    """Test default settings values."""
    default_settings = Settings()
    
    assert default_settings.last_chapter == 1153
    assert default_settings.last_volume == 112
    assert default_settings.log_level == "INFO"
    assert default_settings.scraping_delay == 1.0
    assert default_settings.max_retries == 3


def test_settings_with_env_vars(monkeypatch):
    """Test settings with environment variables."""
    # Set environment variables
    monkeypatch.setenv("OP_LAST_CHAPTER", "1000")
    monkeypatch.setenv("OP_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("OP_SCRAPING_DELAY", "2.0")
    
    # Create new settings instance
    test_settings = Settings()
    
    assert test_settings.last_chapter == 1000
    assert test_settings.log_level == "DEBUG"
    assert test_settings.scraping_delay == 2.0


def test_settings_properties(test_settings):
    """Test settings computed properties."""
    assert test_settings.chapters_json_path == test_settings.data_dir / "chapters.json"
    assert test_settings.volumes_json_path == test_settings.data_dir / "volumes.json"
    assert test_settings.characters_json_path == test_settings.data_dir / "characters.json"
    assert test_settings.chapters_csv_path == test_settings.data_dir / "chapters.csv"
    assert test_settings.characters_csv_path == test_settings.data_dir / "characters.csv"
    assert test_settings.coc_csv_path == test_settings.data_dir / "coc.csv"


def test_ensure_directories(test_settings):
    """Test directory creation."""
    # Directories should not exist initially
    assert not test_settings.data_dir.exists()
    assert not test_settings.log_file.parent.exists()
    
    # Call ensure_directories
    test_settings.ensure_directories()
    
    # Directories should now exist
    assert test_settings.data_dir.exists()
    assert test_settings.log_file.parent.exists()
