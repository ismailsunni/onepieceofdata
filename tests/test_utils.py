"""Tests for utility functions."""

import time
from pathlib import Path
import pytest

from onepieceofdata.utils.helpers import (
    timing_decorator,
    ensure_directory,
    safe_get_nested,
    clean_text,
    format_file_size,
    validate_chapter_number,
    validate_url,
    chunks,
)


def test_timing_decorator():
    """Test timing decorator."""
    @timing_decorator
    def slow_function():
        time.sleep(0.1)
        return "done"
    
    result = slow_function()
    assert result == "done"


def test_ensure_directory(temp_dir):
    """Test directory creation."""
    test_path = temp_dir / "nested" / "directory"
    assert not test_path.exists()
    
    result = ensure_directory(test_path)
    assert test_path.exists()
    assert result == test_path


def test_safe_get_nested():
    """Test safe nested dictionary access."""
    data = {
        "level1": {
            "level2": {
                "level3": "value"
            }
        }
    }
    
    # Successful access
    assert safe_get_nested(data, ["level1", "level2", "level3"]) == "value"
    
    # Failed access with default
    assert safe_get_nested(data, ["level1", "missing"], "default") == "default"
    
    # Failed access without default
    assert safe_get_nested(data, ["missing"]) is None


def test_clean_text():
    """Test text cleaning."""
    # Test basic cleaning
    assert clean_text("  hello world  ") == "hello world"
    
    # Test reference marker removal
    assert clean_text("Some text[ref]") == "Some text"
    
    # Test newline removal
    assert clean_text("line1\nline2\rline3") == "line1 line2 line3"
    
    # Test multiple spaces
    assert clean_text("word1    word2") == "word1 word2"
    
    # Test None input
    assert clean_text(None) == ""
    
    # Test non-string input
    assert clean_text(123) == "123"


def test_format_file_size():
    """Test file size formatting."""
    assert format_file_size(0) == "0 B"
    assert format_file_size(512) == "512.0 B"
    assert format_file_size(1024) == "1.0 KB"
    assert format_file_size(1536) == "1.5 KB"
    assert format_file_size(1024 * 1024) == "1.0 MB"


def test_validate_chapter_number():
    """Test chapter number validation."""
    assert validate_chapter_number(1) is True
    assert validate_chapter_number(100) is True
    assert validate_chapter_number(1500) is True
    
    assert validate_chapter_number(0) is False
    assert validate_chapter_number(-1) is False
    assert validate_chapter_number(3000) is False  # Too high
    assert validate_chapter_number("1") is False  # Wrong type


def test_validate_url():
    """Test URL validation."""
    assert validate_url("https://example.com") is True
    assert validate_url("http://example.com") is True
    assert validate_url("https://onepiece.fandom.com/wiki/Chapter_1") is True
    
    assert validate_url("invalid") is False
    assert validate_url("") is False
    assert validate_url("ftp://example.com") is False
    assert validate_url(None) is False


def test_chunks():
    """Test list chunking."""
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    chunks_list = list(chunks(data, 3))
    expected = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10]]
    
    assert chunks_list == expected
    
    # Test exact division
    chunks_list = list(chunks(data, 5))
    expected = [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]
    assert chunks_list == expected
