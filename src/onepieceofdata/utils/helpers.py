"""Utility functions for One Piece of Data pipeline."""

import time
import functools
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from loguru import logger

F = TypeVar('F', bound=Callable[..., Any])


def timing_decorator(func: F) -> F:
    """
    Decorator to measure function execution time.
    
    Args:
        func: Function to measure
        
    Returns:
        Wrapped function that logs execution time
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.info(f"Function {func.__name__} completed in {elapsed_time:.4f} seconds")
            return result
        except Exception as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.error(f"Function {func.__name__} failed after {elapsed_time:.4f} seconds: {e}")
            raise
    
    return wrapper


def ensure_directory(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to create
        
    Returns:
        Path object of the created directory
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def safe_get_nested(data: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    """
    Safely get a nested value from a dictionary.
    
    Args:
        data: Dictionary to search
        keys: List of keys to traverse
        default: Default value if key path doesn't exist
        
    Returns:
        Value at the key path or default value
    """
    try:
        result = data
        for key in keys:
            result = result[key]
        return result
    except (KeyError, TypeError, IndexError):
        return default


def clean_text(text: str) -> str:
    """
    Clean text by removing extra whitespace and unwanted characters.
    
    Args:
        text: Text to clean
        
    Returns:
        Cleaned text
    """
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    
    # Remove extra whitespace and common unwanted markers
    cleaned = text.strip()
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = " ".join(cleaned.split())  # Remove multiple spaces
    
    # Remove common reference markers
    markers_to_remove = ["[ref]", "[citation needed]", "[note]"]
    for marker in markers_to_remove:
        cleaned = cleaned.replace(marker, "")
    
    return cleaned.strip()


def retry_on_failure(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable:
    """
    Decorator for retrying functions on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry on
        
    Returns:
        Decorator function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    logger.info(f"Retrying in {current_delay:.2f} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            # This should never be reached, but just in case
            raise last_exception
        
        return wrapper
    return decorator


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024.0 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"


def validate_chapter_number(chapter_num: int) -> bool:
    """
    Validate if a chapter number is reasonable.
    
    Args:
        chapter_num: Chapter number to validate
        
    Returns:
        True if valid, False otherwise
    """
    return isinstance(chapter_num, int) and 1 <= chapter_num <= 2000  # Reasonable upper bound


def validate_url(url: str) -> bool:
    """
    Basic URL validation.
    
    Args:
        url: URL to validate
        
    Returns:
        True if URL appears valid, False otherwise
    """
    if not isinstance(url, str):
        return False
    
    url = url.strip()
    return url.startswith(("http://", "https://")) and len(url) > 10


def chunks(lst: List[Any], n: int) -> List[List[Any]]:
    """
    Yield successive n-sized chunks from lst.
    
    Args:
        lst: List to chunk
        n: Chunk size
        
    Yields:
        Lists of size n (or smaller for the last chunk)
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
