"""Logging configuration for One Piece of Data pipeline."""

import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from ..config import settings


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[Path] = None,
    enable_console: bool = True,
) -> None:
    """
    Set up logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (optional)
        enable_console: Whether to enable console logging
    """
    # Remove default logger
    logger.remove()
    
    # Use settings if not provided
    if log_level is None:
        log_level = settings.log_level
    if log_file is None:
        log_file = settings.log_file
    
    # Console logging
    if enable_console:
        logger.add(
            sys.stderr,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                   "<level>{message}</level>",
            colorize=True,
        )
    
    # File logging
    if log_file:
        # Ensure log directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            log_file,
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="10 MB",
            retention="1 week",
            compression="gz",
        )
    
    logger.info(f"Logging initialized with level: {log_level}")
    if log_file:
        logger.info(f"Log file: {log_file}")


def get_logger(name: str):
    """Get a logger instance for a specific module."""
    return logger.bind(name=name)
