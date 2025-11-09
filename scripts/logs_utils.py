"""
Logging utility module for centralized logging configuration.

This module provides a single function to configure logging for the application,
ensuring consistent logging behavior across all modules with both console and file output.
"""

import logging
import os
from datetime import datetime
from typing import Optional

# Module-level flag to ensure logging is initialized only once
_LOGGING_INITIALIZED = False

# Default log format templates
CONSOLE_FORMAT = "[%(levelname)s] %(message)s"
FILE_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(
    logs_dir: Optional[str] = None,
    log_level: int = logging.INFO,
    log_name_prefix: str = "run"
) -> None:
    """
    Configure logging to output to both console and a timestamped log file.
    
    This function is idempotent - calling it multiple times has no effect after
    the first successful initialization. Log files are stored with a unique
    timestamp to prevent overwriting previous logs.
    
    Args:
        logs_dir: Directory path for log files. Defaults to '_logs' subdirectory
                 relative to this module's location.
        log_level: Logging level (e.g., logging.INFO, logging.DEBUG).
        log_name_prefix: Prefix for log filename. Full name will be
                        '{prefix}_{timestamp}.log'.
    
    Example:
        >>> setup_logging(log_name_prefix="workflow", log_level=logging.DEBUG)
        # Creates: logs/2025/11/08/workflow_20251108_143025_123456.log
    """
    global _LOGGING_INITIALIZED
    
    if _LOGGING_INITIALIZED:
        return
    
    # Determine log directory
    if logs_dir is None:
        logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")

    # Generate timestamped directory structure
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    logs_dir = os.path.join(logs_dir, year, month, day)
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate unique log filename with microsecond precision
    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
    log_filename = f"{log_name_prefix}_{timestamp}.log"
    log_path = os.path.join(logs_dir, log_filename)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear any existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add console handler with simple formatting
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(CONSOLE_FORMAT))
    logger.addHandler(console_handler)
    
    # Add file handler with detailed formatting
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(FILE_FORMAT))
    logger.addHandler(file_handler)
    
    logger.info(f"Logging initialized. Log file: {log_path}")
    _LOGGING_INITIALIZED = True
