"""
Logging utility module for centralized logging configuration.

This module provides a single function to configure logging for the application,
ensuring consistent logging behavior across all modules with both console and file output.
"""

import logging
import os
import json
from datetime import datetime
from typing import Optional, Any, Dict

# Module-level flag to ensure logging is initialized only once
_LOGGING_INITIALIZED = False


# JSON log format keys
JSON_LOG_KEYS = ["timestamp", "level", "message", "event_id", "context"]


class JsonLogFormatter(logging.Formatter):
    """
    Custom logging formatter to output logs in structured JSON format.
    """
    def format(self, record: logging.LogRecord) -> str:
        log_record: Dict[str, Any] = {}
        # Timestamp in the required format
        now = datetime.fromtimestamp(record.created)
        log_record["timestamp"] = now.strftime("%Y%m%d_%H%M%S_%f")
        log_record["level"] = record.levelname
        log_record["message"] = record.getMessage()
        # Optional: event_id and context can be passed via extra
        log_record["event_id"] = getattr(record, "event_id", None)
        context = getattr(record, "context", None)
        if context is not None and isinstance(context, dict):
            log_record["context"] = context
        else:
            log_record["context"] = context
        # Only include keys in JSON_LOG_KEYS
        filtered = {k: log_record[k] for k in JSON_LOG_KEYS}
        return json.dumps(filtered, ensure_ascii=False)


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
    
    # Add console handler with context formatting
    class ConsoleFormatter(logging.Formatter):
        def format(self, record):
            msg = super().format(record)
            context = getattr(record, "context", None)
            if context:
                if isinstance(context, dict):
                    context_str = json.dumps(context, ensure_ascii=False)
                else:
                    context_str = str(context)
                return f"{msg}\n{context_str}"
            return msg

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(ConsoleFormatter("[%(levelname)s] %(message)s"))
    logger.addHandler(console_handler)

    # Add file handler with JSON formatting
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.NOTSET)  # Log all levels to file
    file_handler.setFormatter(JsonLogFormatter())
    logger.addHandler(file_handler)

    write_log.info("LOG_INIT", "Logging initialized.", {"log_file": log_path})
    _LOGGING_INITIALIZED = True

# -- Public API --
class write_log:
    @staticmethod
    def info(event_id: str, msg: str, context: dict = None):
        logging.getLogger().info(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def error(event_id: str, msg: str, context: dict = None):
        logging.getLogger().error(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def warn(event_id: str, msg: str, context: dict = None):
        logging.getLogger().warning(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def debug(event_id: str, msg: str, context: dict = None):
        logging.getLogger().debug(msg, extra={"event_id": event_id, "context": context or {}})