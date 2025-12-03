"""
Logging utility module for centralized logging configuration.

This module provides a structured logging system with JSON file output and console formatting.
It includes utilities for log file parsing, analysis, and DataFrame conversion for observability.

Key Features:
- Environment-aware logging with automatic directory organization
- JSON-formatted file logs for structured parsing
- Human-readable console output
- Log aggregation and analysis utilities
- Thread-safe singleton initialization

Public API:
- setup_logging(): Configure logging system (idempotent)
- write_log: Static class with info/error/warn/debug methods
- get_log_files(): Find all log files in directory
- parse_logs(): Parse JSON log files into list of dicts
- filter_warning_error_logs(): Filter logs by severity
- logs_to_dataframe(): Convert logs to pandas DataFrame
- prepare_log_summary(): Group and summarize log entries
"""

import logging
import os
import json
import glob
from datetime import datetime
from typing import Optional, Any, Dict, List

# Module-level flag to ensure logging is initialized only once
_LOGGING_INITIALIZED = False

# JSON log format keys for structured logging
JSON_LOG_KEYS = ["timestamp", "level", "message", "event_id", "context"]

class JsonLogFormatter(logging.Formatter):
    """
    Custom logging formatter that outputs structured JSON log entries.
    
    Each log entry contains: timestamp, level, message, event_id, and context.
    Timestamps use microsecond precision for unique identification.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as a JSON string.
        
        Args:
            record: LogRecord instance from Python logging system
            
        Returns:
            JSON-formatted string with filtered keys
        """
        log_record: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).strftime("%Y%m%d_%H%M%S_%f"),
            "level": record.levelname,
            "message": record.getMessage(),
            "event_id": getattr(record, "event_id", None),
            "context": getattr(record, "context", None)
        }
        
        # Filter to only include defined keys
        filtered = {k: log_record[k] for k in JSON_LOG_KEYS}
        return json.dumps(filtered, ensure_ascii=False)

def get_log_files(logs_dir: str) -> List[str]:
    """
    Recursively find all .log files in the specified directory tree.
    
    Args:
        logs_dir: Root directory path to search for log files
        
    Returns:
        List of absolute paths to .log files
        
    Example:
        >>> get_log_files('observability/test_logs')
        ['observability/test_logs/2025/11/27/workflow_20251127_143025_123456.log', ...]
    """
    pattern = os.path.join(logs_dir, '**', '*.log')
    return glob.glob(pattern, recursive=True)

def parse_logs(log_files: List[str]) -> List[dict]:
    """
    Parse JSON-formatted log files into structured entries.
    
    Each line in the log files is expected to be a valid JSON object.
    Malformed lines are silently skipped.
    
    Args:
        log_files: List of log file paths to parse
        
    Returns:
        List of dictionaries, each representing one log entry
        
    Example:
        >>> entries = parse_logs(['logs/2025/11/27/run.log'])
        >>> entries[0]
        {'timestamp': '20251127_143025_123456', 'level': 'INFO', ...}
    """
    log_entries = []
    for file_path in log_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        log_entries.append(entry)
                    except json.JSONDecodeError:
                        # Skip malformed JSON lines
                        continue
        except (IOError, OSError):
            # Skip files that can't be read
            continue
    return log_entries

def filter_warning_error_logs(log_entries: List[dict]) -> List[dict]:
    """
    Filter log entries to only include WARNING and ERROR severity levels.
    
    Useful for focusing on problematic logs during analysis and debugging.
    
    Args:
        log_entries: List of parsed log entry dictionaries
        
    Returns:
        Filtered list containing only WARNING and ERROR logs
        
    Example:
        >>> all_logs = [{'level': 'INFO', ...}, {'level': 'ERROR', ...}]
        >>> errors = filter_warning_error_logs(all_logs)
        >>> len(errors)
        1
    """
    return [entry for entry in log_entries if entry.get('level') in ('WARNING', 'ERROR')]

def logs_to_dataframe(log_entries: List[dict]):
    """
    Convert parsed log entries into a pandas DataFrame for analysis.
    
    Extracts key fields (timestamp, level, event_id, message) and converts
    timestamp strings to datetime objects for proper sorting.
    
    Args:
        log_entries: List of parsed log entry dictionaries
        
    Returns:
        pandas.DataFrame with log data sorted by timestamp (newest first).
        Returns empty DataFrame if log_entries is empty.
        
    Note:
        Requires pandas to be installed. Import is deferred to avoid
        dependency at module load time.
    """
    import pandas as pd
    
    rows = [
        {
            'timestamp': entry.get('timestamp'),
            'level': entry.get('level'),
            'event_id': entry.get('event_id'),
            'message': entry.get('message'),
        }
        for entry in log_entries
    ]
    
    df = pd.DataFrame(rows)
    
    if not df.empty:
        try:
            df['timestamp'] = pd.to_datetime(
                df['timestamp'], 
                format='%Y%m%d_%H%M%S_%f', 
                errors='coerce'
            )
            df = df.sort_values('timestamp', ascending=False)
        except Exception:
            # If timestamp parsing fails, return unsorted DataFrame
            pass
            
    return df

def prepare_log_summary(df_logs, warn_err_logs):
    """
    Aggregate and summarize logs grouped by level, event_id, and message.
    
    Generates a summary DataFrame with occurrence counts, latest timestamps,
    and sample log entries with full context for debugging.
    
    Args:
        df_logs: pandas DataFrame of log entries (from logs_to_dataframe)
        warn_err_logs: Original list of warning/error log entry dicts
        
    Returns:
        pandas.DataFrame with columns:
            - level: Log severity level
            - event_id: Event identifier
            - message: Log message
            - count: Number of occurrences
            - latest: Human-readable timestamp of most recent occurrence
            - sample_log: Full formatted sample with context
            
    Note:
        Requires pandas. The warn_err_logs list is used to access full context
        objects that may not be in the DataFrame.
    """
    import pandas as pd
    
    summary = df_logs.groupby(['level', 'event_id', 'message']).size().reset_index(name='count')
    samples = []
    latest_times = []
    
    for _, row in summary.iterrows():
        level = row['level']
        event_id = row['event_id']
        message = row['message']
        
        # Filter logs matching this group
        group_df = df_logs[
            (df_logs['level'] == level) & 
            (df_logs['event_id'] == event_id) & 
            (df_logs['message'] == message)
        ]
        
        # Extract latest occurrence for sample
        sample_row = group_df.iloc[0]
        matching_indices = group_df.index
        context_obj = warn_err_logs[matching_indices[0]].get('context', {})
        
        sample_str = (
            f"Timestamp: {sample_row['timestamp']}\n"
            f"Level: {sample_row['level']}\n"
            f"Event ID: {sample_row['event_id']}\n"
            f"Message: {sample_row['message']}\n"
            f"Context: {json.dumps(context_obj, indent=2)}"
        )
        samples.append(sample_str)
        
        # Format latest timestamp
        latest_ts = group_df['timestamp'].max()
        if pd.isnull(latest_ts):
            latest_times.append("")
        else:
            latest_times.append(latest_ts.strftime('%a %d %B %Y %H:%M'))
    
    summary['sample_log'] = samples
    summary['latest'] = latest_times
    
    # Enforce column ordering for consistent presentation
    desired_order = ['level', 'event_id', 'message', 'count', 'latest', 'sample_log']
    ordered_cols = [col for col in desired_order if col in summary.columns]
    extra_cols = [col for col in summary.columns if col not in ordered_cols]
    
    return summary[ordered_cols + extra_cols]


def setup_logging(
    logs_dir: Optional[str] = None,
    log_level: int = logging.INFO,
    log_name_prefix: str = "run"
) -> None:
    """
    Configure logging system with console and file output.
    
    This function is idempotent - subsequent calls after the first initialization
    have no effect. Log files are organized by environment and date, with
    microsecond-precision timestamps to prevent conflicts.
    
    Directory Structure:
        logs/
            {ENV}/
                YYYY/
                    MM/
                        DD/
                            {prefix}_YYYYMMdd_HHMMSS_ffffff.log
    
    Args:
        logs_dir: Base directory for log files. If None, uses
                 'observability/logs/{ENV}/' where ENV comes from APP_ENV.
        log_level: Minimum severity for console output. File output captures all levels.
        log_name_prefix: Prefix for log filename (e.g., "workflow", "run").
    
    Example:
        >>> os.environ['APP_ENV'] = 'test'
        >>> setup_logging(log_name_prefix="workflow", log_level=logging.DEBUG)
        # Creates: observability/test_logs/2025/11/27/workflow_20251127_143025_123456.log
        
    Note:
        Must be called before any logging operations. Subsequent calls are no-ops.
    """
    global _LOGGING_INITIALIZED
    
    if _LOGGING_INITIALIZED:
        return

    # Determine environment-specific logs directory
    if logs_dir is None:
        ENV = os.getenv('APP_ENV', 'default')
        base_dir = os.path.join(os.path.dirname(__file__), '..', 'observability')
        logs_dir = os.path.join(base_dir, "logs", ENV)

    # Create dated directory structure
    now = datetime.now()
    dated_logs_dir = os.path.join(
        logs_dir,
        now.strftime("%Y"),
        now.strftime("%m"),
        now.strftime("%d")
    )
    os.makedirs(dated_logs_dir, exist_ok=True)
    
    # Generate unique log filename with microsecond precision
    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
    log_filename = f"{log_name_prefix}_{timestamp}.log"
    log_path = os.path.join(dated_logs_dir, log_filename)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)  # Allow all levels to propagate to handlers
    
    # Clear any existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Silence noisy third-party library loggers (spotipy, urllib3, requests)
    # These libraries log entire API responses at DEBUG level, which is excessive
    for lib_name in ['urllib3', 'requests', 'spotipy', 'charset_normalizer']:
        lib_logger = logging.getLogger(lib_name)
        lib_logger.setLevel(logging.WARNING)  # Only show warnings and errors from these libs
    
    # Console handler with human-readable formatting
    class ConsoleFormatter(logging.Formatter):
        """Format logs for console output with optional context."""
        
        def format(self, record):
            msg = super().format(record)
            context = getattr(record, "context", None)
            if context:
                context_str = json.dumps(context, ensure_ascii=False) if isinstance(context, dict) else str(context)
                return f"{msg}\n{context_str}"
            return msg

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(ConsoleFormatter("[%(levelname)s] %(message)s"))
    logger.addHandler(console_handler)

    # File handler with JSON formatting for structured parsing
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.NOTSET)
    file_handler.setFormatter(JsonLogFormatter())
    logger.addHandler(file_handler)

    write_log.info("LOG_INIT", "Logging initialized.", {"log_file": log_path})
    _LOGGING_INITIALIZED = True

# Public Logging API
class write_log:
    """
    Structured logging interface with event-based categorization.
    
    All methods accept an event_id for log categorization and optional context
    for structured data. Use this class instead of direct logging calls for
    consistency across the application.
    
    Example:
        >>> write_log.info("DB_CONNECT", "Connecting to database", {"db_path": "/path/to/db"})
        >>> write_log.error("API_FAIL", "Failed to connect", {"error": str(e)})
    """
    
    @staticmethod
    def info(event_id: str, msg: str, context: Optional[dict] = None) -> None:
        """Log informational message with event ID and optional context."""
        logging.getLogger().info(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def error(event_id: str, msg: str, context: Optional[dict] = None) -> None:
        """Log error message with event ID and optional context."""
        logging.getLogger().error(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def warn(event_id: str, msg: str, context: Optional[dict] = None) -> None:
        """Log warning message with event ID and optional context."""
        logging.getLogger().warning(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def debug(event_id: str, msg: str, context: Optional[dict] = None) -> None:
        """Log debug message with event ID and optional context."""
        logging.getLogger().debug(msg, extra={"event_id": event_id, "context": context or {}})