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



# --- Log File Helper Functions ---
from typing import List
import glob

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

def get_log_files(logs_dir: str) -> List[str]:
    """
    Recursively find all .log files in the logs directory.
    Args:
        logs_dir: Path to the logs directory
    Returns:
        List of log file paths
    """
    pattern = os.path.join(logs_dir, '**', '*.log')
    return glob.glob(pattern, recursive=True)

def parse_logs(log_files: List[str]) -> List[dict]:
    """
    Parse JSON log files and return list of log entries.
    Args:
        log_files: List of log file paths
    Returns:
        List of parsed log entries
    """
    log_entries = []
    for file in log_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        log_entries.append(entry)
                    except Exception:
                        continue
        except Exception:
            continue
    return log_entries

def filter_warning_error_logs(log_entries: List[dict]) -> List[dict]:
    """
    Filter log entries to only include WARNING and ERROR levels.
    Args:
        log_entries: List of log entries
    Returns:
        Filtered list containing only WARNING and ERROR logs
    """
    return [entry for entry in log_entries if entry.get('level') in ('WARNING', 'ERROR')]

def logs_to_dataframe(log_entries: List[dict]):
    """
    Convert log entries to a pandas DataFrame.
    Args:
        log_entries: List of log entries
    Returns:
        DataFrame with log data sorted by timestamp
    """
    import pandas as pd
    rows = []
    for entry in log_entries:
        rows.append({
            'timestamp': entry.get('timestamp'),
            'level': entry.get('level'),
            'event_id': entry.get('event_id'),
            'message': entry.get('message'),
        })
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
            pass
    return df

def prepare_log_summary(df_logs, warn_err_logs):
    """
    Prepare a summary of logs grouped by level and event_id with sample logs.
    Args:
        df_logs: DataFrame of log entries
        warn_err_logs: Original list of warning/error log entries
    Returns:
        DataFrame with summary and sample logs
    """
    import pandas as pd
    summary = df_logs.groupby(['level', 'event_id', 'message']).size().reset_index(name='count')
    samples = []
    latest_times = []
    for _, row in summary.iterrows():
        level = row['level']
        event_id = row['event_id']
        message = row['message']
        group_df = df_logs[(df_logs['level'] == level) & (df_logs['event_id'] == event_id) & (df_logs['message'] == message)]
        # Get the latest occurrence (first row since df_logs is sorted by timestamp descending)
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
        # Find latest timestamp in this group and format it
        latest_ts = group_df['timestamp'].max()
        if pd.isnull(latest_ts):
            latest_times.append("")
        else:
            # Format: 'Wed 26 November 2025 19:12'
            latest_times.append(latest_ts.strftime('%a %d %B %Y %H:%M'))
    summary['sample_log'] = samples
    summary['latest'] = latest_times
    # Explicit column ordering for clarity and maintainability
    desired_order = [
        'level',
        'event_id',
        'message',
        'count',
        'latest',
        'sample_log',
    ]
    # Only include columns that exist in the DataFrame (for robustness)
    ordered_cols = [col for col in desired_order if col in summary.columns]
    # Add any extra columns at the end
    extra_cols = [col for col in summary.columns if col not in ordered_cols]
    summary = summary[ordered_cols + extra_cols]
    return summary


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
    
    Logs are automatically organized by environment (APP_ENV) into separate
    directory trees (e.g., test_logs/, prod_logs/).
    
    Args:
        logs_dir: Base directory path for log files. If None, defaults to 
                 '{ENV}_logs' in the observability directory based on APP_ENV.
        log_level: Logging level (e.g., logging.INFO, logging.DEBUG).
        log_name_prefix: Prefix for log filename. Full name will be
                        '{prefix}_{timestamp}.log'.
    
    Example:
        >>> os.environ['APP_ENV'] = 'test'
        >>> setup_logging(log_name_prefix="workflow", log_level=logging.DEBUG)
        # Creates: test_logs/2025/11/26/workflow_20251126_143025_123456.log
    """
    global _LOGGING_INITIALIZED
    
    if _LOGGING_INITIALIZED:
        return

    # Determine environment-specific logs directory
    if logs_dir is None:
        env = os.getenv('APP_ENV', 'test')
        base_dir = os.path.join(os.path.dirname(__file__), '..', 'observability')
        logs_dir = os.path.join(base_dir, f'{env}_logs')

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
    logger.setLevel(logging.NOTSET)  # Allow all levels to propagate to handlers
    
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
    console_handler.setLevel(log_level)  # Only show >=log_level on console
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