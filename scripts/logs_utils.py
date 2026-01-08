"""Logging utility module for centralized logging configuration.

This module provides a structured logging system with JSON file output and console formatting.
It includes utilities for log file parsing, analysis, and DataFrame conversion for observability.

Key Features:
- Environment-aware logging with automatic directory organization
- JSON-formatted file logs for structured parsing
- Human-readable console output
- Log aggregation and analysis utilities
- Thread-safe singleton initialization
- Daily log rotation for long-running processes

Public API:
- setup_logging(): Configure logging system (idempotent)
- write_log: Static class with info/error/warn/debug methods
- get_log_files(): Find all log files in directory
- parse_logs(): Parse JSON log files into list of dicts
- filter_warning_error_logs(): Filter logs by severity
- logs_to_dataframe(): Convert logs to pandas DataFrame
- prepare_log_summary(): Group and summarize log entries
"""

import glob
import json
import logging
import logging.handlers
import os
from datetime import datetime
from typing import Any

# Module-level flag to ensure logging is initialized only once
_LOGGING_INITIALIZED = False

# JSON log format keys for structured logging
JSON_LOG_KEYS = ["timestamp", "level", "message", "event_id", "context"]

class JsonLogFormatter(logging.Formatter):
    """Custom logging formatter that outputs structured JSON log entries.

    Each log entry contains: timestamp, level, message, event_id, and context.
    Timestamps use microsecond precision for unique identification.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string.

        Args:
            record: LogRecord instance from Python logging system

        Returns:
            JSON-formatted string with filtered keys

        """
        log_record: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created).strftime("%Y%m%d_%H%M%S_%f"),
            "level": record.levelname,
            "message": record.getMessage(),
            "event_id": getattr(record, "event_id", None),
            "context": getattr(record, "context", None),
        }

        # Filter to only include defined keys
        filtered = {k: log_record[k] for k in JSON_LOG_KEYS}
        return json.dumps(filtered, ensure_ascii=False)

def get_log_files(logs_dir: str) -> list[str]:
    """Recursively find all .log files in the specified directory tree.

    Includes both direct .log files and rotated log files (e.g., task_scheduler.log.2025-12-30)
    which may be in the root logs_dir or in date-organized subdirectories.

    Args:
        logs_dir: Root directory path to search for log files

    Returns:
        List of absolute paths to .log files

    Example:
        >>> get_log_files('observability/test_logs')
        ['observability/test_logs/2025/11/27/workflow_20251127_143025_123456.log',
         'observability/test_logs/task_scheduler.log.2025-12-30', ...]

    """
    files = []
    
    # Find all .log files recursively in subdirectories
    pattern = os.path.join(logs_dir, "**", "*.log")
    files.extend(glob.glob(pattern, recursive=True))
    
    # Find rotated log files in root directory (e.g., task_scheduler.log.YYYY-MM-DD)
    # These have the format: filename.log.* but don't end in .log
    if os.path.isdir(logs_dir):
        for item in os.listdir(logs_dir):
            item_path = os.path.join(logs_dir, item)
            if os.path.isfile(item_path) and ".log" in item:
                # Include files like task_scheduler.log.2025-12-30
                files.append(item_path)
    
    # Remove duplicates
    return list(set(files))

def parse_logs(log_files: list[str]) -> list[dict]:
    """Parse JSON-formatted log files into structured entries.

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
            with open(file_path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        log_entries.append(entry)
                    except json.JSONDecodeError:
                        # Skip malformed JSON lines
                        continue
        except OSError:
            # Skip files that can't be read
            continue
    return log_entries

def filter_warning_error_logs(log_entries: list[dict]) -> list[dict]:
    """Filter log entries to only include WARNING and ERROR severity levels.

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
    return [entry for entry in log_entries if isinstance(entry, dict) and entry.get("level") in ("WARNING", "ERROR")]

def logs_to_dataframe(log_entries: list[dict]):
    """Convert parsed log entries into a pandas DataFrame for analysis.

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
    import pandas as pd  # noqa: PLC0415

    rows = [
        {
            "timestamp": entry.get("timestamp"),
            "level": entry.get("level"),
            "event_id": entry.get("event_id"),
            "message": entry.get("message"),
        }
        for entry in log_entries
    ]

    df = pd.DataFrame(rows)

    if not df.empty:
        try:
            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                format="%Y%m%d_%H%M%S_%f",
                errors="coerce",
            )
            df = df.sort_values("timestamp", ascending=False)
        except Exception:
            # If timestamp parsing fails, return unsorted DataFrame
            pass

    return df

def prepare_log_summary(df_logs, warn_err_logs):
    """Aggregate and summarize logs grouped by level, event_id, and message.

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
    import pandas as pd  # noqa: PLC0415

    # Create a mapping from DataFrame index to list position for safe lookups
    index_to_position = {idx: pos for pos, idx in enumerate(df_logs.index)}

    summary = df_logs.groupby(["level", "event_id", "message"]).size().reset_index(name="count")
    samples = []
    latest_times = []

    for _, row in summary.iterrows():
        level = row["level"]
        event_id = row["event_id"]
        message = row["message"]

        # Filter logs matching this group
        group_df = df_logs[
            (df_logs["level"] == level) &
            (df_logs["event_id"] == event_id) &
            (df_logs["message"] == message)
        ]

        # Extract latest occurrence for sample
        sample_row = group_df.iloc[0]
        matching_df_index = group_df.index[0]
        list_position = index_to_position.get(matching_df_index, 0)

        # Safely access context with bounds checking
        if list_position < len(warn_err_logs) and isinstance(warn_err_logs[list_position], dict):
            context_obj = warn_err_logs[list_position].get("context", {})
        else:
            context_obj = {}

        sample_str = (
            f"Timestamp: {sample_row['timestamp']}\n"
            f"Level: {sample_row['level']}\n"
            f"Event ID: {sample_row['event_id']}\n"
            f"Message: {sample_row['message']}\n"
            f"Context: {json.dumps(context_obj, indent=2)}"
        )
        samples.append(sample_str)

        # Format latest timestamp
        latest_ts = group_df["timestamp"].max()
        if pd.isnull(latest_ts):
            latest_times.append("")
        else:
            latest_times.append(latest_ts.strftime("%a %d %B %Y %H:%M"))

    summary["sample_log"] = samples
    summary["latest"] = latest_times

    # Enforce column ordering for consistent presentation
    desired_order = ["level", "event_id", "message", "count", "latest", "sample_log"]
    ordered_cols = [col for col in desired_order if col in summary.columns]
    extra_cols = [col for col in summary.columns if col not in ordered_cols]

    return summary[ordered_cols + extra_cols]


def get_task_scheduler_logs(logs_dir: str) -> list[dict]:
    """Extract task scheduler log files from directory.

    Handles both the current log file (task_scheduler.log) and rotated logs
    (task_scheduler.log.YYYY-MM-DD).

    Args:
        logs_dir: Root directory path containing log files

    Returns:
        List of dictionaries with log file metadata:
            - log_id: Unique identifier for the log
            - date: Date of the log (today for current, date suffix for rotated)
            - log_file: Full path to log file
            - display_name: Human-readable name for UI
            - is_current: Whether this is the current (non-rotated) log

    """
    import pandas as pd  # noqa: PLC0415

    logs = []

    # Look for task_scheduler.log and task_scheduler.log.YYYY-MM-DD files
    log_pattern = os.path.join(logs_dir, "task_scheduler.log*")
    log_files = glob.glob(log_pattern)

    for log_path in log_files:
        filename = os.path.basename(log_path)

        if filename == "task_scheduler.log":
            # Current log file
            logs.append({
                "log_id": "task_scheduler_current",
                "date": pd.Timestamp.now(),
                "log_file": log_path,
                "display_name": "Current (Today)",
                "is_current": True,
            })
        elif filename.startswith("task_scheduler.log."):
            # Rotated log file (task_scheduler.log.YYYY-MM-DD)
            date_suffix = filename.replace("task_scheduler.log.", "")
            try:
                log_date = pd.to_datetime(date_suffix, format="%Y-%m-%d")
                display_name = log_date.strftime("%a %d %B %Y")
                logs.append({
                    "log_id": f"task_scheduler_{date_suffix}",
                    "date": log_date,
                    "log_file": log_path,
                    "display_name": display_name,
                    "is_current": False,
                })
            except ValueError:
                # Skip files with unexpected naming format
                continue

    # Sort by date descending (newest first), with current log always first
    logs.sort(key=lambda x: (not x["is_current"], -x["date"].timestamp() if x["date"] else 0))

    return logs


def get_workflow_runs(logs_dir: str) -> list[dict]:
    """Extract unique workflow and task scheduler runs from log files in directory.

    Includes both workflow_* files and task_scheduler log files (current and rotated).


    Args:
        logs_dir: Root directory path containing log files

    Returns:
        List of dictionaries with run metadata:
            - run_id: Unique identifier (filename without extension)
            - timestamp: Parsed datetime of run start
            - log_file: Full path to log file
            - display_name: Human-readable name for UI

    Example:
        >>> runs = get_workflow_runs('observability/logs/test2')
        >>> runs[0]
        {'run_id': 'workflow_20251203_143025_123456',
         'timestamp': datetime(...),
         'log_file': '...',
         'display_name': 'Tue 03 December 2025 14:30'}

    """
    import pandas as pd  # noqa: PLC0415

    log_files = get_log_files(logs_dir)
    runs = []

    for log_path in log_files:
        filename = os.path.basename(log_path)
        run_id = os.path.splitext(filename)[0]

        # Handle task_scheduler.log files (current and rotated)
        if filename == "task_scheduler.log":
            # Current task scheduler log
            runs.append({
                "run_id": "task_scheduler_current",
                "timestamp": pd.Timestamp.now(),
                "log_file": log_path,
                "display_name": "Task Scheduler (Today)",
            })
            continue
        if filename.startswith("task_scheduler.log."):
            # Rotated task scheduler log (task_scheduler.log.YYYY-MM-DD)
            date_suffix = filename.replace("task_scheduler.log.", "")
            try:
                log_date = pd.to_datetime(date_suffix, format="%Y-%m-%d")
                display_name = f"Task Scheduler - {log_date.strftime('%a %d %B %Y')}"
                runs.append({
                    "run_id": f"task_scheduler_{date_suffix}",
                    "timestamp": log_date,
                    "log_file": log_path,
                    "display_name": display_name,
                })
                continue
            except ValueError:
                # Skip files with unexpected naming format
                continue

        # Only include workflow runs (skip ffmpeg, etc.)
        if not run_id.startswith("workflow_"):
            continue

        # Extract timestamp from filename (format: prefix_YYYYMMdd_HHMMSS_ffffff)
        parts = run_id.split("_")
        if len(parts) >= 3:  # noqa: PLR2004
            try:
                # Combine date and time parts
                date_str = parts[-3]  # YYYYMMdd
                time_str = parts[-2]  # HHMMSS
                timestamp_str = f"{date_str}_{time_str}"
                timestamp = pd.to_datetime(timestamp_str, format="%Y%m%d_%H%M%S")

                display_name = f"Workflow - {timestamp.strftime('%a %d %B %Y %H:%M:%S')}"

                runs.append({
                    "run_id": run_id,
                    "timestamp": timestamp,
                    "log_file": log_path,
                    "display_name": display_name,
                })
            except (ValueError, IndexError):
                # Skip files with unexpected naming format
                continue

    # Sort by timestamp descending (newest first)
    runs.sort(key=lambda x: x["timestamp"], reverse=True)

    return runs


# Event IDs that represent key workflow milestones for timeline tracking
_KEY_WORKFLOW_EVENTS = [
    "WORKFLOW_START", "WORKFLOW_COMPLETE", "WORKFLOW_ABORTED",
    "WORKFLOW_INTERRUPTED", "WORKFLOW_FATAL",
    "PLAYLISTS_LOADED", "BATCH_SEARCH_INITIATED", "ASYNC_DOWNLOAD_START",
    "REDOWNLOAD_QUEUE_INITIATED", "XML_EXPORT_SUCCESS",
    "SLSKD_UNAVAILABLE", "RESET_COMPLETE",
]

# Event IDs that are critical for dashboard analysis
# These events are always written to file regardless of log level
_DASHBOARD_CRITICAL_EVENTS = {
    # Metrics used for workflow analysis
    "TRACK_ADD", "TRACK_DELETE", "TRACK_QUALITY_UPGRADE",
    "PLAYLIST_ADD", "PLAYLIST_DELETE",
    "DOWNLOAD_FAILED", "DOWNLOAD_COMPLETE",
    "BATCH_SEARCH_START", "ASYNC_DOWNLOAD_START",
    "TASK_INITIATE_SEARCHES_COMPLETE", "SLSKD_REDOWNLOAD_SEARCHES_INITIATED",
    "PLAYLISTS_PRUNED", "PLAYLIST_TRACKS_PRUNED",
    # Timeline events
    "WORKFLOW_START", "WORKFLOW_COMPLETE", "WORKFLOW_ABORTED",
    "WORKFLOW_INTERRUPTED", "WORKFLOW_FATAL",
    "PLAYLISTS_LOADED", "BATCH_SEARCH_INITIATED",
    "REDOWNLOAD_QUEUE_INITIATED", "XML_EXPORT_SUCCESS",
    "SLSKD_UNAVAILABLE", "RESET_COMPLETE",
}


def _init_workflow_metrics(total_logs: int) -> dict:
    """Initialize the metrics dictionary for workflow analysis."""
    return {
        "total_logs": total_logs,
        "errors": [],
        "warnings": [],
        "tracks_added": 0,
        "tracks_removed": 0,
        "tracks_upgraded": 0,
        "playlists_added": 0,
        "playlists_removed": 0,
        "downloads_completed": 0,
        "downloads_completed_new": 0,
        "downloads_completed_upgrade": 0,
        "downloads_failed": 0,
        "searches_initiated": 0,
        "new_searches": 0,
        "upgrade_searches": 0,
        "event_counts": {},
        "timeline": [],
        "workflow_status": "unknown",
    }


def _update_metrics_for_event(metrics: dict, entry: dict) -> None:
    """Update metrics counters based on the log entry's event_id."""
    event_id = entry.get("event_id", "")
    context = entry.get("context", {}) or {}
    if not isinstance(context, dict):
        context = {}

    # Count events
    metrics["event_counts"][event_id] = metrics["event_counts"].get(event_id, 0) + 1

    # Track specific metrics using a mapping
    event_counter_map = {
        "TRACK_ADD": "tracks_added",
        "TRACK_QUALITY_UPGRADE": "tracks_upgraded",
        "PLAYLIST_ADD": "playlists_added",
        "DOWNLOAD_FAILED": "downloads_failed",
        "SLSKD_SEARCH_CREATE": "searches_initiated",
        "TRACK_DELETE": "tracks_removed",
        "PLAYLIST_DELETE": "playlists_removed",
    }

    if event_id in event_counter_map:
        metrics[event_counter_map[event_id]] += 1
    elif event_id == "DOWNLOAD_COMPLETE":
        metrics["downloads_completed"] += 1
        is_new = context.get("is_new", None)
        if is_new is True:
            metrics["downloads_completed_new"] += 1
        elif is_new is False:
            metrics["downloads_completed_upgrade"] += 1
    elif event_id == "BATCH_SEARCH_START":
        metrics["new_searches"] = int(context.get("total_tracks", 0) or 0)
    elif event_id == "ASYNC_DOWNLOAD_START":
        initiated = context.get("initiated", None)
        total = context.get("total", None)
        initiated_count = initiated if initiated is not None else total
        # Use the largest observed value in case multiple events fire
        metrics["new_searches"] = max(metrics["new_searches"], int(initiated_count or 0))
        metrics["searches_initiated"] += int(initiated_count or 0)
    elif event_id == "TASK_INITIATE_SEARCHES_COMPLETE":
        # Fallback in case ASYNC_DOWNLOAD_START is filtered out
        metrics["new_searches"] = max(metrics["new_searches"], int(context.get("tracks_searched", 0) or 0))
    elif event_id == "SLSKD_REDOWNLOAD_SEARCHES_INITIATED":
        count = context.get("initiated", context.get("initiated_count", 0))
        metrics["upgrade_searches"] = max(metrics["upgrade_searches"], int(count or 0))
    elif event_id == "PLAYLISTS_PRUNED":
        metrics["playlists_removed"] += int(context.get("removed_count", 0) or 0)
    elif event_id == "PLAYLIST_TRACKS_PRUNED":
        # Tracks removed here are only those actually deleted in DB (context should reflect that count)
        metrics["tracks_removed"] += int(context.get("removed", 0) or 0)


def _update_workflow_status(metrics: dict, event_id: str) -> None:
    """Update the workflow status based on completion/failure events."""
    if event_id == "WORKFLOW_COMPLETE":
        metrics["workflow_status"] = "completed"
    elif event_id in ["WORKFLOW_ABORTED", "WORKFLOW_INTERRUPTED", "WORKFLOW_FATAL"]:
        metrics["workflow_status"] = "failed"


def _add_timeline_entry(metrics: dict, entry: dict, pd_module: Any) -> None:
    """Add a timeline entry for key workflow events."""
    event_id = entry.get("event_id", "")
    if event_id not in _KEY_WORKFLOW_EVENTS:
        return

    timestamp = entry.get("timestamp", "")
    message = entry.get("message", "")

    try:
        ts = pd_module.to_datetime(timestamp, format="%Y%m%d_%H%M%S_%f")
        metrics["timeline"].append({
            "timestamp": ts,
            "event_id": event_id,
            "message": message,
            "display_time": ts.strftime("%H:%M:%S"),
        })
    except Exception:
        pass


def analyze_workflow_run(log_file: str) -> dict:
    """Analyze a single workflow run and extract key metrics.

    Args:
        log_file: Path to workflow log file

    Returns:
        Dictionary with run analysis:
            - total_logs: Total number of log entries
            - errors: List of error log entries
            - warnings: List of warning log entries
            - tracks_added: Number of tracks added
            - tracks_upgraded: Number of tracks marked for quality upgrade
            - playlists_added: Number of playlists processed
            - downloads_completed: Number of downloads completed
            - downloads_failed: Number of downloads failed
            - searches_initiated: Number of searches initiated
            - event_counts: Dict of event_id -> count
            - timeline: List of key events with timestamps

    Example:
        >>> analysis = analyze_workflow_run('logs/workflow_20251203.log')
        >>> analysis['tracks_added']
        42

    """
    import pandas as pd  # noqa: PLC0415

    log_entries = parse_logs([log_file])
    metrics = _init_workflow_metrics(len(log_entries))

    for entry in log_entries:
        level = entry.get("level", "")
        event_id = entry.get("event_id", "")

        # Collect errors and warnings
        if level == "ERROR":
            metrics["errors"].append(entry)
        elif level == "WARNING":
            metrics["warnings"].append(entry)

        # Update metrics and timeline
        _update_metrics_for_event(metrics, entry)
        _add_timeline_entry(metrics, entry, pd)
        _update_workflow_status(metrics, event_id)

    # Sort timeline by timestamp
    metrics["timeline"].sort(key=lambda x: x["timestamp"])

    # If no completion event found, mark as incomplete
    if metrics["workflow_status"] == "unknown" and metrics["total_logs"] > 0:
        metrics["workflow_status"] = "incomplete"

    return metrics


class _DashboardAwareFilter(logging.Filter):
    """Custom filter that ensures dashboard-critical logs are always written to file,
    while other logs respect the configured LOG_LEVEL.

    This filter allows:
    1. All WARNING and ERROR logs (for operational issues)
    2. All logs with dashboard-critical event_ids (for dashboard metrics)
    3. Other logs only if their level >= configured LOG_LEVEL

    This ensures the dashboard never loses visibility of workflow metrics
    even if the log level is set to WARNING or ERROR.
    """

    def __init__(self, configured_level: int):
        """Initialize filter with configured log level.

        Args:
            configured_level: Minimum level for non-critical logs (logging.DEBUG, INFO, etc.)

        """
        super().__init__()
        self.configured_level = configured_level

    def filter(self, record: logging.LogRecord) -> bool:
        """Determine if a log record should be written to file.

        Args:
            record: LogRecord to evaluate

        Returns:
            True if log should be written, False otherwise

        """
        # Always allow WARNING and ERROR logs
        if record.levelno >= logging.WARNING:
            return True

        # Always allow logs with dashboard-critical event_ids
        event_id = getattr(record, "event_id", None)
        if event_id in _DASHBOARD_CRITICAL_EVENTS:
            return True

        # For other logs, only allow if level >= configured level
        return record.levelno >= self.configured_level


def setup_logging(  # noqa: PLR0915
    logs_dir: str | None = None,
    log_level: int = logging.INFO,
    log_name_prefix: str = "run",
    rotate_daily: bool = False,
) -> None:
    """Configure logging system with console and file output.

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

    For daemon processes with rotate_daily=True:
        logs/
            {ENV}/
                {prefix}.log  (current log)
                {prefix}.log.YYYY-MM-DD  (rotated logs)

    Args:
        logs_dir: Base directory for log files. If None, uses
                 'observability/logs/{ENV}/' where ENV comes from APP_ENV.
        log_level: Minimum severity for console output. Can also be overridden by LOG_LEVEL env var.
                   File output is affected by LOG_LEVEL, but dashboard-critical events are always written.
        log_name_prefix: Prefix for log filename (e.g., "workflow", "run").
        rotate_daily: If True, use daily log rotation for long-running daemon processes.
                     Logs rotate at midnight and are named with date suffix.

    Environment Variables:
        LOG_LEVEL: One of 'DEBUG', 'INFO', 'WARNING', 'ERROR' (default: 'INFO')
                   Controls the minimum level written to file and console.
                   Dashboard-critical events bypass this for file output.
        APP_ENV: Environment name (test/stage/prod) for log directory organization.

    Example:
        >>> os.environ['APP_ENV'] = 'test'
        >>> os.environ['LOG_LEVEL'] = 'WARNING'
        >>> setup_logging(log_name_prefix="workflow")
        # File gets WARNING, ERROR, and dashboard events; console gets WARNING, ERROR

        >>> os.environ['LOG_LEVEL'] = 'DEBUG'
        >>> setup_logging(log_name_prefix="task_scheduler", rotate_daily=True)
        # File and console both get all DEBUG and above

    Note:
        Must be called before any logging operations. Subsequent calls are no-ops.
        Dashboard-critical logs are always written to file regardless of LOG_LEVEL.

    """
    global _LOGGING_INITIALIZED  # noqa: PLW0603

    if _LOGGING_INITIALIZED:
        return

    # Get configured log level from environment, defaulting to INFO
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    try:
        configured_level = getattr(logging, log_level_str, logging.INFO)
    except AttributeError:
        configured_level = logging.INFO

    # Override function parameter with environment variable if set
    if "LOG_LEVEL" in os.environ:
        log_level = configured_level

    # Determine environment-specific logs directory
    if logs_dir is None:
        ENV = os.getenv("APP_ENV", "default")
        base_dir = os.path.join(os.path.dirname(__file__), "..", "observability")
        logs_dir = os.path.join(base_dir, "logs", ENV)

    if rotate_daily:
        # For daemon processes: single log file with daily rotation
        os.makedirs(logs_dir, exist_ok=True)
        log_filename = f"{log_name_prefix}.log"
        log_path = os.path.join(logs_dir, log_filename)
    else:
        # For short-lived processes: dated directory structure with unique files
        now = datetime.now()
        dated_logs_dir = os.path.join(
            logs_dir,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d"),
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
    for lib_name in ["urllib3", "requests", "spotipy", "charset_normalizer", "watchdog"]:
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
    # Add custom emit to flush immediately after every log (critical for Docker visibility)
    original_emit = console_handler.emit
    def emit_with_flush(record):
        original_emit(record)
        console_handler.flush()
    console_handler.emit = emit_with_flush
    logger.addHandler(console_handler)

    # File handler with JSON formatting for structured parsing
    if rotate_daily:
        # Use TimedRotatingFileHandler for daemon processes - rotates at midnight
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=30,  # Keep 30 days of logs
            encoding="utf-8",
        )
        # Add date suffix to rotated files (e.g., task_scheduler.log.2025-12-10)
        file_handler.suffix = "%Y-%m-%d"
    else:
        # Standard file handler for short-lived processes
        file_handler = logging.FileHandler(log_path, encoding="utf-8")

    # File handler captures all levels but applies dashboard-aware filtering
    file_handler.setLevel(logging.NOTSET)
    file_handler.setFormatter(JsonLogFormatter())
    file_handler.addFilter(_DashboardAwareFilter(configured_level))
    # Add custom emit to flush file handler immediately after every log for durability
    original_file_emit = file_handler.emit
    def file_emit_with_flush(record):
        original_file_emit(record)
        file_handler.flush()
    file_handler.emit = file_emit_with_flush
    logger.addHandler(file_handler)

    write_log.info("LOG_INIT", "Logging initialized.", {
        "log_file": log_path,
        "rotate_daily": rotate_daily,
        "log_level": log_level_str,
        "console_level": log_level_str,
        "dashboard_events_always_logged": True,
    })
    _LOGGING_INITIALIZED = True

# Public Logging API
class write_log:
    """Structured logging interface with event-based categorization.

    All methods accept an event_id for log categorization and optional context
    for structured data. Use this class instead of direct logging calls for
    consistency across the application.

    Example:
        >>> write_log.info("DB_CONNECT", "Connecting to database", {"db_path": "/path/to/db"})
        >>> write_log.error("API_FAIL", "Failed to connect", {"error": str(e)})

    """

    @staticmethod
    def info(event_id: str, msg: str, context: dict | None = None) -> None:
        """Log informational message with event ID and optional context."""
        logging.getLogger().info(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def error(event_id: str, msg: str, context: dict | None = None) -> None:
        """Log error message with event ID and optional context."""
        logging.getLogger().error(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def warn(event_id: str, msg: str, context: dict | None = None) -> None:
        """Log warning message with event ID and optional context."""
        logging.getLogger().warning(msg, extra={"event_id": event_id, "context": context or {}})

    @staticmethod
    def debug(event_id: str, msg: str, context: dict | None = None) -> None:
        """Log debug message with event ID and optional context."""
        logging.getLogger().debug(msg, extra={"event_id": event_id, "context": context or {}})
