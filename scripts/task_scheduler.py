"""
Task-based scheduler for Spotiseek (Radarr-style).

This module implements a task-based scheduling system where each workflow step
runs as an independent task with configurable intervals. Tasks can be triggered
automatically on schedule or manually via the dashboard.

Task System Features:
- Interval-based scheduling (configurable via environment variables)
- Task dependency enforcement (some tasks require others to run first)
- Task execution history tracking in database
- Manual trigger support via API
- Real-time status updates

Available Tasks:
1. scrape_playlists     - Fetch track metadata from Spotify playlists
2. initiate_searches    - Queue new tracks for Soulseek search
3. poll_search_results  - Process completed searches from slskd
4. sync_download_status - Update download status from slskd API
5. mark_quality_upgrades - Identify non-WAV tracks for upgrade
6. process_upgrades     - Initiate quality upgrade searches
7. export_library       - Generate iTunes-compatible XML

Usage:
    # Start the scheduler (runs continuously)
    python -m scripts.task_scheduler

    # Run a specific task manually
    python -m scripts.task_scheduler --run <task_name>

    # Run all tasks in order
    python -m scripts.task_scheduler --run-all
"""

import argparse
import os
import signal
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from dotenv import load_dotenv

# Disable .pyc file generation
sys.dont_write_bytecode = True

# Load environment configuration
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

from scripts.database_management import TrackDB  # noqa: E402
from scripts.logs_utils import setup_logging, write_log  # noqa: E402

# Initialize logging with daily rotation for long-running daemon
setup_logging(log_name_prefix="task_scheduler", rotate_daily=True)

# Validate environment
ENV = os.getenv("APP_ENV")
if not ENV:
    raise OSError(
        "APP_ENV environment variable is not set. Task scheduler is disabled."
    )


class TaskStatus(Enum):
    """Task execution status."""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class TaskDefinition:
    """Definition of a scheduled task."""
    name: str
    display_name: str
    description: str
    function: Callable[[], bool]
    interval_env_var: str
    default_interval_minutes: int
    dependencies: list[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class TaskRun:
    """Record of a task execution."""
    task_name: str
    started_at: datetime
    completed_at: datetime | None = None
    status: TaskStatus = TaskStatus.RUNNING
    error_message: str | None = None
    tracks_processed: int = 0


class TaskRegistry:
    """
    Registry of all available tasks with their configurations.

    This class manages task definitions, scheduling, and execution tracking.
    """

    def __init__(self, db: TrackDB):
        self.db = db
        self.tasks: dict[str, TaskDefinition] = {}
        self.current_runs: dict[str, TaskRun] = {}
        self._lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._scheduler_thread: threading.Thread | None = None

        # Ensure task tables exist
        self._ensure_task_tables()

    def _ensure_task_tables(self) -> None:
        """Create task-related database tables if they don't exist."""
        cursor = self.db.conn.cursor()

        # Task runs history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT NOT NULL,
                started_at DATETIME NOT NULL,
                completed_at DATETIME,
                status TEXT NOT NULL,
                error_message TEXT,
                tracks_processed INTEGER DEFAULT 0
            )
        """)

        # Task state table (for tracking last run, next scheduled run)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS task_state (
                task_name TEXT PRIMARY KEY,
                last_run_at DATETIME,
                last_status TEXT,
                next_run_at DATETIME,
                is_enabled INTEGER DEFAULT 1
            )
        """)

        # Create index for efficient history queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_runs_task_name
            ON task_runs(task_name)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_runs_started_at
            ON task_runs(started_at DESC)
        """)

        self.db.conn.commit()

    def register_task(self, task: TaskDefinition) -> None:
        """Register a task definition."""
        self.tasks[task.name] = task

        # Initialize task state if not exists
        cursor = self.db.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO task_state (task_name, is_enabled)
            VALUES (?, ?)
        """, (task.name, 1 if task.enabled else 0))
        self.db.conn.commit()

    def get_task_interval(self, task_name: str) -> int:
        """
        Get the interval in minutes for a task from environment variable.

        Args:
            task_name: Name of the task

        Returns:
            Interval in minutes
        """
        task = self.tasks.get(task_name)
        if not task:
            return 60  # Default 1 hour

        env_value = os.getenv(task.interval_env_var)
        if env_value:
            try:
                return int(env_value)
            except ValueError:
                write_log.warn("TASK_INTERVAL_INVALID",
                              f"Invalid interval value for {task.interval_env_var}",
                              {"value": env_value})

        return task.default_interval_minutes

    def get_task_state(self, task_name: str) -> dict[str, Any]:
        """Get current state of a task."""
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT task_name, last_run_at, last_status, next_run_at, is_enabled
            FROM task_state WHERE task_name = ?
        """, (task_name,))
        row = cursor.fetchone()

        if row:
            return {
                "task_name": row[0],
                "last_run_at": row[1],
                "last_status": row[2],
                "next_run_at": row[3],
                "is_enabled": bool(row[4]),
                "interval_minutes": self.get_task_interval(task_name),
                "is_running": task_name in self.current_runs
            }

        return {
            "task_name": task_name,
            "last_run_at": None,
            "last_status": None,
            "next_run_at": None,
            "is_enabled": True,
            "interval_minutes": self.get_task_interval(task_name),
            "is_running": False
        }

    def get_all_task_states(self) -> list[dict[str, Any]]:
        """Get states for all registered tasks."""
        states = []
        for task_name, task in self.tasks.items():
            state = self.get_task_state(task_name)
            state["display_name"] = task.display_name
            state["description"] = task.description
            state["dependencies"] = task.dependencies
            states.append(state)
        return states

    def get_task_history(self, task_name: str, limit: int = 50) -> list[dict[str, Any]]:
        """Get execution history for a task."""
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT id, task_name, started_at, completed_at, status,
                   error_message, tracks_processed
            FROM task_runs
            WHERE task_name = ?
            ORDER BY started_at DESC
            LIMIT ?
        """, (task_name, limit))

        history = [
            {
                "id": row[0],
                "task_name": row[1],
                "started_at": row[2],
                "completed_at": row[3],
                "status": row[4],
                "error_message": row[5],
                "tracks_processed": row[6]
            }
            for row in cursor.fetchall()
        ]
        return history

    def get_recent_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get most recent task runs across all tasks."""
        cursor = self.db.conn.cursor()
        cursor.execute("""
            SELECT id, task_name, started_at, completed_at, status,
                   error_message, tracks_processed
            FROM task_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,))

        runs = [
            {
                "id": row[0],
                "task_name": row[1],
                "started_at": row[2],
                "completed_at": row[3],
                "status": row[4],
                "error_message": row[5],
                "tracks_processed": row[6]
            }
            for row in cursor.fetchall()
        ]
        return runs

    def _record_run_start(self, task_name: str) -> int:
        """Record the start of a task run."""
        cursor = self.db.conn.cursor()
        now = datetime.now().isoformat()

        cursor.execute("""
            INSERT INTO task_runs (task_name, started_at, status)
            VALUES (?, ?, ?)
        """, (task_name, now, TaskStatus.RUNNING.value))

        run_id = cursor.lastrowid
        self.db.conn.commit()
        return run_id

    def _record_run_complete(self, run_id: int, status: TaskStatus,
                             error_message: str | None = None, tracks_processed: int = 0) -> None:
        """Record the completion of a task run."""
        cursor = self.db.conn.cursor()
        now = datetime.now().isoformat()

        cursor.execute("""
            UPDATE task_runs
            SET completed_at = ?, status = ?, error_message = ?, tracks_processed = ?
            WHERE id = ?
        """, (now, status.value, error_message, tracks_processed, run_id))

        self.db.conn.commit()

    def _update_task_state(self, task_name: str, status: TaskStatus) -> None:
        """Update the task state after execution."""
        cursor = self.db.conn.cursor()
        now = datetime.now()
        interval = self.get_task_interval(task_name)
        next_run = (now + timedelta(minutes=interval)).isoformat()

        cursor.execute("""
            UPDATE task_state
            SET last_run_at = ?, last_status = ?, next_run_at = ?
            WHERE task_name = ?
        """, (now.isoformat(), status.value, next_run, task_name))

        self.db.conn.commit()

    def check_dependencies(self, task_name: str) -> tuple[bool, list[str]]:
        """
        Check if all dependencies for a task have run recently.

        Returns:
            Tuple of (dependencies_met, list_of_unmet_dependencies)
        """
        task = self.tasks.get(task_name)
        if not task or not task.dependencies:
            return True, []

        unmet = []
        for dep_name in task.dependencies:
            state = self.get_task_state(dep_name)

            # Dependency not run yet
            if not state["last_run_at"]:
                unmet.append(dep_name)
                continue

            # Check if dependency ran successfully in its interval window
            if state["last_status"] != TaskStatus.COMPLETED.value:
                # Last run wasn't successful - still allow if it ran recently
                pass

        return len(unmet) == 0, unmet

    def run_task(self, task_name: str, force: bool = False) -> tuple[bool, str]:
        """
        Execute a task.

        Args:
            task_name: Name of the task to run
            force: If True, run even if dependencies aren't met

        Returns:
            Tuple of (success, message)
        """
        task = self.tasks.get(task_name)
        if not task:
            return False, f"Unknown task: {task_name}"

        # Check if already running
        with self._lock:
            if task_name in self.current_runs:
                return False, f"Task {task_name} is already running"

        # Check dependencies
        if not force:
            deps_met, unmet = self.check_dependencies(task_name)
            if not deps_met:
                return False, f"Dependencies not met: {', '.join(unmet)}"

        # Record run start
        run_id = self._record_run_start(task_name)

        with self._lock:
            self.current_runs[task_name] = TaskRun(
                task_name=task_name,
                started_at=datetime.now()
            )

        write_log.info("TASK_START", f"Starting task: {task.display_name}",
                      {"task_name": task_name})

        try:
            # Execute the task function
            result = task.function()

            # Determine status based on return value
            status = TaskStatus.COMPLETED if result else TaskStatus.FAILED
            error_message = None if result else "Task returned False"

            self._record_run_complete(run_id, status, error_message)
            self._update_task_state(task_name, status)

            if not result:
                write_log.warn("TASK_FAILED", f"Task returned False: {task.display_name}",
                              {"task_name": task_name})

            return True, f"Task {task_name} completed successfully"

        except Exception as e:
            error_msg = str(e)
            self._record_run_complete(run_id, TaskStatus.FAILED, error_msg)
            self._update_task_state(task_name, TaskStatus.FAILED)

            write_log.error("TASK_FAILED", f"Task failed: {task.display_name}",
                           {"task_name": task_name, "error": error_msg})

            return False, f"Task {task_name} failed: {error_msg}"

        finally:
            with self._lock:
                self.current_runs.pop(task_name, None)

    def run_all_tasks(self) -> dict[str, tuple[bool, str]]:
        """
        Run all tasks in dependency order.

        Returns:
            Dictionary of task_name -> (success, message)
        """
        results = {}

        # Build dependency order (topological sort)
        ordered_tasks = self._get_dependency_order()

        for task_name in ordered_tasks:
            task = self.tasks.get(task_name)
            if task and task.enabled:
                success, message = self.run_task(task_name, force=True)
                results[task_name] = (success, message)

        return results

    def _get_dependency_order(self) -> list[str]:
        """Get tasks in topological order based on dependencies."""
        # Simple implementation: tasks with fewer dependencies first
        task_order = []
        remaining = set(self.tasks.keys())
        satisfied = set()

        while remaining:
            # Find tasks whose dependencies are all satisfied
            ready = []
            for task_name in remaining:
                task = self.tasks[task_name]
                if all(dep in satisfied for dep in task.dependencies):
                    ready.append(task_name)

            if not ready:
                # Circular dependency or all remaining have unmet deps
                # Just add remaining tasks in any order
                ready = list(remaining)

            for task_name in ready:
                task_order.append(task_name)
                satisfied.add(task_name)
                remaining.discard(task_name)

        return task_order

    def should_run_task(self, task_name: str) -> bool:
        """Check if a task is due to run based on its schedule."""
        state = self.get_task_state(task_name)

        if not state["is_enabled"]:
            return False

        if state["is_running"]:
            return False

        if not state["next_run_at"]:
            return True  # Never run before

        try:
            next_run = datetime.fromisoformat(state["next_run_at"])
            return datetime.now() >= next_run
        except (ValueError, TypeError):
            return True

    def start_scheduler(self) -> None:
        """Start the background scheduler thread."""
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            write_log.warn("SCHEDULER_ALREADY_RUNNING", "Scheduler is already running")
            return

        self._shutdown_event.clear()
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        write_log.info("SCHEDULER_STARTED", "Task scheduler started")

    def stop_scheduler(self) -> None:
        """Stop the background scheduler."""
        self._shutdown_event.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=10)

    def _scheduler_loop(self) -> None:
        """Main scheduler loop - runs in background thread."""
        # Initialize next_run_at for tasks that haven't run yet
        for task_name in self.tasks:
            state = self.get_task_state(task_name)
            if not state["next_run_at"]:
                interval = self.get_task_interval(task_name)
                next_run = datetime.now() + timedelta(minutes=interval)
                cursor = self.db.conn.cursor()
                cursor.execute("""
                    UPDATE task_state SET next_run_at = ? WHERE task_name = ?
                """, (next_run.isoformat(), task_name))
                self.db.conn.commit()

        while not self._shutdown_event.is_set():
            try:
                # Check each task
                for task_name in self._get_dependency_order():
                    if self._shutdown_event.is_set():
                        break

                    if self.should_run_task(task_name):
                        deps_met, unmet = self.check_dependencies(task_name)
                        if deps_met:
                            self.run_task(task_name)

                # Sleep for a bit before checking again
                self._shutdown_event.wait(timeout=30)

            except Exception as e:
                write_log.error("SCHEDULER_ERROR", "Error in scheduler loop",
                              {"error": str(e)})
                self._shutdown_event.wait(timeout=60)


# Global registry instance (initialized when tasks are registered)
_registry: TaskRegistry | None = None


def get_task_registry() -> TaskRegistry:
    """Get or create the global task registry."""
    global _registry  # noqa: PLW0603
    if _registry is None:
        db = TrackDB()
        _registry = TaskRegistry(db)
        _register_all_tasks(_registry)
    return _registry


def _register_all_tasks(registry: TaskRegistry) -> None:
    """Register all workflow tasks."""
    # Import here to avoid circular imports
    from scripts.workflow import (  # noqa: PLC0415
        task_export_library,
        task_initiate_searches,
        task_mark_quality_upgrades,
        task_poll_search_results,
        task_process_upgrades,
        task_remux_existing_files,
        task_scrape_playlists,
        task_sync_download_status,
    )

    # Task 1: Scrape Spotify Playlists
    registry.register_task(TaskDefinition(
        name="scrape_playlists",
        display_name="Scrape Spotify Playlists",
        description="Fetch track metadata from Spotify playlists defined in CSV",
        function=task_scrape_playlists,
        interval_env_var="TASK_SCRAPE_PLAYLISTS_INTERVAL",
        default_interval_minutes=1440,  # Once per day
        dependencies=[]
    ))

    # Task 2: Initiate Searches
    registry.register_task(TaskDefinition(
        name="initiate_searches",
        display_name="Initiate Soulseek Searches",
        description="Queue new tracks for Soulseek search on slskd",
        function=task_initiate_searches,
        interval_env_var="TASK_INITIATE_SEARCHES_INTERVAL",
        default_interval_minutes=60,  # Every hour
        dependencies=["scrape_playlists"]
    ))

    # Task 3: Poll Search Results
    registry.register_task(TaskDefinition(
        name="poll_search_results",
        display_name="Poll Search Results",
        description="Process completed searches from slskd and initiate downloads",
        function=task_poll_search_results,
        interval_env_var="TASK_POLL_SEARCH_RESULTS_INTERVAL",
        default_interval_minutes=15,  # Every 15 minutes
        dependencies=[]
    ))

    # Task 4: Sync Download Status
    registry.register_task(TaskDefinition(
        name="sync_download_status",
        display_name="Sync Download Status",
        description="Update download status from slskd API to database",
        function=task_sync_download_status,
        interval_env_var="TASK_SYNC_DOWNLOAD_STATUS_INTERVAL",
        default_interval_minutes=5,  # Every 5 minutes
        dependencies=[]
    ))

    # Task 5: Mark Quality Upgrades
    registry.register_task(TaskDefinition(
        name="mark_quality_upgrades",
        display_name="Check for Quality Upgrades",
        description="Identify completed non-WAV tracks for quality upgrade",
        function=task_mark_quality_upgrades,
        interval_env_var="TASK_MARK_QUALITY_UPGRADES_INTERVAL",
        default_interval_minutes=1440,  # Once per day
        dependencies=["sync_download_status"]
    ))

    # Task 6: Process Upgrades
    registry.register_task(TaskDefinition(
        name="process_upgrades",
        display_name="Process Quality Upgrades",
        description="Initiate quality upgrade searches for marked tracks",
        function=task_process_upgrades,
        interval_env_var="TASK_PROCESS_UPGRADES_INTERVAL",
        default_interval_minutes=60,  # Every hour
        dependencies=["mark_quality_upgrades"]
    ))

    # Task 7: Export Library
    registry.register_task(TaskDefinition(
        name="export_library",
        display_name="Export iTunes Library",
        description="Generate iTunes-compatible XML library file",
        function=task_export_library,
        interval_env_var="TASK_EXPORT_LIBRARY_INTERVAL",
        default_interval_minutes=1440,  # Once per day
        dependencies=["sync_download_status"]
    ))

    # Task 8: Remux Existing Files
    registry.register_task(TaskDefinition(
        name="remux_existing_files",
        display_name="Remux Existing Files",
        description="Remux completed files to match current format preferences (lossless->WAV, lossy->MP3)",
        function=task_remux_existing_files,
        interval_env_var="TASK_REMUX_EXISTING_FILES_INTERVAL",
        default_interval_minutes=360,  # Every 6 hours
        dependencies=["sync_download_status"]
    ))

    write_log.info("TASKS_REGISTERED", "All tasks registered",
                  {"count": len(registry.tasks)})


def main():
    """Main entry point for task scheduler."""
    parser = argparse.ArgumentParser(
        description="Spotiseek Task Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--run",
        type=str,
        help="Run a specific task by name"
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Run all tasks in dependency order"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available tasks"
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run as daemon (scheduler runs continuously)"
    )

    args = parser.parse_args()

    registry = get_task_registry()

    if args.list:
        print("\nAvailable Tasks:")
        print("-" * 60)
        for state in registry.get_all_task_states():
            deps = f" (depends on: {', '.join(state['dependencies'])})" if state['dependencies'] else ""
            print(f"  {state['task_name']}: {state['display_name']}")
            print(f"    {state['description']}")
            print(f"    Interval: {state['interval_minutes']} minutes{deps}")
            print()
        return

    if args.run:
        success, message = registry.run_task(args.run, force=True)
        print(message)
        sys.exit(0 if success else 1)

    if args.run_all:
        results = registry.run_all_tasks()
        for task_name, (success, message) in results.items():
            status = "✓" if success else "✗"
            print(f"  {status} {task_name}: {message}")

        all_success = all(success for success, _ in results.values())
        sys.exit(0 if all_success else 1)

    if args.daemon:
        # Run as daemon with scheduler
        def signal_handler():
            print("\nShutdown signal received...")
            registry.stop_scheduler()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        print(f"Starting Spotiseek Task Scheduler (ENV: {ENV})")
        print("Press Ctrl+C to stop\n")

        registry.start_scheduler()

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            registry.stop_scheduler()

    else:
        # Default: show help
        parser.print_help()


if __name__ == "__main__":
    main()
