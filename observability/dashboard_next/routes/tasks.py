"""
Tasks tab routes.

1:1 port of observability/dashboard/tabs/tasks.py's behaviour (quick actions, task
overview, run history, task-scheduler log viewer) onto FastAPI + HTMX -- see
docs/adr/0004-dashboard-migration-parallel-service-cutover.md. Unlike the original's
"trigger action, sleep, rerun the whole page" dance (a Streamlit-specific workaround),
each action here just returns the freshly-rendered fragment directly.
"""

import json
import threading
from datetime import datetime

from fastapi import APIRouter, Query, Request

from observability.dashboard_next.config import ENV, LOGS_DIR
from observability.dashboard_next.templating import templates
from scripts.logs_utils import get_task_scheduler_logs, parse_logs, write_log
from scripts.task_scheduler import get_task_registry

router = APIRouter()

_STATUS_EMOJI = {
    "idle": "⚪",
    "running": "\U0001f535",
    "completed": "\U0001f7e2",
    "failed": "\U0001f534",
    "skipped": "\U0001f7e1",
}
_LEVEL_EMOJI = {
    "ERROR": "\U0001f534",
    "WARNING": "\U0001f7e1",
    "INFO": "\U0001f535",
    "DEBUG": "⚪",
}
_ALL_LEVELS = ["INFO", "WARNING", "ERROR", "DEBUG"]
_DEFAULT_LEVELS = ["INFO", "WARNING", "ERROR"]
_ALL_TASKS_VALUE = ""
_MAX_LOG_ENTRIES = 100


def _status_emoji(status: str | None) -> str:
    return _STATUS_EMOJI.get((status or "idle").lower(), "⚪")


def _format_datetime(dt_str: str | None) -> str:
    if not dt_str:
        return "Never"
    try:
        return datetime.fromisoformat(dt_str).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return dt_str


def _format_interval(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes} min"
    if minutes < 1440:
        hours = minutes / 60
        return f"{hours:.1f} hr" if hours != int(hours) else f"{int(hours)} hr"
    days = minutes / 1440
    return f"{days:.1f} days" if days != int(days) else f"{int(days)} day"


def _overview_context(flash: dict | None = None) -> dict:
    registry = get_task_registry()
    rows = []
    for state in registry.get_all_task_states():
        is_running = state.get("is_running", False)
        status_text = "Running" if is_running else (state.get("last_status") or "Never run").capitalize()
        rows.append({
            "task_name": state["task_name"],
            "display_name": state["display_name"],
            "dependencies": state.get("dependencies") or [],
            "status_emoji": "\U0001f535" if is_running else _status_emoji(state.get("last_status")),
            "status_text": status_text,
            "is_running": is_running,
            "interval_display": _format_interval(state["interval_minutes"]),
            "last_run_display": _format_datetime(state.get("last_run_at")),
            "next_run_display": _format_datetime(state.get("next_run_at")),
        })
    return {"tasks": rows, "flash": flash}


def _history_context(selected_task: str) -> dict:
    registry = get_task_registry()
    history = (
        registry.get_recent_runs(limit=50)
        if selected_task == _ALL_TASKS_VALUE
        else registry.get_task_history(selected_task, limit=50)
    )

    history_rows = [
        {
            "task_name": h["task_name"],
            "status_display": f"{_status_emoji(h.get('status'))} {(h.get('status') or 'Unknown').capitalize()}",
            "started_display": _format_datetime(h.get("started_at")),
            "completed_display": _format_datetime(h.get("completed_at")),
            "tracks_processed": h.get("tracks_processed", 0),
        }
        for h in history
    ]

    failed_runs = [
        {
            "task_name": h["task_name"],
            "started_display": _format_datetime(h.get("started_at")),
            "error_message": h.get("error_message"),
        }
        for h in history
        if h.get("status") == "failed" and h.get("error_message")
    ][:10]

    return {
        "task_names": list(registry.tasks.keys()),
        "selected_task": selected_task,
        "history_rows": history_rows,
        "failed_runs": failed_runs,
    }


def _logs_context(log_id: str | None, levels: list[str]) -> dict:
    log_files = get_task_scheduler_logs(LOGS_DIR)
    if not log_files:
        return {"log_files": [], "selected_log_id": None, "selected_levels": levels, "log_entries": []}

    selected_log_id = log_id if any(log["log_id"] == log_id for log in log_files) else log_files[0]["log_id"]
    selected_log = next(log for log in log_files if log["log_id"] == selected_log_id)

    entries = parse_logs([selected_log["log_file"]])
    filtered = [e for e in entries if e.get("level") in levels] if levels else entries
    filtered.reverse()  # most recent first

    total_count = len(filtered)
    truncated = total_count > _MAX_LOG_ENTRIES
    filtered = filtered[:_MAX_LOG_ENTRIES]

    log_entries = []
    for entry in filtered:
        timestamp = entry.get("timestamp", "")
        try:
            display_time = datetime.strptime(timestamp, "%Y%m%d_%H%M%S_%f").strftime("%H:%M:%S")
        except (ValueError, TypeError):
            display_time = timestamp[:8] if timestamp else ""

        context = entry.get("context")
        log_entries.append({
            "level": entry.get("level", "INFO"),
            "level_emoji": _LEVEL_EMOJI.get(entry.get("level"), "⚪"),
            "display_time": display_time,
            "event_id": entry.get("event_id", ""),
            "message": entry.get("message", ""),
            "context_json": json.dumps(context, indent=2, default=str) if context else None,
        })

    return {
        "log_files": log_files,
        "selected_log_id": selected_log_id,
        "selected_levels": levels,
        "log_entries": log_entries,
        "shown_count": len(log_entries),
        "total_count": total_count,
        "truncated": truncated,
    }


def _full_context() -> dict:
    return {
        **_overview_context(),
        **_history_context(_ALL_TASKS_VALUE),
        **_logs_context(None, _DEFAULT_LEVELS),
    }


@router.get("/tasks/overview")
def tasks_overview(request: Request):
    return templates.TemplateResponse(request, "tabs/_tasks_overview.html", _overview_context())


@router.post("/tasks/run-all")
def tasks_run_all(request: Request):
    registry = get_task_registry()

    def _run_in_background():
        try:
            registry.run_all_tasks()
        except Exception as e:
            write_log.error("DASHBOARD_NEXT_RUN_ALL_FAILED", "Background run-all-tasks failed.", {"error": str(e)})

    threading.Thread(target=_run_in_background, daemon=True).start()
    flash = {"type": "success", "text": "All tasks have been started in the background!"}
    return templates.TemplateResponse(request, "tabs/_tasks_overview.html", _overview_context(flash))


@router.post("/tasks/run/{task_name}")
def tasks_run_one(request: Request, task_name: str):
    registry = get_task_registry()
    success, message = registry.run_task(task_name, force=True)
    flash = {"type": "success" if success else "error", "text": message}
    return templates.TemplateResponse(request, "tabs/_tasks_overview.html", _overview_context(flash))


@router.get("/tasks/history")
def tasks_history(request: Request, task: str = _ALL_TASKS_VALUE):
    return templates.TemplateResponse(request, "tabs/_tasks_history.html", _history_context(task))


@router.get("/tasks/logs")
def tasks_logs(request: Request, log_id: str | None = None, level: list[str] = Query(default=_DEFAULT_LEVELS)):
    return templates.TemplateResponse(request, "tabs/_tasks_logs.html", _logs_context(log_id, level))


@router.get("/tasks")
def tasks_tab(request: Request):
    """The whole Tasks tab: full page on direct navigation, tab fragment on HTMX nav."""
    context = _full_context()

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/tasks_tab.html", context)

    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/tasks_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
