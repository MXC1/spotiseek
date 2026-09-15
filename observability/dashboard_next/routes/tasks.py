"""
Tasks tab routes.

1:1 port of observability/dashboard/tabs/tasks.py's behaviour (quick actions, task
overview, run history, task-scheduler log viewer) onto FastAPI + HTMX -- see
docs/adr/0004-dashboard-migration-parallel-service-cutover.md. Unlike the original's
"trigger action, sleep, rerun the whole page" dance (a Streamlit-specific workaround),
each action here just returns the freshly-rendered fragment directly.
"""

import asyncio
import json
from datetime import datetime

from fastapi import APIRouter, Query, Request

from observability.dashboard_next.config import ENV, LOGS_DIR
from observability.dashboard_next.live_log_bus import TASK_START_RE, detect_level, publish
from observability.dashboard_next.templating import templates
from scripts.docker_control import container_ids_for_service, own_compose_project
from scripts.logs_utils import get_task_scheduler_logs, parse_logs, write_log
from scripts.task_scheduler import get_task_registry

router = APIRouter()

# asyncio.create_task() only holds a weak reference to its Task -- without keeping our
# own strong reference here, a background run-all could be garbage-collected mid-run.
_background_tasks: set[asyncio.Task] = set()

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


def _workflow_container_id() -> str | None:
    """The running `workflow` container's ID, so task runs execute there.

    Tasks must run inside `workflow` -- not here in dashboard-next -- because
    it's the container with the real input_playlists/slskd_docker_data mounts
    and its logs are what the rest of the system (Execution Inspection, etc.)
    expects task activity to show up under.
    """
    project = own_compose_project()
    if not project:
        write_log.warn(
            "DASHBOARD_NEXT_PROJECT_LOOKUP_FAILED",
            "Could not determine this container's own compose project; "
            "workflow container discovery is unscoped and may match unrelated projects.",
        )
    ids = container_ids_for_service("workflow", project)
    return ids[0] if ids else None


async def _run_in_workflow(container_id: str, cli_args: list[str]) -> tuple[int, list[str]]:
    """Run `python -m scripts.task_scheduler <cli_args>` inside the `workflow` container,
    relaying each output line to the Execution Inspection live bus as it's produced --
    `docker exec` output never reaches `docker logs`, so without this a manually
    triggered run would be invisible in that live view. Returns (exit_code, lines)."""
    proc = await asyncio.create_subprocess_exec(
        "docker", "exec", container_id, "python", "-m", "scripts.task_scheduler", *cli_args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    lines: list[str] = []
    current_task: str | None = None
    while proc.stdout is not None:
        raw = await proc.stdout.readline()
        if not raw:
            break
        line = raw.decode("utf-8", errors="replace").rstrip("\n")
        if not line:
            continue
        lines.append(line)
        match = TASK_START_RE.search(line)
        if match:
            current_task = match.group(1).strip()
        await publish({
            "container": "workflow",
            "level": detect_level(line),
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": line,
            "task": current_task,
        })
    returncode = await proc.wait()
    return returncode, lines


@router.post("/tasks/run-all")
async def tasks_run_all(request: Request):
    container_id = _workflow_container_id()
    if not container_id:
        flash = {"type": "error", "text": "Workflow container is not running."}
        return templates.TemplateResponse(request, "tabs/_tasks_overview.html", _overview_context(flash))

    async def _run_in_background():
        try:
            await _run_in_workflow(container_id, ["--run-all"])
        except OSError as e:
            write_log.error("DASHBOARD_NEXT_RUN_ALL_FAILED", "Failed to start run-all-tasks in workflow container.",
                            {"error": str(e)})

    bg_task = asyncio.create_task(_run_in_background())
    _background_tasks.add(bg_task)
    bg_task.add_done_callback(_background_tasks.discard)
    flash = {"type": "success", "text": "All tasks have been started in the background!"}
    return templates.TemplateResponse(request, "tabs/_tasks_overview.html", _overview_context(flash))


@router.post("/tasks/run/{task_name}")
async def tasks_run_one(request: Request, task_name: str):
    container_id = _workflow_container_id()
    if not container_id:
        flash = {"type": "error", "text": "Workflow container is not running."}
        return templates.TemplateResponse(request, "tabs/_tasks_overview.html", _overview_context(flash))

    returncode, lines = await _run_in_workflow(container_id, ["--run", task_name])
    message = lines[-1] if lines else "No output."
    flash = {"type": "success" if returncode == 0 else "error", "text": message}
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
