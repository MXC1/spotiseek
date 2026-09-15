"""
Execution Inspection tab routes.

A live log stream that tails `docker logs -f` for every currently running compose
container over SSE, tagging each line with a best-effort severity so the UI can offer
per-container/per-level show-hide checkboxes (see _log_event_stream and
static/js/execution_inspection_live.js). This replaced an earlier port of
observability/dashboard/tabs/execution_inspection.py's workflow run picker + summary
metrics/timeline/errors view (docs/adr/0004-dashboard-migration-parallel-service-cutover.md);
that view (and scripts.logs_utils.get_workflow_runs/analyze_workflow_run it was built on)
still lives in the deprecated Streamlit dashboard, kept as a rollback path per
docs/adr/0005-defer-dashboard-cutover-keep-streamlit-as-rollback.md.
"""

import asyncio
import contextlib
import json
import re
from collections.abc import AsyncIterator
from datetime import datetime

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from observability.dashboard_next.config import ENV
from observability.dashboard_next.live_log_bus import TASK_START_RE, detect_level, subscribe, unsubscribe
from observability.dashboard_next.templating import templates
from scripts.docker_control import list_running_containers, own_compose_project
from scripts.task_scheduler import get_task_registry

router = APIRouter()

# ---------------------------------------------------------------------------
# Live log stream: tails `docker logs -f` for every currently running compose
# container, over one SSE connection. Container/level/task checkboxes are handled
# entirely client-side (static/js/execution_inspection_live.js) by show/hide,
# not by re-opening the stream -- so all containers/levels/tasks are always sent.
# ---------------------------------------------------------------------------

_ALL_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR"]

# Bucket for lines that aren't attributable to a scheduled task: everything from
# non-workflow containers, plus workflow's own startup/idle-loop chatter before the
# first task of a run starts. Must match the fallback the frontend JS uses for
# data.task === null.
_NO_TASK_LABEL = "Other"

# Matches `docker logs --timestamps`' RFC3339Nano prefix, e.g.
# "2026-09-15T12:34:56.789012345Z the rest of the line".
_DOCKER_TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s?(.*)$")


def _split_docker_timestamp(line: str) -> tuple[str, str]:
    match = _DOCKER_TIMESTAMP_RE.match(line)
    if not match:
        return "", line
    iso_ts, rest = match.groups()
    try:
        display_time = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        display_time = ""
    return display_time, rest


def _live_context() -> dict:
    containers = list_running_containers(own_compose_project())
    task_names = [task.display_name for task in get_task_registry().tasks.values()]
    return {
        "live_containers": sorted({c["service"] for c in containers}),
        "live_levels": _ALL_LOG_LEVELS,
        "live_tasks": [*task_names, _NO_TASK_LABEL],
    }


async def _seed_current_task(container_id: str) -> str | None:
    """Best-effort guess at which task is attributable right now, from further back than
    the live stream's own --tail window covers.

    A single chatty task (e.g. poll_search_results processing dozens of pending
    searches) can easily produce more than the live stream's 200-line backlog on its
    own, so on a fresh connection its own "Starting task: X" line may already have
    scrolled out of that window -- leaving everything misattributed to "Other" until
    the *next* task happens to start. Scanning a much deeper (but still bounded, and
    non-follow so it returns immediately) tail just for this purpose fixes that without
    bloating what's actually displayed.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "logs", "--tail", "5000", container_id,
            # Our own ConsoleFormatter writes through logging.StreamHandler(), which
            # defaults to stderr -- merge it into stdout like the live -f pump does, or
            # every "Starting task" line silently goes missing from this scan.
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _stderr = await proc.communicate()
    except OSError:
        return None

    last_match = None
    for line in stdout.decode("utf-8", errors="replace").splitlines():
        match = TASK_START_RE.search(line)
        if match:
            last_match = match.group(1).strip()
    return last_match


async def _pump_container_logs(container: dict, queue: asyncio.Queue) -> None:
    """Follow one container's logs, pushing parsed entries onto the shared queue."""
    track_tasks = container["service"] == "workflow"
    current_task = await _seed_current_task(container["id"]) if track_tasks else None

    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "logs", "-f", "--tail", "200", "--timestamps", container["id"],
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
    except OSError:
        await queue.put({
            "container": container["service"], "level": "ERROR", "time": "",
            "message": "Could not start `docker logs` for this container.", "task": None,
        })
        return

    try:
        while proc.stdout is not None:
            raw = await proc.stdout.readline()
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            if not line:
                continue
            display_time, message = _split_docker_timestamp(line)
            if track_tasks:
                match = TASK_START_RE.search(message)
                if match:
                    current_task = match.group(1).strip()
            await queue.put({
                "container": container["service"],
                "level": detect_level(message),
                "time": display_time,
                "message": message,
                "task": current_task if track_tasks else None,
            })
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        with contextlib.suppress(Exception):
            await proc.wait()


async def _pump_bus(queue: asyncio.Queue) -> None:
    """Relay manually-triggered task-run lines (routes/tasks.py's `docker exec` into
    `workflow`) into this connection's queue. `docker exec` output never reaches
    `docker logs`, so this bus is the only way those runs show up live."""
    bus_queue = subscribe()
    try:
        while True:
            await queue.put(await bus_queue.get())
    finally:
        unsubscribe(bus_queue)


async def _log_event_stream(request: Request) -> AsyncIterator[str]:
    containers = list_running_containers(own_compose_project())
    if not containers:
        payload = json.dumps({"message": "No running containers found (is the Docker socket mounted?)."})
        yield f"event: error\ndata: {payload}\n\n"
        return

    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    tasks = [asyncio.create_task(_pump_container_logs(c, queue)) for c in containers]
    tasks.append(asyncio.create_task(_pump_bus(queue)))
    yield f"event: init\ndata: {json.dumps({'containers': [c['service'] for c in containers]})}\n\n"

    try:
        while True:
            if await request.is_disconnected():
                break
            try:
                entry = await asyncio.wait_for(queue.get(), timeout=15)
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"
                continue
            yield f"event: log\ndata: {json.dumps(entry)}\n\n"
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@router.get("/execution-inspection/stream")
async def execution_inspection_stream(request: Request):
    return StreamingResponse(
        _log_event_stream(request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/execution-inspection")
def execution_inspection_tab(request: Request):
    """The whole Execution Inspection tab: full page on direct nav, tab fragment on HTMX."""
    context = _live_context()

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/execution_inspection_tab.html", context)

    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/execution_inspection_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
