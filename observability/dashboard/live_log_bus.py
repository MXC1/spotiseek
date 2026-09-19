"""Shared plumbing for the Execution Inspection tab's live log view.

Holds the log-line parsing (level/task detection) used both when tailing a
real container's `docker logs -f` (routes/execution_inspection.py) and when
relaying output from a `docker exec` we spawned ourselves (routes/tasks.py) --
`docker exec` output never reaches `docker logs`, so a manually-triggered task
run has to be pushed into the live view explicitly via `publish()` rather than
picked up by tailing.
"""

import asyncio
import contextlib
import re

# Containers writing our own JSON logs (workflow, dashboard, backup) emit
# "[LEVEL] message" to stdout (see logs_utils.ConsoleFormatter); slskd, a .NET app,
# prefixes lines with its Generic Host short codes (info:/warn:/fail:/...). Neither is
# guaranteed -- multi-line entries (stack traces, our own context-JSON continuation
# lines) won't match anything and fall back to INFO.
_LEVEL_RE = re.compile(
    r"^\[(?P<bracket>TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL)\]"
    r"|^(?P<dotnet>trce|dbug|info|warn|fail|crit):"
    r"|\b(?P<bare>TRACE|DEBUG|INFO|WARNING|WARN|ERROR|FATAL|CRITICAL)\b",
    re.IGNORECASE,
)
_DOTNET_LEVELS = {"trce": "DEBUG", "dbug": "DEBUG", "info": "INFO", "warn": "WARNING", "fail": "ERROR", "crit": "ERROR"}
_NORMALIZE_LEVEL = {"WARN": "WARNING", "FATAL": "ERROR", "CRITICAL": "ERROR"}

# task_scheduler.run_task() logs exactly "Starting task: {task.display_name}" (see
# TASK_START in scripts/task_scheduler.py) and nothing else marks a task boundary.
TASK_START_RE = re.compile(r"Starting task: (.+)$")


def detect_level(line: str) -> str:
    match = _LEVEL_RE.search(line)
    if not match:
        return "INFO"
    token = match.group("bracket") or match.group("dotnet") or match.group("bare") or ""
    if token.lower() in _DOTNET_LEVELS:
        return _DOTNET_LEVELS[token.lower()]
    return _NORMALIZE_LEVEL.get(token.upper(), token.upper())


# ---------------------------------------------------------------------------
# Pub/sub bus: one queue per open Execution Inspection SSE connection, fed by
# whatever route in this process wants to surface a line live (currently just
# manually-triggered task runs in routes/tasks.py).
# ---------------------------------------------------------------------------

_subscribers: set[asyncio.Queue] = set()


def subscribe() -> asyncio.Queue:
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    _subscribers.add(queue)
    return queue


def unsubscribe(queue: asyncio.Queue) -> None:
    _subscribers.discard(queue)


async def publish(entry: dict) -> None:
    for queue in list(_subscribers):
        with contextlib.suppress(asyncio.QueueFull):
            queue.put_nowait(entry)
