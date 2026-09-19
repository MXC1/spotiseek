"""Route tests for the dashboard's Tasks tab (see docs/adr/0003).

Uses a real TaskRegistry (real DB writes to task_runs/task_state) but with fake,
harmless TaskDefinitions instead of the real scripts.workflow task functions -- the
real ones hit Spotify/SoundCloud/slskd over the network, which tests must never do.
"""

import json
import os

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient

import observability.dashboard.routes.tasks as tasks_module
from observability.dashboard.app import app
from observability.dashboard.config import LOGS_DIR, track_db
from scripts.task_scheduler import TaskDefinition, TaskRegistry


@pytest.fixture()
def client():
    return TestClient(app)


class _ImmediateThread:
    """Stand-in for threading.Thread that runs its target synchronously.

    /tasks/run-all fires a real background thread in production. Letting that thread
    keep writing to the shared TrackDB connection after a test function returns races
    with the next test's fixture closing/reopening that same connection (hit this for
    real: an intermittent Windows access-violation crash). Running the "background"
    work synchronously in-process makes these tests deterministic instead.
    """

    def __init__(self, target=None, **kwargs):  # noqa: ARG002 -- swallows daemon=True
        self._target = target

    def start(self):
        self._target()


@pytest.fixture()
def fake_registry(monkeypatch):
    track_db.clear_database()
    registry = TaskRegistry(track_db)
    ran = {"good": 0, "bad": 0}

    def _good_task():
        ran["good"] += 1
        return True

    def _bad_task():
        ran["bad"] += 1
        return False

    def _raising_task():
        raise RuntimeError("boom")

    registry.register_task(TaskDefinition(
        name="fake_good", display_name="Fake Good Task", description="",
        function=_good_task, interval_env_var="FAKE_GOOD_INTERVAL", default_interval_minutes=90,
    ))
    registry.register_task(TaskDefinition(
        name="fake_bad", display_name="Fake Bad Task", description="",
        function=_bad_task, interval_env_var="FAKE_BAD_INTERVAL", default_interval_minutes=30,
        dependencies=["fake_good"],
    ))
    registry.register_task(TaskDefinition(
        name="fake_raising", display_name="Fake Raising Task", description="",
        function=_raising_task, interval_env_var="FAKE_RAISING_INTERVAL", default_interval_minutes=30,
    ))

    monkeypatch.setattr(tasks_module, "get_task_registry", lambda: registry)
    monkeypatch.setattr(tasks_module.threading, "Thread", _ImmediateThread)
    yield registry, ran


@pytest.mark.usefixtures("fake_registry")
def test_tasks_tab_lists_registered_tasks(client):
    response = client.get("/tasks")
    assert response.status_code == 200
    assert "Fake Good Task" in response.text
    assert "Fake Bad Task" in response.text
    assert "Depends on: fake_good" in response.text
    assert response.text.count(">Never<") >= 2  # never run yet


def test_run_task_success_marks_completed(client, fake_registry):
    _, ran = fake_registry
    response = client.post("/tasks/run/fake_good")
    assert response.status_code == 200
    assert ran["good"] == 1
    assert 'flash-success' in response.text
    assert "completed successfully" in response.text
    assert "Completed" in response.text


def test_run_task_returning_false_reports_success_but_marks_failed(client, fake_registry):
    """Documents run_task's existing contract (unchanged by this port): returning False
    from the task function still counts as a successful *execution*, so the flash is
    still 'success' even though the task's own last-run status becomes 'failed'."""
    _, ran = fake_registry
    response = client.post("/tasks/run/fake_bad")
    assert ran["bad"] == 1
    assert 'flash-success' in response.text
    assert "Failed" in response.text


@pytest.mark.usefixtures("fake_registry")
def test_run_task_raising_reports_error(client):
    response = client.post("/tasks/run/fake_raising")
    assert 'flash-error' in response.text
    assert "boom" in response.text


@pytest.mark.usefixtures("fake_registry")
def test_run_unknown_task_reports_error(client):
    response = client.post("/tasks/run/not-a-real-task")
    assert 'flash-error' in response.text
    assert "Unknown task" in response.text


def test_run_all_executes_all_tasks(client, fake_registry):
    """The route fires a real background thread in production; the fake_registry
    fixture makes it run synchronously here (see _ImmediateThread), so by the time this
    request returns, run_all_tasks() has already fully executed."""
    _, ran = fake_registry
    response = client.post("/tasks/run-all")
    assert response.status_code == 200
    assert "started in the background" in response.text
    assert ran["good"] == 1
    assert ran["bad"] == 1


@pytest.mark.usefixtures("fake_registry")
def test_history_filters_by_task(client):
    client.post("/tasks/run/fake_good")
    client.post("/tasks/run/fake_bad")

    all_history = client.get("/tasks/history").text
    assert "fake_good" in all_history
    assert "fake_bad" in all_history

    good_only = client.get("/tasks/history", params={"task": "fake_good"}).text
    assert "<td>fake_good</td>" in good_only
    # The filter dropdown itself always lists every task as an option; only the
    # results table should be scoped to the selected one.
    assert "<td>fake_bad</td>" not in good_only


@pytest.mark.usefixtures("fake_registry")
def test_failed_run_appears_in_failed_runs_section(client):
    client.post("/tasks/run/fake_raising")
    response = client.get("/tasks/history").text
    assert "Failed Runs" in response
    assert "boom" in response


@pytest.mark.usefixtures("fake_registry")
def test_htmx_request_returns_fragment_not_full_page(client):
    response = client.get("/tasks", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "<html" not in response.text
    assert "tasks-tab" in response.text


def _append_log_line(level: str, event_id: str, message: str) -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_path = os.path.join(LOGS_DIR, "task_scheduler.log")
    line = {
        "timestamp": "20260101_000000_000000",
        "level": level,
        "message": message,
        "event_id": event_id,
        "context": {"marker": event_id},
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(line) + "\n")


def test_logs_default_levels_exclude_debug(client):
    _append_log_line("DEBUG", "TEST_DEBUG_MARKER", "a debug-only test line")
    response = client.get("/tasks/logs")
    assert "TEST_DEBUG_MARKER" not in response.text


def test_logs_explicit_debug_level_includes_it(client):
    _append_log_line("DEBUG", "TEST_DEBUG_MARKER_2", "a debug-only test line")
    response = client.get("/tasks/logs", params={"level": ["DEBUG"]})
    assert "TEST_DEBUG_MARKER_2" in response.text


def test_logs_info_entry_shown_by_default(client):
    _append_log_line("INFO", "TEST_INFO_MARKER", "an info test line")
    response = client.get("/tasks/logs")
    assert "TEST_INFO_MARKER" in response.text
