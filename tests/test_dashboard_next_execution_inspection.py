"""Route tests for the dashboard_next Execution Inspection tab (see docs/adr/0004)."""

import json
import os

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient

import observability.dashboard_next.routes.execution_inspection as ei_module
from observability.dashboard_next.app import app
from observability.dashboard_next.config import LOGS_DIR

_RUN_ID = "workflow_20990101_120000_000000"
_LOG_ENTRIES = [
    {"timestamp": "20990101_120000_000000", "level": "INFO", "event_id": "WORKFLOW_START",
     "message": "Workflow started", "context": {}},
    {"timestamp": "20990101_120001_000000", "level": "INFO", "event_id": "PLAYLIST_ADD",
     "message": "Added playlist", "context": {}},
    {"timestamp": "20990101_120002_000000", "level": "INFO", "event_id": "TRACK_ADD",
     "message": "Added track 1", "context": {}},
    {"timestamp": "20990101_120003_000000", "level": "INFO", "event_id": "TRACK_ADD",
     "message": "Added track 2", "context": {}},
    {"timestamp": "20990101_120004_000000", "level": "INFO", "event_id": "DOWNLOAD_COMPLETE",
     "message": "Download done (new)", "context": {"is_new": True}},
    {"timestamp": "20990101_120005_000000", "level": "INFO", "event_id": "DOWNLOAD_COMPLETE",
     "message": "Download done (upgrade)", "context": {"is_new": False}},
    {"timestamp": "20990101_120006_000000", "level": "ERROR", "event_id": "DOWNLOAD_FAILED",
     "message": "Download failed for track X", "context": {"track_id": "abc"}},
    {"timestamp": "20990101_120007_000000", "level": "WARNING", "event_id": "SOME_WARNING",
     "message": "Something suspicious", "context": {"detail": "x"}},
    {"timestamp": "20990101_120008_000000", "level": "INFO", "event_id": "WORKFLOW_COMPLETE",
     "message": "Workflow finished", "context": {}},
]


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def workflow_log():
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_path = os.path.join(LOGS_DIR, f"{_RUN_ID}.log")
    with open(log_path, "w", encoding="utf-8") as f:
        for entry in _LOG_ENTRIES:
            f.write(json.dumps(entry) + "\n")
    yield log_path


@pytest.mark.usefixtures("workflow_log")
def test_tab_lists_run_and_shows_default_summary(client):
    response = client.get("/execution-inspection")
    assert response.status_code == 200
    assert "Thu 01 January 2099" in response.text  # display_name derived from the filename
    assert "COMPLETED" in response.text
    assert "Total Logs" in response.text


@pytest.mark.usefixtures("workflow_log")
def test_run_summary_metrics(client):
    response = client.get("/execution-inspection/run", params={"run_id": _RUN_ID})
    text = response.text
    assert response.status_code == 200
    assert '<span class="metric-value">9</span>' in text  # total_logs
    assert '<span class="metric-value">2</span>' in text  # tracks_added
    assert '<span class="metric-value">1</span>' in text  # playlists_added / errors / warnings (all 1)


@pytest.mark.usefixtures("workflow_log")
def test_run_summary_downloads_split_new_vs_upgrade(client):
    response = client.get("/execution-inspection/run", params={"run_id": _RUN_ID}).text
    assert "Downloads Completed (New)" in response
    assert "Downloads Completed (Upgrade)" in response
    assert "Downloads Failed" in response


@pytest.mark.usefixtures("workflow_log")
def test_timeline_shows_only_key_events(client):
    response = client.get("/execution-inspection/run", params={"run_id": _RUN_ID}).text
    assert "WORKFLOW_START" in response
    assert "WORKFLOW_COMPLETE" in response
    # PLAYLIST_ADD/TRACK_ADD aren't key timeline events, only counted in the metrics.
    assert "<td>PLAYLIST_ADD</td>" not in response
    assert "<td>TRACK_ADD</td>" not in response


@pytest.mark.usefixtures("workflow_log")
def test_errors_and_warnings_sections(client):
    response = client.get("/execution-inspection/run", params={"run_id": _RUN_ID}).text
    assert "Errors (1)" in response
    assert "Warnings (1)" in response
    assert "Download failed for track X" in response
    assert "Something suspicious" in response


@pytest.mark.usefixtures("workflow_log")
def test_htmx_request_returns_fragment_not_full_page(client):
    response = client.get("/execution-inspection", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "<html" not in response.text
    assert "execution-inspection-tab" in response.text


def test_no_runs_shows_empty_state(client, monkeypatch):
    monkeypatch.setattr(ei_module, "get_workflow_runs", lambda _logs_dir: [])
    response = client.get("/execution-inspection")
    assert "No workflow runs found." in response.text
