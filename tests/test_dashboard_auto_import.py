"""Route tests for the dashboard's Auto Import tab (see docs/adr/0003).

do_track_import() writes a real file under the test env's slskd_docker_data/test/imported
directory and updates the real (test-env) DB -- both disposable, same as other
dashboard tests. It never touches slskd over the network: our test track has no
associated search/download UUIDs, so those calls are skipped by do_track_import's own
`if search_uuid:` / `if download_uuid:` guards.
"""

import os
import shutil
import tempfile
import wave

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient

from observability.dashboard.app import app
from observability.dashboard.config import IMPORTED_DIR, track_db
from observability.dashboard.routes import auto_import as ai_module
from scripts.database_management import TrackData


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def reset_state():
    def _reset():
        ai_module._state.update({
            "browse_dir": None, "source_dir": "", "scanned_dir": None,
            "matches": None, "selected_keys": set(),
        })

    track_db.clear_database()
    _reset()
    yield
    _reset()


def _write_silent_wav(path: str) -> None:
    """Write a minimal but genuinely decodable WAV file (untagged, so artist/title
    metadata lookups still miss and matching falls back to filename parsing) --
    real audio bytes are required now that do_track_import() rejects files that
    fail an ffmpeg decode check (see scripts/audio_validation.py).
    """
    with wave.open(path, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(8000)
        w.writeframes(b"\x00\x00" * 100)


@pytest.fixture()
def scan_dir():
    root = tempfile.mkdtemp(prefix="spotiseek_ai_test_")
    sub = os.path.join(root, "Subfolder")
    os.makedirs(sub)
    # No artist/title tags are written, so the match still has to come from filename
    # parsing ("Artist - Title") -- exactly the fallback path production code takes for
    # files with unreadable/missing tags, not a stand-in for it.
    file_path = os.path.join(root, "Test Artist - Test Song.mp3")
    _write_silent_wav(file_path)
    yield root, sub, file_path
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def corrupt_file():
    root = tempfile.mkdtemp(prefix="spotiseek_ai_corrupt_")
    file_path = os.path.join(root, "Test Artist - Test Song.mp3")
    open(file_path, "wb").close()  # zero-byte file, same shape as the real-world bug
    yield root, file_path
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def seeded_track(reset_state):
    track_db.add_track(TrackData(
        track_id="ai-track-1", track_name="Test Song", artist="Test Artist", download_status="not_found",
    ))


@pytest.mark.usefixtures("reset_state")
def test_tab_shows_scan_prompt_when_no_matches(client):
    response = client.get("/auto-import")
    assert response.status_code == 200
    assert "Scan a directory above" in response.text


@pytest.mark.usefixtures("reset_state")
def test_browse_into_and_up(client, scan_dir):
    root, sub, _file = scan_dir
    ai_module._state["browse_dir"] = root

    into_resp = client.get("/auto-import/source-panel", params={"browse_action": "into", "browse_value": "Subfolder"})
    assert sub in into_resp.text
    assert ai_module._state["browse_dir"] == sub

    client.get("/auto-import/source-panel", params={"browse_action": "up"})
    assert ai_module._state["browse_dir"] == root


@pytest.mark.usefixtures("reset_state")
def test_browse_filter_narrows_subdirs(client, scan_dir):
    root, _sub, _file = scan_dir
    os.makedirs(os.path.join(root, "OtherFolder"))
    ai_module._state["browse_dir"] = root

    response = client.get("/auto-import/source-panel", params={"browse_filter": "sub"})
    assert "Subfolder" in response.text
    assert "OtherFolder" not in response.text


@pytest.mark.usefixtures("reset_state")
def test_use_this_directory_sets_source_dir(client, scan_dir):
    root, _sub, _file = scan_dir
    ai_module._state["browse_dir"] = root
    response = client.get("/auto-import/source-panel", params={"browse_action": "use"})
    assert ai_module._state["source_dir"] == root
    assert f'value="{root}"' in response.text


@pytest.mark.usefixtures("seeded_track")
def test_scan_finds_fuzzy_match(client, scan_dir):
    root, _sub, file_path = scan_dir
    response = client.post("/auto-import/scan", data={"source_dir": root})
    assert response.status_code == 200
    assert "flash-success" in response.text
    assert "Test Song" in response.text
    assert "Test Artist" in response.text
    assert os.path.basename(file_path) in response.text
    assert ai_module._state["matches"] is not None
    assert len(ai_module._state["matches"]) >= 1


@pytest.mark.usefixtures("seeded_track")
def test_scan_missing_directory_reports_error(client):
    response = client.post("/auto-import/scan", data={"source_dir": "Z:\\this\\does\\not\\exist"})
    assert "flash-error" in response.text
    assert "Directory not found" in response.text


@pytest.mark.usefixtures("seeded_track")
def test_scan_no_audio_files_reports_error(client):
    empty_dir = tempfile.mkdtemp(prefix="spotiseek_ai_empty_")
    try:
        response = client.post("/auto-import/scan", data={"source_dir": empty_dir})
        assert "flash-error" in response.text
        assert "No audio files found" in response.text
    finally:
        shutil.rmtree(empty_dir, ignore_errors=True)


@pytest.mark.usefixtures("reset_state")
def test_scan_no_incomplete_tracks_reports_success_message(client, scan_dir):
    root, _sub, _file = scan_dir
    response = client.post("/auto-import/scan", data={"source_dir": root})
    assert "flash-success" in response.text
    assert "Nothing to match" in response.text


@pytest.mark.usefixtures("seeded_track")
def test_select_and_clear_selection(client, scan_dir):
    root, _sub, file_path = scan_dir
    client.post("/auto-import/scan", data={"source_dir": root})
    key = f"ai-track-1::{file_path}"

    select_resp = client.post("/auto-import/select", data={"key": key, "checked": "true"})
    assert "1 matches selected" in select_resp.text
    assert key in ai_module._state["selected_keys"]

    matches_resp = client.get("/auto-import/matches-panel").text
    assert "checked" in matches_resp

    clear_data = {"min_score": 70, "search": "", "page": 1, "page_size": 25}
    clear_resp = client.post("/auto-import/clear-selection", data=clear_data)
    assert "0 matches selected" in clear_resp.text
    assert ai_module._state["selected_keys"] == set()


@pytest.mark.usefixtures("seeded_track")
def test_import_selected_writes_file_and_clears_state(client, scan_dir):
    root, _sub, file_path = scan_dir
    client.post("/auto-import/scan", data={"source_dir": root})
    key = f"ai-track-1::{file_path}"
    client.post("/auto-import/select", data={"key": key, "checked": "true"})

    response = client.post("/auto-import/import")
    assert "flash-success" in response.text
    assert "Successfully imported 1 tracks" in response.text
    assert "Scan a directory above" in response.text  # matches cleared, forces a re-scan
    assert ai_module._state["matches"] is None
    assert ai_module._state["selected_keys"] == set()

    imported_files = os.listdir(IMPORTED_DIR)
    assert any("Test_Artist" in f and "Test_Song" in f for f in imported_files)

    cursor = track_db.conn.cursor()
    cursor.execute("SELECT local_file_path, download_status FROM tracks WHERE track_id = ?", ("ai-track-1",))
    local_file_path, status = cursor.fetchone()
    assert local_file_path
    assert status == "completed"


@pytest.mark.usefixtures("seeded_track")
def test_import_rejects_corrupt_file(client, corrupt_file):
    root, file_path = corrupt_file
    files_before = set(os.listdir(IMPORTED_DIR))

    client.post("/auto-import/scan", data={"source_dir": root})
    key = f"ai-track-1::{file_path}"
    client.post("/auto-import/select", data={"key": key, "checked": "true"})

    response = client.post("/auto-import/import")
    assert "flash-error" in response.text
    assert "Failed to import 1 tracks" in response.text

    assert set(os.listdir(IMPORTED_DIR)) == files_before  # nothing new was written

    cursor = track_db.conn.cursor()
    cursor.execute("SELECT local_file_path, download_status FROM tracks WHERE track_id = ?", ("ai-track-1",))
    local_file_path, status = cursor.fetchone()
    assert not local_file_path
    assert status != "completed"


@pytest.mark.usefixtures("reset_state")
def test_htmx_request_returns_fragment_not_full_page(client):
    response = client.get("/auto-import", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "<html" not in response.text
    assert "auto-import-tab" in response.text
