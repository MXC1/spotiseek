"""Route tests for the dashboard's Blacklist tab (see docs/adr/0003).

This is the most destructive action in the whole dashboard -- it deletes a real file
from disk and cannot be undone -- so every test here operates on files created in a
disposable tmp workdir and DB rows created in the disposable test-env DB, never on
anything resembling real data.
"""

import os
import shutil
import tempfile

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient

from observability.dashboard.app import app
from observability.dashboard.config import track_db
from observability.dashboard.routes import blacklist as bl_module
from scripts.database_management import TrackData

_PLAYLIST_URL = "https://open.spotify.com/playlist/bl-test"


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def workdir():
    root = tempfile.mkdtemp(prefix="spotiseek_bl_test_")
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture()
def seeded_tracks(workdir):
    track_db.clear_database()

    # Track downloaded via Soulseek: has username/slskd_file_name, and is linked to a
    # playlist with a real m3u8 file so the comment-revert path is exercised for real.
    file1 = os.path.join(workdir, "song1.mp3")
    with open(file1, "w") as f:
        f.write("fake audio bytes")

    track_db.add_track(TrackData(
        track_id="bl-track-1", track_name="Song One", artist="Artist A",
        download_status="completed", slskd_file_name="folder\\song1.mp3",
    ))
    track_db.update_local_file_path("bl-track-1", file1)
    track_db.update_extension_bitrate("bl-track-1", extension="mp3", bitrate=320)
    track_db.set_download_uuid("bl-track-1", "download-uuid-1", username="someuser")

    m3u8_path = os.path.join(workdir, "playlist.m3u8")
    with open(m3u8_path, "w") as f:
        f.write(f"#EXTM3U\n{file1}\n")
    track_db.add_playlist(_PLAYLIST_URL, m3u8_path=m3u8_path, playlist_name="BL Test Playlist")
    track_db.link_track_to_playlist("bl-track-1", _PLAYLIST_URL)

    # Track imported manually: no username/slskd_file_name to add to the blacklist table.
    file2 = os.path.join(workdir, "song2.mp3")
    with open(file2, "w") as f:
        f.write("fake audio bytes 2")

    track_db.add_track(TrackData(
        track_id="bl-track-2", track_name="Song Two", artist="Artist B", download_status="completed",
    ))
    track_db.update_local_file_path("bl-track-2", file2)
    track_db.update_extension_bitrate("bl-track-2", extension="mp3", bitrate=192)

    return {"file1": file1, "file2": file2, "m3u8_path": m3u8_path}


@pytest.mark.usefixtures("seeded_tracks")
def test_tab_shows_completed_tracks(client):
    response = client.get("/blacklist")
    assert response.status_code == 200
    assert "Song One" in response.text
    assert "Song Two" in response.text
    assert "Found 2 track(s)" in response.text


@pytest.mark.usefixtures("seeded_tracks")
def test_search_filters_tracks(client):
    response = client.get("/blacklist/body", params={"search": "Song One"}).text
    assert "<td>Song One</td>" in response
    assert "<td>Song Two</td>" not in response


def test_blacklist_with_slskd_metadata_full_flow(client, seeded_tracks):
    data = {"search": "", "page": 1, "page_size": 25}
    response = client.post("/blacklist/confirm/bl-track-1", data=data)
    assert "flash-success" in response.text
    assert "Successfully blacklisted: Artist A - Song One" in response.text
    # The track no longer has a local file, so it drops off the completed-tracks list.
    assert "<td>Song One</td>" not in response.text

    assert not os.path.exists(seeded_tracks["file1"])

    cursor = track_db.conn.cursor()
    cursor.execute(
        "SELECT local_file_path, bitrate, extension, username, slskd_file_name, download_status "
        "FROM tracks WHERE track_id = ?", ("bl-track-1",),
    )
    assert cursor.fetchone() == (None, None, None, None, None, "blacklisted")

    assert track_db.is_slskd_blacklisted("someuser", "folder\\song1.mp3")

    with open(seeded_tracks["m3u8_path"], encoding="utf-8") as f:
        content = f.read()
    assert "# bl-track-1 - Artist A - Song One" in content
    assert seeded_tracks["file1"] not in content


def test_blacklist_imported_track_without_slskd_metadata(client, seeded_tracks):
    data = {"search": "", "page": 1, "page_size": 25}
    response = client.post("/blacklist/confirm/bl-track-2", data=data)
    assert "flash-success" in response.text
    assert not os.path.exists(seeded_tracks["file2"])

    cursor = track_db.conn.cursor()
    cursor.execute("SELECT download_status FROM tracks WHERE track_id = ?", ("bl-track-2",))
    assert cursor.fetchone() == ("blacklisted",)


@pytest.mark.usefixtures("seeded_tracks")
def test_blacklist_unknown_track_reports_error(client):
    data = {"search": "", "page": 1, "page_size": 25}
    response = client.post("/blacklist/confirm/not-a-real-track", data=data)
    assert "flash-error" in response.text
    assert "Track not found" in response.text


def test_blacklist_file_delete_failure_leaves_db_untouched(client, seeded_tracks, monkeypatch):
    def _raise(_path):
        raise OSError("disk error")

    monkeypatch.setattr(bl_module.os, "remove", _raise)
    data = {"search": "", "page": 1, "page_size": 25}
    response = client.post("/blacklist/confirm/bl-track-1", data=data)
    assert "flash-error" in response.text
    assert "Failed to delete file" in response.text

    cursor = track_db.conn.cursor()
    cursor.execute("SELECT local_file_path, download_status FROM tracks WHERE track_id = ?", ("bl-track-1",))
    local_file_path, status = cursor.fetchone()
    assert local_file_path == seeded_tracks["file1"]
    assert status == "completed"
    assert os.path.exists(seeded_tracks["file1"])  # never actually deleted


def test_rollback_restores_db_state_on_later_failure(client, seeded_tracks, monkeypatch):
    def _raise(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(bl_module.track_db, "update_track_status", _raise)
    data = {"search": "", "page": 1, "page_size": 25}
    response = client.post("/blacklist/confirm/bl-track-1", data=data)
    assert "flash-error" in response.text
    assert "Failed to blacklist track" in response.text

    # The file delete isn't reversible and did happen; the DB fields should be restored.
    cursor = track_db.conn.cursor()
    cursor.execute(
        "SELECT local_file_path, bitrate, extension, username, slskd_file_name, download_status "
        "FROM tracks WHERE track_id = ?", ("bl-track-1",),
    )
    assert cursor.fetchone() == (seeded_tracks["file1"], 320, "mp3", "someuser", "folder\\song1.mp3", "completed")


@pytest.mark.usefixtures("seeded_tracks")
def test_htmx_request_returns_fragment_not_full_page(client):
    response = client.get("/blacklist", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "<html" not in response.text
    assert "blacklist-tab" in response.text
