"""Route tests for the dashboard's Manual Import tab (see docs/adr/0003).

do_track_import() writes a real file under the test env's slskd_docker_data/test/imported
directory and updates the real (test-env) DB -- both disposable, same as other
dashboard tests. It never touches slskd over the network here: our test tracks have
no associated search/download UUIDs, so those calls are skipped by do_track_import's own
`if search_uuid:` / `if download_uuid:` guards -- no mocking needed.
"""

import io
import os
import wave

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient

from observability.dashboard.app import app
from observability.dashboard.config import IMPORTED_DIR, track_db
from observability.dashboard.routes import manual_import as mi_module
from scripts.database_management import TrackData

_PLAYLIST_URL = "https://open.spotify.com/playlist/mi-test"


def _valid_wav_bytes() -> bytes:
    """A minimal but genuinely decodable WAV, needed wherever a staged upload is
    actually imported -- do_track_import() now rejects files that fail an ffmpeg
    decode check (see scripts/audio_validation.py). Uploads that only exercise the
    precheck step keep using plain garbage bytes, since precheck never decodes them.
    """
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(8000)
        w.writeframes(b"\x00\x00" * 100)
    buf.seek(0)
    return buf.read()


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def seeded_tracks():
    track_db.clear_database()
    mi_module._staged_uploads.clear()
    track_db.add_playlist(_PLAYLIST_URL, playlist_name="MI Test Playlist")

    for i in range(3):
        track_id = f"mi-track-{i}"
        track_db.add_track(TrackData(
            track_id=track_id, track_name=f"Track {i}", artist="Artist X", download_status="not_found",
        ))
        track_db.link_track_to_playlist(track_id, _PLAYLIST_URL)

    yield
    mi_module._staged_uploads.clear()


@pytest.mark.usefixtures("seeded_tracks")
def test_tab_shows_metrics_and_tracks(client):
    response = client.get("/manual-import")
    assert response.status_code == 200
    assert '<span class="metric-value">3</span>' in response.text
    assert "MI Test Playlist" in response.text
    assert "Track 0" in response.text
    assert "Track 1" in response.text
    assert "Track 2" in response.text


def test_no_incomplete_tracks_shows_empty_state(client):
    track_db.clear_database()
    response = client.get("/manual-import")
    assert "No tracks require manual import" in response.text


@pytest.mark.usefixtures("seeded_tracks")
def test_search_filters_tracks(client):
    response = client.get("/manual-import/body", params={"playlist_url": _PLAYLIST_URL, "search": "Track 1"})
    assert "Track 1" in response.text
    assert "Track 0" not in response.text
    assert "Track 2" not in response.text


@pytest.mark.usefixtures("seeded_tracks")
def test_pagination(client):
    # page_size must be one of the fixed dropdown options (10/25/50/100) -- anything else
    # is rejected and falls back to the default, so seed enough tracks to span 2 pages at 10.
    for i in range(3, 12):
        track_id = f"mi-track-{i}"
        track_db.add_track(TrackData(
            track_id=track_id, track_name=f"Track {i}", artist="Artist X", download_status="not_found",
        ))
        track_db.link_track_to_playlist(track_id, _PLAYLIST_URL)

    page1 = client.get("/manual-import/body", params={"playlist_url": _PLAYLIST_URL, "page_size": 10, "page": 1}).text
    page2 = client.get("/manual-import/body", params={"playlist_url": _PLAYLIST_URL, "page_size": 10, "page": 2}).text

    page1_tracks = {f"Track {i}" for i in range(12) if f"<td>Track {i}</td>" in page1}
    page2_tracks = {f"Track {i}" for i in range(12) if f"<td>Track {i}</td>" in page2}
    assert len(page1_tracks) == 10
    assert len(page2_tracks) == 2
    assert not (page1_tracks & page2_tracks)


@pytest.mark.usefixtures("seeded_tracks")
def test_out_of_range_page_falls_back_to_page_1(client):
    params = {"playlist_url": _PLAYLIST_URL, "page_size": 10, "page": 99}
    response = client.get("/manual-import/body", params=params).text
    assert "<td>Track 0</td>" in response


@pytest.mark.usefixtures("seeded_tracks")
def test_precheck_stages_file_and_warns_on_unknown_bitrate(client):
    fake_audio = io.BytesIO(b"not a real mp3 file")
    response = client.post(
        "/manual-import/precheck",
        data={"track_id": "mi-track-0", "artist": "Artist X", "track_name": "Track 0"},
        files={"audio_file": ("song.mp3", fake_audio, "audio/mpeg")},
    )
    assert response.status_code == 200
    assert "song.mp3" in response.text
    assert "Could not determine bitrate" in response.text
    assert "mi-track-0" in mi_module._staged_uploads
    assert 'hx-post="/manual-import/import/mi-track-0"' in response.text


@pytest.mark.usefixtures("seeded_tracks")
def test_import_without_precheck_reports_error(client):
    response = client.post(
        "/manual-import/import/mi-track-0",
        data={"playlist_url": _PLAYLIST_URL, "search": "", "page": 1, "page_size": 25},
    )
    assert "flash-error" in response.text
    assert "choose a file first" in response.text


@pytest.mark.usefixtures("seeded_tracks")
def test_full_import_flow_writes_file_and_updates_db(client):
    valid_audio = io.BytesIO(_valid_wav_bytes())
    client.post(
        "/manual-import/precheck",
        data={"track_id": "mi-track-0", "artist": "Artist X", "track_name": "Track 0"},
        files={"audio_file": ("song.mp3", valid_audio, "audio/mpeg")},
    )

    response = client.post(
        "/manual-import/import/mi-track-0",
        data={"playlist_url": _PLAYLIST_URL, "search": "", "page": 1, "page_size": 25},
    )
    assert "flash-success" in response.text
    assert "Successfully imported: Artist X - Track 0" in response.text
    # Track 0 no longer needs import, so it drops off the incomplete list and the count drops.
    assert '<span class="metric-value">2</span>' in response.text
    assert "<td>Track 0</td>" not in response.text

    imported_files = os.listdir(IMPORTED_DIR)
    assert any("Artist_X" in f and "Track_0" in f for f in imported_files)
    assert "mi-track-0" not in mi_module._staged_uploads


@pytest.mark.usefixtures("seeded_tracks")
def test_import_rejects_corrupt_staged_file(client):
    files_before = set(os.listdir(IMPORTED_DIR))
    fake_audio = io.BytesIO(b"not a real mp3 file")
    client.post(
        "/manual-import/precheck",
        data={"track_id": "mi-track-0", "artist": "Artist X", "track_name": "Track 0"},
        files={"audio_file": ("song.mp3", fake_audio, "audio/mpeg")},
    )

    response = client.post(
        "/manual-import/import/mi-track-0",
        data={"playlist_url": _PLAYLIST_URL, "search": "", "page": 1, "page_size": 25},
    )
    assert "flash-error" in response.text
    assert "not a valid/decodable audio file" in response.text
    # Track 0 still needs import -- rejected files must not update the DB.
    assert '<span class="metric-value">3</span>' in response.text
    assert "<td>Track 0</td>" in response.text

    assert set(os.listdir(IMPORTED_DIR)) == files_before  # nothing new was written
    assert "mi-track-0" not in mi_module._staged_uploads


@pytest.mark.usefixtures("seeded_tracks")
def test_export_xml(client):
    response = client.post("/manual-import/export-xml")
    assert "flash-success" in response.text
    assert "iTunes XML exported to" in response.text


@pytest.mark.usefixtures("seeded_tracks")
def test_htmx_request_returns_fragment_not_full_page(client):
    response = client.get("/manual-import", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "<html" not in response.text
    assert "manual-import-tab" in response.text
