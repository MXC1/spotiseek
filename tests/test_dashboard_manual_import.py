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
_PLAYLIST_SCOPE = f"playlist:{_PLAYLIST_URL}"


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
    response = client.get("/manual-import/body", params={"scope": _PLAYLIST_SCOPE, "search": "Track 1"})
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

    page1 = client.get("/manual-import/body", params={"scope": _PLAYLIST_SCOPE, "page_size": 10, "page": 1}).text
    page2 = client.get("/manual-import/body", params={"scope": _PLAYLIST_SCOPE, "page_size": 10, "page": 2}).text

    page1_tracks = {f"Track {i}" for i in range(12) if f"<td>Track {i}</td>" in page1}
    page2_tracks = {f"Track {i}" for i in range(12) if f"<td>Track {i}</td>" in page2}
    assert len(page1_tracks) == 10
    assert len(page2_tracks) == 2
    assert not (page1_tracks & page2_tracks)


@pytest.mark.usefixtures("seeded_tracks")
def test_out_of_range_page_falls_back_to_page_1(client):
    params = {"scope": _PLAYLIST_SCOPE, "page_size": 10, "page": 99}
    response = client.get("/manual-import/body", params=params).text
    assert "<td>Track 0</td>" in response


@pytest.fixture()
def foldered_tracks():
    """Two playlists in folder "House" (sharing one track), one in "Techno", one at the root,
    plus a track in no playlist at all and one that already has its file."""
    track_db.clear_database()
    mi_module._staged_uploads.clear()

    urls = {name: f"https://open.spotify.com/playlist/folder-{name}" for name in ("a", "b", "c", "root")}
    for name, url in urls.items():
        track_db.add_playlist(url, playlist_name=f"Playlist {name.upper()}")

    layout = {
        "a-only": ["a"], "shared": ["a", "b"], "b-only": ["b"], "c-only": ["c"],
        "root-only": ["root"], "orphan": [], "has-file": ["a"],
    }
    for track_id, playlists in layout.items():
        track_db.add_track(TrackData(
            track_id=track_id, track_name=f"Song {track_id}", artist="Artist F", download_status="not_found",
        ))
        for name in playlists:
            track_db.link_track_to_playlist(track_id, urls[name])
    track_db.update_local_file_path("has-file", "/imported/has-file.mp3")

    track_db.replace_playlist_folder_memberships([
        (urls["root"], "", 1), (urls["a"], "House", 2), (urls["b"], "House", 3), (urls["c"], "Techno", 4),
    ])
    yield urls
    mi_module._staged_uploads.clear()


def _listed_tracks(html: str) -> set[str]:
    return {t for t in ("a-only", "shared", "b-only", "c-only", "root-only", "orphan", "has-file")
            if f"<td>Song {t}</td>" in html}


@pytest.mark.usefixtures("foldered_tracks")
def test_all_playlists_is_default_and_lists_every_missing_track(client):
    html = client.get("/manual-import").text
    assert "Tracks in: All Playlists" in html
    # Playlist-less tracks are included; the one with a file is not.
    assert _listed_tracks(html) == {"a-only", "shared", "b-only", "c-only", "root-only", "orphan"}
    assert '<option value="all" selected>All Playlists (6 tracks)</option>' in html


@pytest.mark.usefixtures("foldered_tracks")
def test_dropdown_lists_folders_and_playlists(client):
    html = client.get("/manual-import").text
    # Folder counts are unique tracks over the union of member playlists ("shared" counts once).
    assert 'value="folder:House"' in html
    assert "House (3 tracks)" in html
    assert "Techno (1 tracks)" in html
    # Root is not a folder, and playlists are still listed individually.
    assert 'value="folder:"' not in html
    assert "Playlist A (2 tracks)" in html  # "has-file" is in A but already imported
    assert "Playlist ROOT (1 tracks)" in html


@pytest.mark.usefixtures("foldered_tracks")
def test_folder_scope_lists_union_of_member_playlists_once(client):
    html = client.get("/manual-import/body", params={"scope": "folder:House"}).text
    assert "Tracks in: House" in html
    assert _listed_tracks(html) == {"a-only", "shared", "b-only"}
    assert html.count("<td>Song shared</td>") == 1
    assert '<option value="folder:House" selected>' in html


@pytest.mark.usefixtures("foldered_tracks")
def test_folder_scope_honours_search(client):
    html = client.get("/manual-import/body", params={"scope": "folder:House", "search": "b-only"}).text
    assert _listed_tracks(html) == {"b-only"}


@pytest.mark.usefixtures("foldered_tracks")
def test_unknown_scope_falls_back_to_all_playlists(client):
    for stale in ("folder:Gone", "playlist:https://example.com/nope", "garbage"):
        html = client.get("/manual-import/body", params={"scope": stale}).text
        assert "Tracks in: All Playlists" in html


@pytest.mark.usefixtures("foldered_tracks")
def test_folder_dropped_once_all_its_tracks_have_files(client):
    for track_id in ("c-only",):
        track_db.update_local_file_path(track_id, f"/imported/{track_id}.mp3")
    html = client.get("/manual-import").text
    assert 'value="folder:Techno"' not in html
    assert 'value="folder:House"' in html


@pytest.mark.usefixtures("foldered_tracks")
def test_import_from_folder_scope_keeps_scope_and_updates_lists(client):
    client.post(
        "/manual-import/precheck",
        data={"track_id": "shared", "artist": "Artist F", "track_name": "Song shared"},
        files={"audio_file": ("song.wav", io.BytesIO(_valid_wav_bytes()), "audio/wav")},
    )
    response = client.post(
        "/manual-import/import/shared",
        data={"scope": "folder:House", "search": "", "page": 1, "page_size": 25},
    )
    assert "flash-success" in response.text
    assert "Tracks in: House" in response.text
    assert _listed_tracks(response.text) == {"a-only", "b-only"}
    assert "House (2 tracks)" in response.text


def test_orphan_only_tracks_still_appear_under_all_playlists(client):
    track_db.clear_database()
    track_db.add_track(TrackData(
        track_id="lonely", track_name="Lonely Song", artist="Artist F", download_status="not_found",
    ))
    html = client.get("/manual-import").text
    assert "No tracks require manual import" not in html
    assert "<td>Lonely Song</td>" in html
    assert "optgroup" not in html  # no folders or playlists exist to group


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
        data={"scope": _PLAYLIST_SCOPE, "search": "", "page": 1, "page_size": 25},
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
        data={"scope": _PLAYLIST_SCOPE, "search": "", "page": 1, "page_size": 25},
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
        data={"scope": _PLAYLIST_SCOPE, "search": "", "page": 1, "page_size": 25},
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
