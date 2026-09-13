"""Route tests for the dashboard_next Overall Stats tab (see docs/adr/0004)."""

import os

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient  # noqa: E402

from observability.dashboard_next.app import app  # noqa: E402
from observability.dashboard_next.config import track_db  # noqa: E402
from scripts.database_management import TrackData  # noqa: E402


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def empty_db():
    """The dashboard_next config module owns one real TrackDB singleton for the whole
    test session (constructed at import time, same as the original Streamlit dashboard's
    config.py) -- so tests reset it via its own clear_database(), not a fresh tmp_path."""
    track_db.clear_database()
    yield track_db
    track_db.clear_database()


@pytest.fixture()
def seeded_db(empty_db):
    empty_db.add_playlist("https://open.spotify.com/playlist/test123", playlist_name="Test Playlist")

    empty_db.add_track(TrackData(
        track_id="stats-a", track_name="A", artist="Artist",
        download_status="completed", extension="mp3", bitrate=320,
    ))
    empty_db.update_local_file_path("stats-a", "/fake/a.mp3")

    empty_db.add_track(TrackData(
        track_id="stats-b", track_name="B", artist="Artist",
        download_status="completed", extension="flac",
    ))
    empty_db.update_local_file_path("stats-b", "/fake/b.flac")

    empty_db.add_track(TrackData(track_id="stats-c", track_name="C", artist="Artist", download_status="pending"))

    empty_db.add_track(TrackData(
        track_id="stats-d", track_name="D", artist="Artist", download_status="failed",
        failed_reason="500 Server Error: Internal Server Error for url XYZ",
    ))
    empty_db.add_track(TrackData(
        track_id="stats-e", track_name="E", artist="Artist", download_status="failed",
        failed_reason="500 Server Error: Internal Server Error for url ABC",
    ))
    return empty_db


def test_empty_database_shows_empty_states(client, empty_db):
    response = client.get("/stats")
    assert response.status_code == 200
    assert "No playlists found in the database." in response.text
    assert "No track status data found in the database." in response.text
    assert "No non-completed track statuses to display in the graph." in response.text
    assert "No extension data found." in response.text
    assert "No bitrate data found." in response.text
    assert "No download status data found." in response.text
    assert "All tracks have local files!" in response.text


def test_playlists_listed(client, seeded_db):
    response = client.get("/stats")
    assert "Test Playlist" in response.text
    assert "https://open.spotify.com/playlist/test123" in response.text


def test_track_status_breakdown_excludes_completed_from_chart_only(client, seeded_db):
    response = client.get("/stats")
    # Chart: only non-completed statuses.
    assert 'aria-label="Track download status, excluding completed"' in response.text
    assert '<span class="bar-label">pending</span>' in response.text
    assert '<span class="bar-label">failed</span>' in response.text
    assert '<span class="bar-label">completed</span>' not in response.text
    # Table: every status, plus a Total row.
    assert ">Total<" in response.text
    assert ">5<" in response.text  # 2 completed + 1 pending + 2 failed


def test_extension_breakdown(client, seeded_db):
    response = client.get("/stats")
    assert "mp3" in response.text
    assert "flac" in response.text


def test_enhanced_bitrate_breakdown_buckets_lossless_separately(client, seeded_db):
    response = client.get("/stats")
    assert ">320<" in response.text
    assert ">Lossless<" in response.text


def test_download_status_breakdown(client, seeded_db):
    """Regression test: this must collapse to exactly 2 rows (Downloaded/Not Downloaded).
    An earlier version's GROUP BY collided with the tracks table's own download_status
    column, silently grouping by the granular per-track status instead."""
    response = client.get("/stats")
    assert response.text.count("<td>Downloaded</td>") == 1
    assert response.text.count("<td>Not Downloaded</td>") == 1
    assert "<td>Downloaded</td><td>2</td>" in response.text
    assert "<td>Not Downloaded</td><td>3</td>" in response.text


def test_failed_reason_normalization_merges_500_errors(client, seeded_db):
    response = client.get("/stats")
    # Two distinct URLs' 500 errors collapse into one row with count 2, not two rows of 1.
    assert response.text.count("500 Server Error: Internal Server Error") == 1
    assert "for url XYZ" not in response.text
    assert "for url ABC" not in response.text
    assert "N/A" in response.text


def test_htmx_request_returns_fragment_not_full_page(client, seeded_db):
    response = client.get("/stats", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "<html" not in response.text
    assert "stats-tab" in response.text


def test_root_redirects_to_stats(client):
    response = client.get("/", follow_redirects=False)
    assert response.status_code in (302, 307)
    assert response.headers["location"] == "/stats"
