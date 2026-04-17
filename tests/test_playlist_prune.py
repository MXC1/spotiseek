"""Tests for deferred orphan cleanup during playlist reorganisation.

Verifies that tracks moved between playlists are not accidentally deleted
when playlists are split, merged, or shuffled (GitHub issue: Prevent
playlist re-shuffles from mass-deleting tracks).
"""

import os

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from scripts.database_management import TrackData, TrackDB
from scripts.workflow import (
    _cleanup_orphaned_tracks,
    _prune_missing_playlists,
    _prune_removed_tracks_for_playlist,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db(tmp_path, monkeypatch):
    """Return a fresh TrackDB backed by a temporary SQLite file."""
    # Override the module-level base dir so TrackDB resolves to our tmp directory.
    test_db_dir = str(tmp_path)
    monkeypatch.setattr("scripts.database_management._BASE_DB_DIR", test_db_dir)
    os.makedirs(os.path.join(test_db_dir, "test"), exist_ok=True)

    resolved_path = os.path.join(test_db_dir, "test", "database_test.db")
    # Ensure no stale singleton for this path
    TrackDB._instances.pop(os.path.abspath(resolved_path), None)

    inst = TrackDB()
    yield inst
    inst.close()
    TrackDB._instances.pop(os.path.abspath(resolved_path), None)


@pytest.fixture(autouse=True)
def _patch_track_db(db, monkeypatch):
    """Patch the module-level ``track_db`` used by workflow helpers."""
    monkeypatch.setattr("scripts.workflow.track_db", db)


@pytest.fixture()
def audio_file(tmp_path):
    """Create a dummy audio file and return its path."""
    path = str(tmp_path / "track.wav")
    with open(path, "w") as f:
        f.write("fake audio")
    return path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_track(  # noqa: PLR0913
    db, track_id, *, artist="Artist", name="Name", source="spotify",
    status="completed", local_file_path=None,
):
    db.add_track(TrackData(
        track_id=track_id,
        track_name=name,
        artist=artist,
        source=source,
        download_status=status,
    ))
    if local_file_path:
        db.update_local_file_path(track_id, local_file_path)


def _link(db, track_id, playlist_url):
    db.link_track_to_playlist(track_id, playlist_url)


def _add_playlist(db, url, name="playlist"):
    db.add_playlist(url, playlist_name=name)


# ---------------------------------------------------------------------------
# Tests - _prune_removed_tracks_for_playlist defers deletion
# ---------------------------------------------------------------------------

class TestPruneRemovedTracksDeferred:
    """Verify _prune_removed_tracks_for_playlist only unlinks, never deletes."""

    def test_unlinked_track_added_to_orphan_candidates(self, db):
        """Track removed from a playlist appears in orphan_candidates."""
        _add_playlist(db, "pl1", "Playlist 1")
        _add_track(db, "t1")
        _add_track(db, "t2")
        _link(db, "t1", "pl1")
        _link(db, "t2", "pl1")

        # Simulate playlist now only containing t1
        orphan_candidates: set[str] = set()
        _prune_removed_tracks_for_playlist(
            "pl1", "Playlist 1", "/fake.m3u8",
            [("t2", "Artist", "Track 2")],
            orphan_candidates,
        )

        assert "t1" in orphan_candidates
        # t1 should still exist in the DB (not deleted yet)
        assert db.get_track_status("t1") is not None

    def test_track_not_deleted_from_db_during_prune(self, db):
        """Track row must survive the per-playlist prune step."""
        _add_playlist(db, "pl1", "Playlist 1")
        _add_track(db, "t1")
        _add_track(db, "t2")
        _add_track(db, "t3")
        _add_track(db, "t4")
        _link(db, "t1", "pl1")
        _link(db, "t2", "pl1")
        _link(db, "t3", "pl1")
        _link(db, "t4", "pl1")

        orphan_candidates: set[str] = set()
        # Current playlist now only has t1, t2 — removing t3, t4 (50% < 75% threshold)
        _prune_removed_tracks_for_playlist(
            "pl1", "Playlist 1", "/fake.m3u8",
            [("t1", "A", "T1"), ("t2", "A", "T2")],
            orphan_candidates,
        )

        # Removed track rows still exist in DB (not deleted yet)
        assert db.get_track_status("t3") is not None
        assert db.get_track_status("t4") is not None
        assert "t3" in orphan_candidates
        assert "t4" in orphan_candidates


# ---------------------------------------------------------------------------
# Tests - _prune_missing_playlists defers deletion
# ---------------------------------------------------------------------------

class TestPruneMissingPlaylistsDeferred:
    """Verify _prune_missing_playlists collects candidates without deleting tracks."""

    def test_tracks_from_removed_playlist_become_candidates(self, db):
        _add_playlist(db, "pl_old", "Old Playlist")
        _add_track(db, "t1")
        _add_track(db, "t2")
        _link(db, "t1", "pl_old")
        _link(db, "t2", "pl_old")

        orphan_candidates: set[str] = set()
        _prune_missing_playlists(["pl_new"], orphan_candidates)

        assert "t1" in orphan_candidates
        assert "t2" in orphan_candidates
        # Tracks should still be in DB
        assert db.get_track_status("t1") is not None
        assert db.get_track_status("t2") is not None

    def test_playlist_row_deleted(self, db):
        _add_playlist(db, "pl_old", "Old Playlist")
        _add_track(db, "t1")
        _link(db, "t1", "pl_old")

        orphan_candidates: set[str] = set()
        _prune_missing_playlists(["pl_new"], orphan_candidates)

        # Playlist row should be removed
        assert "pl_old" not in db.get_all_playlist_urls()


# ---------------------------------------------------------------------------
# Tests - _cleanup_orphaned_tracks
# ---------------------------------------------------------------------------

class TestCleanupOrphanedTracks:
    """Verify the deferred cleanup correctly distinguishes true orphans."""

    def test_true_orphan_deleted(self, db, audio_file):
        """A track with zero playlist references is deleted."""
        _add_track(db, "t_orphan", local_file_path=audio_file)

        _cleanup_orphaned_tracks({"t_orphan"})

        assert db.get_track_status("t_orphan") is None
        assert not os.path.exists(audio_file)

    def test_non_orphan_survives(self, db, audio_file):
        """A track still referenced by a playlist must not be deleted."""
        _add_playlist(db, "pl_new")
        _add_track(db, "t_safe", local_file_path=audio_file)
        _link(db, "t_safe", "pl_new")

        _cleanup_orphaned_tracks({"t_safe"})

        assert db.get_track_status("t_safe") is not None
        assert os.path.exists(audio_file)

    def test_empty_candidates_noop(self, db):  # noqa: ARG002
        """Empty candidate set causes no errors."""
        _cleanup_orphaned_tracks(set())  # Should not raise


# ---------------------------------------------------------------------------
# Tests - Full scenario: playlist split
# ---------------------------------------------------------------------------

class TestPlaylistSplitScenario:
    """End-to-end scenario: splitting one playlist into two new ones.

    Starting state:
        playlist_1 → track_1, track_2

    User changes CSV to:
        playlist_2 → track_1
        playlist_3 → track_2

    Expected: tracks survive, no re-download required.
    """

    def test_tracks_survive_playlist_split(self, db, tmp_path):
        # --- Setup: playlist_1 with track_1 and track_2 (both completed) ---
        audio1 = str(tmp_path / "track1.wav")
        audio2 = str(tmp_path / "track2.wav")
        for p in (audio1, audio2):
            with open(p, "w") as f:
                f.write("audio")

        _add_playlist(db, "pl1", "Playlist 1")
        _add_track(db, "t1", local_file_path=audio1, status="completed")
        _add_track(db, "t2", local_file_path=audio2, status="completed")
        _link(db, "t1", "pl1")
        _link(db, "t2", "pl1")

        # --- Phase 1: Process new playlists (simulates process_playlist) ---
        orphan_candidates: set[str] = set()

        # playlist_2 gets track_1
        _add_playlist(db, "pl2", "Playlist 2")
        db.add_track(TrackData(track_id="t1", track_name="T1", artist="A", source="spotify"))
        _link(db, "t1", "pl2")

        # playlist_3 gets track_2
        _add_playlist(db, "pl3", "Playlist 3")
        db.add_track(TrackData(track_id="t2", track_name="T2", artist="A", source="spotify"))
        _link(db, "t2", "pl3")

        # --- Phase 2: Prune missing playlists ---
        _prune_missing_playlists(["pl2", "pl3"], orphan_candidates)

        # --- Phase 3: Cleanup orphans ---
        _cleanup_orphaned_tracks(orphan_candidates)

        # --- Assertions ---
        # Tracks survive because they are now in new playlists
        assert db.get_track_status("t1") is not None
        assert db.get_track_status("t2") is not None
        assert os.path.exists(audio1)
        assert os.path.exists(audio2)

        # New playlists exist
        all_urls = db.get_all_playlist_urls()
        assert "pl2" in all_urls
        assert "pl3" in all_urls
        # Old playlist is gone
        assert "pl1" not in all_urls


class TestTrackMovedBetweenExistingPlaylists:
    """Scenario: track moved from playlist_1 to playlist_2 (both still in CSV).

    Before: playlist_1=[t1, t2], playlist_2=[t3]
    After:  playlist_1=[t1],     playlist_2=[t3, t2]

    Expected: t2 is not deleted because it ends up in playlist_2.
    """

    def test_track_moved_between_playlists_survives(self, db, tmp_path):
        audio2 = str(tmp_path / "track2.wav")
        with open(audio2, "w") as f:
            f.write("audio")

        # Initial state
        _add_playlist(db, "pl1", "Playlist 1")
        _add_playlist(db, "pl2", "Playlist 2")
        _add_track(db, "t1")
        _add_track(db, "t2", local_file_path=audio2, status="completed")
        _add_track(db, "t3")
        _link(db, "t1", "pl1")
        _link(db, "t2", "pl1")
        _link(db, "t3", "pl2")

        orphan_candidates: set[str] = set()

        # --- Simulate processing playlist_1 (now only has t1) ---
        _prune_removed_tracks_for_playlist(
            "pl1", "Playlist 1", "/fake.m3u8",
            [("t1", "Artist", "Track 1")],
            orphan_candidates,
        )

        # --- Simulate processing playlist_2 (now has t3, t2) ---
        _link(db, "t2", "pl2")  # t2 added to playlist_2

        _prune_removed_tracks_for_playlist(
            "pl2", "Playlist 2", "/fake.m3u8",
            [("t3", "Artist", "Track 3"), ("t2", "Artist", "Track 2")],
            orphan_candidates,
        )

        # --- No missing playlists to prune ---
        _prune_missing_playlists(["pl1", "pl2"], orphan_candidates)

        # --- Cleanup orphans ---
        _cleanup_orphaned_tracks(orphan_candidates)

        # --- Assertions ---
        assert db.get_track_status("t2") is not None, "t2 should survive the move"
        assert os.path.exists(audio2), "t2 audio file should not be deleted"
        # t2 is now linked to pl2 only (unlinked from pl1)
        assert db.get_playlist_usage_count("t2") == 1
