"""Tests for local audio-feature analysis (approachability/happiness/energy).

Covers the database layer (tests/database_management.py additions) and the
pure score-conversion helper in scripts/audio_features.py. Does NOT exercise
actual Essentia inference — essentia-tensorflow is a heavy compiled dependency
intentionally kept out of the host/test environment (see
requirements-audio-features.txt); that layer is only verified with a manual
smoke test inside the workflow container.
"""

import os

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from scripts.audio_features import _prob_to_percent
from scripts.database_management import TrackData, TrackDB

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db(tmp_path, monkeypatch):
    """Return a fresh TrackDB backed by a temporary SQLite file."""
    test_db_dir = str(tmp_path)
    monkeypatch.setattr("scripts.database_management._BASE_DB_DIR", test_db_dir)
    os.makedirs(os.path.join(test_db_dir, "test"), exist_ok=True)

    resolved_path = os.path.join(test_db_dir, "test", "database_test.db")
    TrackDB._instances.pop(os.path.abspath(resolved_path), None)

    inst = TrackDB()
    yield inst
    inst.close()
    TrackDB._instances.pop(os.path.abspath(resolved_path), None)


def _add_track(db, track_id, *, status="completed", local_file_path=None):
    db.add_track(TrackData(
        track_id=track_id,
        track_name="Name",
        artist="Artist",
        download_status=status,
    ))
    if local_file_path:
        db.update_local_file_path(track_id, local_file_path)


# ---------------------------------------------------------------------------
# Tests - get_tracks_needing_audio_analysis
# ---------------------------------------------------------------------------

class TestGetTracksNeedingAudioAnalysis:
    def test_completed_track_with_file_needs_analysis(self, db):
        _add_track(db, "t1", local_file_path="/music/t1.wav")

        pending = db.get_tracks_needing_audio_analysis()

        assert ("t1", "/music/t1.wav") in pending

    def test_track_without_local_file_path_excluded(self, db):
        _add_track(db, "t1", local_file_path=None)

        pending = db.get_tracks_needing_audio_analysis()

        assert not any(track_id == "t1" for track_id, _ in pending)

    def test_non_completed_track_excluded(self, db):
        _add_track(db, "t1", status="pending", local_file_path="/music/t1.wav")

        pending = db.get_tracks_needing_audio_analysis()

        assert not any(track_id == "t1" for track_id, _ in pending)

    def test_already_analyzed_track_excluded(self, db):
        _add_track(db, "t1", local_file_path="/music/t1.wav")
        db.update_audio_features("t1", 80, 60, 40)

        pending = db.get_tracks_needing_audio_analysis()

        assert not any(track_id == "t1" for track_id, _ in pending)

    def test_limit_caps_number_of_tracks_returned(self, db):
        for i in range(5):
            _add_track(db, f"t{i}", local_file_path=f"/music/t{i}.wav")

        pending = db.get_tracks_needing_audio_analysis(limit=2)

        assert len(pending) == 2

    def test_no_limit_returns_all_pending_tracks(self, db):
        for i in range(5):
            _add_track(db, f"t{i}", local_file_path=f"/music/t{i}.wav")

        pending = db.get_tracks_needing_audio_analysis()

        assert len(pending) == 5


# ---------------------------------------------------------------------------
# Tests - update_audio_features / get_track_audio_features
# ---------------------------------------------------------------------------

class TestAudioFeaturesReadWrite:
    def test_update_then_get_roundtrips(self, db):
        _add_track(db, "t1", local_file_path="/music/t1.wav")

        db.update_audio_features("t1", 82, 55, 12)

        assert db.get_track_audio_features("t1") == (82, 55, 12)

    def test_unanalyzed_track_returns_none(self, db):
        _add_track(db, "t1", local_file_path="/music/t1.wav")

        assert db.get_track_audio_features("t1") is None

    def test_unknown_track_returns_none(self, db):
        assert db.get_track_audio_features("does-not-exist") is None


# ---------------------------------------------------------------------------
# Tests - _prob_to_percent
# ---------------------------------------------------------------------------

class TestProbToPercent:
    @pytest.mark.parametrize(("prob", "expected"), [
        (0.0, 0),
        (1.0, 100),
        (0.5, 50),
        (0.821, 82),
        (0.834, 83),
    ])
    def test_converts_and_rounds(self, prob, expected):
        assert _prob_to_percent(prob) == expected

    def test_clamps_out_of_range_values(self):
        assert _prob_to_percent(-0.1) == 0
        assert _prob_to_percent(1.1) == 100
