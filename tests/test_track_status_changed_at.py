"""Tests for tracks.status_changed_at (docs/adr/0009-track-status-changed-at.md).

The column records when a track's download_status last changed VALUE. These tests pin down
the migration (add + backfill, idempotent under the startup race between workflow and
dashboard) and the bump rule at each of the three writers of download_status.
"""

import os
import sqlite3

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from scripts.database_management import TrackData, TrackDB, status_age_seconds

_OLD = "2000-01-01 00:00:00"


def _db_file(tmp_path) -> str:
    """Where TrackDB will put its file for the CURRENT APP_ENV. Read live, not assumed to be
    'test': other test modules change APP_ENV and leave it changed."""
    env = os.environ["APP_ENV"]
    return os.path.join(str(tmp_path), env, f"database_{env}.db")


def _open_db(tmp_path, monkeypatch) -> TrackDB:
    monkeypatch.setattr("scripts.database_management._BASE_DB_DIR", str(tmp_path))
    return TrackDB()


@pytest.fixture()
def db(tmp_path, monkeypatch):
    inst = _open_db(tmp_path, monkeypatch)
    yield inst
    inst.close()
    TrackDB._instances.pop(inst.db_path, None)


def _changed_at(db, track_id) -> str | None:
    return db.conn.execute("SELECT status_changed_at FROM tracks WHERE track_id = ?", (track_id,)).fetchone()[0]


def _age_it(db, track_id) -> None:
    """Rewind a track's status_changed_at so a later bump is distinguishable from 'unchanged'
    (CURRENT_TIMESTAMP has one-second resolution)."""
    db.conn.execute("UPDATE tracks SET status_changed_at = ? WHERE track_id = ?", (_OLD, track_id))
    db.conn.commit()


def _add(db, track_id="t1", status="pending") -> None:
    db.add_track(TrackData(track_id=track_id, track_name="Name", artist="Artist", download_status=status))


# --- migration -----------------------------------------------------------------------------


def test_migration_adds_column_and_backfills_existing_rows(tmp_path, monkeypatch):
    """A database from before this change has the column added and every row backfilled."""
    legacy_file = _db_file(tmp_path)
    os.makedirs(os.path.dirname(legacy_file))
    legacy = sqlite3.connect(legacy_file)
    legacy.execute(
        "CREATE TABLE tracks (track_id TEXT PRIMARY KEY, track_name TEXT NOT NULL, artist TEXT NOT NULL, "
        "source TEXT NOT NULL DEFAULT 'spotify', download_status TEXT NOT NULL, failed_reason TEXT, "
        "slskd_file_name TEXT, local_file_path TEXT, extension TEXT, bitrate INTEGER, "
        "slskd_search_uuid TEXT, slskd_download_uuid TEXT, username TEXT, "
        "added_at DATETIME DEFAULT CURRENT_TIMESTAMP)",
    )
    legacy.executemany(
        "INSERT INTO tracks (track_id, track_name, artist, download_status) VALUES (?, 'N', 'A', 'searching')",
        [("old-1",), ("old-2",)],
    )
    legacy.commit()
    legacy.close()

    db = _open_db(tmp_path, monkeypatch)
    try:
        columns = [row[1] for row in db.conn.execute("PRAGMA table_info(tracks)")]
        assert "status_changed_at" in columns
        assert status_age_seconds(_changed_at(db, "old-1")) is not None
        assert status_age_seconds(_changed_at(db, "old-2")) is not None
        # Backfilled with the migration time, not something ancient.
        assert status_age_seconds(_changed_at(db, "old-1")) < 60
    finally:
        db.close()
        TrackDB._instances.pop(db.db_path, None)


def test_migration_is_idempotent_when_the_column_already_exists(db):
    """The startup race: a second process read the column list before the first one's ALTER
    landed, so it tries to ADD a column that now exists. That must not raise."""
    _add(db)
    _age_it(db, "t1")
    stale_columns_without_the_new_one = [
        row[1] for row in db.conn.execute("PRAGMA table_info(tracks)") if row[1] != "status_changed_at"
    ]

    db._migrate_status_changed_at(db.conn.cursor(), stale_columns_without_the_new_one)

    assert _changed_at(db, "t1") == _OLD  # and an already-set value is never re-backfilled


def test_migration_reraises_alter_errors_that_are_not_a_duplicate_column(db):
    db.conn.execute("DROP TABLE tracks")

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        db._migrate_status_changed_at(db.conn.cursor(), [])  # columns "missing" -> ALTER runs


def test_migration_backfills_null_rows_on_a_later_start(db):
    """Rows written by pre-migration code (e.g. after rolling a deploy back) come back NULL."""
    _add(db)
    db.conn.execute("UPDATE tracks SET status_changed_at = NULL")
    db.conn.commit()

    db._create_tables()

    assert _changed_at(db, "t1") is not None


# --- writers -------------------------------------------------------------------------------


def test_add_track_sets_status_changed_at(db):
    _add(db)
    assert status_age_seconds(_changed_at(db, "t1")) is not None


def test_update_track_status_bumps_only_when_the_value_changes(db):
    _add(db, status="pending")
    _age_it(db, "t1")

    db.update_track_status("t1", "searching")

    assert _changed_at(db, "t1") != _OLD


def test_update_track_status_to_the_same_value_does_not_bump(db):
    """The search retry loops re-set 'searching' on a track already searching."""
    _add(db, status="searching")
    _age_it(db, "t1")

    db.update_track_status("t1", "searching")

    assert _changed_at(db, "t1") == _OLD


def test_failed_to_failed_with_a_new_reason_does_not_bump(db):
    _add(db, status="pending")
    db.update_track_status("t1", "failed", failed_reason="first")
    _age_it(db, "t1")

    db.update_track_status("t1", "failed", failed_reason="second")

    assert _changed_at(db, "t1") == _OLD
    assert db.conn.execute("SELECT failed_reason FROM tracks WHERE track_id = 't1'").fetchone()[0] == "second"


def test_update_track_status_bumps_on_change_into_failed(db):
    _add(db, status="searching")
    _age_it(db, "t1")

    db.update_track_status("t1", "failed", failed_reason="boom")

    assert _changed_at(db, "t1") != _OLD


def test_restore_track_download_metadata_bumps_only_when_status_differs(db):
    _add(db, status="completed")
    _age_it(db, "t1")

    # Same status restored (a rollback that ends where it started): unchanged.
    db.restore_track_download_metadata("t1", "/a.mp3", 320, "mp3", "user", "f.mp3", "completed")
    assert _changed_at(db, "t1") == _OLD

    # A different status restored: bumped.
    db.restore_track_download_metadata("t1", "/a.mp3", 320, "mp3", "user", "f.mp3", "blacklisted")
    assert _changed_at(db, "t1") != _OLD


def test_update_of_a_null_timestamp_row_with_unchanged_status_stays_null(db):
    """Unchanged status never invents a timestamp; the next startup backfill owns that."""
    _add(db, status="searching")
    db.conn.execute("UPDATE tracks SET status_changed_at = NULL")
    db.conn.commit()

    db.update_track_status("t1", "searching")

    assert _changed_at(db, "t1") is None


# --- helper --------------------------------------------------------------------------------


def test_status_age_seconds():
    assert status_age_seconds(None) is None
    assert status_age_seconds("") is None
    assert status_age_seconds("not a timestamp") is None
    assert status_age_seconds(_OLD) > 20 * 365 * 24 * 3600
    assert status_age_seconds("2999-01-01 00:00:00") == 0  # future clamps to 0, never negative
