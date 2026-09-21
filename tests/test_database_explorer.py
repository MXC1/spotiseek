"""Tests for the TrackDB read methods behind the dashboard's Database tab
(docs/adr/0008-dashboard-database-explorer.md).

Covers table discovery, schema/foreign keys, browse (paging, sorting, filtering, identifier
validation), the track detail read, every DB audit check, and the read-only guardrails:
no explorer read may leave a transaction open or change the shared connection's state.
"""

import os

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from scripts.audit_checks import AUDIT_CHECKS, AUDIT_CHECKS_BY_ID
from scripts.constants import STUCK_THRESHOLD_HOURS
from scripts.database_management import EXPLORER_MAX_PAGE_SIZE, TrackData, TrackDB

_URL_A = "https://open.spotify.com/playlist/AAA"
_URL_B = "https://open.spotify.com/playlist/BBB"


@pytest.fixture()
def db(tmp_path, monkeypatch):
    """A fresh TrackDB on a temp file (never a live environment's database)."""
    monkeypatch.setattr("scripts.database_management._BASE_DB_DIR", str(tmp_path))
    inst = TrackDB()
    yield inst
    inst.close()
    TrackDB._instances.pop(inst.db_path, None)


def _track(db, track_id, *, status="completed", name="Name", artist="Artist", path=None, **columns):  # noqa: PLR0913
    db.add_track(TrackData(track_id=track_id, track_name=name, artist=artist, download_status=status))
    if path is not None:
        db.update_local_file_path(track_id, path)
    for column, value in columns.items():
        db.conn.execute(f"UPDATE tracks SET {column} = ? WHERE track_id = ?", (value, track_id))
    db.conn.commit()


def _set_changed_at(db, track_id, hours_ago):
    db.conn.execute(
        "UPDATE tracks SET status_changed_at = datetime('now', ?) WHERE track_id = ?",
        (f"-{hours_ago} hours", track_id),
    )
    db.conn.commit()


@pytest.fixture()
def clean_db(db):
    """A small, fully consistent library: two playlists, three tracks, folder memberships."""
    db.add_playlist(_URL_A, m3u8_path="/m3u8/a.m3u8", playlist_name="Playlist A")
    db.add_playlist(_URL_B, m3u8_path="/m3u8/b.m3u8", playlist_name="Playlist B")
    db.set_playlist_display_order(_URL_A, 0)
    db.set_playlist_display_order(_URL_B, 1)
    _track(db, "t1", path="/imp/1.mp3", name="Alpha", artist="Ann")
    _track(db, "t2", path="/imp/2.mp3", name="Bravo", artist="Bob")
    _track(db, "t3", status="pending", name="Charlie", artist="Cy")
    for track_id, url in (("t1", _URL_A), ("t2", _URL_A), ("t2", _URL_B), ("t3", _URL_B)):
        db.link_track_to_playlist(track_id, url)
    db.replace_playlist_folder_memberships([(_URL_A, "", 0), (_URL_B, "House", 1)])
    return db


def _flagged(db, check_id) -> set[str]:
    """First-column values of the rows a check flags (track_id or playlist_url)."""
    result = db.get_audit_check_rows(check_id, 0, EXPLORER_MAX_PAGE_SIZE)
    return {row[0] for row in result["rows"]}


# --- table discovery / schema ----------------------------------------------------------------


def test_list_tables_orders_core_tables_first_then_others_alphabetically(clean_db):
    clean_db.conn.execute("CREATE TABLE zeta_extra (id INTEGER PRIMARY KEY)")
    clean_db.conn.execute("CREATE TABLE alpha_extra (id INTEGER PRIMARY KEY)")

    names = [name for name, _ in clean_db.list_tables()]

    assert names[:5] == ["tracks", "playlists", "playlist_tracks", "playlist_folder_memberships", "slskd_blacklist"]
    assert names[-2:] == ["alpha_extra", "zeta_extra"]
    assert not any(name.startswith("sqlite_") for name in names)


def test_list_tables_reports_row_counts(clean_db):
    counts = dict(clean_db.list_tables())
    assert counts["tracks"] == 3
    assert counts["playlists"] == 2
    assert counts["playlist_tracks"] == 4
    assert counts["slskd_blacklist"] == 0


def test_get_table_schema_reports_columns_foreign_keys_and_indexes(clean_db):
    schema = clean_db.get_table_schema("playlist_tracks")

    assert [c["name"] for c in schema["columns"]] == ["playlist_url", "track_id"]
    assert {(fk["column"], fk["ref_table"], fk["ref_column"]) for fk in schema["foreign_keys"]} == {
        ("track_id", "tracks", "track_id"),
        ("playlist_url", "playlists", "playlist_url"),
    }
    assert any(idx["name"] == "idx_playlist_tracks_track_id" for idx in schema["indexes"])
    assert clean_db.get_table_schema("tracks")["foreign_keys"] == []


@pytest.mark.parametrize("bad_name", ["nope", "tracks; DROP TABLE tracks", 'tracks"', "sqlite_master", ""])
def test_unknown_or_hostile_table_names_are_rejected_not_interpolated(clean_db, bad_name):
    assert clean_db.get_table_schema(bad_name) is None
    assert clean_db.browse_table(bad_name) is None
    assert dict(clean_db.list_tables())["tracks"] == 3  # nothing was dropped


# --- browse ----------------------------------------------------------------------------------


def test_browse_returns_columns_rows_and_total(clean_db):
    page = clean_db.browse_table("tracks")

    assert "track_id" in page["columns"]
    assert page["total"] == 3
    assert len(page["rows"]) == 3
    assert all(len(row) == len(page["columns"]) for row in page["rows"])


def test_browse_pages_stably_and_clamps_limit(db):
    for i in range(130):
        _track(db, f"id-{i:03d}")

    first = db.browse_table("tracks", offset=0, limit=10_000)
    assert len(first["rows"]) == EXPLORER_MAX_PAGE_SIZE == first["limit"]
    assert first["total"] == 130

    ids = [r[first["columns"].index("track_id")] for r in first["rows"]]
    second = db.browse_table("tracks", offset=100, limit=EXPLORER_MAX_PAGE_SIZE)
    ids += [r[second["columns"].index("track_id")] for r in second["rows"]]
    assert ids == sorted(ids)
    assert len(set(ids)) == 130  # no row repeated or skipped across pages


def test_browse_sorts_ascending_and_descending_by_a_real_column(clean_db):
    asc = clean_db.browse_table("tracks", sort="track_name")
    desc = clean_db.browse_table("tracks", sort="track_name", descending=True)

    col = asc["columns"].index("track_name")
    assert [r[col] for r in asc["rows"]] == ["Alpha", "Bravo", "Charlie"]
    assert [r[col] for r in desc["rows"]] == ["Charlie", "Bravo", "Alpha"]


def test_browse_ignores_a_sort_column_that_does_not_exist(clean_db):
    page = clean_db.browse_table("tracks", sort="x; DROP TABLE tracks --")
    assert page["total"] == 3
    assert dict(clean_db.list_tables())["tracks"] == 3


def test_browse_filters_by_case_insensitive_substring_and_combines_columns(clean_db):
    by_name = clean_db.browse_table("tracks", filters={"track_name": "ALPH"})
    assert by_name["total"] == 1

    both = clean_db.browse_table("tracks", filters={"download_status": "complete", "artist": "b"})
    col = both["columns"].index("track_id")
    assert [r[col] for r in both["rows"]] == ["t2"]  # completed AND artist contains 'b' (Bob)


def test_browse_filter_treats_like_wildcards_literally(clean_db):
    _track(clean_db, "pct", name="100% Pure", artist="Zed")

    assert clean_db.browse_table("tracks", filters={"track_name": "100%"})["total"] == 1
    assert clean_db.browse_table("tracks", filters={"track_name": "%"})["total"] == 1  # not "match everything"
    assert clean_db.browse_table("tracks", filters={"track_name": "_"})["total"] == 0


def test_browse_ignores_filters_on_unknown_columns_and_blank_values(clean_db):
    page = clean_db.browse_table("tracks", filters={"nope": "x", "artist": ""})
    assert page["total"] == 3


def test_browse_filter_matches_non_text_columns_via_cast(clean_db):
    clean_db.update_extension_bitrate("t1", extension="mp3", bitrate=320)

    page = clean_db.browse_table("tracks", filters={"bitrate": "32"})

    assert page["total"] == 1


def test_browse_table_with_a_composite_primary_key(clean_db):
    page = clean_db.browse_table("playlist_tracks", sort="track_id", descending=True)
    assert page["total"] == 4
    assert page["rows"][0][page["columns"].index("track_id")] == "t3"


# --- track detail ----------------------------------------------------------------------------


def test_track_detail_returns_row_playlists_and_folders(clean_db):
    detail = clean_db.get_track_detail("t2")

    assert detail["track"]["track_name"] == "Bravo"
    assert detail["track"]["local_file_path"] == "/imp/2.mp3"
    by_url = {p["playlist_url"]: p for p in detail["playlists"]}
    assert set(by_url) == {_URL_A, _URL_B}
    assert by_url[_URL_A]["folders"] == [""]  # root
    assert by_url[_URL_B]["folders"] == ["House"]
    assert by_url[_URL_B]["playlist_name"] == "Playlist B"
    assert by_url[_URL_B]["m3u8_path"] == "/m3u8/b.m3u8"
    assert all(p["in_playlists_table"] for p in detail["playlists"])
    assert detail["status_age_seconds"] is not None


def test_track_detail_flags_a_link_to_a_missing_playlist(clean_db):
    clean_db.conn.execute("INSERT INTO playlist_tracks VALUES ('https://gone', 't1')")
    clean_db.conn.commit()

    playlists = {p["playlist_url"]: p for p in clean_db.get_track_detail("t1")["playlists"]}

    assert playlists["https://gone"]["in_playlists_table"] is False
    assert playlists["https://gone"]["playlist_name"] is None


def test_track_detail_matches_blacklist_entry_using_normalised_file_name(clean_db):
    _track(clean_db, "bl", slskd_file_name="Music/Some Folder/song.mp3", username="peer")
    clean_db.add_slskd_blacklist("peer", "Music/Some Folder/song.mp3", reason="manual_blacklist")
    clean_db.add_slskd_blacklist("someone-else", "Music/Some Folder/song.mp3")

    entries = clean_db.get_track_detail("bl")["blacklist_entries"]

    assert [(e["username"], e["reason"]) for e in entries] == [("peer", "manual_blacklist")]
    assert entries[0]["slskd_file_name"] == "Some Folder\\song.mp3"


def test_track_detail_has_no_blacklist_entries_without_soulseek_metadata(clean_db):
    assert clean_db.get_track_detail("t1")["blacklist_entries"] == []


def test_track_detail_for_a_missing_track_is_none(clean_db):
    assert clean_db.get_track_detail("no-such-track") is None


def test_track_detail_status_age_reflects_status_changed_at(clean_db):
    _set_changed_at(clean_db, "t3", hours_ago=5)

    age = clean_db.get_track_detail("t3")["status_age_seconds"]

    assert 5 * 3600 - 5 <= age <= 5 * 3600 + 60


def test_track_ids_containing_slashes_round_trip(clean_db):
    """SoundCloud track_ids are URL slugs like 'artist/track'."""
    _track(clean_db, "lobsta-b/7th-element-vip", name="VIP")
    assert clean_db.get_track_detail("lobsta-b/7th-element-vip")["track"]["track_name"] == "VIP"


# --- audit checks ----------------------------------------------------------------------------


def test_every_check_has_a_unique_id_and_runs_clean_on_a_consistent_library(clean_db):
    assert len({c.id for c in AUDIT_CHECKS}) == len(AUDIT_CHECKS)
    for check in AUDIT_CHECKS:
        assert clean_db.count_audit_check(check.id) == 0, check.id
        result = clean_db.get_audit_check_rows(check.id)
        assert result["rows"] == [], check.id
        assert result["columns"], check.id  # columns are reported even when nothing is flagged


def test_unknown_audit_check_ids_return_none(clean_db):
    assert clean_db.count_audit_check("nope") is None
    assert clean_db.get_audit_check_rows("nope") is None


def test_playlist_tracks_missing_track(clean_db):
    clean_db.conn.execute("INSERT INTO playlist_tracks VALUES (?, 'ghost')", (_URL_A,))
    clean_db.conn.commit()

    assert clean_db.count_audit_check("playlist_tracks_missing_track") == 1
    assert _flagged(clean_db, "playlist_tracks_missing_track") == {_URL_A}


def test_playlist_tracks_missing_playlist(clean_db):
    clean_db.conn.execute("INSERT INTO playlist_tracks VALUES ('https://gone', 't1')")
    clean_db.conn.commit()

    assert _flagged(clean_db, "playlist_tracks_missing_playlist") == {"https://gone"}


def test_tracks_in_no_playlist(clean_db):
    _track(clean_db, "lonely")

    assert _flagged(clean_db, "tracks_in_no_playlist") == {"lonely"}


def test_folder_memberships_for_an_unknown_playlist(clean_db):
    clean_db.replace_playlist_folder_memberships([(_URL_A, "", 0), (_URL_B, "House", 1), ("https://gone", "X", 2)])

    assert _flagged(clean_db, "folder_memberships_unknown_playlist") == {"https://gone"}


def test_playlists_without_membership(clean_db):
    clean_db.add_playlist("https://open.spotify.com/playlist/CCC", playlist_name="C")

    assert _flagged(clean_db, "playlists_without_membership") == {"https://open.spotify.com/playlist/CCC"}


@pytest.mark.parametrize("blank", [None, "", "   "])
def test_completed_without_file_path_treats_blank_as_missing(clean_db, blank):
    clean_db.conn.execute("UPDATE tracks SET local_file_path = ? WHERE track_id = 't1'", (blank,))
    clean_db.conn.commit()

    assert _flagged(clean_db, "completed_without_file_path") == {"t1"}


def test_blacklisted_with_file_path_ignores_quality_upgrades_that_keep_the_old_file(clean_db):
    """redownload_pending/searching/downloading legitimately keep the previous file's path."""
    _track(clean_db, "upgrading", status="redownload_pending", path="/imp/u.mp3")
    _track(clean_db, "upgrading2", status="searching", path="/imp/u2.mp3")
    _track(clean_db, "bad", status="blacklisted", path="/imp/bad.mp3")
    _track(clean_db, "fine", status="blacklisted")

    assert _flagged(clean_db, "blacklisted_with_file_path") == {"bad"}


def test_searching_without_search_uuid(clean_db):
    _track(clean_db, "s-bad", status="searching")
    _track(clean_db, "s-ok", status="searching", slskd_search_uuid="uuid-1")

    assert _flagged(clean_db, "searching_without_search_uuid") == {"s-bad"}


def test_active_download_without_uuid_or_username(clean_db):
    _track(clean_db, "d-none", status="downloading")
    _track(clean_db, "q-none", status="queued")
    _track(clean_db, "d-ok", status="downloading", slskd_download_uuid="u", username="peer")
    _track(clean_db, "d-no-user", status="downloading", slskd_download_uuid="u")
    _track(clean_db, "p", status="pending")

    assert _flagged(clean_db, "active_download_without_uuid") == {"d-none", "q-none"}
    assert _flagged(clean_db, "active_download_without_username") == {"d-none", "q-none", "d-no-user"}


def test_failed_without_reason(clean_db):
    _track(clean_db, "f-bad", status="failed")
    clean_db.update_track_status("f-bad", "failed")  # failed_reason written NULL
    _track(clean_db, "f-ok", status="pending")
    clean_db.update_track_status("f-ok", "failed", failed_reason="boom")

    assert _flagged(clean_db, "failed_without_reason") == {"f-bad"}


def test_stuck_tracks_flags_only_in_flight_statuses_past_their_own_threshold(clean_db):
    thresholds = STUCK_THRESHOLD_HOURS
    cases = {
        "searching-old": ("searching", thresholds["searching"] + 1),
        "searching-fresh": ("searching", thresholds["searching"] - 1),
        # Same age, different status: queued may sit far longer than searching may.
        "queued-mid": ("queued", thresholds["searching"] + 1),
        "queued-old": ("queued", thresholds["queued"] + 1),
        "downloading-old": ("downloading", thresholds["downloading"] + 1),
        "redownload-old": ("redownload_pending", thresholds["redownload_pending"] + 1),
        "pending-old": ("pending", thresholds["pending"] + 1),
        # Final outcomes are never stuck, however old.
        "completed-ancient": ("completed", 24 * 365),
        "failed-ancient": ("failed", 24 * 365),
        "blacklisted-ancient": ("blacklisted", 24 * 365),
    }
    for track_id, (status, hours) in cases.items():
        _track(clean_db, track_id, status=status)
        _set_changed_at(clean_db, track_id, hours)
    # Unknown age is excluded rather than guessed at.
    _track(clean_db, "searching-unknown", status="searching")
    clean_db.conn.execute("UPDATE tracks SET status_changed_at = NULL WHERE track_id = 'searching-unknown'")
    clean_db.conn.commit()

    assert _flagged(clean_db, "stuck_tracks") == {
        "searching-old", "queued-old", "downloading-old", "redownload-old", "pending-old",
    }


def test_stuck_tracks_reports_hours_in_status_worst_first(clean_db):
    _track(clean_db, "a", status="downloading")
    _track(clean_db, "b", status="downloading")
    _set_changed_at(clean_db, "a", 10)
    _set_changed_at(clean_db, "b", 30)

    result = clean_db.get_audit_check_rows("stuck_tracks")

    assert [r[0] for r in result["rows"]] == ["b", "a"]
    hours = result["columns"].index("hours_in_status")
    assert result["rows"][0][hours] == pytest.approx(30, abs=0.1)


def test_audit_rows_page_and_clamp(clean_db):
    for i in range(130):
        _track(clean_db, f"lonely-{i:03d}")

    assert clean_db.count_audit_check("tracks_in_no_playlist") == 130
    first = clean_db.get_audit_check_rows("tracks_in_no_playlist", 0, 10_000)
    assert len(first["rows"]) == EXPLORER_MAX_PAGE_SIZE
    tail = clean_db.get_audit_check_rows("tracks_in_no_playlist", 100, 100)
    assert len(tail["rows"]) == 30


def test_check_ids_are_the_lookup_keys():
    assert set(AUDIT_CHECKS_BY_ID) == {c.id for c in AUDIT_CHECKS}


# --- inputs for the disk checks / health -----------------------------------------------------


def test_disk_check_inputs(clean_db):
    _track(clean_db, "blank-path", status="completed")
    clean_db.conn.execute("UPDATE tracks SET local_file_path = '  ' WHERE track_id = 'blank-path'")
    clean_db.conn.commit()

    completed = clean_db.get_completed_track_files()
    assert [row[0] for row in completed] == ["t1", "t2"]  # completed + non-blank path only
    assert completed[0] == ("t1", "Ann", "Alpha", "/imp/1.mp3")

    assert clean_db.get_playlist_m3u8_paths() == [
        (_URL_A, "Playlist A", "/m3u8/a.m3u8"),
        (_URL_B, "Playlist B", "/m3u8/b.m3u8"),
    ]
    assert sorted(clean_db.get_all_local_file_paths()) == ["/imp/1.mp3", "/imp/2.mp3"]


def test_database_health_reports_file_facts_without_changing_settings(clean_db):
    health = clean_db.get_database_health()

    assert health["journal_mode"] == "delete"
    assert health["synchronous"] == "FULL"
    assert health["file_size_bytes"] > 0
    assert health["page_count"] > 0
    assert health["freelist_count"] >= 0
    assert health["journal_file_present"] is False
    assert clean_db.get_database_health()["journal_mode"] == "delete"  # reading it didn't alter it


def test_quick_check_is_ok_on_a_healthy_database(clean_db):
    assert clean_db.run_quick_check() == ["ok"]


# --- guardrails (ADR 0008 decision 5) --------------------------------------------------------


def test_explorer_reads_leave_the_shared_connection_untouched(clean_db):
    """The connection is shared across request threads and the dashboard's own writes: no
    explorer read may leave a transaction (and its lock) open, or change connection state."""
    conn = clean_db.conn
    before = (conn.row_factory, conn.isolation_level, conn.execute("PRAGMA journal_mode").fetchone()[0],
              conn.execute("PRAGMA synchronous").fetchone()[0])

    clean_db.list_tables()
    clean_db.get_table_schema("tracks")
    clean_db.browse_table("tracks", sort="track_name", filters={"artist": "a"})
    clean_db.get_track_detail("t2")
    for check in AUDIT_CHECKS:
        clean_db.count_audit_check(check.id)
        clean_db.get_audit_check_rows(check.id)
    clean_db.get_completed_track_files()
    clean_db.get_playlist_m3u8_paths()
    clean_db.get_all_local_file_paths()
    clean_db.get_database_health()
    clean_db.run_quick_check()

    assert not conn.in_transaction
    assert (conn.row_factory, conn.isolation_level, conn.execute("PRAGMA journal_mode").fetchone()[0],
            conn.execute("PRAGMA synchronous").fetchone()[0]) == before


def test_explorer_reads_never_write(clean_db):
    """total_changes counts every row written on this connection."""
    before = clean_db.conn.total_changes

    clean_db.list_tables()
    clean_db.browse_table("tracks")
    clean_db.get_track_detail("t1")
    for check in AUDIT_CHECKS:
        clean_db.get_audit_check_rows(check.id)
    clean_db.run_quick_check()

    assert clean_db.conn.total_changes == before
