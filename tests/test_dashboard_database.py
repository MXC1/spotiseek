"""Route tests for the dashboard's Database tab (docs/adr/0008-dashboard-database-explorer.md).

Like the other dashboard tests these run against the disposable `test` environment's
database (the dashboard config owns one TrackDB singleton for the whole session, so tests
reset it with clear_database()) and against files in a tmp workdir -- never real data.
"""

import os
import re
import shutil
import sqlite3
import tempfile

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient

from observability.dashboard.app import app
from observability.dashboard.config import track_db
from observability.dashboard.routes import database as db_module
from scripts.constants import STUCK_THRESHOLD_HOURS
from scripts.database_management import TrackData

_URL_A = "https://open.spotify.com/playlist/AAA"
_URL_B = "https://open.spotify.com/playlist/BBB"
_HX = {"HX-Request": "true"}


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def workdir():
    root = tempfile.mkdtemp(prefix="spotiseek_dbtab_test_")
    yield root
    shutil.rmtree(root, ignore_errors=True)


def _track(track_id, *, status="completed", name="Name", artist="Artist", path=None, **columns):
    track_db.add_track(TrackData(track_id=track_id, track_name=name, artist=artist, download_status=status))
    if path is not None:
        track_db.update_local_file_path(track_id, path)
    for column, value in columns.items():
        track_db.conn.execute(f"UPDATE tracks SET {column} = ? WHERE track_id = ?", (value, track_id))
    track_db.conn.commit()


def _hours_ago(track_id, hours):
    track_db.conn.execute(
        "UPDATE tracks SET status_changed_at = datetime('now', ?) WHERE track_id = ?", (f"-{hours} hours", track_id),
    )
    track_db.conn.commit()


@pytest.fixture()
def empty_db():
    track_db.clear_database()
    yield track_db
    track_db.clear_database()


@pytest.fixture()
def library(empty_db, workdir):
    """A small consistent library on disk: real audio + m3u8 files for one completed track,
    a placeholder-only m3u8 entry for a pending one."""
    song = os.path.join(workdir, "alpha.mp3")
    with open(song, "w") as f:
        f.write("fake audio")
    m3u8_a = os.path.join(workdir, "a.m3u8")
    with open(m3u8_a, "w") as f:
        f.write(f"#EXTM3U\n{song}\n# t3 - Cy - Charlie\n")
    m3u8_b = os.path.join(workdir, "b.m3u8")
    with open(m3u8_b, "w") as f:
        f.write("#EXTM3U\n# t3 - Cy - Charlie\n")

    empty_db.add_playlist(_URL_A, m3u8_path=m3u8_a, playlist_name="Playlist A")
    empty_db.add_playlist(_URL_B, m3u8_path=m3u8_b, playlist_name="Playlist B")
    empty_db.set_playlist_display_order(_URL_A, 0)
    empty_db.set_playlist_display_order(_URL_B, 1)
    _track("t1", path=song, name="Alpha", artist="Ann")
    _track("t3", status="pending", name="Charlie", artist="Cy")
    for tid, url in (("t1", _URL_A), ("t3", _URL_A), ("t3", _URL_B)):
        empty_db.link_track_to_playlist(tid, url)
    empty_db.replace_playlist_folder_memberships([(_URL_A, "", 0), (_URL_B, "House", 1)])
    return {"song": song, "m3u8_a": m3u8_a, "m3u8_b": m3u8_b, "workdir": workdir}


# --- shell / navigation ----------------------------------------------------------------------


def test_nav_has_a_database_link_between_blacklist_and_docs(client):
    html = client.get("/stats").text

    assert 'href="/database"' in html
    assert html.index('href="/blacklist"') < html.index('href="/database"') < html.index('href="/docs/overview"')


@pytest.mark.usefixtures("library")
def test_database_tab_full_page_and_fragment(client):
    full = client.get("/database")
    fragment = client.get("/database", headers=_HX)

    assert full.status_code == fragment.status_code == 200
    assert "<html" in full.text
    assert "<html" not in fragment.text
    assert "db-table-chip" in fragment.text


@pytest.mark.usefixtures("library")
def test_tables_list_shows_every_table_with_row_counts(client):
    html = client.get("/database", headers=_HX).text

    for table in ("tracks", "playlists", "playlist_tracks", "playlist_folder_memberships", "slskd_blacklist"):
        assert f'href="/database?table={table}"' in html
    chips = dict(re.findall(r'db-table-name">(\w+)</span>\s*<span class="db-table-count">([\d,]+)<', html))
    assert chips["tracks"] == "2"
    assert chips["playlist_tracks"] == "3"


@pytest.mark.usefixtures("library")
def test_sub_nav_marks_the_active_view(client):
    tables = client.get("/database", headers=_HX).text
    audit = client.get("/database/audit", headers=_HX).text

    assert re.search(r'db-sublink active"\s+href="/database"', tables)
    assert re.search(r'db-sublink active"\s+href="/database/audit"', audit)


# --- pushed URL is canonical -----------------------------------------------------------------


@pytest.mark.usefixtures("library")
def test_htmx_response_pushes_a_canonical_url_without_empty_filters(client):
    response = client.get("/database?table=tracks&f__artist=&f__track_name=alp&sort=&dir=asc&page_size=25", headers=_HX)

    assert response.headers["HX-Push-Url"] == "/database?table=tracks&f__track_name=alp"


@pytest.mark.usefixtures("library")
def test_refresh_replaces_history_instead_of_pushing(client):
    response = client.get("/database?table=tracks", headers={**_HX, "X-Refresh": "1"})

    assert response.headers["HX-Replace-Url"] == "/database?table=tracks"
    assert "HX-Push-Url" not in response.headers


@pytest.mark.usefixtures("library")
def test_non_htmx_requests_get_no_history_headers(client):
    response = client.get("/database?table=tracks")

    assert "HX-Push-Url" not in response.headers
    assert "HX-Replace-Url" not in response.headers


# --- table browser ---------------------------------------------------------------------------


@pytest.mark.usefixtures("library")
def test_browsing_a_table_shows_rows_headers_and_pk_fk_tags(client):
    html = client.get("/database?table=playlist_tracks", headers=_HX).text

    assert "Rows 1&ndash;3 of 3" in html
    assert ">track_id" in html
    assert html.count('title="Foreign key: click a value to follow it"') == 2
    assert "https://open.spotify.com/playlist/AAA" in html


@pytest.mark.usefixtures("library")
def test_track_ids_in_the_tracks_table_link_to_the_detail_view(client):
    html = client.get("/database?table=tracks", headers=_HX).text

    assert 'href="/database/track?track_id=t1"' in html
    assert 'href="/database/track?track_id=t3"' in html


@pytest.mark.usefixtures("library")
def test_foreign_key_cells_are_click_through_links(client):
    html = client.get("/database?table=playlist_tracks", headers=_HX).text

    assert 'href="/database/track?track_id=t1"' in html  # -> tracks.track_id becomes the detail view
    playlist_href = "/database?table=playlists&amp;f__playlist_url=https%3A%2F%2Fopen.spotify.com%2Fplaylist%2FAAA"
    assert playlist_href in html or playlist_href.replace("&amp;", "&") in html


@pytest.mark.usefixtures("library")
def test_following_a_playlist_link_lands_on_exactly_that_playlist(client):
    html = client.get(
        "/database?table=playlists&f__playlist_url=https%3A%2F%2Fopen.spotify.com%2Fplaylist%2FAAA", headers=_HX,
    ).text

    assert "Playlist A" in html
    assert "Playlist B" not in html
    assert "Rows 1&ndash;1 of 1" in html


@pytest.mark.usefixtures("library")
def test_filter_and_sort_are_applied_and_reflected_in_the_form(client):
    html = client.get("/database?table=tracks&f__artist=an&sort=track_name&dir=desc", headers=_HX).text

    assert "Rows 1&ndash;1 of 1" in html
    assert "Alpha" in html
    assert "Charlie" not in html
    assert 'value="an"' in html  # the filter box keeps its text
    assert "Clear filters" in html
    assert 'name="dir" value="desc"' in html


@pytest.mark.usefixtures("library")
def test_clicking_the_sorted_column_again_reverses_the_direction(client):
    asc = client.get("/database?table=tracks&sort=track_name", headers=_HX).text
    desc = client.get("/database?table=tracks&sort=track_name&dir=desc", headers=_HX).text

    assert "sort=track_name&amp;dir=desc" in asc  # next click flips to descending
    assert 'hx-get="/database?table=tracks&amp;sort=track_name"' in desc  # and back


@pytest.mark.usefixtures("empty_db")
def test_paging_uses_valid_page_sizes_and_clamps_the_page(client):
    for i in range(30):
        _track(f"id-{i:02d}")

    first = client.get("/database?table=tracks&page_size=10", headers=_HX).text
    assert "Rows 1&ndash;10 of 30" in first
    assert "Page 1 of 3" in first
    assert 'hx-get="/database?table=tracks&amp;page=2&amp;page_size=10"' in first

    beyond = client.get("/database?table=tracks&page=99&page_size=10", headers=_HX).text
    assert "Page 3 of 3" in beyond
    assert "Rows 21&ndash;30 of 30" in beyond

    weird = client.get("/database?table=tracks&page_size=7", headers=_HX).text  # not an offered size -> default
    assert "Rows 1&ndash;25 of 30" in weird


@pytest.mark.usefixtures("library")
@pytest.mark.parametrize("bad", ["nope", "tracks; DROP TABLE tracks", 'tracks"'])
def test_unknown_or_hostile_table_names_are_reported_not_executed(client, bad):
    response = client.get("/database", params={"table": bad}, headers=_HX)

    assert response.status_code == 200
    assert "There is no table named" in response.text
    assert track_db.list_tables()[0] == ("tracks", 2)  # still there


@pytest.mark.usefixtures("library")
def test_values_are_html_escaped(client):
    _track("xss", name="<script>alert(1)</script>")

    html = client.get("/database?table=tracks", headers=_HX).text

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


@pytest.mark.usefixtures("library")
def test_null_values_are_shown_distinctly(client):
    html = client.get("/database?table=tracks", headers=_HX).text

    assert '<span class="db-null">NULL</span>' in html


# --- track detail ----------------------------------------------------------------------------


def test_track_detail_shows_status_file_playlists_and_full_row(client, library):
    html = client.get("/database/track?track_id=t1", headers=_HX).text

    assert "Ann &ndash; Alpha" in html
    assert '<span class="db-badge">completed</span>' in html
    assert "final status" in html
    assert 'db-badge db-badge-ok">exists' in html
    assert library["song"].replace("\\", "\\") in html.replace("&#39;", "'")  # the path is shown in full
    assert "Playlist A" in html
    assert "Listed with its file path" in html
    assert "(root)" in html
    assert "Full row" in html
    assert "status_changed_at" in html


def test_track_detail_reports_a_missing_file(client, library):
    os.remove(library["song"])

    html = client.get("/database/track?track_id=t1", headers=_HX).text

    assert "db-badge-bad" in html
    assert ">missing<" in html


@pytest.mark.usefixtures("library")
def test_track_detail_shows_placeholder_state_per_playlist_and_its_folders(client):
    html = client.get("/database/track?track_id=t3", headers=_HX).text

    assert html.count("Still a placeholder comment") == 2  # a pending track is a placeholder in both m3u8s
    assert "House" in html
    assert "No file path is recorded" in html


def test_track_detail_flags_completed_track_still_a_placeholder(client, library):
    """Drift: the track is completed, but its m3u8 still holds the placeholder comment."""
    with open(library["m3u8_a"], "w") as f:
        f.write("#EXTM3U\n# t1 - Ann - Alpha\n")

    html = client.get("/database/track?track_id=t1", headers=_HX).text

    assert 'db-badge-bad">Still a placeholder comment' in html


def test_track_detail_flags_a_missing_m3u8(client, library):
    os.remove(library["m3u8_a"])

    html = client.get("/database/track?track_id=t1", headers=_HX).text

    assert "m3u8 file is missing" in html


@pytest.mark.usefixtures("library")
def test_track_detail_marks_a_track_stuck_past_its_threshold(client):
    _track("s1", status="searching")
    _hours_ago("s1", STUCK_THRESHOLD_HOURS["searching"] + 2)

    html = client.get("/database/track?track_id=s1", headers=_HX).text

    assert "STUCK" in html
    assert f"over {STUCK_THRESHOLD_HOURS['searching']}h" in html


@pytest.mark.usefixtures("library")
def test_track_detail_shows_an_in_flight_track_within_threshold_as_ok(client):
    _track("s2", status="searching")

    html = client.get("/database/track?track_id=s2", headers=_HX).text

    assert "STUCK" not in html
    assert "in-flight, within" in html


@pytest.mark.usefixtures("library")
def test_track_detail_lists_a_matching_blacklist_entry(client):
    _track("bl", slskd_file_name="Folder/song.mp3", username="peer")
    track_db.add_slskd_blacklist("peer", "Folder/song.mp3", reason="manual_blacklist")

    html = client.get("/database/track?track_id=bl", headers=_HX).text

    assert "manual_blacklist" in html
    assert "peer" in html


@pytest.mark.usefixtures("library")
def test_track_ids_with_slashes_work_through_the_url(client):
    _track("lobsta-b/7th-element-vip", name="VIP")

    listing = client.get("/database?table=tracks", headers=_HX).text
    assert 'href="/database/track?track_id=lobsta-b%2F7th-element-vip"' in listing

    detail = client.get("/database/track", params={"track_id": "lobsta-b/7th-element-vip"}, headers=_HX)
    assert "VIP" in detail.text
    assert detail.headers["HX-Push-Url"] == "/database/track?track_id=lobsta-b%2F7th-element-vip"


@pytest.mark.usefixtures("library")
def test_track_detail_for_an_unknown_track(client):
    response = client.get("/database/track?track_id=nope", headers=_HX)

    assert response.status_code == 200  # htmx doesn't swap 4xx responses, so this stays a normal page
    assert "There is no track with ID" in response.text


# --- audit view ------------------------------------------------------------------------------


@pytest.mark.usefixtures("library")
def test_audit_view_is_clean_on_a_consistent_library(client):
    html = client.get("/database/audit", headers=_HX).text

    assert "found nothing" in html
    sections = ("Referential integrity", "Status / field consistency", "Stuck tracks", "Disk checks", "Database health")
    for title in sections:
        assert title in html
    assert "Show rows" not in html  # nothing flagged, so nothing to expand


@pytest.mark.usefixtures("library")
def test_audit_view_counts_flagged_rows_and_offers_them(client):
    _track("lonely")  # in no playlist
    _track("cnp", status="completed")  # completed, no file path
    track_db.link_track_to_playlist("cnp", _URL_A)

    html = client.get("/database/audit", headers=_HX).text

    assert "issue(s) flagged" in html
    assert 'hx-get="/database/audit/check/tracks_in_no_playlist"' in html
    assert 'hx-get="/database/audit/check/completed_without_file_path"' in html
    assert 'hx-get="/database/audit/check/searching_without_search_uuid"' not in html


@pytest.mark.usefixtures("library")
def test_audit_check_rows_link_to_the_track_detail_view(client):
    _track("lonely")

    html = client.get("/database/audit/check/tracks_in_no_playlist").text

    assert 'href="/database/track?track_id=lonely"' in html
    assert "1 row(s)" in html
    assert "<th>track_id</th>" in html


@pytest.mark.usefixtures("library")
def test_audit_check_rows_link_playlists_to_their_row(client):
    track_db.conn.execute("INSERT INTO playlist_tracks VALUES (?, 'ghost')", (_URL_A,))
    track_db.conn.commit()

    html = client.get("/database/audit/check/playlist_tracks_missing_track").text

    assert "/database?table=playlists&amp;f__playlist_url=" in html


@pytest.mark.usefixtures("library")
def test_audit_check_rows_paginate(client):
    for i in range(30):
        _track(f"lonely-{i:02d}")

    page1 = client.get("/database/audit/check/tracks_in_no_playlist").text
    page2 = client.get("/database/audit/check/tracks_in_no_playlist?page=2").text

    assert "30 row(s) &middot; page 1 of 2" in page1
    assert 'hx-get="/database/audit/check/tracks_in_no_playlist?page=2"' in page1
    assert "page 2 of 2" in page2
    assert "lonely-24" not in page2 and "lonely-25" in page2


@pytest.mark.usefixtures("library")
def test_unknown_audit_check_is_reported(client):
    response = client.get("/database/audit/check/nope")

    assert response.status_code == 200
    assert "Unknown audit check" in response.text


@pytest.mark.usefixtures("library")
def test_stuck_tracks_appear_in_the_audit_view(client):
    _track("stuck-one", status="downloading", slskd_download_uuid="u", username="peer")
    _hours_ago("stuck-one", STUCK_THRESHOLD_HOURS["downloading"] + 1)

    assert 'hx-get="/database/audit/check/stuck_tracks"' in client.get("/database/audit", headers=_HX).text
    rows = client.get("/database/audit/check/stuck_tracks").text
    assert "stuck-one" in rows
    assert "<th>hours_in_status</th>" in rows


# --- disk checks (only on POST) --------------------------------------------------------------


@pytest.mark.usefixtures("library")
def test_disk_checks_do_not_run_on_a_get(client):
    assert client.get("/database/audit/disk/completed_files_missing").status_code == 405
    assert client.get("/database/audit/health").status_code == 405


def test_completed_files_missing_check(client, library):
    _track("gone", path=os.path.join(library["workdir"], "vanished.mp3"))

    html = client.post("/database/audit/disk/completed_files_missing").text

    assert "1 found" in html
    assert "vanished.mp3" in html
    assert 'href="/database/track?track_id=gone"' in html
    assert "alpha.mp3" not in html  # the file that does exist isn't flagged


@pytest.mark.usefixtures("library")
def test_completed_files_missing_check_is_clean_when_everything_exists(client):
    assert "nothing flagged" in client.post("/database/audit/disk/completed_files_missing").text


def test_m3u8_missing_check(client, library):
    os.remove(library["m3u8_b"])
    track_db.add_playlist("https://open.spotify.com/playlist/NOPATH", playlist_name="No Path")

    html = client.post("/database/audit/disk/m3u8_missing").text

    assert "2 found" in html
    assert "File not found" in html
    assert "No m3u8 path recorded" in html
    assert "Playlist A" not in html


def test_orphan_files_check(client, library, monkeypatch):
    imported = os.path.join(library["workdir"], "imported")
    os.makedirs(imported)
    tracked = os.path.join(imported, "tracked.mp3")
    orphan = os.path.join(imported, "orphan.flac")
    for path in (tracked, orphan, os.path.join(imported, "cover.jpg")):
        with open(path, "w") as f:
            f.write("x")
    _track("tr", path=tracked)
    monkeypatch.setattr(db_module, "IMPORTED_DIR", imported)

    html = client.post("/database/audit/disk/orphan_files").text

    assert "1 found" in html
    assert "orphan.flac" in html
    assert "tracked.mp3" not in html
    assert "cover.jpg" not in html


def test_orphan_files_check_reports_a_missing_imported_dir(client, library, monkeypatch):
    monkeypatch.setattr(db_module, "IMPORTED_DIR", os.path.join(library["workdir"], "nope"))

    html = client.post("/database/audit/disk/orphan_files").text

    assert "does not exist in this container" in html


@pytest.mark.usefixtures("library")
def test_unknown_disk_check_is_reported(client):
    assert "Unknown disk check" in client.post("/database/audit/disk/nope").text


@pytest.mark.usefixtures("library")
def test_health_check_reports_file_facts_and_quick_check(client):
    html = client.post("/database/audit/health").text

    assert "quick_check: ok" in html
    assert "Journal mode" in html
    assert "delete" in html
    assert "FULL" in html


# --- resilience + guardrails -----------------------------------------------------------------


@pytest.mark.usefixtures("library")
def test_a_locked_database_becomes_a_message_not_a_500(client, monkeypatch):
    def locked(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(track_db, "list_tables", locked)
    monkeypatch.setattr(track_db, "count_audit_check", locked)

    tables = client.get("/database", headers=_HX)
    audit = client.get("/database/audit", headers=_HX)

    assert tables.status_code == audit.status_code == 200
    assert "Database read failed: database is locked" in tables.text
    assert "Database read failed: database is locked" in audit.text


@pytest.mark.usefixtures("library")
def test_routes_never_write_and_never_leave_a_transaction_open(client):
    """The connection is shared with the dashboard's own writes: viewing must be inert."""
    before = track_db.conn.total_changes

    for url in ("/database", "/database?table=tracks&f__artist=a&sort=artist", "/database/audit",
                "/database/audit/check/tracks_in_no_playlist", "/database/track?track_id=t1"):
        assert client.get(url, headers=_HX).status_code == 200
    for url in ("/database/audit/disk/completed_files_missing", "/database/audit/disk/m3u8_missing",
                "/database/audit/disk/orphan_files", "/database/audit/health"):
        assert client.post(url).status_code == 200

    assert track_db.conn.total_changes == before
    assert not track_db.conn.in_transaction
    assert track_db.conn.row_factory is None
