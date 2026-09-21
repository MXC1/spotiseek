"""Tests for the Database tab's filesystem checks (docs/adr/0008-dashboard-database-explorer.md).

Everything runs against files created in pytest's tmp_path; nothing here touches real data.
"""

import os

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from observability.dashboard import disk_checks as dc


def _write(path, text="x"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


# --- file_facts ------------------------------------------------------------------------------


def test_file_facts_for_an_existing_file(tmp_path):
    path = _write(tmp_path / "a.mp3", "12345")

    facts = dc.file_facts(path)

    assert facts.exists is True
    assert facts.size_bytes == 5
    assert len(facts.modified) == len("2000-01-01 00:00:00")


def test_file_facts_for_a_missing_path_or_a_directory(tmp_path):
    assert dc.file_facts(str(tmp_path / "gone.mp3")).exists is False
    assert dc.file_facts(str(tmp_path)).exists is False  # a directory isn't a track file


@pytest.mark.parametrize("blank", [None, "", "   "])
def test_file_facts_is_none_when_no_path_is_recorded(blank):
    assert dc.file_facts(blank) is None


# --- m3u8_line_state -------------------------------------------------------------------------


def test_m3u8_state_resolved_path(tmp_path):
    song = str(tmp_path / "song.mp3")
    m3u8 = _write(tmp_path / "p.m3u8", f"#EXTM3U\n# other - A - B\n{song}\n")

    assert dc.m3u8_line_state(m3u8, "t1", song) == dc.STATE_RESOLVED


def test_m3u8_state_placeholder_comment_matches_how_m3u8_manager_writes_it(tmp_path):
    m3u8 = _write(tmp_path / "p.m3u8", "#EXTM3U\n# t1 - Artist - Name\n")

    assert dc.m3u8_line_state(m3u8, "t1", None) == dc.STATE_PLACEHOLDER
    assert dc.m3u8_line_state(m3u8, "t1", "/not/listed.mp3") == dc.STATE_PLACEHOLDER


def test_m3u8_state_prefers_the_resolved_path_over_a_stale_placeholder(tmp_path):
    m3u8 = _write(tmp_path / "p.m3u8", "# t1 - A - B\n/imp/1.mp3\n")

    assert dc.m3u8_line_state(m3u8, "t1", "/imp/1.mp3") == dc.STATE_RESOLVED


def test_m3u8_state_does_not_confuse_track_ids_that_share_a_prefix(tmp_path):
    """'# t1 - ' must not match a comment for track 't10'."""
    m3u8 = _write(tmp_path / "p.m3u8", "# t10 - A - B\n")

    assert dc.m3u8_line_state(m3u8, "t1", None) == dc.STATE_NOT_LISTED


def test_m3u8_state_tolerates_windows_line_endings(tmp_path):
    song = "E:\\music\\song.mp3"
    path = tmp_path / "p.m3u8"
    path.write_bytes(f"#EXTM3U\r\n{song}\r\n".encode())

    assert dc.m3u8_line_state(str(path), "t1", song) == dc.STATE_RESOLVED


def test_m3u8_state_for_missing_or_unrecorded_files(tmp_path):
    assert dc.m3u8_line_state(str(tmp_path / "gone.m3u8"), "t1", None) == dc.STATE_M3U8_MISSING
    assert dc.m3u8_line_state(None, "t1", None) == dc.STATE_NO_M3U8_PATH
    assert dc.m3u8_line_state("  ", "t1", None) == dc.STATE_NO_M3U8_PATH


def test_every_state_has_a_label():
    for state in (dc.STATE_RESOLVED, dc.STATE_PLACEHOLDER, dc.STATE_NOT_LISTED,
                  dc.STATE_M3U8_MISSING, dc.STATE_NO_M3U8_PATH, dc.STATE_UNREADABLE):
        assert dc.M3U8_STATE_LABELS[state]


# --- find_missing_* --------------------------------------------------------------------------


def test_find_missing_track_files_without_a_resolver_stats_paths_as_recorded(tmp_path):
    here = _write(tmp_path / "here.mp3")
    rows = [("t1", "A", "One", here), ("t2", "B", "Two", str(tmp_path / "gone.mp3"))]

    result = dc.find_missing_track_files(rows)

    assert result.missing == [rows[1]]
    assert (result.checked, result.not_checked, result.not_checked_example) == (2, 0, None)


def test_find_missing_m3u8s_flags_unrecorded_paths_and_absent_files(tmp_path):
    ok = _write(tmp_path / "ok.m3u8")
    playlists = [
        ("u-ok", "OK", ok),
        ("u-gone", "Gone", str(tmp_path / "gone.m3u8")),
        ("u-none", "None", None),
        ("u-blank", "Blank", "  "),
    ]

    problems = dc.find_missing_m3u8s(playlists)

    assert [p[0] for p in problems] == ["u-gone", "u-none", "u-blank"]
    assert problems[0][3] == "File not found"
    assert problems[1][3] == "No m3u8 path recorded"


# --- find_orphan_files -----------------------------------------------------------------------


def test_orphan_files_lists_only_untracked_audio_files(tmp_path):
    root = tmp_path / "imported"
    tracked = _write(root / "Artist" / "tracked.mp3")
    orphan_a = _write(root / "Artist" / "orphan.flac", "abc")
    orphan_b = _write(root / "loose.WAV")  # extension match is case-insensitive
    _write(root / "cover.jpg")  # not audio
    _write(root / "notes.txt")

    listed, total, dir_exists = dc.find_orphan_files(str(root), [tracked])

    assert dir_exists is True
    assert total == 2
    assert [path for path, _ in listed] == sorted([orphan_a, orphan_b])
    assert dict(listed)[orphan_a] == 3  # size in bytes


def test_orphan_files_compares_paths_after_normalising_them(tmp_path):
    root = tmp_path / "imported"
    tracked = _write(root / "sub" / "song.mp3")
    unnormalised = os.path.join(str(root), "sub", "..", "sub", "song.mp3")

    listed, total, _ = dc.find_orphan_files(str(root), [unnormalised])

    assert (listed, total) == ([], 0)
    assert tracked  # (the tracked file really exists; it just isn't an orphan)


def test_orphan_files_caps_the_listing_but_reports_the_true_total(tmp_path, monkeypatch):
    monkeypatch.setattr(dc, "MAX_LISTED_FILES", 3)
    root = tmp_path / "imported"
    for i in range(7):
        _write(root / f"f{i}.mp3")

    listed, total, _ = dc.find_orphan_files(str(root), [])

    assert len(listed) == 3
    assert total == 7


def test_orphan_files_reports_a_missing_directory(tmp_path):
    assert dc.find_orphan_files(str(tmp_path / "nope"), []) == ([], 0, False)


# --- PathResolver: what this container can actually see ---------------------------------------
#
# Regression: the first version of the file checks stat'ed every recorded path directly, but the
# dashboard container mounts only imported/ (and, now, a read-only alias of downloads/). On the
# real library that flagged all 3,371 completed tracks whose files live in downloads/ as
# "missing". A path outside the known-mounted roots must be "not visible", never "missing".


@pytest.fixture()
def layout(tmp_path):
    """recorded: where the workflow container says files are. visible: where this container
    can see them. downloads is only visible through the alias."""
    recorded = tmp_path / "recorded"
    view = tmp_path / "view"
    (recorded / "imported").mkdir(parents=True)
    (view / "downloads").mkdir(parents=True)
    resolver = dc.PathResolver(roots=(
        (str(recorded / "imported"), str(recorded / "imported")),
        (str(recorded / "downloads"), str(view / "downloads")),
    ))
    return {"recorded": recorded, "view": view, "resolver": resolver}


def test_resolver_leaves_paths_under_an_identity_root_alone(layout):
    imported = layout["recorded"] / "imported" / "a.mp3"

    assert layout["resolver"].resolve(str(imported)) == str(imported)


def test_resolver_translates_paths_under_an_alias_root(layout):
    recorded = layout["recorded"] / "downloads" / "Album" / "song.mp3"

    resolved = layout["resolver"].resolve(str(recorded))

    assert os.path.normcase(resolved) == os.path.normcase(str(layout["view"] / "downloads" / "Album" / "song.mp3"))


def test_resolver_says_none_when_the_alias_is_not_mounted(layout):
    layout["view"].joinpath("downloads").rmdir()

    assert layout["resolver"].resolve(str(layout["recorded"] / "downloads" / "a.mp3")) is None


def test_resolver_is_default_deny_for_locations_it_does_not_know(layout, tmp_path):
    assert layout["resolver"].resolve(str(tmp_path / "somewhere" / "else.mp3")) is None
    assert layout["resolver"].resolve(r"E:\Music\host_path.mp3") is None  # a Windows host path


def test_resolver_matches_whole_path_components_only(layout):
    """'downloads-old' must not be treated as being under 'downloads'."""
    assert layout["resolver"].resolve(str(layout["recorded"] / "downloads-old" / "a.mp3")) is None


def test_resolver_normalises_dotdot_before_matching(layout):
    sneaky = os.path.join(str(layout["recorded"]), "imported", "..", "downloads", "a.mp3")

    resolved = layout["resolver"].resolve(sneaky)

    assert os.path.normcase(resolved) == os.path.normcase(str(layout["view"] / "downloads" / "a.mp3"))


def test_file_facts_through_a_resolver_reports_exists_missing_and_not_visible(layout):
    (layout["view"] / "downloads" / "here.mp3").write_bytes(b"12345")
    resolver = layout["resolver"]
    recorded_downloads = layout["recorded"] / "downloads"

    exists = dc.file_facts(str(recorded_downloads / "here.mp3"), resolver)
    missing = dc.file_facts(str(recorded_downloads / "gone.mp3"), resolver)
    invisible = dc.file_facts(str(layout["recorded"].parent / "elsewhere" / "x.mp3"), resolver)

    assert (exists.exists, exists.size_bytes) == (True, 5)
    assert missing.exists is False
    assert invisible.exists is None  # not False: absence can't be claimed for a place we can't see
    assert invisible.size_bytes is None


def test_find_missing_track_files_separates_missing_from_not_checked(layout, tmp_path):
    (layout["view"] / "downloads" / "here.mp3").write_bytes(b"x")
    recorded_downloads = layout["recorded"] / "downloads"
    elsewhere = str(tmp_path / "elsewhere" / "x.mp3")
    rows = [
        ("present", "A", "One", str(recorded_downloads / "here.mp3")),
        ("missing", "B", "Two", str(recorded_downloads / "gone.mp3")),
        ("unseen-1", "C", "Three", elsewhere),
        ("unseen-2", "D", "Four", elsewhere),
    ]

    result = dc.find_missing_track_files(rows, layout["resolver"])

    assert [row[0] for row in result.missing] == ["missing"]
    assert result.checked == 2
    assert result.not_checked == 2
    assert result.not_checked_example == elsewhere


def test_nothing_is_called_missing_when_no_location_is_visible(layout):
    """The reported bug, in miniature: with downloads/ not mounted, every downloads-path track
    used to come back 'missing'."""
    layout["view"].joinpath("downloads").rmdir()
    rows = [(f"t{i}", "A", "N", str(layout["recorded"] / "downloads" / f"{i}.mp3")) for i in range(50)]

    result = dc.find_missing_track_files(rows, layout["resolver"])

    assert result.missing == []
    assert (result.checked, result.not_checked) == (0, 50)
