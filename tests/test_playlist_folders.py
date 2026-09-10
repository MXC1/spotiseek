"""Tests for playlist folders.

Covers the three layers of the feature:
- CSV parsing into folder-aware playlist occurrences (``read_playlist_entries_from_csv``)
- membership storage and rebuild (``playlist_folder_memberships`` table)
- iTunes XML export: folder dicts, duplicate entries for multi-folder playlists,
  parent linkage, union track lists, and deterministic output.
"""

import os
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

os.environ.setdefault("APP_ENV", "test")

from scripts.database_management import TrackData, TrackDB
from scripts.workflow import read_playlist_entries_from_csv, read_playlists_from_csv
from scripts.xml_exporter import export_itunes_xml

# ---------------------------------------------------------------------------
# Fixtures / helpers
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


def _write_csv(tmp_path, text):
    path = tmp_path / "playlists.csv"
    path.write_text(text, encoding="utf-8")
    return str(path)


def _add_downloaded_track(db, track_id, local_file_path):
    db.add_track(TrackData(
        track_id=track_id, track_name=f"Name {track_id}", artist="Artist",
        source="spotify", download_status="completed",
    ))
    db.update_local_file_path(track_id, local_file_path)


def _plist_value(el):
    if el.tag in ("true", "false"):
        return el.tag == "true"
    if el.tag == "integer":
        return int(el.text)
    if el.tag == "array":
        return [_dict_to_map(c) if c.tag == "dict" else _plist_value(c) for c in el]
    if el.tag == "dict":
        return _dict_to_map(el)
    return el.text


def _dict_to_map(dict_el):
    out = {}
    kids = list(dict_el)
    for i in range(0, len(kids) - 1, 2):
        if kids[i].tag == "key":
            out[kids[i].text] = _plist_value(kids[i + 1])
    return out


def _playlist_dicts(xml_path):
    """Return the Playlists array of an exported library as a list of plain dicts."""
    root_dict = ET.parse(xml_path).getroot().find("dict")
    kids = list(root_dict)
    key_idx = next(i for i, c in enumerate(kids) if c.tag == "key" and c.text == "Playlists")
    return [_dict_to_map(d) for d in kids[key_idx + 1].findall("dict")]


FIXTURE_CSV = """\
https://sc/root-a # root a
https://spotify/root-b # root b

# Folder Called ABC
https://sc/abc-1 # abc one
https://spotify/abc-2 # abc two
https://sc/root-a # root-a also lives in ABC

# Folder Called DEF
https://spotify/def-1
https://sc/def-2
"""


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

class TestParser:
    def test_root_occurrences_have_empty_folder(self, tmp_path):
        entries = read_playlist_entries_from_csv(_write_csv(tmp_path, FIXTURE_CSV))
        assert [e.url for e in entries if e.folder == ""] == [
            "https://sc/root-a", "https://spotify/root-b",
        ]

    def test_folder_scope_runs_to_next_heading(self, tmp_path):
        entries = read_playlist_entries_from_csv(_write_csv(tmp_path, FIXTURE_CSV))
        assert [e.url for e in entries if e.folder == "Folder Called ABC"] == [
            "https://sc/abc-1", "https://spotify/abc-2", "https://sc/root-a",
        ]
        assert [e.url for e in entries if e.folder == "Folder Called DEF"] == [
            "https://spotify/def-1", "https://sc/def-2",
        ]

    def test_membership_is_additive(self, tmp_path):
        entries = read_playlist_entries_from_csv(_write_csv(tmp_path, FIXTURE_CSV))
        assert sorted(e.folder for e in entries if e.url == "https://sc/root-a") == [
            "", "Folder Called ABC",
        ]

    def test_blank_lines_do_not_close_a_folder(self, tmp_path):
        entries = read_playlist_entries_from_csv(
            _write_csv(tmp_path, "# F\nhttps://a\n\n\nhttps://b\n"),
        )
        assert [(e.url, e.folder) for e in entries] == [("https://a", "F"), ("https://b", "F")]

    def test_bare_hash_is_a_separator_not_a_folder(self, tmp_path):
        entries = read_playlist_entries_from_csv(
            _write_csv(tmp_path, "https://a\n#\nhttps://b\n"),
        )
        assert [e.folder for e in entries] == ["", ""]

    def test_bare_hash_inside_a_folder_keeps_the_scope(self, tmp_path):
        entries = read_playlist_entries_from_csv(
            _write_csv(tmp_path, "# F\nhttps://a\n#   \nhttps://b\n"),
        )
        assert [e.folder for e in entries] == ["F", "F"]

    def test_inline_annotation_is_stripped(self, tmp_path):
        entries = read_playlist_entries_from_csv(
            _write_csv(tmp_path, "https://a # some note # with hashes\n"),
        )
        assert entries[0].url == "https://a"

    def test_same_name_headings_merge(self, tmp_path):
        entries = read_playlist_entries_from_csv(
            _write_csv(tmp_path, "# ABC\nhttps://a\n# DEF\nhttps://b\n# ABC\nhttps://c\n"),
        )
        assert [e.url for e in entries if e.folder == "ABC"] == ["https://a", "https://c"]

    def test_csv_sequence_is_dense_and_ordered(self, tmp_path):
        entries = read_playlist_entries_from_csv(_write_csv(tmp_path, FIXTURE_CSV))
        assert [e.csv_sequence for e in entries] == list(range(len(entries)))

    def test_read_playlists_from_csv_dedupes_by_first_appearance(self, tmp_path):
        assert read_playlists_from_csv(_write_csv(tmp_path, FIXTURE_CSV)) == [
            "https://sc/root-a", "https://spotify/root-b",
            "https://sc/abc-1", "https://spotify/abc-2",
            "https://spotify/def-1", "https://sc/def-2",
        ]


# ---------------------------------------------------------------------------
# Membership storage
# ---------------------------------------------------------------------------

class TestMembershipStorage:
    def test_replace_populates_rows_in_order(self, db):
        db.replace_playlist_folder_memberships([
            ("u1", "", 0), ("u2", "ABC", 1), ("u1", "ABC", 2),
        ])
        assert db.get_playlist_folder_memberships() == [
            ("u1", "", 0), ("u2", "ABC", 1), ("u1", "ABC", 2),
        ]

    def test_replace_wipes_stale_rows(self, db):
        db.replace_playlist_folder_memberships([("u1", "ABC", 0), ("u2", "ABC", 1)])
        db.replace_playlist_folder_memberships([("u1", "DEF", 0)])
        assert db.get_playlist_folder_memberships() == [("u1", "DEF", 0)]

    def test_duplicate_pair_collapses_first_wins(self, db):
        db.replace_playlist_folder_memberships([("u1", "ABC", 0), ("u1", "ABC", 9)])
        assert db.get_playlist_folder_memberships() == [("u1", "ABC", 0)]

    def test_rows_ordered_by_csv_sequence(self, db):
        db.replace_playlist_folder_memberships([
            ("u3", "Z", 2), ("u1", "", 0), ("u2", "A", 1),
        ])
        assert [r[2] for r in db.get_playlist_folder_memberships()] == [0, 1, 2]

    def test_delete_playlist_removes_its_memberships(self, db):
        db.replace_playlist_folder_memberships([("u1", "ABC", 0), ("u2", "ABC", 1)])
        db.delete_playlist("u1")
        assert db.get_playlist_folder_memberships() == [("u2", "ABC", 1)]


# ---------------------------------------------------------------------------
# iTunes XML export
# ---------------------------------------------------------------------------

class TestXmlExport:
    @pytest.fixture()
    def library(self, db, tmp_path):
        audio = tmp_path / "audio.wav"
        audio.write_text("fake audio", encoding="utf-8")
        for track_id in ("t1", "t2", "t3", "t4"):
            _add_downloaded_track(db, track_id, str(audio))

        db.add_playlist("u_root", playlist_name="Root One")
        db.add_playlist("u_abc", playlist_name="ABC One")
        db.add_playlist("u_multi", playlist_name="Multi")
        db.link_track_to_playlist("t1", "u_root")
        db.link_track_to_playlist("t2", "u_abc")
        db.link_track_to_playlist("t3", "u_multi")
        db.link_track_to_playlist("t4", "u_multi")

        # u_root -> root; u_abc -> ABC; u_multi -> root AND ABC AND DEF
        db.replace_playlist_folder_memberships([
            ("u_root", "", 0),
            ("u_multi", "", 1),
            ("u_abc", "ABC", 2),
            ("u_multi", "ABC", 3),
            ("u_multi", "DEF", 4),
        ])

        xml_path = str(tmp_path / "Library.xml")
        export_itunes_xml(xml_path)
        return xml_path

    def test_folder_dicts_emitted_for_each_folder(self, library):
        folders = [d for d in _playlist_dicts(library) if d.get("Folder") is True]
        assert {d["Name"] for d in folders} == {"ABC", "DEF"}
        assert all("All Items" in d for d in folders)

    def test_root_playlist_has_no_parent(self, library):
        root_one = [d for d in _playlist_dicts(library) if d["Name"] == "Root One"]
        assert len(root_one) == 1
        assert "Parent Persistent ID" not in root_one[0]

    def test_multi_folder_playlist_is_duplicated_with_distinct_ids(self, library):
        multi = [d for d in _playlist_dicts(library) if d["Name"] == "Multi"]
        assert len(multi) == 3  # root + ABC + DEF
        assert len({d["Playlist Persistent ID"] for d in multi}) == 3
        parents = sorted(d.get("Parent Persistent ID", "") for d in multi)
        assert parents[0] == ""          # the root copy
        assert parents[1] and parents[2]  # the two foldered copies

    def test_members_reference_their_folder_as_parent(self, library):
        dicts = _playlist_dicts(library)
        abc = next(d for d in dicts if d.get("Folder") and d["Name"] == "ABC")
        members = [d for d in dicts if d.get("Parent Persistent ID") == abc["Playlist Persistent ID"]]
        assert {d["Name"] for d in members} == {"ABC One", "Multi"}

    def test_playlist_listed_only_in_folders_has_no_root_copy(self, library):
        abc_one = [d for d in _playlist_dicts(library) if d["Name"] == "ABC One"]
        assert len(abc_one) == 1
        assert abc_one[0].get("Parent Persistent ID")

    def test_folder_items_are_the_union_of_member_tracks(self, library):
        abc = next(d for d in _playlist_dicts(library) if d.get("Folder") and d["Name"] == "ABC")
        # ABC = u_abc (t2) + u_multi (t3, t4) -> 3 unique tracks
        assert len(abc["Playlist Items"]) == 3

    def test_root_playlists_sort_before_folders(self, library):
        dicts = _playlist_dicts(library)
        first_folder = next(i for i, d in enumerate(dicts) if d.get("Folder"))
        assert all(not dicts[i].get("Parent Persistent ID") for i in range(first_folder))
        assert not any(dicts[i].get("Folder") for i in range(first_folder))

    def test_export_is_byte_identical_on_repeat(self, library, tmp_path):
        again = str(tmp_path / "Library-again.xml")
        export_itunes_xml(again)
        assert Path(library).read_text(encoding="utf-8") == Path(again).read_text(encoding="utf-8")

    def test_flat_export_when_no_memberships_recorded(self, db, tmp_path):
        audio = tmp_path / "audio.wav"
        audio.write_text("fake audio", encoding="utf-8")
        _add_downloaded_track(db, "t1", str(audio))
        db.add_playlist("u1", playlist_name="Only One")
        db.link_track_to_playlist("t1", "u1")
        db.set_playlist_display_order("u1", 0)

        xml_path = str(tmp_path / "Flat.xml")
        export_itunes_xml(xml_path)

        dicts = _playlist_dicts(xml_path)
        assert len(dicts) == 1
        assert dicts[0]["Name"] == "Only One"
        assert not dicts[0].get("Folder")
