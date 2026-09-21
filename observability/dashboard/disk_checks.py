"""Filesystem-side checks for the Database tab.

See docs/adr/0008-dashboard-database-explorer.md. These compare what the database says
against what is actually on disk, so they live here rather than in TrackDB. They are
read-only (stat / directory walk / reading an m3u8) and are only ever run on demand: the
track detail view runs `file_facts` and `m3u8_line_state` for one track, and the Audit
view runs the three `find_*` scans when you click Run.

Each `find_*` takes rows the caller already fetched from TrackDB (a single bounded read),
so no database lock is held while files are being stat'ed.
"""

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import NamedTuple

from scripts.constants import SUPPORTED_AUDIO_FORMATS

# Rows an orphan-file scan hands back; the total found is always reported separately.
MAX_LISTED_FILES = 500

# m3u8_line_state() results
STATE_RESOLVED = "resolved_path"
STATE_PLACEHOLDER = "placeholder"
STATE_NOT_LISTED = "not_listed"
STATE_M3U8_MISSING = "m3u8_missing"
STATE_NO_M3U8_PATH = "no_m3u8_path"
STATE_UNREADABLE = "unreadable"

M3U8_STATE_LABELS = {
    STATE_RESOLVED: "Listed with its file path",
    STATE_PLACEHOLDER: "Still a placeholder comment",
    STATE_NOT_LISTED: "Not listed in the file",
    STATE_M3U8_MISSING: "m3u8 file is missing",
    STATE_NO_M3U8_PATH: "No m3u8 path recorded",
    STATE_UNREADABLE: "m3u8 file could not be read",
}


def _normalise(path: str) -> str:
    return os.path.normcase(os.path.normpath(path))


def _is_under(norm_path: str, norm_root: str) -> bool:
    """True if `norm_path` is `norm_root` or inside it. Compares whole path components, so
    `/x/downloads-old/a.mp3` is not under `/x/downloads`."""
    return norm_path == norm_root or norm_path.startswith(norm_root.rstrip(os.sep) + os.sep)


@dataclass(frozen=True)
class PathResolver:
    """Map a track's recorded file path to a path THIS container can stat -- or say it can't.

    A track's recorded path is written by the workflow container, so it names a location in
    that container's filesystem, and the dashboard container mounts only some of those
    directories (imported/ at the same path, downloads/ read-only under an alias). A file
    outside them isn't missing, it is invisible from here, and calling it "missing" would be
    false: the first version of the file checks did exactly that and flagged all 3,371
    completed tracks whose files live in downloads/. So `resolve` is default-deny: it returns
    None for any path not under a root positively known to be mounted, and callers report
    those as "not checked" instead.

    `roots` are (recorded_root, visible_root) pairs: a path under recorded_root is looked up
    under visible_root. If visible_root doesn't exist as a directory the mount is absent, and
    paths under it resolve to None too.
    """

    roots: tuple[tuple[str, str], ...]

    def resolve(self, recorded_path: str) -> str | None:
        norm = _normalise(recorded_path)
        for recorded_root, visible_root in self.roots:
            root = _normalise(recorded_root)
            if _is_under(norm, root):
                if not os.path.isdir(visible_root):
                    return None
                return os.path.join(visible_root, os.path.relpath(norm, root))
        return None


@dataclass(frozen=True)
class FileFacts:
    """What is on disk at a track's recorded local_file_path.

    `exists` is three-valued: True, False, or None when the file's location isn't visible
    from this container so its existence can't be determined at all.
    """

    path: str
    exists: bool | None
    size_bytes: int | None = None
    modified: str | None = None  # UTC, "YYYY-MM-DD HH:MM:SS"


def file_facts(path: str | None, resolver: PathResolver | None = None) -> FileFacts | None:
    """Existence, size and mtime of `path`; None if the track records no path at all.

    With a `resolver`, a path in a location this container can't see comes back with
    exists=None. Without one, `path` is stat'ed as given.
    """
    if not path or not path.strip():
        return None
    target = path if resolver is None else resolver.resolve(path)
    if target is None:
        return FileFacts(path=path, exists=None)
    try:
        stat = os.stat(target)
    except OSError:
        return FileFacts(path=path, exists=False)
    if not os.path.isfile(target):
        return FileFacts(path=path, exists=False)
    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return FileFacts(path=path, exists=True, size_bytes=stat.st_size, modified=modified)


def m3u8_line_state(m3u8_path: str | None, track_id: str, local_file_path: str | None) -> str:
    """Where a track stands in one playlist's m3u8: its resolved file path, its placeholder
    comment (`# track_id - artist - name`, see m3u8_manager), or neither.

    Matches exactly how m3u8_manager writes and updates entries: a completed track's line
    is its local_file_path verbatim, and a pending one is a comment starting `# <track_id> - `.
    """
    if not m3u8_path or not m3u8_path.strip():
        return STATE_NO_M3U8_PATH
    if not os.path.isfile(m3u8_path):
        return STATE_M3U8_MISSING
    try:
        with open(m3u8_path, encoding="utf-8", errors="replace") as f:
            lines = [line.rstrip("\r\n") for line in f]
    except OSError:
        return STATE_UNREADABLE

    wanted_path = local_file_path.strip() if local_file_path else ""
    if wanted_path and any(line.strip() == wanted_path for line in lines):
        return STATE_RESOLVED
    placeholder_prefix = f"# {track_id} - "
    if any(line.startswith(placeholder_prefix) for line in lines):
        return STATE_PLACEHOLDER
    return STATE_NOT_LISTED


class MissingFilesResult(NamedTuple):
    """Outcome of find_missing_track_files.

    `missing` are the rows whose file is verifiably absent. `checked` counts every row whose
    location this container can see; `not_checked` counts the rest (their files may or may
    not exist -- nothing can be said), with `not_checked_example` one such recorded path.
    """

    missing: list[tuple[str, str, str, str]]
    checked: int
    not_checked: int
    not_checked_example: str | None


def find_missing_track_files(
    completed_tracks: list[tuple[str, str, str, str]], resolver: PathResolver | None = None,
) -> MissingFilesResult:
    """Completed tracks whose recorded file isn't on disk.

    Takes (track_id, artist, track_name, local_file_path) rows. With a `resolver`, a row whose
    location isn't visible from this container is counted as not checked rather than missing;
    without one, every path is stat'ed as recorded.
    """
    missing = []
    checked = not_checked = 0
    example = None
    for row in completed_tracks:
        target = row[3] if resolver is None else resolver.resolve(row[3])
        if target is None:
            not_checked += 1
            example = example or row[3]
            continue
        checked += 1
        if not os.path.isfile(target):
            missing.append(row)
    return MissingFilesResult(missing, checked, not_checked, example)


def find_missing_m3u8s(playlists: list[tuple[str, str | None, str | None]]) -> list[tuple[str, str | None, str, str]]:
    """Playlists whose m3u8 file is missing, as (playlist_url, playlist_name, m3u8_path, reason).

    A playlist with no recorded m3u8 path counts too -- it can never have had one written.
    """
    problems = []
    for url, name, m3u8_path in playlists:
        if not m3u8_path or not m3u8_path.strip():
            problems.append((url, name, "", "No m3u8 path recorded"))
        elif not os.path.isfile(m3u8_path):
            problems.append((url, name, m3u8_path, "File not found"))
    return problems


def find_orphan_files(imported_dir: str, known_paths: list[str]) -> tuple[list[tuple[str, int]], int, bool]:
    """Audio files under `imported_dir` that no track's local_file_path points at.

    Returns (listed, total_found, dir_exists): `listed` is (path, size_bytes) for up to
    MAX_LISTED_FILES files, sorted by path, and `total_found` is the full count. Only files
    with a supported audio extension are considered -- imported/ can hold other things
    (cover art, temp files) that are not tracks and are not what this check is about.
    """
    if not os.path.isdir(imported_dir):
        return [], 0, False
    known = {_normalise(p) for p in known_paths}
    orphans: list[tuple[str, int]] = []
    for root, _dirs, files in os.walk(imported_dir):
        for name in files:
            if os.path.splitext(name)[1].lstrip(".").lower() not in SUPPORTED_AUDIO_FORMATS:
                continue
            full = os.path.join(root, name)
            if _normalise(full) in known:
                continue
            try:
                size = os.path.getsize(full)
            except OSError:
                size = 0
            orphans.append((full, size))
    orphans.sort(key=lambda item: item[0])
    return orphans[:MAX_LISTED_FILES], len(orphans), True
