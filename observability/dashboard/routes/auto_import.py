"""
Auto Import tab routes.

1:1 port of observability/dashboard/tabs/auto_import.py's behaviour (directory browser,
recursive scan + mutagen metadata extraction, rapidfuzz-based fuzzy matching against
tracks missing local files, paginated/filterable checkbox-selectable results, batch
import) onto FastAPI + HTMX + Alpine -- see
docs/adr/0004-dashboard-migration-parallel-service-cutover.md.

Selection state (which matches are checked) and scan results must survive across many
separate requests (paging, filtering, checkbox toggles) -- this is exactly the
"in-memory server-side session" case docs/adr/0003-dashboard-rewrite-fastapi-htmx.md
anticipated. A single process-global dict is enough (not cookie-keyed, unlike a real
multi-user session store): this is a local, single-user tool, the same reasoning
manual_import.py's `_staged_uploads` already relies on.

One deliberate simplification vs the original: imports run synchronously behind an
`.htmx-indicator` spinner rather than reporting item-by-item progress (Streamlit's
`st.progress` bar). Building real incremental progress would need polling or SSE
infrastructure for a cosmetic nicety in a local single-user tool; not worth it here.
"""

import os
import string
from typing import Any

from fastapi import APIRouter, Form, Request
from mutagen import File as MutagenFile
from rapidfuzz import fuzz

from observability.dashboard.config import ENV, IS_DOCKER, track_db
from observability.dashboard.import_helpers import do_track_import, is_quality_worse_than_mp3_320
from observability.dashboard.templating import templates
from scripts.constants import SUPPORTED_AUDIO_FORMATS
from scripts.logs_utils import write_log

router = APIRouter()

_AUDIO_EXTENSIONS = {f".{ext}" for ext in SUPPORTED_AUDIO_FORMATS}
_MIN_TITLE_SCORE = 80
_MIN_ARTIST_SCORE = 70
_PAGE_SIZES = [25, 50, 100, 200]
_DEFAULT_PAGE_SIZE = 25
_DEFAULT_MIN_SCORE = 70
_MAX_SUBDIRS_SHOWN = 200
_MAX_LOW_QUALITY_SHOWN = 20

_state: dict[str, Any] = {
    "browse_dir": None,
    "source_dir": "",
    "scanned_dir": None,
    "matches": None,  # list[dict] from the last scan, or None until a scan runs
    "selected_keys": set(),
}


def _match_key(track_id: str, file_path: str) -> str:
    return f"{track_id}::{file_path}"


# ---------------------------------------------------------------------------
# Filesystem browsing
# ---------------------------------------------------------------------------

def _get_browse_roots() -> list[str]:
    if os.name == "nt":
        roots = [f"{d}:\\" for d in string.ascii_uppercase if os.path.exists(f"{d}:\\")]
        if roots:
            return roots
    return ["/"]


def _default_browse_dir(roots: list[str]) -> str:
    if IS_DOCKER:
        for candidate in ("/mnt", "/app"):
            if os.path.isdir(candidate):
                return candidate
    return roots[0]


def _list_subdirectories(path: str) -> list[str]:
    try:
        entries = os.scandir(path)
    except OSError:
        return []
    dirs = []
    with entries:
        for entry in entries:
            try:
                if entry.is_dir():
                    dirs.append(entry.name)
            except OSError:
                continue
    return sorted(dirs, key=str.lower)


def _ensure_browse_dir() -> str:
    if not _state["browse_dir"] or not os.path.isdir(_state["browse_dir"]):
        _state["browse_dir"] = _default_browse_dir(_get_browse_roots())
    return _state["browse_dir"]


# ---------------------------------------------------------------------------
# Scanning & matching
# ---------------------------------------------------------------------------

def _extract_scan_metadata(file_path: str, extension: str) -> dict:
    info = {
        "metadata_artist": None, "metadata_title": None,
        "bitrate": None, "is_low_quality": False, "quality_warning": None,
    }
    try:
        audio = MutagenFile(file_path, easy=True)
        if audio:
            for tag in ("artist", "albumartist", "performer"):
                if audio.get(tag):
                    info["metadata_artist"] = audio[tag][0]
                    break
            if audio.get("title"):
                info["metadata_title"] = audio["title"][0]

        audio_full = MutagenFile(file_path, easy=False)
        if audio_full is not None and getattr(audio_full.info, "bitrate", None):
            info["bitrate"] = int(audio_full.info.bitrate / 1000)

        is_worse, reason = is_quality_worse_than_mp3_320(extension, info["bitrate"])
        info["is_low_quality"] = is_worse
        info["quality_warning"] = reason if is_worse else None
    except Exception as e:
        write_log.debug("AUTO_IMPORT_METADATA_FAIL", "Failed to extract metadata.",
                         {"file_path": file_path, "error": str(e)})
    return info


def scan_directory_for_audio_files(directory: str) -> list[dict]:
    """Recursively scan a directory for audio files and extract metadata."""
    audio_files = []
    if not os.path.isdir(directory):
        write_log.warn("AUTO_IMPORT_INVALID_DIR", "Invalid directory path.", {"directory": directory})
        return audio_files

    write_log.info("AUTO_IMPORT_SCAN_START", "Starting directory scan.", {"directory": directory})

    for root, _dirs, files in os.walk(directory):
        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in _AUDIO_EXTENSIONS:
                continue

            file_path = os.path.join(root, filename)
            extension = ext.lstrip(".")
            file_info = {
                "file_path": file_path,
                "filename": filename,
                "extension": extension,
                "parsed_artist": None,
                "parsed_title": None,
                **_extract_scan_metadata(file_path, extension),
            }

            name_without_ext = os.path.splitext(filename)[0]
            if " - " in name_without_ext:
                parts = name_without_ext.split(" - ", 1)
                file_info["parsed_artist"] = parts[0].strip()
                file_info["parsed_title"] = parts[1].strip()
            else:
                file_info["parsed_title"] = name_without_ext.strip()

            audio_files.append(file_info)

    write_log.info("AUTO_IMPORT_SCAN_COMPLETE", "Directory scan complete.",
                    {"directory": directory, "files_found": len(audio_files)})
    return audio_files


def get_best_artist_title(file_info: dict) -> tuple[str, str]:
    artist = file_info.get("metadata_artist") or file_info.get("parsed_artist") or ""
    title = file_info.get("metadata_title") or file_info.get("parsed_title") or ""
    return artist, title


def calculate_match_score(file_info: dict, track: dict) -> dict:
    """Fuzzy match score between a source file and a DB track.

    Both title and artist must independently clear their thresholds -- prevents
    false positives from an artist-only or title-only match.
    """
    file_artist, file_title = get_best_artist_title(file_info)
    track_artist = track.get("artist") or ""
    track_title = track.get("track_name") or ""

    scores = []

    if file_title and file_artist and track_artist:
        title_score = fuzz.token_sort_ratio(file_title.lower(), track_title.lower())
        artist_score = fuzz.token_sort_ratio(file_artist.lower(), track_artist.lower())
        if title_score >= _MIN_TITLE_SCORE and artist_score >= _MIN_ARTIST_SCORE:
            scores.append({
                "score": (title_score * 0.5) + (artist_score * 0.5),
                "match_type": "artist+title", "title_score": title_score, "artist_score": artist_score,
            })

    if file_artist and file_title and track_artist:
        title_score = fuzz.token_sort_ratio(file_title.lower(), track_title.lower())
        artist_score = fuzz.token_sort_ratio(file_artist.lower(), track_artist.lower())
        if title_score >= _MIN_TITLE_SCORE and artist_score >= _MIN_ARTIST_SCORE:
            file_combined = f"{file_artist} - {file_title}"
            track_combined = f"{track_artist} - {track_title}"
            scores.append({
                "score": fuzz.token_sort_ratio(file_combined.lower(), track_combined.lower()),
                "match_type": "combined_string", "title_score": title_score, "artist_score": artist_score,
            })

    if track_artist and track_title:
        filename_lower = file_info["filename"].lower()
        title_in_filename = fuzz.partial_ratio(track_title.lower(), filename_lower)
        artist_in_filename = fuzz.partial_ratio(track_artist.lower(), filename_lower)
        if title_in_filename >= _MIN_TITLE_SCORE and artist_in_filename >= _MIN_ARTIST_SCORE:
            track_combined = f"{track_artist} - {track_title}"
            scores.append({
                "score": fuzz.token_sort_ratio(filename_lower, track_combined.lower()),
                "match_type": "filename", "title_score": title_in_filename, "artist_score": artist_in_filename,
            })

    if not scores:
        return {"score": 0, "match_type": "no_match", "title_score": 0, "artist_score": 0}
    return max(scores, key=lambda x: x["score"])


def find_matches_for_tracks(audio_files: list[dict], tracks: list[dict]) -> list[dict]:
    matches = []
    for track in tracks:
        for file_info in audio_files:
            score_info = calculate_match_score(file_info, track)
            file_artist, file_title = get_best_artist_title(file_info)
            matches.append({
                "track_id": track["track_id"],
                "track_name": track["track_name"],
                "track_artist": track["artist"],
                "track_playlists": track["playlists"],
                "file_path": file_info["file_path"],
                "file_name": file_info["filename"],
                "file_artist": file_artist,
                "file_title": file_title,
                "file_extension": file_info["extension"],
                "file_bitrate": file_info.get("bitrate"),
                "is_low_quality": file_info.get("is_low_quality", False),
                "quality_warning": file_info.get("quality_warning"),
                "score": score_info["score"],
                "match_type": score_info["match_type"],
            })
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches


def get_score_color(score: float) -> str:
    if score >= 90:
        return "\U0001f7e2"
    if score >= 70:
        return "\U0001f7e1"
    if score >= 50:
        return "\U0001f7e0"
    return "\U0001f534"


# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------

def _source_panel_context(browse_filter: str = "", flash: dict | None = None) -> dict:
    browse_dir = _ensure_browse_dir()
    subdirs = _list_subdirectories(browse_dir)
    if browse_filter:
        subdirs = [d for d in subdirs if browse_filter.lower() in d.lower()]
    parent_dir = os.path.dirname(browse_dir.rstrip("\\/")) or browse_dir
    can_go_up = parent_dir != browse_dir and os.path.isdir(parent_dir)
    roots = _get_browse_roots()

    return {
        "is_docker": IS_DOCKER,
        "roots": roots,
        "multi_root": len(roots) > 1,
        "browse_dir": browse_dir,
        "browse_filter": browse_filter,
        "can_go_up": can_go_up,
        "subdirs": subdirs[:_MAX_SUBDIRS_SHOWN],
        "subdirs_total": len(subdirs),
        "subdirs_truncated": len(subdirs) > _MAX_SUBDIRS_SHOWN,
        "source_dir": _state["source_dir"],
        "scan_flash": flash,
    }


def _selection_summary_context() -> dict:
    matches = _state["matches"] or []
    lookup = {_match_key(m["track_id"], m["file_path"]): m for m in matches}
    selected_keys = [k for k in _state["selected_keys"] if k in lookup]
    low_quality = [lookup[k] for k in selected_keys if lookup[k].get("is_low_quality")]
    return {
        "selected_count": len(selected_keys),
        "low_quality_selected": low_quality[:_MAX_LOW_QUALITY_SHOWN],
        "low_quality_selected_total": len(low_quality),
    }


def _matches_context(min_score: int, search: str, page: int, page_size: int) -> dict:
    matches = _state["matches"]
    if matches is None:
        return {"matches": None}

    search_lower = (search or "").lower()
    filtered = [
        m for m in matches
        if m["score"] >= min_score
        and (not search_lower
             or search_lower in m["track_name"].lower()
             or search_lower in m["track_artist"].lower()
             or search_lower in m["file_name"].lower())
    ]

    page_size = page_size if page_size in _PAGE_SIZES else _DEFAULT_PAGE_SIZE
    total = len(filtered)
    total_pages = max((total + page_size - 1) // page_size, 1)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * page_size
    end = min(start + page_size, total)
    page_matches = filtered[start:end]

    rows = []
    for m in page_matches:
        key = _match_key(m["track_id"], m["file_path"])
        rows.append({
            **m,
            "key": key,
            "selected": key in _state["selected_keys"],
            "score_emoji": get_score_color(m["score"]),
        })

    return {
        "matches": matches,
        "scanned_dir": _state["scanned_dir"],
        "min_score": min_score,
        "search": search or "",
        "page_size": page_size,
        "page_sizes": _PAGE_SIZES,
        "page": page,
        "total_pages": total_pages,
        "total_matches": total,
        "start_idx": start + 1 if total else 0,
        "end_idx": end,
        "rows": rows,
        **_selection_summary_context(),
    }


def _body_context(flash: dict | None = None) -> dict:
    return {
        "env_value": ENV,
        **_source_panel_context(flash=flash),
        **_matches_context(_DEFAULT_MIN_SCORE, "", 1, _DEFAULT_PAGE_SIZE),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/auto-import/source-panel")
def auto_import_source_panel(
    request: Request,
    browse_action: str = "none",
    browse_value: str = "",
    browse_filter: str = "",
):
    browse_dir = _ensure_browse_dir()

    if browse_action == "root" and browse_value:
        _state["browse_dir"] = browse_value
    elif browse_action == "up":
        parent = os.path.dirname(browse_dir.rstrip("\\/")) or browse_dir
        if os.path.isdir(parent):
            _state["browse_dir"] = parent
    elif browse_action == "into" and browse_value:
        candidate = os.path.join(browse_dir, browse_value)
        if os.path.isdir(candidate):
            _state["browse_dir"] = candidate
    elif browse_action == "use":
        _state["source_dir"] = browse_dir

    context = _source_panel_context(browse_filter)
    return templates.TemplateResponse(request, "tabs/_auto_import_source_panel.html", context)


@router.post("/auto-import/scan")
def auto_import_scan(request: Request, source_dir: str = Form(...)):
    _state["source_dir"] = source_dir

    if not os.path.isdir(source_dir):
        _state["matches"] = None
        flash = {"type": "error", "text": f"Directory not found: {source_dir}"}
        return templates.TemplateResponse(request, "tabs/_auto_import_body.html", _body_context(flash))

    audio_files = scan_directory_for_audio_files(source_dir)
    if not audio_files:
        _state["matches"] = None
        flash = {"type": "error", "text": "No audio files found in the specified directory."}
        return templates.TemplateResponse(request, "tabs/_auto_import_body.html", _body_context(flash))

    tracks = [
        {"track_id": r[0], "track_name": r[1], "artist": r[2], "status": r[3], "playlists": r[4] or "Unknown"}
        for r in track_db.get_all_incomplete_tracks_with_playlists()
    ]
    if not tracks:
        _state["matches"] = None
        flash = {"type": "success", "text": "All tracks have been downloaded! Nothing to match."}
        return templates.TemplateResponse(request, "tabs/_auto_import_body.html", _body_context(flash))

    matches = find_matches_for_tracks(audio_files, tracks)
    _state["matches"] = matches
    _state["scanned_dir"] = source_dir
    _state["selected_keys"] = set()
    flash = {"type": "success", "text": f"Found {len(audio_files)} audio files and {len(matches)} potential matches."}
    return templates.TemplateResponse(request, "tabs/_auto_import_body.html", _body_context(flash))


@router.get("/auto-import/matches-panel")
def auto_import_matches_panel(
    request: Request,
    min_score: int = _DEFAULT_MIN_SCORE,
    search: str = "",
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
):
    return templates.TemplateResponse(
        request, "tabs/_auto_import_matches_panel.html", _matches_context(min_score, search, page, page_size),
    )


@router.post("/auto-import/select")
def auto_import_select(request: Request, key: str = Form(...), checked: str = Form(...)):
    if checked == "true":
        _state["selected_keys"].add(key)
    else:
        _state["selected_keys"].discard(key)
    return templates.TemplateResponse(request, "tabs/_auto_import_selection_summary.html", _selection_summary_context())


@router.post("/auto-import/clear-selection")
def auto_import_clear_selection(
    request: Request,
    min_score: int = Form(_DEFAULT_MIN_SCORE),
    search: str = Form(""),
    page: int = Form(1),
    page_size: int = Form(_DEFAULT_PAGE_SIZE),
):
    _state["selected_keys"] = set()
    return templates.TemplateResponse(
        request, "tabs/_auto_import_matches_panel.html", _matches_context(min_score, search, page, page_size),
    )


@router.post("/auto-import/import")
def auto_import_import(request: Request):
    matches = _state["matches"] or []
    lookup = {_match_key(m["track_id"], m["file_path"]): m for m in matches}

    success_count = 0
    fail_count = 0
    imported_track_ids: set[str] = set()

    for key in list(_state["selected_keys"]):
        match = lookup.get(key)
        if not match or match["track_id"] in imported_track_ids:
            continue
        success, message = do_track_import(
            match["track_id"], match["file_path"], match["track_artist"], match["track_name"],
        )
        if success:
            success_count += 1
            imported_track_ids.add(match["track_id"])
        else:
            fail_count += 1
            write_log.warn("AUTO_IMPORT_TRACK_FAILED", message, {"track_id": match["track_id"]})

    _state["selected_keys"] = set()
    _state["matches"] = None
    _state["scanned_dir"] = None

    parts = []
    if success_count:
        parts.append(f"Successfully imported {success_count} tracks!")
    if fail_count:
        parts.append(f"Failed to import {fail_count} tracks.")
    flash = {
        "type": "error" if success_count == 0 and fail_count > 0 else "success",
        "text": " ".join(parts) or "Nothing was selected.",
    }
    return templates.TemplateResponse(request, "tabs/_auto_import_body.html", _body_context(flash))


@router.get("/auto-import")
def auto_import_tab(request: Request):
    """The whole Auto Import tab: full page on direct navigation, tab fragment on HTMX nav."""
    context = _body_context()

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/auto_import_tab.html", context)

    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/auto_import_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
