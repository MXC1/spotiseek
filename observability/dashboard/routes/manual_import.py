"""
Manual Import tab routes.

1:1 port of observability/dashboard/tabs/manual_import.py's behaviour (playlist/track
picker with search+paging, single-track file upload with a pre-import quality check,
on-demand iTunes XML export) onto FastAPI + HTMX -- see
docs/adr/0004-dashboard-migration-parallel-service-cutover.md. DB access goes through
TrackDB (docs/adr/0003) instead of the original's ad-hoc sqlite3.connect() calls.

The quality-check-before-import step needs the uploaded bytes to survive between two
separate requests (the file input's on-change precheck, then the later Import click)
without asking the user to pick the file twice. A per-process in-memory dict staging the
saved temp file by track_id is enough for this: this is a local, single-user tool (see
docs/adr/0003's access-model decision), so there is exactly one browser ever using it --
no need for real cookie-backed sessions to keep two users' staged uploads apart.
"""

import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, File, Form, Request, UploadFile

from observability.dashboard.config import DOWNLOADS_ROOT, ENV, IS_DOCKER, XML_DIR, track_db
from observability.dashboard.import_helpers import (
    do_track_import,
    extract_metadata_from_file,
    is_quality_worse_than_mp3_320,
)
from observability.dashboard.templating import templates
from scripts.logs_utils import write_log
from scripts.xml_exporter import export_itunes_xml

router = APIRouter()

_PAGE_SIZES = [10, 25, 50, 100]
_DEFAULT_PAGE_SIZE = 25

# The dropdown's `scope` values: "all" (every track missing a file, playlist or not),
# "folder:<folder name>" (union of a folder's member playlists), "playlist:<playlist url>".
_SCOPE_ALL = "all"
_FOLDER_PREFIX = "folder:"
_PLAYLIST_PREFIX = "playlist:"
_ALL_LABEL = "All Playlists"

# track_id -> {"temp_path", "filename", "artist", "track_name"}, bridging the precheck
# upload to the later Import click without re-uploading the file.
_staged_uploads: dict[str, dict] = {}


def _cleanup_staged(track_id: str) -> None:
    staged = _staged_uploads.pop(track_id, None)
    if staged and os.path.exists(staged["temp_path"]):
        try:
            os.unlink(staged["temp_path"])
        except OSError:
            pass


def _export_itunes_xml() -> tuple[bool, str]:
    try:
        xml_path = os.path.join(XML_DIR, f"library_{ENV}.xml")
        downloads_path = DOWNLOADS_ROOT
        if IS_DOCKER:
            host_base_path = os.getenv("HOST_BASE_PATH")
            if host_base_path and downloads_path.startswith("/app/"):
                downloads_path = downloads_path.replace("/app/", f"{host_base_path}/", 1)
        music_folder_url = f"file://localhost/{downloads_path.replace(os.sep, '/')}/"
        export_itunes_xml(xml_path, music_folder_url)
        write_log.info("MANUAL_IMPORT_XML_EXPORTED", "Exported iTunes XML from manual import tab.",
                        {"xml_path": xml_path, "music_folder_url": music_folder_url})
        return True, f"iTunes XML exported to {xml_path}"
    except Exception as e:
        write_log.error("MANUAL_IMPORT_XML_EXPORT_FAIL", "Failed to export iTunes XML from manual import tab.",
                         {"error": str(e)})
        return False, f"Failed to export iTunes XML: {e}"


def _fetch_tracks(scope: str, search: str, offset: int, limit: int):
    """Dispatch a validated scope value to the matching TrackDB query."""
    if scope.startswith(_FOLDER_PREFIX):
        return track_db.get_incomplete_tracks_for_folder(scope[len(_FOLDER_PREFIX):], search, offset, limit)
    if scope.startswith(_PLAYLIST_PREFIX):
        return track_db.get_incomplete_tracks_for_playlist(scope[len(_PLAYLIST_PREFIX):], search, offset, limit)
    return track_db.get_incomplete_tracks(search, offset, limit)


def _tracks_context(scope: str | None, search: str, page: int, page_size: int, total_incomplete: int) -> dict:
    playlists = track_db.get_playlists_with_incomplete_counts()
    folders = track_db.get_folders_with_incomplete_counts()

    # Every dropdown value -> its display name; anything else (stale, or a folder/playlist that
    # has since been fully imported and dropped off the list) falls back to All Playlists.
    label_by_scope = {
        _SCOPE_ALL: _ALL_LABEL,
        **{f"{_FOLDER_PREFIX}{name}": name for name, _ in folders},
        **{f"{_PLAYLIST_PREFIX}{url}": name for name, url, _ in playlists},
    }
    if scope not in label_by_scope:
        scope = _SCOPE_ALL

    page_size = page_size if page_size in _PAGE_SIZES else _DEFAULT_PAGE_SIZE
    page = max(page, 1)
    offset = (page - 1) * page_size

    rows, total = _fetch_tracks(scope, search, offset, page_size)
    if offset != 0 and offset >= max(total, 1):
        page, offset = 1, 0
        rows, total = _fetch_tracks(scope, search, offset, page_size)

    tracks = [{"track_id": r[0], "track_name": r[1], "artist": r[2], "status": r[3]} for r in rows]

    return {
        "all_scope": {"value": _SCOPE_ALL, "label": _ALL_LABEL, "count": total_incomplete},
        "folder_scopes": [
            {"value": f"{_FOLDER_PREFIX}{name}", "label": name, "count": count} for name, count in folders
        ],
        "playlist_scopes": [
            {"value": f"{_PLAYLIST_PREFIX}{url}", "label": name, "count": count} for name, url, count in playlists
        ],
        "total_associations": sum(count for _, _, count in playlists),
        "selected_scope": scope,
        "selected_scope_label": label_by_scope[scope],
        "search": search or "",
        "page": page,
        "page_size": page_size,
        "page_sizes": _PAGE_SIZES,
        "total_for_scope": total,
        "max_page": max((total + page_size - 1) // page_size, 1),
        "tracks": tracks,
    }


def _body_context(scope: str | None, search: str, page: int, page_size: int) -> dict:
    if track_db is None:
        return {"db_error": "Database is not available."}

    total_incomplete = track_db.get_total_incomplete_tracks()
    context = {
        "db_error": None,
        "env_value": ENV,
        "total_incomplete": total_incomplete,
        "flash": None,
    }
    if total_incomplete:
        context.update(_tracks_context(scope, search, page, page_size, total_incomplete))
    return context


@router.get("/manual-import/body")
def manual_import_body(
    request: Request,
    scope: str | None = None,
    search: str = "",
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
):
    return templates.TemplateResponse(
        request, "tabs/_manual_import_body.html", _body_context(scope, search, page, page_size),
    )


@router.post("/manual-import/precheck")
async def manual_import_precheck(
    request: Request,
    track_id: str = Form(...),
    artist: str = Form(...),
    track_name: str = Form(...),
    audio_file: UploadFile = File(...),
):
    _cleanup_staged(track_id)

    suffix = Path(audio_file.filename or "").suffix
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await audio_file.read())
        temp_path = tmp.name

    metadata = extract_metadata_from_file(temp_path)
    is_worse, reason = is_quality_worse_than_mp3_320(metadata.get("extension") or "", metadata.get("bitrate"))

    _staged_uploads[track_id] = {
        "temp_path": temp_path, "filename": audio_file.filename, "artist": artist, "track_name": track_name,
    }

    return templates.TemplateResponse(request, "tabs/_manual_import_upload_result.html", {
        "track_id": track_id,
        "filename": audio_file.filename,
        "quality_warning": reason if is_worse else None,
    })


@router.post("/manual-import/import/{track_id:path}")
def manual_import_import(
    request: Request,
    track_id: str,
    scope: str = Form(_SCOPE_ALL),
    search: str = Form(""),
    page: int = Form(1),
    page_size: int = Form(_DEFAULT_PAGE_SIZE),
):
    staged = _staged_uploads.get(track_id)
    if not staged:
        flash = {"type": "error", "text": "No file staged for this track -- choose a file first."}
    else:
        success, message = do_track_import(track_id, staged["temp_path"], staged["artist"], staged["track_name"])
        _cleanup_staged(track_id)
        flash = {"type": "success" if success else "error", "text": message}

    context = _body_context(scope, search, page, page_size)
    context["flash"] = flash
    return templates.TemplateResponse(request, "tabs/_manual_import_body.html", context)


@router.post("/manual-import/export-xml")
def manual_import_export_xml(request: Request):
    success, message = _export_itunes_xml()
    return templates.TemplateResponse(request, "tabs/_manual_import_export_status.html", {
        "flash": {"type": "success" if success else "error", "text": message},
    })


@router.get("/manual-import")
def manual_import_tab(request: Request):
    """The whole Manual Import tab: full page on direct navigation, tab fragment on HTMX nav."""
    context = _body_context(None, "", 1, _DEFAULT_PAGE_SIZE)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/manual_import_tab.html", context)

    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/manual_import_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
