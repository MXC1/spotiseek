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

from observability.dashboard_next.config import DOWNLOADS_ROOT, ENV, IS_DOCKER, XML_DIR, track_db
from observability.dashboard_next.import_helpers import (
    do_track_import,
    extract_metadata_from_file,
    is_quality_worse_than_mp3_320,
)
from observability.dashboard_next.templating import templates
from scripts.logs_utils import write_log
from scripts.xml_exporter import export_itunes_xml

router = APIRouter()

_PAGE_SIZES = [10, 25, 50, 100]
_DEFAULT_PAGE_SIZE = 25

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


def _tracks_context(playlist_url: str | None, search: str, page: int, page_size: int) -> dict:
    playlists = track_db.get_playlists_with_incomplete_counts()
    if not playlists:
        return {"playlists": []}

    playlist_by_url = {p[1]: p[0] for p in playlists}
    if playlist_url not in playlist_by_url:
        playlist_url = playlists[0][1]
    selected_playlist_name = playlist_by_url[playlist_url]

    page_size = page_size if page_size in _PAGE_SIZES else _DEFAULT_PAGE_SIZE
    page = max(page, 1)
    offset = (page - 1) * page_size

    rows, total = track_db.get_incomplete_tracks_for_playlist(playlist_url, search, offset, page_size)
    if offset != 0 and offset >= max(total, 1):
        page, offset = 1, 0
        rows, total = track_db.get_incomplete_tracks_for_playlist(playlist_url, search, offset, page_size)

    tracks = [
        {"playlist_url": r[0], "track_id": r[1], "track_name": r[2], "artist": r[3], "status": r[4]}
        for r in rows
    ]

    return {
        "playlists": playlists,
        "selected_playlist_url": playlist_url,
        "selected_playlist_name": selected_playlist_name,
        "search": search or "",
        "page": page,
        "page_size": page_size,
        "page_sizes": _PAGE_SIZES,
        "total_for_playlist": total,
        "max_page": max((total + page_size - 1) // page_size, 1),
        "tracks": tracks,
    }


def _body_context(playlist_url: str | None, search: str, page: int, page_size: int) -> dict:
    if track_db is None:
        return {"db_error": "Database is not available."}

    tracks_ctx = _tracks_context(playlist_url, search, page, page_size)
    total_associations = sum(p[2] for p in tracks_ctx["playlists"])

    return {
        "db_error": None,
        "env_value": ENV,
        "total_incomplete": track_db.get_total_incomplete_tracks(),
        "total_associations": total_associations,
        "flash": None,
        **tracks_ctx,
    }


@router.get("/manual-import/body")
def manual_import_body(
    request: Request,
    playlist_url: str | None = None,
    search: str = "",
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
):
    return templates.TemplateResponse(
        request, "tabs/_manual_import_body.html", _body_context(playlist_url, search, page, page_size),
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
    playlist_url: str = Form(...),
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

    context = _body_context(playlist_url, search, page, page_size)
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
