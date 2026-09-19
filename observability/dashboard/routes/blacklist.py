"""
Blacklist tab routes.

1:1 port of observability/dashboard/tabs/blacklist.py's behaviour (search+paging over
completed tracks, track selection, multi-step blacklist with rollback) onto FastAPI +
HTMX + Alpine -- see docs/adr/0004-dashboard-migration-parallel-service-cutover.md. DB
access goes through new TrackDB methods instead of the original's ad-hoc
sqlite3.connect() calls, per ADR-0003 -- including the multi-step blacklist_track()
write itself, which the original ran over three separate ad-hoc connections plus
track_db.

This is the most destructive action in the whole dashboard: it deletes a real file from
disk and cannot be undone. Never trigger it against real data outside of automated
tests -- see the project memory note on this discipline.
"""

import os

from fastapi import APIRouter, Form, Request

from observability.dashboard.config import ENV, track_db
from observability.dashboard.templating import templates
from scripts.logs_utils import write_log

router = APIRouter()

_PAGE_SIZES = [10, 25, 50, 100]
_DEFAULT_PAGE_SIZE = 25


def _revert_track_to_comment_in_m3u8(
    m3u8_path: str, track_id: str, artist: str, track_name: str, local_file_path: str,
) -> None:
    """Replace a track's file-path line in an m3u8 with its original comment line.

    Mirrors how update_track_in_m3u8 works in reverse: completing a track replaces the
    "# track_id - artist - track_name" comment with the file path, so blacklisting
    replaces the file path back with that comment.
    """
    try:
        with open(m3u8_path, encoding="utf-8") as f:
            lines = f.readlines()

        new_lines = []
        track_found = False
        for line in lines:
            if line.strip() == local_file_path.strip() and not track_found:
                new_lines.append(f"# {track_id} - {artist} - {track_name}\n")
                track_found = True
            else:
                new_lines.append(line)

        if not track_found:
            comment_line = f"# {track_id} - {artist} - {track_name}\n"
            if comment_line not in new_lines:
                new_lines.append(comment_line)

        with open(m3u8_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

        write_log.debug("M3U8_REVERT_SUCCESS", "Reverted track to comment in M3U8 file.",
                         {"m3u8_path": m3u8_path, "track_id": track_id, "local_file_path": local_file_path})
    except Exception as e:
        write_log.error("M3U8_REVERT_FAIL", "Failed to revert track to comment in M3U8 file.",
                         {"m3u8_path": m3u8_path, "track_id": track_id, "error": str(e)})


def _blacklist_track(track: dict) -> tuple[bool, str]:
    """Blacklist a track: add to slskd_blacklist (if from Soulseek), delete the local
    file, clear its download metadata, mark it blacklisted, and revert m3u8 entries.

    Rolls back the DB changes (not the file deletion, which isn't reversible) if
    anything after the file delete fails, so a mid-failure never leaves the DB pointing
    at a file that no longer exists.
    """
    track_id = track["track_id"]
    artist = track["artist"]
    track_name = track["track_name"]
    local_file_path = track["local_file_path"]
    username = track["username"]
    slskd_file_name = track["slskd_file_name"]

    try:
        if username and slskd_file_name:
            track_db.add_slskd_blacklist(username=username, slskd_file_name=slskd_file_name, reason="manual_blacklist")
            write_log.info("BLACKLIST_ADDED", "Added track to blacklist via dashboard.",
                            {"track_id": track_id, "username": username, "slskd_file_name": slskd_file_name})
        else:
            write_log.info("BLACKLIST_IMPORTED_TRACK",
                            "Blacklisting imported track (no Soulseek metadata to add to blacklist table).",
                            {"track_id": track_id})

        if local_file_path and os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                write_log.info("BLACKLIST_FILE_DELETED", "Deleted blacklisted track file.",
                                {"track_id": track_id, "file_path": local_file_path})
            except Exception as e:
                write_log.error("BLACKLIST_FILE_DELETE_FAIL", "Failed to delete blacklisted track file.",
                                 {"track_id": track_id, "file_path": local_file_path, "error": str(e)})
                return False, f"Failed to delete file: {e}"

        previous_state = {
            "local_file_path": local_file_path,
            "bitrate": track.get("bitrate"),
            "extension": track.get("extension"),
            "username": track.get("username"),
            "slskd_file_name": track.get("slskd_file_name"),
            "download_status": "completed",
        }

        try:
            track_db.clear_track_download_metadata(track_id)
            write_log.info("BLACKLIST_DB_CLEARED", "Cleared all download metadata for blacklisted track.",
                            {"track_id": track_id})

            track_db.update_track_status(track_id, "blacklisted")
            write_log.info("BLACKLIST_STATUS_SET", "Set track status to blacklisted.", {"track_id": track_id})

            for playlist_url in track_db.get_playlists_for_track(track_id):
                m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
                if m3u8_path and os.path.exists(m3u8_path):
                    try:
                        _revert_track_to_comment_in_m3u8(m3u8_path, track_id, artist, track_name, local_file_path)
                    except Exception as m3u8_error:
                        write_log.error("BLACKLIST_M3U8_UPDATE_FAIL", "Failed to update M3U8 for blacklisted track.",
                                         {"m3u8_path": m3u8_path, "track_id": track_id, "error": str(m3u8_error)})
                        raise

        except Exception as step_error:
            try:
                track_db.restore_track_download_metadata(
                    track_id,
                    previous_state["local_file_path"], previous_state["bitrate"], previous_state["extension"],
                    previous_state["username"], previous_state["slskd_file_name"], previous_state["download_status"],
                )
                write_log.warn("BLACKLIST_ROLLBACK_SUCCESS", "Rolled back DB changes after blacklist failure.",
                                {"track_id": track_id})
            except Exception as rollback_error:
                write_log.error("BLACKLIST_ROLLBACK_FAIL", "Failed to roll back DB changes after blacklist failure.",
                                 {"track_id": track_id, "original_error": str(step_error),
                                  "rollback_error": str(rollback_error)})
            raise

        return True, f"Successfully blacklisted: {artist} - {track_name}"

    except Exception as e:
        write_log.error("BLACKLIST_TRACK_FAIL", "Failed to blacklist track.", {"track_id": track_id, "error": str(e)})
        return False, f"Failed to blacklist track: {e}"


def _body_context(search: str, page: int, page_size: int, flash: dict | None = None) -> dict:
    if track_db is None:
        return {"db_error": "Database is not available."}

    page_size = page_size if page_size in _PAGE_SIZES else _DEFAULT_PAGE_SIZE
    page = max(page, 1)
    offset = (page - 1) * page_size

    rows_raw, total = track_db.search_completed_tracks(search, offset, page_size)
    if offset != 0 and offset >= max(total, 1):
        page, offset = 1, 0
        rows_raw, total = track_db.search_completed_tracks(search, offset, page_size)

    rows = [
        {
            "track_id": r[0], "track_name": r[1], "artist": r[2], "local_file_path": r[3],
            "extension": r[4], "bitrate": r[5], "username": r[6], "slskd_file_name": r[7],
        }
        for r in rows_raw
    ]

    return {
        "db_error": None,
        "env_value": ENV,
        "search": search or "",
        "page": page,
        "page_size": page_size,
        "page_sizes": _PAGE_SIZES,
        "total": total,
        "rows": rows,
        "flash": flash,
    }


@router.get("/blacklist/body")
def blacklist_body(request: Request, search: str = "", page: int = 1, page_size: int = _DEFAULT_PAGE_SIZE):
    return templates.TemplateResponse(request, "tabs/_blacklist_body.html", _body_context(search, page, page_size))


@router.post("/blacklist/confirm/{track_id:path}")
def blacklist_confirm(
    request: Request,
    track_id: str,
    search: str = Form(""),
    page: int = Form(1),
    page_size: int = Form(_DEFAULT_PAGE_SIZE),
):
    row = track_db.get_completed_track_by_id(track_id)
    if not row:
        flash = {"type": "error", "text": "Track not found or no longer has a local file."}
    else:
        track = {
            "track_id": row[0], "track_name": row[1], "artist": row[2], "local_file_path": row[3],
            "extension": row[4], "bitrate": row[5], "username": row[6], "slskd_file_name": row[7],
        }
        success, message = _blacklist_track(track)
        flash = {"type": "success" if success else "error", "text": message}

    context = _body_context(search, page, page_size, flash)
    return templates.TemplateResponse(request, "tabs/_blacklist_body.html", context)


@router.get("/blacklist")
def blacklist_tab(request: Request):
    """The whole Blacklist tab: full page on direct navigation, tab fragment on HTMX nav."""
    context = _body_context("", 1, _DEFAULT_PAGE_SIZE)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/blacklist_tab.html", context)

    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/blacklist_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
