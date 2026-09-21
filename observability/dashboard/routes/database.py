"""
Database tab routes: a read-only explorer over the current environment's database.

See docs/adr/0008-dashboard-database-explorer.md (and 0009 for the stuck-track age). Two
sub-views -- Tables (a browser over every table, plus a per-track detail page) and Audit
(canned checks) -- for the hot environment only.

Everything here is read-only and goes through the TrackDB singleton's explorer read
methods; there is no SQL in this module and no write path. Page/sort/filter state lives in
request params (like the Blacklist tab), and every URL a template needs is built here so
the pushed browser URL is always the canonical one. The disk checks (file existence,
m3u8 state, orphan files) only ever run on request -- the track detail view for one track,
the Audit view's Run buttons for the whole library.
"""

import sqlite3
from math import ceil
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from observability.dashboard import disk_checks
from observability.dashboard.config import DOWNLOADS_ROOT, DOWNLOADS_VIEW_DIR, ENV, IMPORTED_DIR, track_db
from observability.dashboard.templating import templates
from scripts.audit_checks import AUDIT_CHECKS, GROUP_ORDER
from scripts.constants import STUCK_THRESHOLD_HOURS
from scripts.logs_utils import write_log

router = APIRouter()

_PAGE_SIZES = [10, 25, 50, 100]
_DEFAULT_PAGE_SIZE = 25
_FILTER_PREFIX = "f__"  # a per-column filter travels as ?f__<column>=<text>

# The disk checks the Audit view offers, keyed by the id in /database/audit/disk/{id}.
_DISK_CHECKS = {
    "completed_files_missing": (
        "Completed tracks whose file is missing",
        "A track is marked completed and records a file path, but nothing is on disk there.",
    ),
    "m3u8_missing": (
        "Playlists whose m3u8 is missing",
        "A playlist has no m3u8 path recorded, or the file it points at doesn't exist.",
    ),
    "orphan_files": (
        "Files in imported/ that no track points at",
        "Audio files under imported/ that no track's recorded file path refers to.",
    ),
}


# --- URL + value helpers ---------------------------------------------------------------------


def _browse_url(
    table: str | None = None,
    *,
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
    sort: str | None = None,
    direction: str = "asc",
    filters: dict[str, str] | None = None,
) -> str:
    """Canonical URL for a Tables view; parameters at their defaults are omitted."""
    params: list[tuple[str, str]] = []
    if table:
        params.append(("table", table))
        if page > 1:
            params.append(("page", str(page)))
        if page_size != _DEFAULT_PAGE_SIZE:
            params.append(("page_size", str(page_size)))
        if sort:
            params.append(("sort", sort))
            if direction == "desc":
                params.append(("dir", "desc"))
        params.extend((f"{_FILTER_PREFIX}{col}", text) for col, text in (filters or {}).items() if text)
    return "/database" + (f"?{urlencode(params)}" if params else "")


def _track_url(track_id: str) -> str:
    # A query param, not a path segment: SoundCloud track_ids are slugs like "artist/track".
    return "/database/track?" + urlencode({"track_id": track_id})


def _display(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes | bytearray):
        return f"<{len(value)} bytes>"
    return str(value)


def _cell(value: object, href: str | None = None) -> dict:
    return {"text": _display(value), "is_null": value is None, "href": href if value is not None else None}


def _result_cell_href(column: str, value: object) -> str | None:
    """Links for cells in audit-check results: a track_id opens the track, a playlist_url its row."""
    if value is None:
        return None
    if column == "track_id":
        return _track_url(str(value))
    if column == "playlist_url":
        return _browse_url("playlists", filters={"playlist_url": str(value)})
    return None


def _result_rows(columns: list[str], rows: list[tuple]) -> list[list[dict]]:
    return [[_cell(value, _result_cell_href(col, value)) for col, value in zip(columns, row, strict=True)] for row in rows]


def _page_size(value: int) -> int:
    return value if value in _PAGE_SIZES else _DEFAULT_PAGE_SIZE


def _resolver() -> disk_checks.PathResolver:
    """The locations of recorded file paths this container can actually see: imported/ (mounted
    at its recorded path) and downloads/ (mounted read-only under an alias). Anything else is
    reported as "not checked" rather than "missing". Read at call time so tests can point the
    module-level directories at temp dirs."""
    return disk_checks.PathResolver(roots=((IMPORTED_DIR, IMPORTED_DIR), (DOWNLOADS_ROOT, DOWNLOADS_VIEW_DIR)))


# --- rendering -------------------------------------------------------------------------------


def _guarded(builder, *args, **kwargs) -> dict:
    """Run a context builder, turning "no database" and any SQLite failure (e.g. `database is
    locked` while the workflow is writing) into a message instead of a 500."""
    if track_db is None:
        return {"db_error": "Database is not available."}
    try:
        return builder(*args, **kwargs)
    except sqlite3.Error as e:
        write_log.error("DB_EXPLORER_READ_FAIL", "Database explorer read failed.", {"error": str(e)})
        return {"db_error": f"Database read failed: {e}"}


def _render(request: Request, context: dict, subview_template: str, active_sub: str) -> HTMLResponse:
    """The whole Database tab: a full page on direct navigation, a tab fragment on HTMX nav.

    On HTMX requests the response carries the canonical URL to push (HX-Push-Url), or to
    replace the current one (HX-Replace-Url) for the Refresh button, so a filter form's
    empty params never end up in the address bar and Refresh doesn't add history entries.
    """
    context = {
        "subview_template": subview_template,
        "active_sub": active_sub,
        "url": "/database",
        "env_name": (ENV or "default").upper(),
        **context,
    }
    if request.headers.get("HX-Request") == "true":
        response = templates.TemplateResponse(request, "tabs/database_tab.html", context)
        header = "HX-Replace-Url" if request.headers.get("X-Refresh") else "HX-Push-Url"
        response.headers[header] = context["url"]
        return response

    context["content_template"] = "tabs/database_tab.html"
    return templates.TemplateResponse(request, "base.html", context)


# --- Tables view -----------------------------------------------------------------------------


def _tables_context(
    table: str, page: int, page_size: int, sort: str, direction: str, filters: dict[str, str],
) -> dict:
    page_size = _page_size(page_size)
    direction = "desc" if direction == "desc" else "asc"
    context: dict = {
        "db_error": None,
        "tables": track_db.list_tables(),
        "selected": None,
        "table_error": None,
        "url": _browse_url(),
    }
    if not table:
        return context

    context["selected"] = table
    result = track_db.browse_table(
        table, offset=(max(page, 1) - 1) * page_size, limit=page_size,
        sort=sort or None, descending=direction == "desc", filters=filters,
    )
    if result is None:
        context["table_error"] = f"There is no table named {table!r} in this database."
        return context

    columns = result["columns"]
    total_pages = max(1, ceil(result["total"] / page_size))
    if page > total_pages:  # e.g. a filter narrowed the results while on a late page
        page = total_pages
        result = track_db.browse_table(
            table, offset=(page - 1) * page_size, limit=page_size,
            sort=sort or None, descending=direction == "desc", filters=filters,
        )
    page = max(page, 1)
    sort = sort if sort in columns else ""
    filters = {col: text for col, text in filters.items() if col in columns and text}

    foreign_keys = {fk["column"]: fk for fk in result["schema"]["foreign_keys"]}

    def href(column: str, value: object) -> str | None:
        if value is None:
            return None
        if table == "tracks" and column == "track_id":
            return _track_url(str(value))
        fk = foreign_keys.get(column)
        if fk is None:
            return None
        if fk["ref_table"] == "tracks" and fk["ref_column"] == "track_id":
            return _track_url(str(value))
        return _browse_url(fk["ref_table"], filters={fk["ref_column"]: str(value)})

    def view_url(**overrides) -> str:
        state = {"page": page, "page_size": page_size, "sort": sort, "direction": direction, "filters": filters}
        return _browse_url(table, **{**state, **overrides})

    headers = []
    for col in result["schema"]["columns"]:
        name = col["name"]
        is_sorted = name == sort
        next_direction = "desc" if is_sorted and direction == "asc" else "asc"
        headers.append({
            "name": name,
            "pk": bool(col["pk"]),
            "fk": name in foreign_keys,
            "sorted": is_sorted,
            "arrow": ("▲" if direction == "asc" else "▼") if is_sorted else "",
            "sort_url": view_url(page=1, sort=name, direction=next_direction),
            "filter": filters.get(name, ""),
        })

    context.update({
        "url": view_url(),
        "table_schema": result["schema"],
        "headers": headers,
        "rows": [[_cell(value, href(col, value)) for col, value in zip(columns, row, strict=True)]
                 for row in result["rows"]],
        "total": result["total"],
        "page": page,
        "total_pages": total_pages,
        "page_size": page_size,
        "page_sizes": _PAGE_SIZES,
        "sort": sort,
        "direction": direction,
        "has_filters": bool(filters),
        "clear_filters_url": view_url(page=1, filters={}),
        "first_url": view_url(page=1),
        "prev_url": view_url(page=page - 1) if page > 1 else None,
        "next_url": view_url(page=page + 1) if page < total_pages else None,
        "last_url": view_url(page=total_pages),
        "first_row": (page - 1) * page_size + 1 if result["total"] else 0,
        "last_row": min(page * page_size, result["total"]),
    })
    return context


@router.get("/database")
def tables_view(
    request: Request,
    table: str = "",
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
    sort: str = "",
    direction: str = Query("asc", alias="dir"),
):
    filters = {
        key[len(_FILTER_PREFIX):]: value
        for key, value in request.query_params.items()
        if key.startswith(_FILTER_PREFIX)
    }
    context = _guarded(_tables_context, table, page, page_size, sort, direction, filters)
    return _render(request, context, "tabs/_database_tables.html", "tables")


# --- Track detail ----------------------------------------------------------------------------


def _track_context(track_id: str) -> dict:
    detail = track_db.get_track_detail(track_id)
    context: dict = {"db_error": None, "track_id": track_id, "detail": None, "url": _track_url(track_id)}
    if detail is None:
        return context

    track = detail["track"]
    status = track.get("download_status")
    age_seconds = detail["status_age_seconds"]
    threshold_hours = STUCK_THRESHOLD_HOURS.get(status)
    playlists = []
    for playlist in detail["playlists"]:
        state = disk_checks.m3u8_line_state(playlist["m3u8_path"], track_id, track.get("local_file_path"))
        playlists.append({
            **playlist,
            "href": _browse_url("playlists", filters={"playlist_url": playlist["playlist_url"]}),
            "folder_labels": [folder or "(root)" for folder in playlist["folders"]],
            "m3u8_state": state,
            "m3u8_label": disk_checks.M3U8_STATE_LABELS[state],
            # A completed track should be listed by its file path; anything else pending
            # should still be a placeholder. A missing/unlisted entry is always drift.
            "m3u8_warn": state in {
                disk_checks.STATE_M3U8_MISSING, disk_checks.STATE_NO_M3U8_PATH,
                disk_checks.STATE_NOT_LISTED, disk_checks.STATE_UNREADABLE,
            } or (status == "completed" and state == disk_checks.STATE_PLACEHOLDER),
        })

    context["detail"] = {
        "track": track,
        "fields": [(name, _cell(value)) for name, value in track.items()],
        "status": status,
        "status_age_seconds": age_seconds,
        "stuck": threshold_hours is not None and age_seconds is not None and age_seconds > threshold_hours * 3600,
        "stuck_threshold_hours": threshold_hours,
        "playlists": playlists,
        "blacklist_entries": detail["blacklist_entries"],
        "file": disk_checks.file_facts(track.get("local_file_path"), _resolver()),
        "back_url": _browse_url("tracks"),
    }
    return context


@router.get("/database/track")
def track_detail(request: Request, track_id: str = ""):
    context = _guarded(_track_context, track_id)
    return _render(request, context, "tabs/_database_track.html", "tables")


# --- Audit view ------------------------------------------------------------------------------


def _audit_context() -> dict:
    groups = []
    for group in GROUP_ORDER:
        checks = [
            {"id": c.id, "title": c.title, "description": c.description, "count": track_db.count_audit_check(c.id)}
            for c in AUDIT_CHECKS
            if c.group == group
        ]
        groups.append({"name": group, "checks": checks, "flagged": sum(c["count"] for c in checks)})
    return {
        "db_error": None,
        "groups": groups,
        "total_flagged": sum(g["flagged"] for g in groups),
        "disk_checks": [{"id": cid, "title": title, "description": desc} for cid, (title, desc) in _DISK_CHECKS.items()],
        "url": "/database/audit",
    }


@router.get("/database/audit")
def audit_view(request: Request):
    context = _guarded(_audit_context)
    return _render(request, context, "tabs/_database_audit.html", "audit")


@router.get("/database/audit/check/{check_id}", response_class=HTMLResponse)
def audit_check_rows(request: Request, check_id: str, page: int = 1):
    """The rows one DB audit check flags, one page at a time (an HTMX fragment)."""

    def build() -> dict:
        check = next((c for c in AUDIT_CHECKS if c.id == check_id), None)
        if check is None:
            return {"db_error": f"Unknown audit check {check_id!r}."}
        total = track_db.count_audit_check(check_id)
        total_pages = max(1, ceil(total / _DEFAULT_PAGE_SIZE))
        current = min(max(page, 1), total_pages)
        result = track_db.get_audit_check_rows(check_id, (current - 1) * _DEFAULT_PAGE_SIZE, _DEFAULT_PAGE_SIZE)
        base = f"/database/audit/check/{check_id}"
        return {
            "db_error": None,
            "check_id": check_id,
            "columns": result["columns"],
            "rows": _result_rows(result["columns"], result["rows"]),
            "total": total,
            "page": current,
            "total_pages": total_pages,
            "prev_url": f"{base}?page={current - 1}" if current > 1 else None,
            "next_url": f"{base}?page={current + 1}" if current < total_pages else None,
        }

    return templates.TemplateResponse(request, "tabs/_database_check_rows.html", _guarded(build))


@router.post("/database/audit/disk/{check_id}", response_class=HTMLResponse)
def run_disk_check(request: Request, check_id: str):
    """Run one disk check and return its result (an HTMX fragment). Only ever on request."""

    def build() -> dict:
        if check_id not in _DISK_CHECKS:
            return {"db_error": f"Unknown disk check {check_id!r}."}
        note = None
        coverage = None
        if check_id == "completed_files_missing":
            columns = ["track_id", "artist", "track_name", "local_file_path"]
            result = disk_checks.find_missing_track_files(track_db.get_completed_track_files(), _resolver())
            found, total = result.missing, len(result.missing)
            coverage = {"checked": result.checked, "not_checked": result.not_checked}
            if result.not_checked:
                note = (
                    f"{result.not_checked:,} of {result.checked + result.not_checked:,} completed tracks were not "
                    "checked: their files are in a location this dashboard container cannot see, so they can't be "
                    "called missing. Visible locations are imported/ and the read-only downloads mount "
                    f"(see docker-compose.yml). Example: {result.not_checked_example}"
                )
        elif check_id == "m3u8_missing":
            columns = ["playlist_url", "playlist_name", "m3u8_path", "reason"]
            found = disk_checks.find_missing_m3u8s(track_db.get_playlist_m3u8_paths())
            total = len(found)
        else:
            columns = ["path", "size_bytes"]
            found, total, dir_exists = disk_checks.find_orphan_files(IMPORTED_DIR, track_db.get_all_local_file_paths())
            if not dir_exists:
                note = f"The imported/ directory ({IMPORTED_DIR}) does not exist in this container."
        return _disk_result(check_id, columns, found[: disk_checks.MAX_LISTED_FILES], total, note, coverage)

    return templates.TemplateResponse(request, "tabs/_database_disk_result.html", _guarded(build))


def _disk_result(
    check_id: str, columns: list[str], rows: list[tuple], total: int, note: str | None, coverage: dict | None = None,
) -> dict:
    title, description = _DISK_CHECKS[check_id]
    return {
        "db_error": None,
        "title": title,
        "description": description,
        "columns": columns,
        "rows": _result_rows(columns, rows),
        "total": total,
        "listed": len(rows),
        "note": note,
        "coverage": coverage,  # {"checked", "not_checked"} for checks that can be partially blind, else None
    }


@router.post("/database/audit/health", response_class=HTMLResponse)
def run_health_check(request: Request):
    """Database file facts plus PRAGMA quick_check (an HTMX fragment). Only ever on request."""

    def build() -> dict:
        health = track_db.get_database_health()
        quick_check = track_db.run_quick_check()
        return {"db_error": None, "health": health, "quick_check": quick_check, "ok": quick_check == ["ok"]}

    return templates.TemplateResponse(request, "tabs/_database_health.html", _guarded(build))
