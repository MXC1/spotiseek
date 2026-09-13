"""
Overall Stats tab routes.

1:1 port of observability/dashboard/tabs/overall_stats.py's behaviour onto FastAPI +
HTMX -- see docs/adr/0004-dashboard-migration-parallel-service-cutover.md. Unlike the
original, every query goes through the TrackDB singleton instead of an ad-hoc
sqlite3.connect(), per docs/adr/0003-dashboard-rewrite-fastapi-htmx.md. The Plotly bar
chart is replaced with a small hand-rolled CSS bar chart -- Plotly.js was never part of
the vendored-JS decision, and a handful of bars doesn't need a charting library.
"""

import os

from fastapi import APIRouter, Request
from mutagen import File as MutagenFile

from observability.dashboard_next.config import ENV, track_db
from observability.dashboard_next.templating import templates
from scripts.constants import LOSSLESS_FORMATS

router = APIRouter()


def _effective_bitrate_kbps(file_path: str) -> int | None:
    """Estimate a file's bitrate from size/duration when no stored bitrate exists."""
    try:
        if not file_path or not os.path.exists(file_path):
            return None
        size_bytes = os.path.getsize(file_path)
        audio = MutagenFile(file_path, easy=False)
        duration = getattr(getattr(audio, "info", None), "length", None)
        if not duration or duration <= 0:
            return None
        return round((size_bytes * 8) / duration / 1000)
    except Exception:
        return None


def _enhanced_bitrate_breakdown(rows: list[tuple[str | None, int | None, str]]) -> list[tuple[str, int]]:
    """Bucket tracks into known bitrates, a Lossless aggregate, and computed-effective bitrates."""
    lossless_count = 0
    known_counts: dict[int, int] = {}
    unknown_effective_counts: dict[int, int] = {}
    unknown_unmeasured = 0

    for ext, bitrate, path in rows:
        if (ext or "").lower() in LOSSLESS_FORMATS:
            lossless_count += 1
            continue
        if bitrate is not None and str(bitrate).strip():
            try:
                br_int = int(bitrate)
            except ValueError:
                br_int = None
            if br_int is not None:
                known_counts[br_int] = known_counts.get(br_int, 0) + 1
                continue
        eff = _effective_bitrate_kbps(path)
        if eff is not None:
            unknown_effective_counts[eff] = unknown_effective_counts.get(eff, 0) + 1
        else:
            unknown_unmeasured += 1

    display: list[tuple[str, int]] = []
    for br_val, cnt in sorted(known_counts.items(), key=lambda x: (x[1], x[0]), reverse=True):
        display.append((str(br_val), cnt))
    if lossless_count:
        display.append(("Lossless", lossless_count))
    for eff_val, cnt in sorted(unknown_effective_counts.items(), key=lambda x: (x[1], x[0]), reverse=True):
        display.append((f"Unknown (Effective) {eff_val}", cnt))
    if unknown_unmeasured:
        display.append(("Unknown (Unmeasured)", unknown_unmeasured))
    return display


def _normalize_failed_reasons(rows: list[tuple[str, str, int]]) -> list[tuple[str, str, int]]:
    """Collapse per-URL "500 Server Error" variants into one bucket, matching the original tab."""
    merged: dict[tuple[str, str], int] = {}
    for status, reason, count in rows:
        if isinstance(reason, str) and reason.startswith("500 Server Error: Internal Server Error"):
            reason = "500 Server Error: Internal Server Error"
        key = (status, reason)
        merged[key] = merged.get(key, 0) + count
    result = [(status, reason, count) for (status, reason), count in merged.items()]
    result.sort(key=lambda row: row[2], reverse=True)
    return result


def _build_context() -> dict:
    if track_db is None:
        return {"db_error": "Database is not available."}

    status_rows = track_db.get_track_status_breakdown()
    total_status_count = sum(count for _, count in status_rows)

    chart_source = [(status, count) for status, count in status_rows if status.lower() != "completed"]
    max_chart_count = max((count for _, count in chart_source), default=0)
    chart_rows = [
        (status, count, round(count / max_chart_count * 100, 1) if max_chart_count else 0)
        for status, count in chart_source
    ]

    return {
        "db_error": None,
        "playlists": track_db.get_playlists(),
        "status_rows": status_rows,
        "total_status_count": total_status_count,
        "chart_rows": chart_rows,
        "extension_rows": track_db.get_extension_breakdown(),
        "bitrate_rows": _enhanced_bitrate_breakdown(track_db.get_tracks_with_local_files()),
        "download_status_rows": track_db.get_download_status_breakdown(),
        "failed_reason_rows": _normalize_failed_reasons(track_db.get_failed_reason_breakdown()),
    }


@router.get("/stats")
def stats_tab(request: Request):
    """The whole Overall Stats tab: full page on direct navigation, tab fragment on HTMX nav."""
    context = _build_context()

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/stats_tab.html", context)

    context["active_tab"] = "stats"
    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/stats_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
