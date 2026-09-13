"""
Execution Inspection tab routes.

1:1 port of observability/dashboard/tabs/execution_inspection.py's behaviour (workflow
run picker + summary metrics, timeline, errors/warnings) onto FastAPI + HTMX -- see
docs/adr/0004-dashboard-migration-parallel-service-cutover.md.
"""

import json
import os

from fastapi import APIRouter, Request

from observability.dashboard_next.config import ENV, LOGS_DIR
from observability.dashboard_next.templating import templates
from scripts.logs_utils import analyze_workflow_run, get_workflow_runs

router = APIRouter()

_STATUS_EMOJI = {
    "completed": "\U0001f7e2",
    "failed": "\U0001f534",
    "incomplete": "\U0001f7e1",
    "unknown": "⚪",
}


def _entry_block(entry: dict) -> str:
    return (
        f"Event: {entry.get('event_id', 'N/A')}\n"
        f"Message: {entry.get('message', 'N/A')}\n"
        f"Context: {json.dumps(entry.get('context', {}), indent=2)}"
    )


def _summary_context(run_id: str | None) -> dict:
    runs = get_workflow_runs(LOGS_DIR)
    if not runs:
        return {"runs": [], "selected_run_id": None, "run": None}

    selected_run = next((r for r in runs if r["run_id"] == run_id), runs[0])
    analysis = analyze_workflow_run(selected_run["log_file"])

    downloads_new = analysis.get("downloads_completed_new", 0)
    downloads_upgrade = analysis.get("downloads_completed_upgrade", 0)

    timeline_rows = [
        {"time": item["display_time"], "event": item["event_id"], "message": item["message"]}
        for item in analysis["timeline"]
    ]

    return {
        "runs": runs,
        "selected_run_id": selected_run["run_id"],
        "run": selected_run,
        "status": analysis["workflow_status"],
        "status_emoji": _STATUS_EMOJI.get(analysis["workflow_status"], "⚪"),
        "log_filename": os.path.basename(selected_run["log_file"]),
        "metric_cards": [
            [
                ("Total Logs", analysis["total_logs"]),
                ("Errors", len(analysis["errors"])),
                ("Warnings", len(analysis["warnings"])),
            ],
            [
                ("Searches (New)", analysis["new_searches"]),
                ("Searches (Upgrade)", analysis["upgrade_searches"]),
            ],
            [
                ("Playlists Added", analysis["playlists_added"]),
                ("Playlists Removed", analysis.get("playlists_removed", 0)),
            ],
            [
                ("Tracks Added", analysis["tracks_added"]),
                ("Tracks Removed", analysis.get("tracks_removed", 0)),
            ],
            [
                ("Quality Upgrades", analysis["tracks_upgraded"]),
            ],
            [
                ("Downloads Completed (New)", downloads_new),
                ("Downloads Completed (Upgrade)", downloads_upgrade),
                ("Downloads Failed", analysis["downloads_failed"]),
            ],
        ],
        "timeline_rows": timeline_rows,
        "error_blocks": [_entry_block(e) for e in analysis["errors"]],
        "warning_blocks": [_entry_block(w) for w in analysis["warnings"]],
        "error_count": len(analysis["errors"]),
        "warning_count": len(analysis["warnings"]),
    }


@router.get("/execution-inspection/run")
def execution_inspection_run(request: Request, run_id: str | None = None):
    return templates.TemplateResponse(
        request, "tabs/_execution_inspection_summary.html", _summary_context(run_id),
    )


@router.get("/execution-inspection")
def execution_inspection_tab(request: Request):
    """The whole Execution Inspection tab: full page on direct nav, tab fragment on HTMX."""
    context = _summary_context(None)

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/execution_inspection_tab.html", context)

    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/execution_inspection_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
