"""
The Spotiseek dashboard: a FastAPI + HTMX app, one tab per route module -- see
docs/adr/0003-dashboard-rewrite-fastapi-htmx.md. Originally built tab-by-tab as a
parallel `dashboard-next` service alongside the old Streamlit dashboard
(docs/adr/0004-dashboard-migration-parallel-service-cutover.md); that migration is
complete (docs/adr/0006-complete-dashboard-cutover.md) and this is now the only
dashboard, at observability/dashboard/, serving port 8501.

Usage:
    uvicorn observability.dashboard.app:app --host 0.0.0.0 --port 8501
"""

import os
from typing import Any

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from observability.dashboard.config import ENV
from observability.dashboard.routes.auto_import import router as auto_import_router
from observability.dashboard.routes.blacklist import router as blacklist_router
from observability.dashboard.routes.database import router as database_router
from observability.dashboard.routes.docs import router as docs_router
from observability.dashboard.routes.execution_inspection import router as execution_inspection_router
from observability.dashboard.routes.manual_import import router as manual_import_router
from observability.dashboard.routes.stats import router as stats_router
from observability.dashboard.routes.tasks import router as tasks_router

app = FastAPI(title=f"Spotiseek Dashboard ({(ENV or 'default').upper()})")


class _RevalidatingStaticFiles(StaticFiles):
    """StaticFiles that never lets the browser serve a stale copy from its heuristic
    cache. Starlette's default sends no Cache-Control at all, so browsers apply their
    own freshness guess (commonly ~10% of the file's age since Last-Modified) and can
    keep serving old JS/CSS for a while after a change -- exactly the kind of thing
    that's confusing on a dashboard whose static assets get edited often. `no-cache`
    forces a conditional GET (If-None-Match/If-Modified-Since) on every load; a cheap
    304 comes back when nothing changed, so this doesn't add a real fetch cost.
    """

    def file_response(self, *args: Any, **kwargs: Any) -> Any:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache"
        return response


_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", _RevalidatingStaticFiles(directory=_static_dir), name="static")

app.include_router(stats_router)
app.include_router(tasks_router)
app.include_router(execution_inspection_router)
app.include_router(manual_import_router)
app.include_router(auto_import_router)
app.include_router(blacklist_router)
app.include_router(database_router)
app.include_router(docs_router)


@app.get("/")
def index():
    # Matches the original Streamlit app's tab order: Overall Stats was tab 1.
    return RedirectResponse(url="/stats")
