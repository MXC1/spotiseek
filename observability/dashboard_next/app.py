"""
FastAPI replacement for the Streamlit dashboard.

Runs as the temporary `dashboard-next` service on port 8502 while tabs are migrated
one at a time -- see docs/adr/0003-dashboard-rewrite-fastapi-htmx.md and
docs/adr/0004-dashboard-migration-parallel-service-cutover.md.

Usage:
    uvicorn observability.dashboard_next.app:app --host 0.0.0.0 --port 8502
"""

import os

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from observability.dashboard_next.config import DEFAULT_DOC_SLUG, ENV
from observability.dashboard_next.routes.docs import router as docs_router

app = FastAPI(title=f"Spotiseek Dashboard ({(ENV or 'default').upper()})")

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")

app.include_router(docs_router)


@app.get("/")
def index():
    return RedirectResponse(url=f"/docs/{DEFAULT_DOC_SLUG}")
