"""
Docs tab routes.

1:1 port of observability/dashboard/tabs/docs.py's behaviour (same four documents,
same default selection) onto FastAPI + HTMX -- see
docs/adr/0004-dashboard-migration-parallel-service-cutover.md.
"""

import os

import markdown
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from observability.dashboard_next.config import DEFAULT_DOC_SLUG, DOC_FILES, DOCS_DIR, ENV

router = APIRouter()

_templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
templates = Jinja2Templates(directory=_templates_dir)

_MARKDOWN_EXTENSIONS = ["fenced_code", "tables"]


def _render_doc(slug: str) -> str:
    """Load and render one document's markdown to HTML, matching docs.py's error handling."""
    _, file_path = DOC_FILES[slug]
    if not os.path.exists(file_path):
        return f"<p><em>File not found: {file_path}</em></p>"
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
    except OSError as e:
        return f"<p><em>Error reading file: {e}</em></p>"
    if not content:
        return "<p><em>Document is empty.</em></p>"
    return markdown.markdown(content, extensions=_MARKDOWN_EXTENSIONS)


@router.get("/docs/{slug}")
def docs_tab(request: Request, slug: str = DEFAULT_DOC_SLUG):
    """The whole Docs tab: full page on direct navigation, tab fragment on HTMX nav."""
    if slug not in DOC_FILES:
        slug = DEFAULT_DOC_SLUG

    context = {
        "docs": DOC_FILES,
        "selected_slug": slug,
        "content_html": _render_doc(slug),
        "docs_dir": DOCS_DIR,
    }

    if request.headers.get("HX-Request") == "true":
        return templates.TemplateResponse(request, "tabs/docs_tab.html", context)

    context["active_tab"] = "docs"
    context["env_name"] = (ENV or "default").upper()
    context["content_template"] = "tabs/docs_tab.html"
    return templates.TemplateResponse(request, "base.html", context)
