"""Route tests for the dashboard_next Docs tab (see docs/adr/0004)."""

import os
import re

import pytest

# Ensure APP_ENV is set before importing project modules that read it at import time.
os.environ.setdefault("APP_ENV", "test")

from fastapi.testclient import TestClient

from observability.dashboard_next.app import app
from observability.dashboard_next.config import DOC_FILES


@pytest.fixture()
def client():
    return TestClient(app)


def test_default_doc_renders_full_page(client):
    response = client.get("/docs/overview")
    assert response.status_code == 200
    assert "<html" in response.text
    assert "Spotiseek" in response.text


@pytest.mark.parametrize("slug", list(DOC_FILES.keys()))
def test_each_doc_renders(client, slug):
    response = client.get(f"/docs/{slug}")
    assert response.status_code == 200
    assert "<html" in response.text


def test_unknown_doc_falls_back_to_default(client):
    response = client.get("/docs/not-a-real-doc")
    assert response.status_code == 200
    assert 'class="doc-link active"' in response.text or "doc-link active" in response.text


def test_htmx_request_returns_fragment_not_full_page(client):
    response = client.get("/docs/overview", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "<html" not in response.text
    assert "docs-tab" in response.text


def _active_doc_slug(html: str) -> str | None:
    match = re.search(r'class="doc-link active"\s+href="/docs/([\w-]+)"', html)
    return match.group(1) if match else None


def test_switching_docs_moves_the_active_highlight(client):
    """Regression test: an earlier version only swapped the content pane, leaving the
    sidebar's highlighted doc stuck on whatever was selected at initial page load."""
    overview = client.get("/docs/overview", headers={"HX-Request": "true"}).text
    guide = client.get("/docs/dashboard-guide", headers={"HX-Request": "true"}).text

    assert _active_doc_slug(overview) == "overview"
    assert _active_doc_slug(guide) == "dashboard-guide"
