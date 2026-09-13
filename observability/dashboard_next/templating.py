"""Shared Jinja2Templates instance, used by every dashboard_next route module."""

import json
import os

from fastapi.templating import Jinja2Templates

_templates_dir = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=_templates_dir)

# For safely embedding a Python value as a JSON literal inside an `hx-vals='js:{...}'`
# expression -- Jinja's normal autoescaping still HTML-escapes the surrounding attribute
# quotes/entities on top of this, so values with quotes or special characters round-trip
# correctly through the browser's HTML parsing before HTMX evaluates the JS.
templates.env.filters["tojson"] = json.dumps
