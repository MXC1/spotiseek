"""Shared Jinja2Templates instance, used by every dashboard_next route module."""

import os

from fastapi.templating import Jinja2Templates

_templates_dir = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=_templates_dir)
