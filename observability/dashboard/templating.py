"""Shared Jinja2Templates instance, used by every dashboard route module."""

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


def _duration(seconds: int | None) -> str:
    """Human-readable elapsed time ("3d 4h", "5h 12m", "42s"); "unknown" for None."""
    if seconds is None:
        return "unknown"
    days, rem = divmod(int(seconds), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _filesize(size_bytes: int | None) -> str:
    """Human-readable file size ("1.4 MB"); "-" for None."""
    if size_bytes is None:
        return "-"
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size_bytes} B"  # unreachable; keeps the return type total


templates.env.filters["duration"] = _duration
templates.env.filters["filesize"] = _filesize
