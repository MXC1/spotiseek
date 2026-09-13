"""
dashboard_next configuration module.

Temporary FastAPI replacement for observability/dashboard/config.py -- see
docs/adr/0003-dashboard-rewrite-fastapi-htmx.md and
docs/adr/0004-dashboard-migration-parallel-service-cutover.md. Grows one tab's worth of
config at a time as the migration progresses; only the Docs tab's paths exist so far.
This module should be imported first by other dashboard_next modules.
"""

import os
import sys

sys.dont_write_bytecode = True

from dotenv import load_dotenv

_dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
load_dotenv(_dotenv_path)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), ".."))

from scripts.logs_utils import setup_logging  # noqa: E402

ENV = os.getenv("APP_ENV")

# Environment-specific paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

# Documentation paths -- slug -> (display name, file path)
DOCS_DIR = os.path.join(BASE_DIR, "docs")
DOC_FILES = {
    "overview": ("Overview", os.path.join(BASE_DIR, "README.md")),
    "dashboard-guide": ("Dashboard Guide", os.path.join(DOCS_DIR, "DASHBOARD.md")),
    "configuration": ("Configuration", os.path.join(DOCS_DIR, "CONFIGURATION.md")),
    "troubleshooting": ("Troubleshooting", os.path.join(DOCS_DIR, "TROUBLESHOOTING.md")),
}
DEFAULT_DOC_SLUG = "overview"

if ENV:
    setup_logging(log_name_prefix="task_scheduler", rotate_daily=True)
