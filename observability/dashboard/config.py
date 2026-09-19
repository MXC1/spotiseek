"""
Dashboard configuration module -- see docs/adr/0003-dashboard-rewrite-fastapi-htmx.md.

This module should be imported first by other observability.dashboard modules.
"""

import os
import sys

sys.dont_write_bytecode = True

from dotenv import load_dotenv

_dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
load_dotenv(_dotenv_path)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), ".."))

from scripts.database_management import TrackDB  # noqa: E402
from scripts.logs_utils import setup_logging, write_log  # noqa: E402

ENV = os.getenv("APP_ENV")

# Environment-specific paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
OUTPUT_ENV_DIR = os.path.join(BASE_DIR, "output", ENV or "default")
DB_PATH = os.path.join(OUTPUT_ENV_DIR, f"database_{ENV}.db") if ENV else ""
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", ENV or "default")
XML_DIR = OUTPUT_ENV_DIR
IMPORTED_DIR = os.path.join(BASE_DIR, "slskd_docker_data", ENV or "default", "imported")
DOWNLOADS_ROOT = os.path.join(BASE_DIR, "slskd_docker_data", ENV or "default", "downloads")
IS_DOCKER = os.path.exists("/.dockerenv")

# Documentation paths -- slug -> (display name, file path)
DOCS_DIR = os.path.join(BASE_DIR, "docs")
DOC_FILES = {
    "overview": ("Overview", os.path.join(BASE_DIR, "README.md")),
    "dashboard-guide": ("Dashboard Guide", os.path.join(DOCS_DIR, "DASHBOARD.md")),
    "configuration": ("Configuration", os.path.join(DOCS_DIR, "CONFIGURATION.md")),
    "troubleshooting": ("Troubleshooting", os.path.join(DOCS_DIR, "TROUBLESHOOTING.md")),
}
DEFAULT_DOC_SLUG = "overview"

# Database singleton (None if ENV not set, mirroring observability/dashboard/config.py)
track_db = None
if ENV:
    os.makedirs(OUTPUT_ENV_DIR, exist_ok=True)
    os.makedirs(IMPORTED_DIR, exist_ok=True)
    setup_logging(log_name_prefix="task_scheduler", rotate_daily=True)
    try:
        track_db = TrackDB()
    except Exception as e:
        write_log.error("DASHBOARD_NEXT_DB_INIT_FAIL", "Failed to initialize database.", {"db_path": DB_PATH, "error": str(e)})
        track_db = None
