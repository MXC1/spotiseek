"""
Dashboard configuration module.

Contains environment-specific paths, constants, and shared state.
This module should be imported first by other dashboard modules.
"""

import os
import sys

# Disable .pyc file generation
sys.dont_write_bytecode = True

from dotenv import load_dotenv

# Load environment variables from .env file
_dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
load_dotenv(_dotenv_path)

# Add parent directory to path to import from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), '..'))

from scripts.logs_utils import setup_logging, write_log
from scripts.database_management import TrackDB

# Get environment from environment variable
ENV = os.getenv("APP_ENV")

# Environment-specific paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", ENV or "default")
OUTPUT_ENV_DIR = os.path.join(BASE_DIR, "output", ENV or "default")
DB_PATH = os.path.join(OUTPUT_ENV_DIR, f"database_{ENV}.db") if ENV else ""
IMPORTED_DIR = os.path.join(BASE_DIR, "slskd_docker_data", ENV or "default", "imported")
DOWNLOADS_ROOT = os.path.join(BASE_DIR, "slskd_docker_data", ENV or "default", "downloads")
XML_DIR = OUTPUT_ENV_DIR
M3U8_DIR = os.path.join(OUTPUT_ENV_DIR, "m3u8s")

# Documentation paths
DOCS_DIR = os.path.join(BASE_DIR, "docs")
DOC_FILES = {
    "Overview": os.path.join(BASE_DIR, "README.md"),
    "Dashboard Guide": os.path.join(DOCS_DIR, "DASHBOARD.md"),
    "Configuration": os.path.join(DOCS_DIR, "CONFIGURATION.md"),
    "Troubleshooting": os.path.join(DOCS_DIR, "TROUBLESHOOTING.md"),
}

# Check if running in Docker
IS_DOCKER = os.path.exists("/.dockerenv")

# Cache TTL values for different operation types
CACHE_TTL_SHORT = 300   # 5 minutes for stats queries
CACHE_TTL_LONG = 600    # 10 minutes for expensive log analysis
CACHE_TTL_MEDIUM = 180  # 3 minutes for import data

# Initialize database singleton (None if ENV not set)
track_db = None
if ENV:
    # Ensure directories exist
    os.makedirs(IMPORTED_DIR, exist_ok=True)
    os.makedirs(XML_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(M3U8_DIR, exist_ok=True)
    
    # Initialize logging - use task_scheduler logs for unified logging
    setup_logging(log_name_prefix="task_scheduler", rotate_daily=True)
    
    # Initialize database
    try:
        _db_existed = os.path.exists(DB_PATH)
        track_db = TrackDB()
        if not _db_existed and os.path.exists(DB_PATH):
            write_log.info("DASHBOARD_DB_CREATED", "Database file was created during initialization.", {"db_path": DB_PATH})
    except Exception as e:
        write_log.error("DASHBOARD_DB_INIT_FAIL", "Failed to initialize database.", {"db_path": DB_PATH, "error": str(e)})
        track_db = None
