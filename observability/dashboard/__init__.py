"""
Spotiseek Dashboard Package.

This package contains the modular components for the Streamlit dashboard.
"""

from .config import (
    ENV,
    BASE_DIR,
    LOGS_DIR,
    OUTPUT_ENV_DIR,
    DB_PATH,
    IMPORTED_DIR,
    DOWNLOADS_ROOT,
    XML_DIR,
    M3U8_DIR,
    IS_DOCKER,
    CACHE_TTL_SHORT,
    CACHE_TTL_MEDIUM,
    CACHE_TTL_LONG,
    track_db,
)

from .helpers import (
    require_database,
    sanitize_filename,
    normalize_docker_path,
    is_quality_worse_than_mp3_320,
    extract_metadata_from_file,
    compute_effective_bitrate_kbps,
    do_track_import,
)

__all__ = [
    # Config
    "ENV",
    "BASE_DIR",
    "LOGS_DIR",
    "OUTPUT_ENV_DIR",
    "DB_PATH",
    "IMPORTED_DIR",
    "DOWNLOADS_ROOT",
    "XML_DIR",
    "M3U8_DIR",
    "IS_DOCKER",
    "CACHE_TTL_SHORT",
    "CACHE_TTL_MEDIUM",
    "CACHE_TTL_LONG",
    "track_db",
    # Helpers
    "require_database",
    "sanitize_filename",
    "normalize_docker_path",
    "is_quality_worse_than_mp3_320",
    "extract_metadata_from_file",
    "compute_effective_bitrate_kbps",
    "do_track_import",
]
