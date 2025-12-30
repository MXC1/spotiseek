"""Workflow orchestrator for Spotiseek.

This module provides task functions for the task-based scheduler, coordinating:
1. Reading playlist URLs from CSV
2. Fetching track metadata via the unified playlist scraper (auto-detects Spotify vs SoundCloud)
3. Initiating downloads via Soulseek
4. Remuxing all downloads to preferred formats (lossless -> WAV, lossy -> MP3 320kbps)
5. Tracking download status in the database
6. Updating M3U8 playlist files
7. Exporting iTunes-compatible XML library

Key Features:
- Environment-aware configuration (test/prod/stage)
- Multi-platform playlist support (Spotify, SoundCloud)
- Playlist and track processing with error isolation
- Automatic format standardization (lossless -> WAV, lossy -> MP3 320kbps)
- Download status synchronization
- Quality upgrade system (upgrades lossy tracks to lossless when available)
- iTunes library export for music player integration

Task Functions (used by task_scheduler.py):
- task_scrape_playlists(): Fetch track metadata from playlists
- task_initiate_searches(): Queue tracks for Soulseek search
- task_poll_search_results(): Process completed searches
- task_sync_download_status(): Update download status from slskd
- task_mark_quality_upgrades(): Mark non-WAV tracks for upgrade
- task_process_upgrades(): Initiate quality upgrade searches
- task_export_library(): Generate iTunes-compatible XML
- task_remux_existing_files(): Remux files to match format preferences
"""

import os
import re
import subprocess
import sys
from datetime import datetime

from dotenv import load_dotenv

# Disable .pyc file generation for cleaner development
sys.dont_write_bytecode = True

# Load environment configuration
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

from scripts.constants import LOSSLESS_FORMATS, LOSSY_FORMATS  # noqa: E402
from scripts.database_management import TrackData, TrackDB  # noqa: E402
from scripts.logs_utils import setup_logging, write_log  # noqa: E402
from scripts.m3u8_manager import update_track_in_m3u8, write_playlist_m3u8  # noqa: E402
from scripts.playlist_scraper import get_tracks_from_playlist  # noqa: E402
from scripts.soulseek_client import (  # noqa: E402
    download_tracks_async,
    process_pending_searches,
    process_redownload_queue,
    query_download_status,
    remove_download_from_slskd,
    wait_for_slskd_ready,
)
from scripts.xml_exporter import export_itunes_xml  # noqa: E402

# Initialize logging with environment-specific directory
setup_logging(log_name_prefix="workflow")
write_log.debug("ENV_LOAD", "Environment variables loaded.", {"dotenv_path": dotenv_path})

# Remuxing mode configuration from environment
PREFER_MP3 = os.getenv("PREFER_MP3", "false").lower() in ("true", "1", "yes")

# Validate environment configuration
ENV = os.getenv("APP_ENV")
if not ENV:
    raise OSError(
        "APP_ENV environment variable is not set. Workflow execution is disabled. "
        "Set APP_ENV to 'test', 'stage', or 'prod'.",
    )

write_log.info("ENV", "Running in environment.", {"ENV": ENV})


# Configuration Management

class WorkflowConfig:
    """Centralized configuration for workflow execution.

    All paths are environment-aware and automatically created if they don't exist.
    """

    def __init__(self, env: str):
        """Initialize workflow configuration for specified environment.

        Args:
            env: Environment name ('test', 'stage', or 'prod')

        """
        self.env = env
        self.base_dir = os.path.dirname(os.path.dirname(__file__))

        # Playlist configuration
        self.playlists_dir = os.path.abspath(os.path.join(self.base_dir, "input_playlists"))
        self.playlists_csv = os.path.join(self.playlists_dir, f"playlists_{env}.csv")

        # Unified output structure: output/{ENV}/
        output_env_dir = os.path.abspath(os.path.join(self.base_dir, "output", env))

        # Database configuration
        self.database_dir = output_env_dir
        self.db_path = os.path.join(self.database_dir, f"database_{env}.db")

        # M3U8 files configuration (output/{ENV}/m3u8s)
        self.m3u8_dir = os.path.abspath(os.path.join(output_env_dir, "m3u8s"))

        # XML export configuration (XML file lives directly in output/{ENV}/)
        self.xml_dir = output_env_dir

        # Downloads configuration
        self.downloads_root = os.path.abspath(os.path.join(self.base_dir, "slskd_docker_data", env, "downloads"))

        # Logs configuration
        self.logs_dir = os.path.abspath(os.path.join(self.base_dir, "observability", "logs", ENV))

        # Create all necessary directories
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        directories = [
            self.database_dir,
            self.m3u8_dir,
            self.xml_dir,
            self.logs_dir,
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def get_xml_export_path(self) -> str:
        """Get the path for iTunes XML library export (library_{ENV}.xml)."""
        return os.path.join(self.xml_dir, f"library_{self.env}.xml")

    def get_music_folder_url(self) -> str:
        """Get the music folder URL for iTunes XML export.

        Handles Docker container to host path conversion if needed.
        """
        downloads_path = self.downloads_root
        host_base_path = os.getenv("HOST_BASE_PATH")

        # Convert Docker container path to host path if running in Docker
        if host_base_path and downloads_path.startswith("/app/"):
            downloads_path = downloads_path.replace("/app/", f"{host_base_path}/", 1)

        # Format as file:// URL
        return f"file://localhost/{downloads_path.replace(os.sep, '/')}/"


# Initialize configuration
config = WorkflowConfig(ENV)

# Initialize database connection
track_db = TrackDB()


# Playlist Processing Functions

def read_playlists_from_csv(csv_path: str) -> list[str]:
    """Read playlist URLs from a CSV file.

    Each row should contain one playlist URL. Empty rows and comment lines (starting with #) are skipped.
    Inline comments after URLs (using #) are also supported.

    Args:
        csv_path: Path to CSV file containing playlist URLs

    Returns:
        List of playlist URL strings

    Raises:
        FileNotFoundError: If CSV file doesn't exist

    Example:
        >>> urls = read_playlists_from_csv("playlists/test/playlists_test.csv")
        >>> len(urls)
        5

    """
    write_log.info("PLAYLISTS_READ", "Reading playlists from CSV.", {"csv_path": csv_path})

    playlists = []
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        for raw_line in csvfile:
            # Strip whitespace
            stripped = raw_line.strip()

            # Skip empty lines or lines starting with #
            if not stripped or stripped.startswith("#"):
                continue

            # Remove inline comments (text after #)
            url = stripped.split("#")[0].strip()

            # Add URL if it's not empty after removing comments
            if url:
                playlists.append(url)

    write_log.info("PLAYLISTS_READ_SUCCESS", "Successfully read playlists.", {"count": len(playlists)})
    return playlists


def sanitize_playlist_name(playlist_name: str) -> str:
    """Sanitize playlist name for use as filename on Windows.

    Removes or replaces characters that are invalid in Windows filenames.

    Args:
        playlist_name: Original playlist name from Spotify

    Returns:
        Sanitized name safe for Windows filenames

    Example:
        >>> sanitize_playlist_name('My Playlist: Best Songs!')
        'My_Playlist_Best_Songs'

    """
    # Replace invalid Windows filename characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*,]', "_", playlist_name)
    # Replace spaces with underscores
    sanitized = sanitized.replace(" ", "_")
    return sanitized


def _delete_local_file(local_file_path: str | None, track_id: str) -> None:
    """Remove a local audio file if present."""
    if not local_file_path:
        return

    try:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            write_log.info(
                "AUDIO_FILE_DELETED",
                "Deleted audio file for removed track.",
                {"track_id": track_id, "file_path": local_file_path},
            )
        else:
            write_log.debug(
                "AUDIO_FILE_MISSING",
                "Audio file already absent during deletion.",
                {"track_id": track_id, "file_path": local_file_path},
            )
    except Exception as e:
        write_log.warn(
            "AUDIO_FILE_DELETE_FAIL",
            "Failed to delete audio file.",
            {"track_id": track_id, "file_path": local_file_path, "error": str(e)},
        )


def _remove_track_if_orphaned(track_id: str, local_file_path: str | None) -> None:
    """Delete track row and file when no playlists reference it."""
    remaining = track_db.get_playlist_usage_count(track_id)
    if remaining > 0:
        return

    track_db.delete_track(track_id)
    _delete_local_file(local_file_path, track_id)


def _rewrite_playlist_m3u8_from_db(playlist_url: str, m3u8_path: str) -> None:
    """Rewrite an M3U8 file to match current DB state for a playlist."""
    if not m3u8_path:
        return

    tracks = track_db.get_playlist_tracks_with_metadata(playlist_url)

    if not tracks:
        if os.path.exists(m3u8_path):
            try:
                os.remove(m3u8_path)
                write_log.info(
                    "M3U8_DELETE_EMPTY_PLAYLIST",
                    "Removed M3U8 file for empty playlist.",
                    {"playlist_url": playlist_url, "m3u8_path": m3u8_path},
                )
            except Exception as e:
                write_log.warn(
                    "M3U8_DELETE_EMPTY_FAIL",
                    "Failed to delete empty playlist M3U8 file.",
                    {"playlist_url": playlist_url, "m3u8_path": m3u8_path, "error": str(e)},
                )
        return

    try:
        lines = ["#EXTM3U\n"]
        for track_id, artist, track_name, local_file_path in tracks:
            if local_file_path:
                lines.append(f"{local_file_path}\n")
            else:
                artist_text = artist or ""
                name_text = track_name or ""
                lines.append(f"# {track_id} - {artist_text} - {name_text}\n")

        with open(m3u8_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        write_log.error(
            "M3U8_REWRITE_FAIL",
            "Failed to rewrite M3U8 file from DB state.",
            {"playlist_url": playlist_url, "m3u8_path": m3u8_path, "error": str(e)},
        )


def _prune_removed_tracks_for_playlist(
    playlist_url: str,
    playlist_name: str,
    m3u8_path: str,
    current_tracks: list[tuple[str, str, str]],
) -> None:
    """Remove tracks that are no longer present in the Spotify playlist."""
    current_ids = {track[0] for track in current_tracks}
    existing_ids = set(track_db.get_track_ids_for_playlist(playlist_url))
    removed_ids = existing_ids - current_ids

    if not removed_ids:
        return

    removed_count = 0
    for track_id in removed_ids:
        local_file_path = track_db.get_local_file_path(track_id)
        track_db.unlink_track_from_playlist(track_id, playlist_url)
        _remove_track_if_orphaned(track_id, local_file_path)
        removed_count += 1

    _rewrite_playlist_m3u8_from_db(playlist_url, m3u8_path)

    write_log.info(
        "PLAYLIST_TRACKS_PRUNED",
        "Removed tracks no longer present in playlist input.",
        {"playlist_url": playlist_url, "playlist_name": playlist_name, "removed": removed_count},
    )


def _prune_missing_playlists(input_playlist_urls: list[str]) -> None:
    """Remove playlists absent from input CSV and clean up orphaned tracks/files."""
    desired = set(input_playlist_urls)
    existing = set(track_db.get_all_playlist_urls())
    missing = existing - desired

    if not missing:
        return

    for playlist_url in missing:
        m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
        tracks = track_db.get_playlist_tracks_with_metadata(playlist_url)
        track_db.delete_playlist(playlist_url)

        if m3u8_path and os.path.exists(m3u8_path):
            try:
                os.remove(m3u8_path)
                write_log.info(
                    "M3U8_DELETE_PLAYLIST_REMOVED",
                    "Deleted M3U8 file for removed playlist.",
                    {"playlist_url": playlist_url, "m3u8_path": m3u8_path},
                )
            except Exception as e:
                write_log.warn(
                    "M3U8_DELETE_PLAYLIST_FAIL",
                    "Failed to delete M3U8 file for removed playlist.",
                    {"playlist_url": playlist_url, "m3u8_path": m3u8_path, "error": str(e)},
                )

        for track_id, _, _, local_file_path in tracks:
            _remove_track_if_orphaned(track_id, local_file_path)

    write_log.info(
        "PLAYLISTS_PRUNED",
        "Pruned playlists not present in input CSV.",
        {"removed_count": len(missing)},
    )


def process_playlist(playlist_url: str) -> list[tuple[str, str, str]]:
    """Process a single playlist: fetch tracks and add to database.

    This function:
    1. Fetches playlist name and tracks from Spotify or SoundCloud
    2. Generates M3U8 file path
    3. Adds playlist to database
    4. Creates M3U8 file with track comments
    5. Adds tracks to database
    6. Links tracks to playlist in database

    Args:
        playlist_url: Playlist URL (Spotify or SoundCloud)

    Returns:
        List of tracks to be downloaded: [(track_id, artist, track_name), ...]

    Note:
        Errors are logged but don't stop processing. Individual track
        failures are isolated and won't affect other tracks.

    """
    write_log.info("PLAYLIST_PROCESS", "Processing playlist.", {"playlist_url": playlist_url})

    # Fetch playlist metadata and tracks from the appropriate platform
    try:
        playlist_name, tracks, source = get_tracks_from_playlist(playlist_url)
        write_log.info("PLAYLIST_FETCH_SUCCESS", "Fetched tracks from playlist.",
                      {"playlist_name": playlist_name, "track_count": len(tracks), "source": source})
    except Exception as e:
        write_log.error("PLAYLIST_FETCH_FAIL", "Failed to get tracks for playlist.",
                       {"playlist_url": playlist_url, "error": str(e)})
        return None

    # Generate M3U8 file path with sanitized playlist name
    safe_name = sanitize_playlist_name(playlist_name)
    m3u8_path = os.path.join(config.m3u8_dir, f"{safe_name}.m3u8")

    # Add playlist to database
    try:
        playlist_id = track_db.add_playlist(playlist_url, m3u8_path, playlist_name)
        track_db.update_playlist_m3u8_path(playlist_url, m3u8_path)
        track_db.update_playlist_name(playlist_url, playlist_name)
        write_log.debug("PLAYLIST_DB_SUCCESS", "Playlist added to database.",
                       {"playlist_id": playlist_id, "playlist_url": playlist_url})
    except Exception as e:
        write_log.error("PLAYLIST_DB_FAIL", "Failed to add playlist to database.",
                       {"playlist_url": playlist_url, "error": str(e)})
        return None

    try:
        _prune_removed_tracks_for_playlist(playlist_url, playlist_name, m3u8_path, tracks)
    except Exception as e:
        write_log.error(
            "PLAYLIST_PRUNE_FAIL",
            "Failed to prune removed tracks for playlist.",
            {"playlist_url": playlist_url, "error": str(e)},
        )

    # Create M3U8 file with track metadata as comments (only if it doesn't exist)
    if not os.path.exists(m3u8_path):
        try:
            write_playlist_m3u8(m3u8_path, tracks)
            write_log.info("M3U8_CREATE", "Created new M3U8 file.", {"m3u8_path": m3u8_path})
        except Exception as e:
            write_log.error("M3U8_WRITE_FAIL", "Failed to write M3U8 file for playlist.",
                           {"playlist_url": playlist_url, "m3u8_path": m3u8_path, "error": str(e)})
    else:
        write_log.debug("M3U8_EXISTS", "M3U8 file already exists, preserving it.", {"m3u8_path": m3u8_path})

    # Add tracks to database and collect for batch download
    tracks_to_download = []
    for track in tracks:
        try:
            track_id, artist, track_name = track

            # Add track to database (INSERT OR IGNORE - won't duplicate)
            track_db.add_track(TrackData(
                track_id=track_id,
                track_name=track_name,
                artist=artist,
                source=source,
            ))

            # Link track to playlist in database
            track_db.link_track_to_playlist(track_id, playlist_url)

            # Only collect tracks that need to be searched for
            # Skip tracks that are already being processed or completed
            current_status = track_db.get_track_status(track_id)
            skip_statuses = {
                "completed", "queued", "downloading", "searching",
                "requested", "inprogress", "redownload_pending",
            }

            if current_status not in skip_statuses:
                tracks_to_download.append(track)

        except Exception as e:
            track_name = track[2] if len(track) > 2 else str(track)  # noqa: PLR2004
            write_log.error("TRACK_PROCESS_FAIL", "Failed to process track.",
                           {"track": track_name, "error": str(e)})
            # Update database status if possible
            if len(track) > 0:
                try:
                    track_db.update_track_status(track[0], "failed", failed_reason=str(e))
                except Exception:
                    pass  # If status update fails, continue anyway

    try:
        _rewrite_playlist_m3u8_from_db(playlist_url, m3u8_path)
    except Exception as e:
        write_log.error(
            "M3U8_FINAL_REWRITE_FAIL",
            "Failed to rewrite M3U8 after playlist processing.",
            {"playlist_url": playlist_url, "error": str(e)},
        )

    return tracks_to_download


# Download Status Management Functions

def update_download_statuses() -> None:
    """Query slskd API for download status and update database accordingly.

    This function:
    1. Queries all active downloads from slskd API
    2. Maps slskd UUIDs back to Spotify IDs via database
    3. Updates track status and local file paths in database
    4. Updates M3U8 files with completed download paths

    Note:
        This function should be called periodically during workflow execution
        to keep the database synchronized with actual download states.

    """
    write_log.info("DOWNLOAD_STATUS_UPDATE", "Checking download statuses from slskd.")

    download_statuses = query_download_status()

    for status in download_statuses:
        username = status.get("username")
        directories = status.get("directories", [])
        for directory in directories:
            files = directory.get("files", [])
            for file in files:
                _update_file_status(file, username)


def mark_tracks_for_quality_upgrade() -> None:
    """Identify completed tracks that don't meet quality requirements and mark for upgrade.

    This function:
    1. Queries all tracks with status='completed'
    2. Checks if they meet quality requirements based on PREFER_MP3 setting
    3. Marks ineligible tracks as 'redownload_pending' for quality upgrade

    Quality requirements:
    - If PREFER_MP3 is False: Target is WAV (lossless, all formats)
      - Non-WAV files are marked for upgrade to lossless
    - If PREFER_MP3 is True: Target is MP3 320kbps (lossy but high quality)
      - Non-MP3 files are marked for upgrade
      - MP3 files with bitrate < 320kbps are marked for upgrade
    - The actual upgrade decision (whether a better file exists) happens during search

    Note:
        This function can be called multiple times safely. Tracks that meet quality
        requirements will not be marked for upgrade, while those that don't will be
        queued for search.

    """
    write_log.info("QUALITY_UPGRADE_SCAN", "Scanning completed tracks for quality upgrade opportunities.")

    # Get all completed tracks
    completed_tracks = track_db.get_tracks_by_status("completed")

    if not completed_tracks:
        write_log.debug("QUALITY_UPGRADE_NO_COMPLETED", "No completed tracks found to check for upgrades.")
        return

    write_log.info(
        "QUALITY_UPGRADE_CHECKING",
        f"Checking {len(completed_tracks)} completed tracks for upgrade eligibility.",
    )

    upgrade_count = 0

    for track_row in completed_tracks:
        track_id = track_row[0]  # First column is track_id

        # Get current file properties
        current_extension = track_db.get_track_extension(track_id)
        current_bitrate = track_db.get_track_bitrate(track_id)

        # Determine if track meets quality requirements
        meets_requirements = False

        if PREFER_MP3:
            # Target: MP3 320kbps
            if current_extension and current_extension.lower() == "mp3":
                # MP3 file: check if it's high quality (320kbps)
                if current_bitrate and current_bitrate >= 320:
                    meets_requirements = True
        else:
            # Target: WAV (lossless)
            if current_extension and current_extension.lower() == "wav":
                meets_requirements = True

        # Mark for upgrade if doesn't meet requirements
        if not meets_requirements:
            track_db.update_track_status(track_id, "redownload_pending")
            upgrade_count += 1
            write_log.debug(
                "QUALITY_UPGRADE_MARKED",
                "Marked track for quality upgrade.",
                {
                    "track_id": track_id,
                    "extension": current_extension or "unknown",
                    "bitrate": current_bitrate or "unknown",
                },
            )

    if upgrade_count > 0:
        target = "MP3 320kbps" if PREFER_MP3 else "WAV"
        write_log.info(
            "QUALITY_UPGRADE_MARKED_COMPLETE",
            f"Marked {upgrade_count} tracks for quality upgrade (target: {target}).",
            {"marked_count": upgrade_count, "total_completed": len(completed_tracks)},
        )
    else:
        target = "MP3 320kbps" if PREFER_MP3 else "WAV"
        write_log.info(
            "QUALITY_UPGRADE_NO_CANDIDATES",
            f"All completed tracks already meet quality requirements (target: {target})."
        )


def _update_file_status(file: dict, username: str | None = None) -> None:
    """Update database status for a single download file.

    Maps slskd file states to database status values and updates accordingly.
    For completed downloads, also updates the local file path and M3U8 files.
    For failed downloads, removes the download from slskd to prevent duplicate logs.

    Args:
        file: File object from slskd API containing id, state, filename
        username: Soulseek username the download is from (used for removing failed downloads)

    """
    slskd_uuid = file.get("id")
    track_id = track_db.get_track_id_by_slskd_download_uuid(slskd_uuid)
    download_username = username or track_db.get_username_by_slskd_uuid(slskd_uuid)

    if not track_id:
        write_log.debug("SLSKD_UUID_UNKNOWN", "No track ID found for slskd UUID.",
                       {"slskd_uuid": slskd_uuid})
        return

    state = file.get("state")
    write_log.debug("FILE_STATUS_UPDATE", "Updating file status.",
                   {"track_id": track_id, "state": state})

    # Define failed states for comparison
    failed_states = (
        "Completed, Errored", "Completed, TimedOut", "Completed, Cancelled",
        "Completed, Rejected", "Completed, Aborted",
    )

    # Handle successful downloads
    if state == "Completed, Succeeded":
        _handle_completed_download(file, track_id)

    # Handle failed downloads - remove from slskd to prevent duplicate logs
    elif state in failed_states:
        failed_reason = (
            file.get("error")
            or file.get("errorMessage")
            or file.get("message")
            or state
        )
        track_db.update_track_status(track_id, "failed", failed_reason=failed_reason)
        write_log.info(
            "DOWNLOAD_FAILED",
            "Download failed.",
            {"track_id": track_id, "state": state, "failed_reason": failed_reason},
        )

    # Handle queued downloads
    elif state == "Queued, Remotely":
        track_db.update_track_status(track_id, "queued")

    # Handle in-progress downloads
    elif state == "InProgress":
        track_db.update_track_status(track_id, "downloading")

    # Handle unknown states
    else:
        normalized_state = state.lower().replace(" ", "_").replace(",", "")
        track_db.update_track_status(track_id, normalized_state)
        write_log.debug("DOWNLOAD_STATE_UNKNOWN", "Unknown download state encountered.",
                       {"track_id": track_id, "state": state})

    if state.startswith("Completed"):
        if download_username and slskd_uuid:
            remove_download_from_slskd(download_username, slskd_uuid)
        else:
            write_log.warn("DOWNLOAD_REMOVE_SKIP", "Cannot remove failed download - missing username or UUID.",
                            {"track_id": track_id, "slskd_uuid": slskd_uuid, "username": download_username})


def _should_skip_completed_download(track_id: str) -> bool:
    """Check if a completed download should be skipped from processing.

    Skips tracks that are either marked for redownload (quality upgrade pending)
    or already fully processed to prevent redundant remuxing.

    Args:
        track_id: Track identifier

    Returns:
        True if the download should be skipped, False otherwise

    """
    current_status = track_db.get_track_status(track_id)
    if current_status == "redownload_pending":
        write_log.debug("DOWNLOAD_SKIP_REDOWNLOAD", "Skipping status update for track marked for redownload.",
                       {"track_id": track_id})
        return True

    if current_status == "completed":
        write_log.debug("DOWNLOAD_ALREADY_PROCESSED", "Skipping already completed download.",
                       {"track_id": track_id})
        return True

    return False


def _compute_download_local_path(file: dict) -> str | None:
    """Compute the local file path for a downloaded file from slskd.

    Extracts the last two path components (parent folder and filename) from
    the slskd file path to create a cleaner local path structure.

    Args:
        file: File object from slskd API containing 'filename' key

    Returns:
        Absolute local file path, or None if filename is missing

    Example:
        >>> file = {"filename": "Collection\\Artist\\Album\\Track.mp3"}
        >>> _compute_download_local_path(file)
        "/downloads/Album/Track.mp3"

    """
    filename_rel = file.get("filename")
    if not filename_rel:
        return None

    normalized_path = filename_rel.replace("\\", "/")
    path_parts = normalized_path.split("/")

    if len(path_parts) >= 2:  # noqa: PLR2004
        relative_path = "/".join(path_parts[-2:])
    elif len(path_parts) == 1:
        relative_path = path_parts[0]
    else:
        relative_path = filename_rel

    return os.path.join(config.downloads_root, relative_path)


def _is_duplicate_record(track_id: str, local_file_path: str) -> bool:
    """Check if a download is a duplicate record (same file already tracked).

    Prevents reprocessing old slskd records that appear after quality upgrade
    resets or when slskd history contains already-processed downloads.

    Args:
        track_id: Track identifier
        local_file_path: Path to the newly downloaded file

    Returns:
        True if this file is already tracked in the database, False otherwise

    """
    existing_path = track_db.get_local_file_path(track_id)
    if not existing_path:
        return False

    existing_normalized = existing_path.replace("\\", "/").lower()
    new_normalized = local_file_path.replace("\\", "/").lower()
    paths_match = (
        existing_normalized == new_normalized
        or os.path.basename(existing_normalized) == os.path.basename(new_normalized)
    )
    if paths_match:
        write_log.debug("DOWNLOAD_DUPLICATE_RECORD", "Skipping old slskd record - file already tracked.",
                       {"track_id": track_id, "existing_path": existing_path, "slskd_path": local_file_path})
    return paths_match


def _extract_extension_bitrate(file: dict, local_file_path: str) -> tuple[str | None, int | None]:
    """Extract file extension and bitrate from slskd file metadata.

    Attempts to get extension from file metadata first, falling back to
    extracting it from the filename if not available.

    Args:
        file: File object from slskd API
        local_file_path: Local file path (used as fallback for extension)

    Returns:
        Tuple of (extension, bitrate). Extension is lowercase, bitrate in kbps.
        Either value may be None if not determinable.

    """
    extension = None
    if file.get("extension"):
        extension = file["extension"].lower()
    elif "." in local_file_path:
        extension = local_file_path.rsplit(".", 1)[-1].lower()

    bitrate = file.get("bitRate") or file.get("bitrate")
    try:
        bitrate = int(bitrate) if bitrate is not None else None
    except (ValueError, TypeError):
        bitrate = None

    return extension, bitrate


def _remux_completed_download(track_id: str, local_file_path: str, extension: str | None, bitrate: int | None) -> str:
    """Remux a completed download to the preferred format based on configuration.

    Remuxing behavior depends on PREFER_MP3 setting:
    - If True: All formats are converted to MP3 320kbps
    - If False: Lossless (FLAC/ALAC/APE) -> WAV, Lossy (OGG/M4A/etc) -> MP3 320kbps

    MP3 and WAV files are already in preferred format and are not remuxed.

    Args:
        track_id: Track identifier
        local_file_path: Path to the downloaded file
        extension: File extension (lowercase)
        bitrate: File bitrate in kbps (for MP3 files)

    Returns:
        Final file path after remuxing (may be same as input if no remux needed)

    """
    # Exclude 'wav' from lossless set - already in target format
    lossless_to_remux = LOSSLESS_FORMATS - {"wav"}
    final_path = local_file_path

    if PREFER_MP3:
        if extension in lossless_to_remux or extension in LOSSY_FORMATS or extension == "wav":
            final_path = _remux_lossy_to_mp3(local_file_path, track_id, extension) or local_file_path
    elif extension in lossless_to_remux:
        final_path = _remux_lossless_to_wav(local_file_path, track_id, extension) or local_file_path
    elif extension in LOSSY_FORMATS:
        final_path = _remux_lossy_to_mp3(local_file_path, track_id, extension) or local_file_path
    elif extension == "mp3":
        track_db.update_extension_bitrate(track_id, extension="mp3", bitrate=bitrate)
    elif extension == "wav":
        track_db.update_extension_bitrate(track_id, extension="wav", bitrate=None)

    return final_path


def _handle_completed_download(file: dict, track_id: str) -> None:
    """Process a successfully completed download.

    This function:
    1. Checks if track is marked for redownload (skips status update if so)
    2. Extracts the file path from slskd response
    3. Constructs the local file path
    4. Checks if this is an old slskd record being reprocessed (same file already tracked)
    5. Updates the database with the local file path
    6. Updates all M3U8 files that contain this track

    Args:
        file: File object from slskd API
        track_id: Track identifier

    """
    if _should_skip_completed_download(track_id):
        return

    local_file_path = _compute_download_local_path(file)
    if not local_file_path:
        write_log.warn("DOWNLOAD_NO_FILENAME", "Completed download has no filename.",
                      {"track_id": track_id})
        track_db.update_track_status(track_id, "completed")
        return

    if _is_duplicate_record(track_id, local_file_path):
        return

    extension, bitrate = _extract_extension_bitrate(file, local_file_path)
    final_path = _remux_completed_download(track_id, local_file_path, extension, bitrate)

    existing_path = track_db.get_local_file_path(track_id)
    track_db.update_local_file_path(track_id, final_path)
    write_log.debug(
        "DOWNLOAD_COMPLETE",
        "Download completed successfully.",
        {
            "track_id": track_id,
            "local_file_path": final_path,
            "is_new": not bool(existing_path),
        },
    )
    _update_m3u8_files_for_track(track_id, final_path)
    track_db.update_track_status(track_id, "completed")

def _is_audio_valid(audio_path: str) -> bool:
    """Use ffmpeg to check if an audio file is valid and decodable.
    Returns True if valid, False otherwise.
    """
    try:
        result = subprocess.run([
            "ffmpeg", "-v", "error", "-i", audio_path, "-f", "null", "-",
        ], check=False, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        write_log.error(
            "AUDIO_CHECK_FAIL",
            "Failed to check audio file integrity.",
            {"audio_path": audio_path, "error": str(e)},
        )
        return False


def _get_ffmpeg_log_path() -> str:
    """Get the path for the FFmpeg remux log file.

    Creates dated directory structure and returns path to shared log file.

    Returns:
        Absolute path to ffmpeg_remux.log

    """
    base_dir = os.path.dirname(os.path.dirname(__file__))
    logs_dir = os.path.join(base_dir, "observability", "logs", ENV)
    now = datetime.now()
    dated_logs_dir = os.path.join(
        logs_dir,
        now.strftime("%Y"),
        now.strftime("%m"),
        now.strftime("%d"),
    )
    os.makedirs(dated_logs_dir, exist_ok=True)
    return os.path.join(dated_logs_dir, "ffmpeg_remux.log")


def _run_ffmpeg_remux(
    input_path: str,
    output_path: str,
    ffmpeg_args: list[str],
    log_context: dict[str, str],
) -> bool:
    """Run FFmpeg to remux an audio file with logging.

    Args:
        input_path: Path to input file (already normalized with forward slashes)
        output_path: Path to output file (already normalized with forward slashes)
        ffmpeg_args: FFmpeg codec arguments (e.g., ["-codec:a", "pcm_s16le", "-ar", "44100"])
        log_context: Dict with 'track_id', 'source_ext', 'target_ext' for logging

    Returns:
        True if remux succeeded, False otherwise

    """
    track_id = log_context["track_id"]
    source_ext = log_context["source_ext"]
    target_ext = log_context["target_ext"]

    ffmpeg_cmd = ["ffmpeg", "-y", "-i", input_path, *ffmpeg_args, output_path]
    ffmpeg_log_file = _get_ffmpeg_log_path()
    now = datetime.now()

    write_log.debug(
        f"FFMPEG_REMUX_{source_ext.upper()}",
        f"Remuxing {source_ext.upper()} to {target_ext.upper()}.",
        {"input": input_path, "output": output_path, "ffmpeg_log_file": ffmpeg_log_file},
    )

    with open(ffmpeg_log_file, "a", encoding="utf-8") as logf:
        logf.write(
            f"\n--- Remux {now.strftime('%Y-%m-%d %H:%M:%S')} "
            f"| Track ID: {track_id} | Input: {input_path} | Output: {output_path} ---\n",
        )
        subprocess.run(ffmpeg_cmd, check=True, stdout=logf, stderr=subprocess.STDOUT)

    return True


def _cleanup_original_file(original_path: str, new_path: str, track_id: str, extension: str) -> None:
    """Remove the original file after successful remux.

    Args:
        original_path: Path to original file
        new_path: Path to remuxed file
        track_id: Track identifier for logging
        extension: Original file extension for logging

    """
    if not os.path.exists(original_path) or original_path == new_path:
        return

    try:
        os.remove(original_path)
        write_log.debug(
            "ORIGINAL_FILE_REMOVED",
            f"Deleted original {extension.upper()} file after remuxing.",
            {"track_id": track_id, "removed_file": original_path},
        )
    except Exception as e:
        write_log.warn(
            "ORIGINAL_FILE_DELETE_FAILED",
            f"Failed to delete original {extension.upper()} file after remuxing.",
            {"track_id": track_id, "file_path": original_path, "error": str(e)},
        )


def _handle_corrupt_audio(track_id: str, file_path: str, extension: str, is_lossless: bool) -> None:
    """Handle corrupt audio file by updating status and blacklisting.

    Args:
        track_id: Track identifier
        file_path: Path to corrupt file
        extension: File extension
        is_lossless: Whether the file was lossless format

    """
    event_id = "LOSSLESS_INVALID" if is_lossless else "LOSSY_INVALID"
    write_log.warn(
        event_id,
        f"{'Lossless' if is_lossless else 'Lossy'} file failed integrity check. Skipping remux.",
        {"track_id": track_id, "file_path": file_path, "extension": extension},
    )
    track_db.update_track_status(track_id, "corrupt")
    slskd_uuid_to_blacklist = track_db.get_download_uuid_by_track_id(track_id)
    if slskd_uuid_to_blacklist:
        track_db.add_slskd_blacklist(slskd_uuid_to_blacklist, reason=f"corrupt_{extension}")


def _remux_lossless_to_wav(local_file_path: str, track_id: str, extension: str) -> str:
    """Remux a lossless audio file (FLAC, ALAC, APE) to WAV.
    Update extension/bitrate in DB if successful.
    Returns the new WAV path if successful, else original path.
    """
    wav_path = os.path.splitext(local_file_path)[0] + ".wav"
    ffmpeg_input = local_file_path.replace("\\", "/")
    ffmpeg_output = wav_path.replace("\\", "/")

    try:
        # Check audio integrity before remuxing
        if not _is_audio_valid(ffmpeg_input):
            _handle_corrupt_audio(track_id, ffmpeg_input, extension, is_lossless=True)
            return local_file_path

        # Remux to WAV (16-bit, 44.1kHz)
        ffmpeg_args = ["-codec:a", "pcm_s16le", "-ar", "44100"]
        log_context = {"track_id": track_id, "source_ext": extension, "target_ext": "wav"}
        _run_ffmpeg_remux(ffmpeg_input, ffmpeg_output, ffmpeg_args, log_context)

        track_db.update_extension_bitrate(track_id, extension="wav", bitrate=None)
        write_log.debug(
            "REMUX_SUCCESS",
            f"{extension.upper()} remuxed to WAV.",
            {"track_id": track_id, "wav_path": wav_path},
        )

        _cleanup_original_file(local_file_path, wav_path, track_id, extension)
        return wav_path

    except Exception as e:
        write_log.error(
            "REMUX_FAIL",
            f"Failed to remux {extension.upper()} to WAV.",
            {"track_id": track_id, "error": str(e)},
        )
        track_db.update_extension_bitrate(track_id, extension=extension)
        return local_file_path


def _remux_lossy_to_mp3(local_file_path: str, track_id: str, extension: str) -> str:
    """Remux a lossy audio file (OGG, M4A, AAC, WMA, OPUS) to MP3 320kbps.
    Update extension/bitrate in DB if successful.
    Returns the new MP3 path if successful, else original path.
    """
    mp3_path = os.path.splitext(local_file_path)[0] + ".mp3"
    ffmpeg_input = local_file_path.replace("\\", "/")
    ffmpeg_output = mp3_path.replace("\\", "/")

    try:
        # Check audio integrity before remuxing
        if not _is_audio_valid(ffmpeg_input):
            _handle_corrupt_audio(track_id, ffmpeg_input, extension, is_lossless=False)
            return local_file_path

        # Remux to MP3 320kbps
        ffmpeg_args = ["-codec:a", "libmp3lame", "-b:a", "320k"]
        log_context = {"track_id": track_id, "source_ext": extension, "target_ext": "mp3"}
        _run_ffmpeg_remux(ffmpeg_input, ffmpeg_output, ffmpeg_args, log_context)

        track_db.update_extension_bitrate(track_id, extension="mp3", bitrate=320)
        write_log.info(
            "REMUX_SUCCESS",
            f"{extension.upper()} remuxed to MP3 320kbps.",
            {"track_id": track_id, "mp3_path": mp3_path},
        )

        _cleanup_original_file(local_file_path, mp3_path, track_id, extension)
        return mp3_path

    except Exception as e:
        write_log.error(
            "REMUX_FAIL",
            f"Failed to remux {extension.upper()} to MP3.",
            {"track_id": track_id, "error": str(e)},
        )
        track_db.update_extension_bitrate(track_id, extension=extension)
        return local_file_path

def _update_m3u8_files_for_track(track_id: str, local_file_path: str) -> None:
    """Update all M3U8 files that contain a specific track.

    Replaces the track comment line with the actual file path in all
    playlists that contain this track.

    Args:
        track_id: Track identifier
        local_file_path: Absolute path to the downloaded file

    """
    try:
        # Get all playlists that contain this track
        playlist_urls = track_db.get_playlists_for_track(track_id)

        if not playlist_urls:
            write_log.debug("TRACK_NO_PLAYLISTS", "Track not linked to any playlists.",
                           {"track_id": track_id})
            return

        # Update M3U8 file for each playlist
        for playlist_url in playlist_urls:
            m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)

            if m3u8_path:
                update_track_in_m3u8(m3u8_path, track_id, local_file_path)
                write_log.debug("M3U8_TRACK_UPDATED", "Updated track in M3U8 file.",
                              {"track_id": track_id, "m3u8_path": m3u8_path})
            else:
                write_log.warn("M3U8_PATH_MISSING", "No M3U8 path found for playlist.",
                              {"playlist_url": playlist_url})

    except Exception as e:
        write_log.error("M3U8_UPDATE_FAIL", "Failed to update M3U8 files for track.",
                       {"track_id": track_id, "error": str(e)})


# ============================================================================
# TASK FUNCTIONS (for task-based scheduler)
# ============================================================================
# Each task function can be executed independently by the task scheduler.
# They return True on success, False on failure.

def task_scrape_playlists() -> bool:
    """Task: Scrape Spotify playlists and add tracks to database.

    This task:
    1. Reads playlist URLs from the CSV file
    2. Fetches track metadata from Spotify API
    3. Adds playlists and tracks to the database
    4. Creates M3U8 playlist files (if they don't exist)

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_SCRAPE_START", "Starting playlist scrape task.")

    try:
        # Load playlists from CSV
        try:
            playlists = read_playlists_from_csv(config.playlists_csv)
            write_log.info("PLAYLISTS_LOADED", "Loaded playlists from CSV.",
                          {"count": len(playlists)})
        except FileNotFoundError:
            fallback_csv = os.path.abspath(os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "input_playlists", "playlists.csv",
            ))
            write_log.warn("PLAYLISTS_CSV_MISSING",
                          "Primary playlists CSV not found, falling back to default.",
                          {"primary_csv": config.playlists_csv, "fallback_csv": fallback_csv})
            playlists = read_playlists_from_csv(fallback_csv)
            write_log.info("PLAYLISTS_LOADED_FALLBACK",
                          "Loaded playlists from fallback CSV.",
                          {"count": len(playlists)})

        # Persist CSV order into database for downstream ordering (XML, dashboard)
        try:
            for idx, purl in enumerate(playlists):
                track_db.set_playlist_display_order(purl, idx)
        except Exception as e:
            write_log.error(
                "PLAYLIST_ORDER_SET_FAIL",
                "Failed to set playlist display order from CSV.",
                {"error": str(e)},
            )

        _prune_missing_playlists(playlists)

        # Process each playlist
        total_tracks = 0
        for playlist_url in playlists:
            tracks = process_playlist(playlist_url)
            if tracks:
                total_tracks += len(tracks)

        write_log.info("TASK_SCRAPE_COMPLETE", "Playlist scrape task completed.",
                      {"playlists_processed": len(playlists), "tracks_found": total_tracks})
        return True

    except Exception as e:
        write_log.error("TASK_SCRAPE_FAILED", "Playlist scrape task failed.",
                       {"error": str(e)})
        return False


def task_initiate_searches() -> bool:
    """Task: Initiate Soulseek searches for tracks that need downloading.

    This task:
    1. Queries the database for tracks needing searches: 'pending', 'new', 'not_found', 'no_suitable_file'
    2. Skips tracks already in 'searching' status (pending search)
    3. Initiates async searches on slskd for each eligible track

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_INITIATE_SEARCHES_START", "Starting search initiation task.")

    try:
        # Wait for slskd to be ready
        if not wait_for_slskd_ready(max_wait_seconds=60, poll_interval=2):
            write_log.error("SLSKD_UNAVAILABLE",
                           "slskd service is not available. Cannot initiate searches.")
            return False

        # Get tracks that need searching
        candidates_statuses = ["pending", "new", "not_found", "no_suitable_file", "corrupt"]
        tracks_to_search = []

        for status in candidates_statuses:
            rows = track_db.get_tracks_by_status(status)
            for track_row in rows:
                # Skip if there is already an active search for this track
                track_id = track_row[0]
                current_status = track_db.get_track_status(track_id)
                if current_status == "searching":
                    write_log.debug(
                        "TASK_INITIATE_SEARCH_SKIP_SEARCHING",
                        "Skipping track with active search.",
                        {"track_id": track_id},
                    )
                    continue
                track_name = track_row[1]
                artist = track_row[2]
                tracks_to_search.append((track_id, artist, track_name))

        if not tracks_to_search:
            write_log.info("TASK_INITIATE_SEARCHES_NO_TRACKS",
                          "No tracks pending search.")
            return True

        # Initiate searches
        download_tracks_async(tracks_to_search)

        write_log.info("TASK_INITIATE_SEARCHES_COMPLETE",
                      "Search initiation task completed.",
                      {"tracks_searched": len(tracks_to_search)})
        return True

    except Exception as e:
        write_log.error("TASK_INITIATE_SEARCHES_FAILED",
                       "Search initiation task failed.",
                       {"error": str(e)})
        return False


def task_poll_search_results() -> bool:
    """Task: Poll slskd for completed searches and initiate downloads.

    This task:
    1. Checks for searches that have completed on slskd
    2. Processes search results and selects best files
    3. Initiates downloads for matched tracks

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_POLL_SEARCH_START", "Starting search polling task.")

    try:
        # Wait for slskd to be ready
        if not wait_for_slskd_ready(max_wait_seconds=60, poll_interval=2):
            write_log.error("SLSKD_UNAVAILABLE",
                           "slskd service is not available. Cannot poll searches.")
            return False

        # Process pending searches
        process_pending_searches()

        write_log.info("TASK_POLL_SEARCH_COMPLETE", "Search polling task completed.")
        return True

    except Exception as e:
        write_log.error("TASK_POLL_SEARCH_FAILED", "Search polling task failed.",
                       {"error": str(e)})
        return False


def task_sync_download_status() -> bool:
    """Task: Sync download status from slskd API to database.

    This task:
    1. Queries slskd for all active downloads
    2. Updates track status in the database
    3. Handles completed downloads (update file paths, M3U8 files)

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_SYNC_STATUS_START", "Starting download status sync task.")

    try:
        # Wait for slskd to be ready
        if not wait_for_slskd_ready(max_wait_seconds=60, poll_interval=2):
            write_log.error("SLSKD_UNAVAILABLE",
                           "slskd service is not available. Cannot sync status.")
            return False

        # Update download statuses
        update_download_statuses()

        write_log.info("TASK_SYNC_STATUS_COMPLETE", "Download status sync task completed.")
        return True

    except Exception as e:
        write_log.error("TASK_SYNC_STATUS_FAILED", "Download status sync task failed.",
                       {"error": str(e)})
        return False


def task_mark_quality_upgrades() -> bool:
    """Task: Mark completed non-WAV tracks for quality upgrade.

    This task:
    1. Scans all completed downloads
    2. Identifies tracks that are not in WAV format
    3. Marks them for potential quality upgrade

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_MARK_UPGRADES_START", "Starting quality upgrade marking task.")

    try:
        mark_tracks_for_quality_upgrade()

        write_log.info("TASK_MARK_UPGRADES_COMPLETE",
                      "Quality upgrade marking task completed.")
        return True

    except Exception as e:
        write_log.error("TASK_MARK_UPGRADES_FAILED",
                       "Quality upgrade marking task failed.",
                       {"error": str(e)})
        return False


def task_process_upgrades() -> bool:
    """Task: Process the quality upgrade queue.

    This task:
    1. Gets all tracks marked for redownload
    2. Initiates searches for better quality versions

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_PROCESS_UPGRADES_START", "Starting quality upgrade processing task.")

    try:
        # Wait for slskd to be ready
        if not wait_for_slskd_ready(max_wait_seconds=60, poll_interval=2):
            write_log.error("SLSKD_UNAVAILABLE",
                           "slskd service is not available. Cannot process upgrades.")
            return False

        # Process redownload queue
        process_redownload_queue()

        write_log.info("TASK_PROCESS_UPGRADES_COMPLETE",
                      "Quality upgrade processing task completed.")
        return True

    except Exception as e:
        write_log.error("TASK_PROCESS_UPGRADES_FAILED",
                       "Quality upgrade processing task failed.",
                       {"error": str(e)})
        return False


def task_export_library() -> bool:
    """Task: Export iTunes-compatible XML library.

    This task:
    1. Reads all completed tracks from database
    2. Generates iTunes-compatible XML file
    3. Saves to configured XML directory

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_EXPORT_LIBRARY_START", "Starting library export task.")

    try:
        xml_path = config.get_xml_export_path()
        music_folder_url = config.get_music_folder_url()

        export_itunes_xml(xml_path, music_folder_url)

        write_log.info("TASK_EXPORT_LIBRARY_COMPLETE",
                      "Library export task completed.",
                      {"xml_path": xml_path})
        return True

    except Exception as e:
        write_log.error("TASK_EXPORT_LIBRARY_FAILED", "Library export task failed.",
                       {"error": str(e)})
        return False


def _determine_remux_target(
    current_extension: str,
    lossless_formats: set[str],
    lossy_formats: set[str],
) -> tuple[bool, str | None]:
    """Determine if a file needs remuxing and what the target format should be.

    Decision logic based on PREFER_MP3 setting:
    - If True: All non-MP3 files should be converted to MP3
    - If False: Lossless -> WAV, Lossy -> MP3

    Args:
        current_extension: Current file extension (lowercase)
        lossless_formats: Set of lossless format extensions
        lossy_formats: Set of lossy format extensions

    Returns:
        Tuple of (needs_remux, target_format). target_format is None if no remux needed.

    """
    if PREFER_MP3:
        return current_extension != "mp3", "mp3" if current_extension != "mp3" else None

    if current_extension in lossless_formats:
        return current_extension != "wav", "wav" if current_extension != "wav" else None

    if current_extension in lossy_formats:
        return current_extension != "mp3", "mp3" if current_extension != "mp3" else None

    return False, None


def _remux_single_track(track_id: str, lossless_formats: set[str], lossy_formats: set[str]) -> str:
    """Remux a single track to match current format preferences.

    Handles the complete remux workflow for one track:
    1. Validates file exists and has determinable extension
    2. Determines if remuxing is needed based on current settings
    3. Performs remux and updates database/M3U8 files

    Args:
        track_id: Track identifier
        lossless_formats: Set of lossless format extensions
        lossy_formats: Set of lossy format extensions

    Returns:
        Status string: "remuxed", "skipped", or "error"

    """
    status = "skipped"
    local_file_path = track_db.get_local_file_path(track_id)
    if not local_file_path:
        write_log.debug("TASK_REMUX_NO_PATH", "Track has no file path, skipping.",
                        {"track_id": track_id})
        return status

    if not os.path.exists(local_file_path):
        write_log.warn("TASK_REMUX_FILE_NOT_FOUND", "File not found, skipping.",
                       {"track_id": track_id, "path": local_file_path})
        return status

    current_extension = track_db.get_track_extension(track_id)
    if not current_extension and "." in local_file_path:
        current_extension = local_file_path.rsplit(".", 1)[-1].lower()

    if not current_extension:
        write_log.warn("TASK_REMUX_NO_EXTENSION", "Cannot determine file extension.",
                       {"track_id": track_id, "path": local_file_path})
        return status

    needs_remux, target_format = _determine_remux_target(current_extension, lossless_formats, lossy_formats)
    if not needs_remux or not target_format:
        return status

    write_log.info("TASK_REMUX_FILE", "Remuxing file to target format.",
                  {"track_id": track_id, "current_ext": current_extension,
                   "target_format": target_format, "path": local_file_path})

    try:
        new_path: str | None = None

        if target_format == "wav":
            new_path = _remux_lossless_to_wav(local_file_path, track_id, current_extension)
        elif target_format == "mp3":
            new_path = _remux_lossy_to_mp3(local_file_path, track_id, current_extension)

        if new_path and new_path != local_file_path:
            track_db.update_local_file_path(track_id, new_path)
            _update_m3u8_files_for_track(track_id, new_path)
            write_log.info("TASK_REMUX_SUCCESS", "File remuxed successfully.",
                          {"track_id": track_id, "old_path": local_file_path,
                           "new_path": new_path})
            status = "remuxed"

    except Exception as e:
        write_log.error("TASK_REMUX_FILE_ERROR", "Failed to remux file.",
                        {"track_id": track_id, "path": local_file_path,
                         "error": str(e)})
        status = "error"

    return status


def task_remux_existing_files() -> bool:
    """Task: Remux existing files to match current format preferences.

    This task addresses two scenarios:
    1. Files that failed to remux during download (e.g., program crash)
    2. Files that need conversion after user changes PREFER_MP3 setting

    The task:
    1. Gets all completed tracks from database
    2. Determines target format based on PREFER_MP3:
       - If True: All files should be MP3 320kbps
       - If False: Lossless should be WAV, lossy should be MP3 320kbps
    3. Remuxes files that don't match target format
    4. Updates database with new paths and extensions
    5. Updates M3U8 playlists with new file paths

    This task is interruptible - it processes files one at a time and can be
    safely stopped at any point.

    Returns:
        True if successful, False if failed

    """
    write_log.info("TASK_REMUX_EXISTING_START", "Starting existing files remux task.",
                  {"prefer_mp3": PREFER_MP3})

    try:
        # Get all completed tracks
        completed_tracks = track_db.get_tracks_by_status("completed")

        if not completed_tracks:
            write_log.info("TASK_REMUX_NO_FILES", "No completed tracks to check.")
            return True

        write_log.info("TASK_REMUX_CHECKING", f"Checking {len(completed_tracks)} completed tracks.")

        # Include mp3 in lossy for the remux target check
        lossy_with_mp3 = LOSSY_FORMATS | {"mp3"}

        remuxed_count = 0
        skipped_count = 0
        error_count = 0

        for track_row in completed_tracks:
            track_id = track_row[0]
            result = _remux_single_track(track_id, LOSSLESS_FORMATS, lossy_with_mp3)

            if result == "remuxed":
                remuxed_count += 1
            elif result == "error":
                error_count += 1
            else:
                skipped_count += 1

        write_log.info("TASK_REMUX_EXISTING_COMPLETE",
                      "Existing files remux task completed.",
                      {"remuxed": remuxed_count, "skipped": skipped_count,
                       "errors": error_count, "total": len(completed_tracks)})
        return True

    except Exception as e:
        write_log.error("TASK_REMUX_EXISTING_FAILED",
                       "Existing files remux task failed.",
                       {"error": str(e)})
        return False
