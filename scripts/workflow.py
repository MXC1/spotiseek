"""
Workflow orchestrator for Spotiseek.

This module coordinates the complete workflow of:
1. Reading playlist URLs from CSV
2. Fetching track metadata from Spotify
3. Initiating downloads via Soulseek
4. Remuxing all downloads to preferred formats (lossless -> WAV, lossy -> MP3 320kbps)
5. Tracking download status in the database
6. Updating M3U8 playlist files
7. Exporting iTunes-compatible XML library

Key Features:
- Environment-aware configuration (test/prod/stage)
- Playlist and track processing with error isolation
- Automatic format standardization (lossless -> WAV, lossy -> MP3 320kbps)
- Download status synchronization
- Quality upgrade system (upgrades lossy tracks to lossless when available)
- iTunes library export for music player integration

Workflow Stages:
1. Playlist Processing: Read CSV, fetch Spotify metadata, create M3U8 files
2. Track Processing: Add tracks to database, initiate Soulseek downloads
3. Status Updates: Sync download status from slskd API, remux to preferred formats
4. Quality Upgrades: Redownload non-WAV (lossy) tracks for lossless upgrades
5. Library Export: Generate iTunes XML for music player integration

Public API:
- main(): Primary workflow execution function
"""

import argparse
import csv
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

from scripts.database_management import TrackData, TrackDB  # noqa: E402
from scripts.logs_utils import setup_logging, write_log  # noqa: E402
from scripts.m3u8_manager import (  # noqa: E402
    delete_all_m3u8_files,
    update_track_in_m3u8,
    write_playlist_m3u8,
)
from scripts.soulseek_client import (  # noqa: E402
    download_tracks_async,
    process_pending_searches,
    process_redownload_queue,
    query_download_status,
    remove_download_from_slskd,
    wait_for_slskd_ready,
)
from scripts.spotify_scraper import get_tracks_from_playlist  # noqa: E402
from scripts.xml_exporter import export_itunes_xml  # noqa: E402

# Initialize logging with environment-specific directory
setup_logging(log_name_prefix="workflow")
write_log.debug("ENV_LOAD", "Environment variables loaded.", {"dotenv_path": dotenv_path})

# Validate environment configuration
ENV = os.getenv("APP_ENV")
if not ENV:
    raise OSError(
        "APP_ENV environment variable is not set. Workflow execution is disabled. "
        "Set APP_ENV to 'test', 'stage', or 'prod'."
    )

write_log.info("ENV", "Running in environment.", {"ENV": ENV})


# Configuration Management

class WorkflowConfig:
    """
    Centralized configuration for workflow execution.

    All paths are environment-aware and automatically created if they don't exist.
    """

    def __init__(self, env: str):
        """
        Initialize workflow configuration for specified environment.

        Args:
            env: Environment name ('test', 'stage', or 'prod')
        """
        self.env = env
        self.base_dir = os.path.dirname(os.path.dirname(__file__))

        # Playlist configuration
        self.playlists_dir = os.path.abspath(os.path.join(self.base_dir, "input_playlists"))
        self.playlists_csv = os.path.join(self.playlists_dir, f"playlists_{env}.csv")

        # Database configuration
        self.database_dir = os.path.abspath(os.path.join(self.base_dir, "database", env))
        self.db_path = os.path.join(self.database_dir, f"database_{env}.db")

        # M3U8 files configuration
        self.m3u8_dir = os.path.abspath(os.path.join(self.base_dir, "database", "m3u8s", env))

        # XML export configuration
        self.xml_dir = os.path.abspath(os.path.join(self.base_dir, "database", "xml", env))

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
            self.logs_dir
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def get_xml_export_path(self) -> str:
        """Get the path for iTunes XML library export."""
        return os.path.join(self.xml_dir, "spotiseek_library.xml")

    def get_music_folder_url(self) -> str:
        """
        Get the music folder URL for iTunes XML export.

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
    """
    Read playlist URLs from a CSV file.

    Each row should contain one playlist URL. Empty rows are skipped.

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

    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        playlists = [row[0] for row in reader if row]

    write_log.info("PLAYLISTS_READ_SUCCESS", "Successfully read playlists.", {"count": len(playlists)})
    return playlists


def sanitize_playlist_name(playlist_name: str) -> str:
    """
    Sanitize playlist name for use as filename on Windows.

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
    sanitized = re.sub(r'[<>:"/\\|?*,]', '_', playlist_name)
    # Replace spaces with underscores
    sanitized = sanitized.replace(' ', '_')
    return sanitized


def process_playlist(playlist_url: str) -> list[tuple[str, str, str]]:
    """
    Process a single playlist: fetch tracks and add to database.

    This function:
    1. Fetches playlist name and tracks from Spotify
    2. Generates M3U8 file path
    3. Adds playlist to database
    4. Creates M3U8 file with track comments
    5. Adds tracks to database
    6. Links tracks to playlist in database

    Args:
        playlist_url: Spotify playlist URL

    Returns:
        List of tracks to be downloaded: [(spotify_id, artist, track_name), ...]

    Note:
        Errors are logged but don't stop processing. Individual track
        failures are isolated and won't affect other tracks.
    """
    write_log.info("PLAYLIST_PROCESS", "Processing playlist.", {"playlist_url": playlist_url})

    # Fetch playlist metadata and tracks from Spotify
    try:
        playlist_name, tracks = get_tracks_from_playlist(playlist_url)
        write_log.info("SPOTIFY_FETCH_SUCCESS", "Fetched tracks from Spotify playlist.",
                      {"playlist_name": playlist_name, "track_count": len(tracks)})
    except Exception as e:
        write_log.error("SPOTIFY_FETCH_FAIL", "Failed to get tracks for playlist.",
                       {"playlist_url": playlist_url, "error": str(e)})
        return

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
        return

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
            spotify_id, artist, track_name = track

            # Add track to database (INSERT OR IGNORE - won't duplicate)
            track_db.add_track(TrackData(
                spotify_id=spotify_id,
                track_name=track_name,
                artist=artist
            ))

            # Link track to playlist in database
            track_db.link_track_to_playlist(spotify_id, playlist_url)

            # Only collect tracks that need to be searched for
            # Skip tracks that are already being processed or completed
            current_status = track_db.get_track_status(spotify_id)
            skip_statuses = {
                "completed", "queued", "downloading", "searching",
                "requested", "inprogress", "redownload_pending"
            }

            if current_status not in skip_statuses:
                tracks_to_download.append(track)
            else:
                write_log.debug("TRACK_SKIP_ALREADY_PROCESSING", "Skipping track already in progress.",
                               {"spotify_id": spotify_id, "track_name": track_name, "status": current_status})

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

    return tracks_to_download


# Download Status Management Functions

def update_download_statuses() -> None:
    """
    Query slskd API for download status and update database accordingly.

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
    """
    Identify completed tracks that are not in lossless WAV format and mark them for quality upgrade.

    This function:
    1. Queries all tracks with status='completed'
    2. Checks their file extension
    3. Marks non-WAV tracks as 'redownload_pending' for quality upgrade

    Quality upgrade logic:
    - WAV files are optimal quality (all lossless formats are remuxed to WAV)
    - All non-WAV formats are marked for potential upgrade to lossless
    - The actual upgrade decision (whether a better file exists) happens during search

    Note:
        This function should be called before process_redownload_queue() to ensure
        all eligible tracks are queued for quality checks.
    """
    write_log.info("QUALITY_UPGRADE_SCAN", "Scanning completed tracks for quality upgrade opportunities.")

    # Get all completed tracks
    completed_tracks = track_db.get_tracks_by_status("completed")

    if not completed_tracks:
        write_log.debug("QUALITY_UPGRADE_NO_COMPLETED", "No completed tracks found to check for upgrades.")
        return

    write_log.info(
        "QUALITY_UPGRADE_CHECKING",
        f"Checking {len(completed_tracks)} completed tracks for upgrade eligibility."
    )

    upgrade_count = 0
    for track_row in completed_tracks:
        spotify_id = track_row[0]  # First column is spotify_id

        # Get current file extension
        current_extension = track_db.get_track_extension(spotify_id)

        # Mark for upgrade if not WAV (or if extension is unknown/null)
        if not current_extension or current_extension.lower() != "wav":
            track_db.update_track_status(spotify_id, "redownload_pending")
            upgrade_count += 1
            write_log.debug("QUALITY_UPGRADE_MARKED", "Marked track for quality upgrade.",
                          {"spotify_id": spotify_id, "current_extension": current_extension or "unknown"})

    if upgrade_count > 0:
        write_log.info("QUALITY_UPGRADE_MARKED_COMPLETE",
                      f"Marked {upgrade_count} tracks for quality upgrade (non-WAV files).",
                      {"marked_count": upgrade_count, "total_completed": len(completed_tracks)})
    else:
        write_log.info("QUALITY_UPGRADE_NO_CANDIDATES",
                      "All completed tracks are already WAV format or already queued for upgrade.")


def _update_file_status(file: dict, username: str | None = None) -> None:
    """
    Update database status for a single download file.

    Maps slskd file states to database status values and updates accordingly.
    For completed downloads, also updates the local file path and M3U8 files.
    For failed downloads, removes the download from slskd to prevent duplicate logs.

    Args:
        file: File object from slskd API containing id, state, filename
        username: Soulseek username the download is from (used for removing failed downloads)
    """
    slskd_uuid = file.get("id")
    spotify_id = track_db.get_spotify_id_by_slskd_download_uuid(slskd_uuid)
    download_username = username or track_db.get_username_by_slskd_uuid(slskd_uuid)

    if not spotify_id:
        write_log.debug("SLSKD_UUID_UNKNOWN", "No Spotify ID found for slskd UUID.",
                       {"slskd_uuid": slskd_uuid})
        return

    state = file.get("state")
    write_log.debug("FILE_STATUS_UPDATE", "Updating file status.",
                   {"spotify_id": spotify_id, "state": state})

    # Define failed states for comparison
    failed_states = (
        "Completed, Errored", "Completed, TimedOut", "Completed, Cancelled",
        "Completed, Rejected", "Completed, Aborted"
    )

    # Handle successful downloads
    if state == "Completed, Succeeded":
        _handle_completed_download(file, spotify_id)

    # Handle failed downloads - remove from slskd to prevent duplicate logs
    elif state in failed_states:
        failed_reason = (
            file.get("error")
            or file.get("errorMessage")
            or file.get("message")
            or state
        )
        track_db.update_track_status(spotify_id, "failed", failed_reason=failed_reason)
        write_log.info(
            "DOWNLOAD_FAILED",
            "Download failed.",
            {"spotify_id": spotify_id, "state": state, "failed_reason": failed_reason}
        )

    # Handle queued downloads
    elif state == "Queued, Remotely":
        track_db.update_track_status(spotify_id, "queued")

    # Handle in-progress downloads
    elif state == "InProgress":
        track_db.update_track_status(spotify_id, "downloading")

    # Handle unknown states
    else:
        normalized_state = state.lower().replace(" ", "_").replace(",", "")
        track_db.update_track_status(spotify_id, normalized_state)
        write_log.debug("DOWNLOAD_STATE_UNKNOWN", "Unknown download state encountered.",
                       {"spotify_id": spotify_id, "state": state})

    # Remove below functionality because I think it is causing a bug

    # if state.startswith("Completed"):
    #     if download_username and slskd_uuid:
    #         remove_download_from_slskd(download_username, slskd_uuid)
    #     else:
    #         write_log.warn("DOWNLOAD_REMOVE_SKIP", "Cannot remove failed download - missing username or UUID.",
    #                         {"spotify_id": spotify_id, "slskd_uuid": slskd_uuid, "username": download_username})

def _handle_completed_download(file: dict, spotify_id: str) -> None:
    """
    Process a successfully completed download.

    This function:
    1. Checks if track is marked for redownload (skips status update if so)
    2. Extracts the file path from slskd response
    3. Constructs the local file path
    4. Checks if this is an old slskd record being reprocessed (same file already tracked)
    5. Updates the database with the local file path
    6. Updates all M3U8 files that contain this track

    Args:
        file: File object from slskd API
        spotify_id: Spotify track identifier
    """
    # Check current status - don't overwrite redownload_pending or reprocess completed tracks
    current_status = track_db.get_track_status(spotify_id)
    if current_status == "redownload_pending":
        write_log.debug("DOWNLOAD_SKIP_REDOWNLOAD", "Skipping status update for track marked for redownload.",
                       {"spotify_id": spotify_id})
        return

    # Skip if already processed to prevent redundant remuxing
    if current_status == "completed":
        write_log.debug("DOWNLOAD_ALREADY_PROCESSED", "Skipping already completed download.",
                       {"spotify_id": spotify_id})
        return

    filename_rel = file.get("filename")

    if not filename_rel:
        write_log.warn("DOWNLOAD_NO_FILENAME", "Completed download has no filename.",
                      {"spotify_id": spotify_id})
        track_db.update_track_status(spotify_id, "completed")
        return

    # Extract only the last subfolder and filename for cleaner paths
    # e.g., "Collection\Artist\Album\Track.mp3" -> "Album\Track.mp3"
    normalized_path = filename_rel.replace("\\", "/")
    path_parts = normalized_path.split("/")

    if len(path_parts) >= 2:  # noqa: PLR2004
        # Keep last two components: parent folder and filename
        relative_path = "/".join(path_parts[-2:])
    elif len(path_parts) == 1:
        # Just the filename
        relative_path = path_parts[0]
    else:
        relative_path = filename_rel

    # Keep forward slashes for Linux/Docker environment
    local_file_path = os.path.join(config.downloads_root, relative_path)

    # Check if this is an old slskd record being reprocessed (e.g., after quality upgrade reset)
    # This prevents double-counting when the same file is seen again from slskd history
    existing_path = track_db.get_local_file_path(spotify_id)
    if existing_path:
        # Normalize paths for comparison
        existing_normalized = existing_path.replace("\\", "/").lower()
        new_normalized = local_file_path.replace("\\", "/").lower()
        paths_match = (
            existing_normalized == new_normalized
            or os.path.basename(existing_normalized) == os.path.basename(new_normalized)
        )
        if paths_match:
            write_log.debug("DOWNLOAD_DUPLICATE_RECORD", "Skipping old slskd record - file already tracked.",
                           {"spotify_id": spotify_id, "existing_path": existing_path, "slskd_path": local_file_path})
            return

    # Determine file extension and bitrate from file metadata
    extension = None
    if file.get("extension"):
        extension = file["extension"].lower()
    elif "." in local_file_path:
        extension = local_file_path.rsplit(".", 1)[-1].lower()

    # Extract actual bitrate from file metadata
    bitrate = file.get("bitRate") or file.get("bitrate")
    try:
        bitrate = int(bitrate) if bitrate is not None else None
    except (ValueError, TypeError):
        bitrate = None

    # Define lossless and lossy format categories
    lossless_formats = {'flac', 'alac', 'ape'}
    lossy_formats = {'ogg', 'm4a', 'aac', 'wma', 'opus'}
    
    final_path = local_file_path
    
    # Remux lossless formats to WAV (except WAV itself)
    if extension in lossless_formats:
        final_path = _remux_lossless_to_wav(local_file_path, spotify_id, extension) or local_file_path
    # Remux lossy formats to MP3 320kbps (except MP3 itself)
    elif extension in lossy_formats:
        final_path = _remux_lossy_to_mp3(local_file_path, spotify_id, extension) or local_file_path
    # MP3 and WAV files are already in preferred format, no remuxing needed
    elif extension == "mp3":
        track_db.update_extension_bitrate(spotify_id, extension="mp3", bitrate=bitrate)
    elif extension == "wav":
        track_db.update_extension_bitrate(spotify_id, extension="wav", bitrate=None)

    existing_path = track_db.get_local_file_path(spotify_id)
    track_db.update_local_file_path(spotify_id, final_path)
    write_log.debug(
        "DOWNLOAD_COMPLETE",
        "Download completed successfully.",
        {
            "spotify_id": spotify_id,
            "local_file_path": final_path,
            "is_new": not bool(existing_path)
        }
    )
    _update_m3u8_files_for_track(spotify_id, final_path)
    track_db.update_track_status(spotify_id, "completed")

def _is_audio_valid(audio_path: str) -> bool:
    """
    Use ffmpeg to check if an audio file is valid and decodable.
    Returns True if valid, False otherwise.
    """
    try:
        result = subprocess.run([
            "ffmpeg", "-v", "error", "-i", audio_path, "-f", "null", "-"
        ], check=False, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        write_log.error(
            "AUDIO_CHECK_FAIL",
            "Failed to check audio file integrity.",
            {"audio_path": audio_path, "error": str(e)}
        )
        return False


def _remux_lossless_to_wav(local_file_path: str, spotify_id: str, extension: str) -> str:
    """
    Remux a lossless audio file (FLAC, ALAC, APE) to WAV.
    Update extension/bitrate in DB if successful.
    Returns the new WAV path if successful, else original path.
    """
    wav_path = os.path.splitext(local_file_path)[0] + ".wav"
    try:
        ffmpeg_input = local_file_path.replace("\\", "/")
        ffmpeg_output = wav_path.replace("\\", "/")

        # Check audio integrity before remuxing
        if not _is_audio_valid(ffmpeg_input):
            write_log.warn(
                "LOSSLESS_INVALID",
                "Lossless file failed integrity check. Skipping remux.",
                {"spotify_id": spotify_id, "file_path": ffmpeg_input, "extension": extension}
            )
            track_db.update_track_status(spotify_id, "corrupt")
            slskd_uuid_to_blacklist = track_db.get_download_uuid_by_spotify_id(spotify_id)
            if slskd_uuid_to_blacklist:
                track_db.add_slskd_blacklist(slskd_uuid_to_blacklist, reason=f"corrupt_{extension}")
            return local_file_path

        ffmpeg_cmd = [
            "ffmpeg", "-y", "-i", ffmpeg_input,
            "-codec:a", "pcm_s16le", "-ar", "44100", ffmpeg_output
        ]
        # Compose ffmpeg log file path in the same logs dir as workflow logs
        base_dir = os.path.dirname(os.path.dirname(__file__))
        logs_dir = os.path.join(base_dir, 'observability', "logs", ENV)
        now = datetime.now()
        dated_logs_dir = os.path.join(
            logs_dir,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d")
        )
        os.makedirs(dated_logs_dir, exist_ok=True)
        # Use a single log file per workflow run (date-based, no timestamp)
        ffmpeg_log_file = os.path.join(
            dated_logs_dir,
            "ffmpeg_remux.log"
        )
        write_log.debug(
            "FFMPEG_REMUX_LOSSLESS",
            f"Remuxing {extension.upper()} to WAV.",
            {"input": ffmpeg_input, "output": ffmpeg_output, "ffmpeg_log_file": ffmpeg_log_file}
        )
        with open(ffmpeg_log_file, "a", encoding="utf-8") as logf:
            logf.write(
                f"\n--- Remux {now.strftime('%Y-%m-%d %H:%M:%S')} "
                f"| Spotify ID: {spotify_id} | Input: {ffmpeg_input} | Output: {ffmpeg_output} ---\n"
            )
            subprocess.run(ffmpeg_cmd, check=True, stdout=logf, stderr=subprocess.STDOUT)
        track_db.update_extension_bitrate(spotify_id, extension="wav", bitrate=None)
        write_log.debug(
            "REMUX_SUCCESS",
            f"{extension.upper()} remuxed to WAV.",
            {"spotify_id": spotify_id, "wav_path": wav_path, "ffmpeg_log_file": ffmpeg_log_file}
        )
        return wav_path
    except Exception as e:
        write_log.error("REMUX_FAIL", f"Failed to remux {extension.upper()} to WAV.", {"spotify_id": spotify_id, "error": str(e)})
        track_db.update_extension_bitrate(spotify_id, extension=extension)
        return local_file_path


def _remux_lossy_to_mp3(local_file_path: str, spotify_id: str, extension: str) -> str:
    """
    Remux a lossy audio file (OGG, M4A, AAC, WMA, OPUS) to MP3 320kbps.
    Update extension/bitrate in DB if successful.
    Returns the new MP3 path if successful, else original path.
    """
    mp3_path = os.path.splitext(local_file_path)[0] + ".mp3"
    try:
        ffmpeg_input = local_file_path.replace("\\", "/")
        ffmpeg_output = mp3_path.replace("\\", "/")

        # Check audio integrity before remuxing
        if not _is_audio_valid(ffmpeg_input):
            write_log.warn(
                "LOSSY_INVALID",
                "Lossy file failed integrity check. Skipping remux.",
                {"spotify_id": spotify_id, "file_path": ffmpeg_input, "extension": extension}
            )
            track_db.update_track_status(spotify_id, "corrupt")
            slskd_uuid_to_blacklist = track_db.get_download_uuid_by_spotify_id(spotify_id)
            if slskd_uuid_to_blacklist:
                track_db.add_slskd_blacklist(slskd_uuid_to_blacklist, reason=f"corrupt_{extension}")
            return local_file_path

        ffmpeg_cmd = [
            "ffmpeg", "-y", "-i", ffmpeg_input,
            "-codec:a", "libmp3lame", "-b:a", "320k", ffmpeg_output
        ]
        # Compose ffmpeg log file path in the same logs dir as workflow logs
        base_dir = os.path.dirname(os.path.dirname(__file__))
        logs_dir = os.path.join(base_dir, 'observability', "logs", ENV)
        now = datetime.now()
        dated_logs_dir = os.path.join(
            logs_dir,
            now.strftime("%Y"),
            now.strftime("%m"),
            now.strftime("%d")
        )
        os.makedirs(dated_logs_dir, exist_ok=True)
        # Use a single log file per workflow run (date-based, no timestamp)
        ffmpeg_log_file = os.path.join(
            dated_logs_dir,
            "ffmpeg_remux.log"
        )
        write_log.debug(
            "FFMPEG_REMUX_LOSSY",
            f"Remuxing {extension.upper()} to MP3 320kbps.",
            {"input": ffmpeg_input, "output": ffmpeg_output, "ffmpeg_log_file": ffmpeg_log_file}
        )
        with open(ffmpeg_log_file, "a", encoding="utf-8") as logf:
            logf.write(
                f"\n--- Remux {now.strftime('%Y-%m-%d %H:%M:%S')} "
                f"| Spotify ID: {spotify_id} | Input: {ffmpeg_input} | Output: {ffmpeg_output} ---\n"
            )
            subprocess.run(ffmpeg_cmd, check=True, stdout=logf, stderr=subprocess.STDOUT)
        track_db.update_extension_bitrate(spotify_id, extension="mp3", bitrate=320)
        write_log.info(
            "REMUX_SUCCESS",
            f"{extension.upper()} remuxed to MP3 320kbps.",
            {"spotify_id": spotify_id, "mp3_path": mp3_path, "ffmpeg_log_file": ffmpeg_log_file}
        )
        return mp3_path
    except Exception as e:
        write_log.error("REMUX_FAIL", f"Failed to remux {extension.upper()} to MP3.", {"spotify_id": spotify_id, "error": str(e)})
        track_db.update_extension_bitrate(spotify_id, extension=extension)
        return local_file_path

def _update_m3u8_files_for_track(spotify_id: str, local_file_path: str) -> None:
    """
    Update all M3U8 files that contain a specific track.

    Replaces the track comment line with the actual file path in all
    playlists that contain this track.

    Args:
        spotify_id: Spotify track identifier
        local_file_path: Absolute path to the downloaded file
    """
    try:
        # Get all playlists that contain this track
        playlist_urls = track_db.get_playlists_for_track(spotify_id)

        if not playlist_urls:
            write_log.debug("TRACK_NO_PLAYLISTS", "Track not linked to any playlists.",
                           {"spotify_id": spotify_id})
            return

        # Update M3U8 file for each playlist
        for playlist_url in playlist_urls:
            m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)

            if m3u8_path:
                update_track_in_m3u8(m3u8_path, spotify_id, local_file_path)
                write_log.debug("M3U8_TRACK_UPDATED", "Updated track in M3U8 file.",
                              {"spotify_id": spotify_id, "m3u8_path": m3u8_path})
            else:
                write_log.warn("M3U8_PATH_MISSING", "No M3U8 path found for playlist.",
                              {"playlist_url": playlist_url})

    except Exception as e:
        write_log.error("M3U8_UPDATE_FAIL", "Failed to update M3U8 files for track.",
                       {"spotify_id": spotify_id, "error": str(e)})


# Database Reset Function

def reset_database() -> None:
    """
    Clear the database and delete all M3U8 files.

    This is a destructive operation that:
    1. Deletes the database file
    2. Recreates empty tables
    3. Removes all M3U8 playlist files

    Production safety:
        Requires explicit confirmation when ENV is "prod".
    """
    global track_db  # noqa: PLW0603

    write_log.info("RESET_START", "Clearing database and M3U8 files.")

    # Clear database (includes production safeguards)
    track_db.clear_database()

    # Delete all M3U8 files
    delete_all_m3u8_files(config.m3u8_dir)
    write_log.info("M3U8_DELETE_COMPLETE", "All M3U8 files deleted.",
                  {"m3u8_dir": config.m3u8_dir})

    # Re-initialize database connection
    track_db = TrackDB()
    write_log.info("RESET_COMPLETE", "Database reset complete.")


# ============================================================================
# TASK FUNCTIONS (for task-based scheduler)
# ============================================================================
# Each task function can be executed independently by the task scheduler.
# They return True on success, False on failure.

def task_scrape_playlists() -> bool:
    """
    Task: Scrape Spotify playlists and add tracks to database.

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
                "input_playlists", "playlists.csv"
            ))
            write_log.warn("PLAYLISTS_CSV_MISSING",
                          "Primary playlists CSV not found, falling back to default.",
                          {"primary_csv": config.playlists_csv, "fallback_csv": fallback_csv})
            playlists = read_playlists_from_csv(fallback_csv)
            write_log.info("PLAYLISTS_LOADED_FALLBACK",
                          "Loaded playlists from fallback CSV.",
                          {"count": len(playlists)})

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
    """
    Task: Initiate Soulseek searches for tracks that need downloading.

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
        candidates_statuses = ["pending", "new", "not_found", "no_suitable_file"]
        tracks_to_search = []

        for status in candidates_statuses:
            rows = track_db.get_tracks_by_status(status)
            for track_row in rows:
                # Skip if there is already an active search for this track
                spotify_id = track_row[0]
                current_status = track_db.get_track_status(spotify_id)
                if current_status == "searching":
                    write_log.debug(
                        "TASK_INITIATE_SEARCH_SKIP_SEARCHING",
                        "Skipping track with active search.",
                        {"spotify_id": spotify_id}
                    )
                    continue
                track_name = track_row[1]
                artist = track_row[2]
                tracks_to_search.append((spotify_id, artist, track_name))

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
    """
    Task: Poll slskd for completed searches and initiate downloads.

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
    """
    Task: Sync download status from slskd API to database.

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
    """
    Task: Mark completed non-WAV tracks for quality upgrade.

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
    """
    Task: Process the quality upgrade queue.

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
    """
    Task: Export iTunes-compatible XML library.

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


# Main Workflow Function

def main(reset_db: bool = False) -> None:
    """
    Main workflow execution function.

    Coordinates the complete process:
    1. Optional database reset
    2. Read playlists from CSV
    3. Process each playlist (fetch tracks, initiate downloads)
    4. Update download statuses
    5. Process quality upgrade queue (redownload non-WAV tracks)
    6. Export iTunes-compatible XML library

    Args:
        reset_db: If True, clear the database before starting

    Note:
        The workflow is designed to be resilient - individual failures
        are logged but don't stop the entire process.
    """
    global track_db  # noqa: PLW0602

    write_log.info("WORKFLOW_START", "Starting Spotiseek workflow.", {"env": ENV})

    # Reset database if requested
    if reset_db:
        reset_database()

    # Wait for slskd to be ready before proceeding
    write_log.info("SLSKD_HEALTH_CHECK_START", "Checking slskd availability.")
    if not wait_for_slskd_ready(max_wait_seconds=120, poll_interval=2):
        write_log.error("SLSKD_UNAVAILABLE",
                       "slskd service is not available. Cannot proceed with downloads.")
        write_log.info("WORKFLOW_ABORTED", "Workflow aborted due to slskd unavailability.")
        track_db.close()
        return

    # Process any pending searches from previous runs (restart-safe)
    write_log.info("PENDING_SEARCHES_CHECK", "Checking for completed searches from previous runs.")
    process_pending_searches()

    # Update download statuses for any completed downloads
    write_log.info("DOWNLOAD_STATUS_CHECK", "Checking download statuses.")
    update_download_statuses()

    # Load playlists from CSV, fallback to playlists/playlists.csv if not found
    try:
        playlists = read_playlists_from_csv(config.playlists_csv)
        write_log.info("PLAYLISTS_LOADED", "Loaded playlists from CSV.", {"count": len(playlists)})
    except FileNotFoundError:
        fallback_csv = os.path.abspath(
            os.path.join(os.path.dirname(os.path.dirname(__file__)), "input_playlists", "playlists.csv")
        )
        write_log.warn(
            "PLAYLISTS_CSV_MISSING",
            "Primary playlists CSV not found, falling back to default.",
            {"primary_csv": config.playlists_csv, "fallback_csv": fallback_csv}
        )
        try:
            playlists = read_playlists_from_csv(fallback_csv)
            write_log.info(
                "PLAYLISTS_LOADED_FALLBACK",
                "Loaded playlists from fallback CSV.",
                {"count": len(playlists)}
            )
        except FileNotFoundError:
            write_log.error(
                "PLAYLISTS_CSV_MISSING_BOTH",
                "Neither environment nor fallback playlists CSV file found.",
                {"primary_csv": config.playlists_csv, "fallback_csv": fallback_csv}
            )
            return
        except Exception as e:
            write_log.error(
                "PLAYLISTS_CSV_FAIL_FALLBACK",
                "Failed to read fallback playlists CSV.",
                {"csv_path": fallback_csv, "error": str(e)}
            )
            return
    except Exception as e:
        write_log.error(
            "PLAYLISTS_CSV_FAIL",
            "Failed to read playlists CSV.",
            {"csv_path": config.playlists_csv, "error": str(e)}
        )
        return

    # Process each playlist and collect all tracks for batch download
    all_tracks = []
    for playlist_url in playlists:
        tracks = process_playlist(playlist_url)
        all_tracks.extend(tracks)

    # Initiate searches for all new tracks (fire-and-forget)
    write_log.info("BATCH_SEARCH_START", "Initiating searches for all new tracks.",
                  {"total_tracks": len(all_tracks)})
    download_tracks_async(all_tracks)
    write_log.info("BATCH_SEARCH_INITIATED", "All searches initiated. They will continue in slskd.",
                  {"total_tracks": len(all_tracks)})

    # Mark completed non-WAV tracks for quality upgrade
    write_log.info("QUALITY_UPGRADE_MARK_START", "Marking completed tracks for quality upgrade.")
    mark_tracks_for_quality_upgrade()

    # Initiate quality upgrade searches (fire-and-forget)
    write_log.info("REDOWNLOAD_QUEUE_START", "Initiating quality upgrade searches.")
    process_redownload_queue()
    write_log.info("REDOWNLOAD_QUEUE_INITIATED", "Quality upgrade searches initiated. They will continue in slskd.")

    # Note: All searches are now running in slskd and will complete asynchronously.
    # They will be processed on the next workflow run via process_pending_searches()

    # Export playlists and tracks to iTunes-style XML
    try:
        xml_path = config.get_xml_export_path()
        music_folder_url = config.get_music_folder_url()

        export_itunes_xml(xml_path, music_folder_url)
    except Exception as e:
        write_log.error("XML_EXPORT_FAIL", "Failed to export iTunes XML.",
                       {"xml_path": xml_path, "error": str(e)})

    write_log.info("WORKFLOW_COMPLETE", "Workflow completed successfully.")
    track_db.close()


# Entry Point

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spotiseek Workflow: Scrape Spotify playlists and download tracks via Soulseek",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python workflow.py                    # Normal execution
  python workflow.py --reset            # Clear database and start fresh

Environment:
  Set APP_ENV environment variable to 'test', 'stage', or 'prod' before running.
        """
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear the database before running the workflow (requires confirmation in production)"
    )
    args = parser.parse_args()

    try:
        main(reset_db=args.reset)
    except KeyboardInterrupt:
        write_log.info("WORKFLOW_INTERRUPTED", "Workflow interrupted by user.")
        track_db.close()
        sys.exit(130)
    except Exception as e:
        write_log.error("WORKFLOW_FATAL", "Fatal error in workflow.", {"error": str(e)})
        track_db.close()
        sys.exit(1)
