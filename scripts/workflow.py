"""
Workflow orchestrator for Spotiseek.

This module coordinates the complete workflow of:
1. Reading playlist URLs from CSV
2. Fetching track metadata from Spotify
3. Initiating downloads via Soulseek
4. Tracking download status in the database
5. Updating M3U8 playlist files
6. Exporting iTunes-compatible XML library

Key Features:
- Environment-aware configuration (test/prod/stage)
- Playlist and track processing with error isolation
- Download status synchronization
- Quality upgrade system for existing tracks
- iTunes library export for music player integration

Workflow Stages:
1. Playlist Processing: Read CSV, fetch Spotify metadata, create M3U8 files
2. Track Processing: Add tracks to database, initiate Soulseek downloads
3. Status Updates: Sync download status from slskd API
4. Quality Upgrades: Redownload non-WAV tracks for better quality
5. Library Export: Generate iTunes XML for music player integration

Public API:
- main(): Primary workflow execution function
"""

import os
import sys
import csv
import re
import argparse
from typing import List, Tuple

from dotenv import load_dotenv

# Disable .pyc file generation for cleaner development
sys.dont_write_bytecode = True

# Load environment configuration
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

from logs_utils import setup_logging, write_log
from database_management import TrackDB
from spotify_scraper import get_tracks_from_playlist
from soulseek_client import download_track, download_tracks_async, query_download_status, process_redownload_queue
from m3u8_manager import delete_all_m3u8_files, write_playlist_m3u8, update_track_in_m3u8
from xml_exporter import export_itunes_xml

# Initialize logging with environment-specific directory
setup_logging(log_name_prefix="workflow")
write_log.debug("ENV_LOAD", "Environment variables loaded.", {"dotenv_path": dotenv_path})

# Validate environment configuration
ENV = os.getenv("APP_ENV")
if not ENV:
    raise EnvironmentError(
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
        self.playlists_dir = os.path.abspath(os.path.join(self.base_dir, "playlists", env))
        self.playlists_csv = os.path.join(self.playlists_dir, f"playlists_{env}.csv")
        
        # Database configuration
        self.database_dir = os.path.abspath(os.path.join(self.base_dir, "database", env))
        self.db_path = os.path.join(self.database_dir, f"database_{env}.db")
        
        # M3U8 files configuration
        self.m3u8_dir = os.path.abspath(os.path.join(self.base_dir, "database", "m3u8s", env))
        
        # Downloads configuration
        self.downloads_root = os.path.abspath(os.path.join(self.base_dir, "slskd_docker_data", "downloads"))
        
        # Logs configuration
        self.logs_dir = os.path.abspath(os.path.join(self.base_dir, "observability", "logs", ENV))
        
        # Create all necessary directories
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        directories = [
            self.database_dir,
            self.m3u8_dir,
            self.logs_dir
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def get_xml_export_path(self) -> str:
        """Get the path for iTunes XML library export."""
        return os.path.abspath(os.path.join(self.database_dir, "spotiseek_library.xml"))
    
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
track_db = TrackDB(db_path=config.db_path)


# Playlist Processing Functions

def read_playlists_from_csv(csv_path: str) -> List[str]:
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


def process_playlist(playlist_url: str) -> List[Tuple[str, str, str]]:
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

    # Create M3U8 file with track metadata as comments
    try:
        write_playlist_m3u8(m3u8_path, tracks)
    except Exception as e:
        write_log.error("M3U8_WRITE_FAIL", "Failed to write M3U8 file for playlist.", 
                       {"playlist_url": playlist_url, "m3u8_path": m3u8_path, "error": str(e)})

    # Add tracks to database and collect for batch download
    tracks_to_download = []
    for track in tracks:
        try:
            spotify_id, artist, track_name = track
            
            # Add track to database (INSERT OR IGNORE - won't duplicate)
            track_db.add_track(spotify_id=spotify_id, track_name=track_name, artist=artist)
            
            # Link track to playlist in database
            track_db.link_track_to_playlist(spotify_id, playlist_url)
            
            # Collect track for batch download
            tracks_to_download.append(track)
            
        except Exception as e:
            track_name = track[2] if len(track) > 2 else str(track)
            write_log.error("TRACK_PROCESS_FAIL", "Failed to process track.", 
                           {"track": track_name, "error": str(e)})
            # Update database status if possible
            if len(track) > 0:
                try:
                    track_db.update_track_status(track[0], "failed")
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
        directories = status.get("directories", [])
        for directory in directories:
            files = directory.get("files", [])
            for file in files:
                _update_file_status(file)


def _update_file_status(file: dict) -> None:
    """
    Update database status for a single download file.
    
    Maps slskd file states to database status values and updates accordingly.
    For completed downloads, also updates the local file path and M3U8 files.
    
    Args:
        file: File object from slskd API containing id, state, filename
    """
    slskd_uuid = file.get("id")
    spotify_id = track_db.get_spotify_id_by_slskd_uuid(slskd_uuid)
    
    if not spotify_id:
        write_log.debug("SLSKD_UUID_UNKNOWN", "No Spotify ID found for slskd UUID.", 
                       {"slskd_uuid": slskd_uuid})
        return
    
    state = file.get("state")
    write_log.debug("FILE_STATUS_UPDATE", "Updating file status.", 
                   {"spotify_id": spotify_id, "state": state})
    
    # Handle successful downloads
    if state == "Completed, Succeeded":
        _handle_completed_download(file, spotify_id)
    
    # Handle failed downloads
    elif state in ("Completed, Errored", "Completed, TimedOut", "Completed, Cancelled"):
        track_db.update_track_status(spotify_id, "failed")
        write_log.warn("DOWNLOAD_FAILED", "Download failed.", 
                      {"spotify_id": spotify_id, "state": state})
    
    # Handle queued downloads
    elif state == "Queued, Remotely":
        track_db.update_track_status(spotify_id, "queued")
    
    # Handle in-progress downloads
    elif state == "InProgress":
        track_db.update_track_status(spotify_id, "in_progress")
    
    # Handle unknown states
    else:
        normalized_state = state.lower().replace(" ", "_").replace(",", "")
        track_db.update_track_status(spotify_id, normalized_state)
        write_log.debug("DOWNLOAD_STATE_UNKNOWN", "Unknown download state encountered.", 
                       {"spotify_id": spotify_id, "state": state})


def _handle_completed_download(file: dict, spotify_id: str) -> None:
    """
    Process a successfully completed download.
    
    This function:
    1. Checks if track is marked for redownload (skips status update if so)
    2. Extracts the file path from slskd response
    3. Constructs the local file path
    4. Updates the database with the local file path
    5. Updates all M3U8 files that contain this track
    
    Args:
        file: File object from slskd API
        spotify_id: Spotify track identifier
    """
    # Check current status - don't overwrite redownload_pending
    current_status = track_db.get_track_status(spotify_id)
    if current_status == "redownload_pending":
        write_log.debug("DOWNLOAD_SKIP_REDOWNLOAD", "Skipping status update for track marked for redownload.", 
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
    
    if len(path_parts) >= 2:
        # Keep last two components: parent folder and filename
        relative_path = "/".join(path_parts[-2:])
    elif len(path_parts) == 1:
        # Just the filename
        relative_path = path_parts[0]
    else:
        relative_path = filename_rel
    
    # Convert back to Windows path separators
    relative_path = relative_path.replace("/", "\\")
    local_file_path = os.path.join(config.downloads_root, relative_path)
    
    # If FLAC, remux and use new path; otherwise, use as is
    extension = None
    if file.get("extension"):
        extension = file["extension"].lower()
    elif "." in local_file_path:
        extension = local_file_path.rsplit(".", 1)[-1].lower()

    final_path = local_file_path
    if extension == "flac":
        final_path = _remux_flac_to_mp3(local_file_path, spotify_id, file) or local_file_path

    track_db.update_local_file_path(spotify_id, final_path)
    write_log.info("DOWNLOAD_COMPLETE", "Download completed successfully.", 
                  {"spotify_id": spotify_id, "local_file_path": final_path})
    _update_m3u8_files_for_track(spotify_id, final_path)
    track_db.update_track_status(spotify_id, "completed")
    
def _remux_flac_to_mp3(local_file_path: str, spotify_id: str, file: dict) -> str:
    """
    Remux a FLAC file to 320kbps MP3. Update extension/bitrate in DB if successful.
    Returns the new MP3 path if successful, else None.
    """
    def _is_flac_valid(flac_path: str) -> bool:
        """
        Use ffmpeg to check if a FLAC file is valid and decodable.
        Returns True if valid, False otherwise.
        """
        import subprocess
        try:
            result = subprocess.run([
                "ffmpeg", "-v", "error", "-i", flac_path, "-f", "null", "-"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return result.returncode == 0
        except Exception as e:
            write_log.error("FLAC_CHECK_FAIL", "Failed to check FLAC integrity.", {"flac_path": flac_path, "error": str(e)})
            return False

    mp3_path = os.path.splitext(local_file_path)[0] + ".mp3"
    try:
        import subprocess
        from datetime import datetime
        ffmpeg_input = local_file_path.replace("\\", "/")
        ffmpeg_output = mp3_path.replace("\\", "/")

        # Check FLAC integrity before remuxing
        if not _is_flac_valid(ffmpeg_input):
            write_log.error("FLAC_INVALID", "FLAC file failed integrity check. Skipping remux.", {"spotify_id": spotify_id, "flac_path": ffmpeg_input})
            track_db.update_track_status(spotify_id, "corrupt")
            slskd_uuid_to_blacklist = track_db.get_sldkd_uuid_by_spotify_id(spotify_id)
            if slskd_uuid_to_blacklist:
                track_db.add_slskd_blacklist(slskd_uuid_to_blacklist, reason="corrupt_flac")
            return local_file_path

        ffmpeg_cmd = [
            "ffmpeg", "-y", "-i", ffmpeg_input,
            "-codec:a", "libmp3lame", "-b:a", "320k", ffmpeg_output
        ]
        # Compose ffmpeg log file path in the same logs dir as workflow logs
        env = os.getenv('APP_ENV', 'default')
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
        ffmpeg_log_file = os.path.join(
            dated_logs_dir,
            f"ffmpeg_remux_{now.strftime('%Y%m%d_%H%M%S_%f')}.log"
        )
        write_log.info("FFMPEG_REMUX", "Remuxing FLAC to MP3 320kbps.", {"input": ffmpeg_input, "output": ffmpeg_output, "ffmpeg_log_file": ffmpeg_log_file})
        with open(ffmpeg_log_file, "w", encoding="utf-8") as logf:
            subprocess.run(ffmpeg_cmd, check=True, stdout=logf, stderr=subprocess.STDOUT)
        track_db.update_extension_bitrate(spotify_id, extension="mp3", bitrate=320)
        write_log.info("REMUX_SUCCESS", "FLAC remuxed to MP3 320kbps.", {"spotify_id": spotify_id, "mp3_path": ffmpeg_output, "ffmpeg_log_file": ffmpeg_log_file})
        return ffmpeg_output
    except Exception as e:
        write_log.error("REMUX_FAIL", "Failed to remux FLAC to MP3.", {"spotify_id": spotify_id, "error": str(e)})
        track_db.update_extension_bitrate(spotify_id, extension="flac")
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
    global track_db
    
    write_log.info("RESET_START", "Clearing database and M3U8 files.")
    
    # Clear database (includes production safeguards)
    track_db.clear_database()
    
    # Delete all M3U8 files
    delete_all_m3u8_files(config.m3u8_dir)
    write_log.info("M3U8_DELETE_COMPLETE", "All M3U8 files deleted.", 
                  {"m3u8_dir": config.m3u8_dir})
    
    # Re-initialize database connection
    track_db = TrackDB(db_path=config.db_path)
    write_log.info("RESET_COMPLETE", "Database reset complete.")


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
    global track_db
    
    write_log.info("WORKFLOW_START", "Starting Spotiseek workflow.", {"env": ENV})
    
    # Reset database if requested
    if reset_db:
        reset_database()

    # Load playlists from CSV, fallback to playlists/playlists.csv if not found
    try:
        playlists = read_playlists_from_csv(config.playlists_csv)
        write_log.info("PLAYLISTS_LOADED", "Loaded playlists from CSV.", {"count": len(playlists)})
    except FileNotFoundError:
        fallback_csv = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "playlists", "playlists.csv"))
        write_log.warn("PLAYLISTS_CSV_MISSING", "Primary playlists CSV not found, falling back to default.", {"primary_csv": config.playlists_csv, "fallback_csv": fallback_csv})
        try:
            playlists = read_playlists_from_csv(fallback_csv)
            write_log.info("PLAYLISTS_LOADED_FALLBACK", "Loaded playlists from fallback CSV.", {"count": len(playlists)})
        except FileNotFoundError:
            write_log.error("PLAYLISTS_CSV_MISSING_BOTH", "Neither environment nor fallback playlists CSV file found.", {"primary_csv": config.playlists_csv, "fallback_csv": fallback_csv})
            return
        except Exception as e:
            write_log.error("PLAYLISTS_CSV_FAIL_FALLBACK", "Failed to read fallback playlists CSV.", {"csv_path": fallback_csv, "error": str(e)})
            return
    except Exception as e:
        write_log.error("PLAYLISTS_CSV_FAIL", "Failed to read playlists CSV.", {"csv_path": config.playlists_csv, "error": str(e)})
        return

    # Process each playlist and collect all tracks for batch download
    all_tracks = []
    for playlist_url in playlists:
        tracks = process_playlist(playlist_url)
        all_tracks.extend(tracks)
    
    # Batch process all track downloads asynchronously
    write_log.info("BATCH_DOWNLOAD_START", "Starting batch download of all tracks.", 
                  {"total_tracks": len(all_tracks)})
    download_tracks_async(all_tracks)
    
    # Update download statuses after batch processing
    update_download_statuses()

    # Process redownload queue (quality upgrades for non-WAV files)
    write_log.info("REDOWNLOAD_QUEUE_START", "Processing quality upgrade queue.")
    process_redownload_queue()
    update_download_statuses()
    
    # Export playlists and tracks to iTunes-style XML
    try:
        xml_path = config.get_xml_export_path()
        music_folder_url = config.get_music_folder_url()
        
        export_itunes_xml(xml_path, music_folder_url)
        write_log.info("XML_EXPORT_SUCCESS", "Exported iTunes library XML.", 
                      {"xml_path": xml_path})
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