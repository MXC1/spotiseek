"""
Workflow orchestrator for Spotiseek.

This module coordinates the complete workflow of:
1. Reading playlist URLs from CSV
2. Fetching track metadata from Spotify
3. Initiating downloads via Soulseek
4. Tracking download status in the database
"""
import sys

sys.dont_write_bytecode = True # Disable .pyc file generation
import argparse
import csv
import logging
import os
import re
from typing import List, Tuple

from dotenv import load_dotenv
from logs_utils import setup_logging
# Load environment configuration and initialize logging
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)
setup_logging(log_name_prefix="workflow")
logging.debug(f"Environment variables loaded from {dotenv_path}")

from database_management import TrackDB
from m3u8_management import delete_all_m3u8_files, write_playlist_m3u8
from scrape_spotify_playlist import get_tracks_from_playlist
from slskd_downloader import download_track, query_download_status

# Validate environment configuration
ENV = os.getenv("APP_ENV")
if not ENV:
    raise EnvironmentError(
        "APP_ENV environment variable is not set. Workflow execution is disabled."
    )

logging.info(f"Running in {ENV} environment")

# Configuration
PLAYLISTS_CSV = f"../playlists/{ENV}_playlists.csv"
DOWNLOADS_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "slskd_docker_data", "downloads")
)
M3U8S_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "database", "m3u8s")
)
os.makedirs(M3U8S_DIR, exist_ok=True)

# Initialize database connection
track_db = TrackDB()

def read_playlists(csv_path: str) -> List[str]:
    """
    Read playlist URLs from a CSV file.
    
    Args:
        csv_path: Path to CSV file containing playlist URLs (one per row)
    
    Returns:
        List of playlist URL strings
    """
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        return [row[0] for row in reader if row]


def process_playlist(playlist_url: str) -> None:
    """
    Process a single playlist: fetch tracks and initiate downloads.

    Args:
        playlist_url: Spotify playlist URL
    """
    logging.info(f"Processing playlist: {playlist_url}")

    try:
        # Generate m3u8 file path for this playlist, sanitize for Windows
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', playlist_url.replace('https://', ''))
        m3u8_path = os.path.join(M3U8S_DIR, f"{safe_name}.m3u8")

        # Add playlist to database and get its ID, saving m3u8 path
        playlist_id = track_db.add_playlist(playlist_url, m3u8_path)

        # Also update m3u8_path in case playlist existed before
        track_db.update_playlist_m3u8_path(playlist_url, m3u8_path)

        tracks = get_tracks_from_playlist(playlist_url)
        logging.info(f"Found {len(tracks)} tracks in playlist.")
    except Exception as e:
        logging.error(f"Failed to get tracks for playlist {playlist_url}: {e}")
        return

    # Write commented rows for each track to the m3u8 file using m3u8_management
    try:
        write_playlist_m3u8(m3u8_path, tracks)
    except Exception as e:
        logging.error(f"Failed to write m3u8 file for playlist {playlist_url}: {e}")

    # Process each track individually
    for track in tracks:
        process_track(track)

        # Link track to playlist in the database
        try:
            track_db.link_track_to_playlist(track[0], playlist_url)  # Pass the playlist URL instead of the ID
        except Exception as e:
            logging.error(f"Failed to link track {track[0]} to playlist {playlist_url}: {e}")


def process_track(track: Tuple[str, str, str]) -> None:
    """
    Add a track to the database and initiate download.
    
    Args:
        track: Tuple of (spotify_id, artist, track_name)
    """
    try:
        spotify_id, artist, track_name = track
        track_db.add_track(spotify_id=spotify_id, track_name=track_name, artist=artist)
        download_track(artist, track_name, spotify_id)
        
    except Exception as e:
        # Extract track name for error logging
        track_name = track[2] if len(track) > 2 else str(track)
        logging.error(f"Failed to process track '{track_name}': {e}")
        
        # Update database status if possible
        if len(track) > 0:
            track_db.update_track_status(track[0], "failed")

def update_download_statuses() -> None:
    """
    Query slskd API for download status and update database accordingly.
    
    This function:
    1. Queries all active downloads from slskd
    2. Maps slskd UUIDs back to Spotify IDs
    3. Updates track status and local file paths in database
    """
    logging.info("Checking download statuses...")
    download_statuses = query_download_status()
    
    for status in download_statuses:
        for directory in status.get("directories", []):
            for file in directory.get("files", []):
                _update_file_status(file)


def _update_file_status(file: dict) -> None:
    """
    Update database status for a single download file.
    
    Args:
        file: File object from slskd API containing id, state, filename
    """
    slskd_uuid = file.get("id")
    spotify_id = track_db.get_spotify_id_by_slskd_uuid(slskd_uuid)
    
    if not spotify_id:
        logging.warning(f"No Spotify ID found for slskd_uuid={slskd_uuid}")
        return
    
    state = file.get("state")
    
    # Handle successful downloads
    if state == "Completed, Succeeded":
        _handle_completed_download(file, spotify_id)
        
    # Handle failed downloads
    elif state in ("Completed, Errored", "Completed, TimedOut", "Completed, Cancelled"):
        track_db.update_track_status(spotify_id, "failed")
        
    # Handle queued downloads
    elif state == "Queued, Remotely":
        track_db.update_track_status(spotify_id, "queued")
        
    # Handle in-progress downloads
    elif state == "inprogress":
        track_db.update_track_status(spotify_id, "in_progress")
        
    # Handle unknown states
    else:
        track_db.update_track_status(spotify_id, state.lower())


def _handle_completed_download(file: dict, spotify_id: str) -> None:
    """
    Process a successfully completed download.
    
    Updates the database with the local file path and marks status as completed.
    
    Args:
        file: File object from slskd API
        spotify_id: Spotify track identifier
    """
    filename_rel = file.get("filename")
    

    if filename_rel:
        folder, file_name = os.path.split(filename_rel)
        last_subfolder = os.path.basename(folder) if folder else None

        logging.debug(
            f"Completed file: {file_name}, subfolder: {last_subfolder}"
        )

        if last_subfolder and file_name:
            local_file_path = os.path.join(DOWNLOADS_ROOT, last_subfolder, file_name)
            track_db.update_local_file_path(spotify_id, local_file_path)

            # Update the relevant m3u8 file: replace the comment line for this track with the file path
            try:
                playlist_urls = track_db.get_playlists_for_track(spotify_id)
                for playlist_url in playlist_urls:
                    m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
                    if not m3u8_path or not os.path.exists(m3u8_path):
                        continue
                    # Read and update the m3u8 file
                    with open(m3u8_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    comment_prefix = f"# {spotify_id} - "
                    new_lines = []
                    replaced = False
                    for line in lines:
                        if line.startswith(comment_prefix) and not replaced:
                            new_lines.append(local_file_path + '\n')
                            replaced = True
                        else:
                            new_lines.append(line)
                    if replaced:
                        with open(m3u8_path, 'w', encoding='utf-8') as f:
                            f.writelines(new_lines)
            except Exception as e:
                logging.error(f"Failed to update m3u8 file for completed track {spotify_id}: {e}")

    track_db.update_track_status(spotify_id, "completed")


def main(reset_db: bool = False) -> None:
    """
    Main workflow execution function.
    
    Coordinates the complete process:
    1. Optional database reset
    2. Read playlists from CSV
    3. Process each playlist (fetch tracks, initiate downloads)
    4. Update download statuses
    5. Clean up database connection
    
    Args:
        reset_db: If True, clear the database before starting
    """
    global track_db
    
    if reset_db:
        logging.info("--reset flag detected. Clearing database before starting workflow.")
        track_db.clear_database()
        # Delete all m3u8 files in the database/m3u8s directory
        m3u8_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'database', 'm3u8s')
        delete_all_m3u8_files(m3u8_dir)
        logging.info(f"All .m3u8 files in {m3u8_dir} have been deleted.")
        # Re-initialize after clearing (singleton pattern ensures clean state)
        track_db = TrackDB()

    logging.info("Starting workflow...")

    # Load playlists from CSV
    try:
        playlists = read_playlists(PLAYLISTS_CSV)
        logging.info(f"Found {len(playlists)} playlists.")
    except FileNotFoundError:
        logging.error(f"Playlists CSV file not found: {PLAYLISTS_CSV}")
        return
    except Exception as e:
        logging.error(f"Failed to read playlists CSV: {e}")
        return

    # Process each playlist
    for playlist_url in playlists:
        process_playlist(playlist_url)
        update_download_statuses()

    logging.info("Workflow completed.")
    track_db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Spotiseek Workflow: Scrape Spotify playlists and download tracks via Soulseek"
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
        logging.info("Workflow interrupted by user.")
        track_db.close()
    except Exception as e:
        logging.error(f"Fatal error in workflow: {e}")
        track_db.close()
        exit(1)