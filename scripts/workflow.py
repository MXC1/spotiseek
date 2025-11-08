# Ensure logging is initialized before importing other modules
from logs_utils import setup_logging
setup_logging(log_name_prefix="workflow")
from database.database_management import TrackDB
track_db = TrackDB()

# Import other modules after logging is set up
import csv
import logging
from spotify.scrape_spotify_playlist import get_tracks_from_playlist
from soulseek.slskd_downloader import download_track, query_download_status

# Path to the playlists CSV file
PLAYLISTS_CSV = "../playlists.csv"

def read_playlists(csv_path):
    """Read playlist URLs from a CSV file."""
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        return [row[0] for row in reader]

def process_playlist(playlist_url):
    logging.info(f"Processing playlist: {playlist_url}")
    # Get tracks from the playlist
    try:
        tracks = get_tracks_from_playlist(playlist_url)
        logging.info(f"Found {len(tracks)} tracks in playlist.")
    except Exception as e:
        logging.error(f"Failed to get tracks for playlist {playlist_url}: {e}")
        return

    # Add tracks to the database and download them
    for track in tracks:
        process_track(track)

def process_track(track):
    try:
        spotify_id, artist, track_name = track
        track_db.add_track(spotify_id=spotify_id, track_name=track_name, artist=artist)
        download_track(artist, track_name, spotify_id)
    except Exception as e:
        track_name = track[2] if len(track) > 2 else str(track)
        logging.error(f"Failed to download track '{track_name}': {e}")
        track_db.update_track_status(track[0], "failed")

def update_download_statuses():
    logging.info("Checking download statuses...")
    download_statuses = query_download_status()
    for status in download_statuses:
        for directory in status.get("directories", []):
            for file in directory.get("files", []):
                slskd_uuid = file.get("id")
                spotify_id = track_db.get_spotify_id_by_slskd_uuid(slskd_uuid)
                if not spotify_id:
                    logging.warning(f"No Spotify ID found for slskd_uuid={slskd_uuid}")
                    continue
                state = file.get("state")
                if state == "Completed, Succeeded":
                    track_db.update_track_status(spotify_id, "completed")
                elif state == "Completed, Errored":
                    track_db.update_track_status(spotify_id, "failed")
                elif state == "Queued, Remotely":
                    track_db.update_track_status(spotify_id, "queued")
                elif state == "inprogress":
                    track_db.update_track_status(spotify_id, "in_progress")
                else:
                    track_db.update_track_status(spotify_id, state.lower())

def main():
    logging.info("Starting workflow...")

    # Read playlists from the CSV file
    playlists = read_playlists(PLAYLISTS_CSV)
    logging.info(f"Found {len(playlists)} playlists.")

    for playlist_url in playlists:
        process_playlist(playlist_url)
        update_download_statuses()

    logging.info("Workflow completed.")

    # Close the database connection
    track_db.close()

if __name__ == "__main__":
    main()