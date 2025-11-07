import csv
import logging
from logs_utils import setup_logging
from spotify.scrape_spotify_playlist import get_tracks_from_playlist
from soulseek.slskd_downloader import download_track
from database.database_management import TrackDB

setup_logging(log_name_prefix="workflow")

# Path to the playlists CSV file
PLAYLISTS_CSV = "../playlists.csv"

# Initialize the database
track_db = TrackDB()

def read_playlists(csv_path):
    """Read playlist URLs from a CSV file."""
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        return [row[0] for row in reader]

def main():
    logging.info("Starting workflow...")

    # Read playlists from the CSV file
    playlists = read_playlists(PLAYLISTS_CSV)
    logging.info(f"Found {len(playlists)} playlists.")

    for playlist_url in playlists:
        logging.info(f"Processing playlist: {playlist_url}")

        # Get tracks from the playlist
        try:
            tracks = get_tracks_from_playlist(playlist_url)
            logging.info(f"Found {len(tracks)} tracks in playlist.")
        except Exception as e:
            logging.error(f"Failed to get tracks for playlist {playlist_url}: {e}")
            continue

        # Add tracks to the database and download them
        for track in tracks:
            try:
                spotify_id, artist, track_name = track
                track_db.add_track(spotify_id=spotify_id, track_name=track_name, artist=artist)
                download_track(artist, track_name, spotify_id)
            except Exception as e:
                logging.error(f"Failed to download track '{track_name}': {e}")
                track_db.update_track_status(spotify_id, "failed")

    logging.info("Workflow completed.")

    # Close the database connection
    track_db.close()

if __name__ == "__main__":
    main()