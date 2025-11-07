import csv
import logging
from logs_utils import setup_logging
from spotify.scrape_spotify_playlist import get_tracks_from_playlist
from soulseek.slskd_downloader import download_track

setup_logging(log_name_prefix="workflow")

# Path to the playlists CSV file
PLAYLISTS_CSV = "../playlists.csv"

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

        # Download the tracks
        for track in tracks:
            try:
                artist, track_name = track.rsplit(" ", 1)
                download_track(artist, track_name)
            except Exception as e:
                logging.error(f"Failed to download track '{track}': {e}")

    logging.info("Workflow completed.")

if __name__ == "__main__":
    main()