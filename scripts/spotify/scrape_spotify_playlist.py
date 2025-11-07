import os
import logging
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from logs_utils import setup_logging


setup_logging(log_name_prefix="scrape_spotify_playlist")
load_dotenv()

def clean_name(s):
    """Clean track or artist names by removing unwanted characters."""
    s = s.replace(',', '')
    s = s.replace(' - ', ' ')
    s = s.replace('&', '')
    s = ' '.join(s.split())
    return s

def get_tracks_from_playlist(playlist_url):
    """Fetch and clean track names and Spotify IDs from a Spotify playlist URL."""
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        logging.error("Spotify API credentials are not set.")
        raise ValueError("Missing Spotify API credentials.")

    logging.info("Authenticating with Spotify API...")
    try:
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
    except Exception as e:
        logging.error(f"Failed to authenticate with Spotify: {e}")
        raise

    # Extract playlist ID from URL
    import re
    match = re.search(r'playlist/([a-zA-Z0-9]+)', playlist_url)
    if not match:
        logging.error("Invalid playlist URL.")
        raise ValueError("Invalid playlist URL.")
    playlist_id = match.group(1)

    logging.info(f"Fetching tracks for playlist ID: {playlist_id}")
    try:
        results = sp.playlist_tracks(playlist_id)
    except Exception as e:
        logging.error(f"Failed to fetch playlist tracks: {e}")
        raise

    tracks = results['items']
    while results['next']:
        try:
            results = sp.next(results)
            tracks.extend(results['items'])
        except Exception as e:
            logging.warning(f"Failed to fetch next page of tracks: {e}")
            break

    logging.info(f"Found {len(tracks)} tracks.")
    cleaned_tracks = []
    for idx, item in enumerate(tracks, 1):
        track = item['track']
        if not track:
            logging.warning(f"Track {idx} is missing track data. Skipping.")
            continue
        spotify_id = track.get('id')
        artists = ' '.join([clean_name(artist['name']) for artist in track['artists']])
        name = clean_name(track['name'])
        cleaned_tracks.append((spotify_id, artists, name))

    return cleaned_tracks

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Get tracks from a Spotify playlist URL.")
    parser.add_argument("playlist_url", help="Spotify playlist URL")
    args = parser.parse_args()

    try:
        tracks = get_tracks_from_playlist(args.playlist_url)
        for track in tracks:
            print(track)
    except Exception as e:
        logging.error(f"Error: {e}")
