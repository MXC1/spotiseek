

import os
import sys
import logging
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s [%(levelname)s] %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
)

load_dotenv()

def main():
	import argparse

	parser = argparse.ArgumentParser(description="Get tracks from a Spotify playlist URL.")
	parser.add_argument("playlist_url", help="Spotify playlist URL")
	args = parser.parse_args()

	# Set your Spotify API credentials here or use environment variables
	client_id = os.getenv("SPOTIFY_CLIENT_ID")
	client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

	if not client_id or not client_secret or client_id == "YOUR_SPOTIFY_CLIENT_ID" or client_secret == "YOUR_SPOTIFY_CLIENT_SECRET":
		logging.error("Please set your Spotify API client_id and client_secret as environment variables or in the script.")
		sys.exit(1)

	logging.info("Authenticating with Spotify API...")
	try:
		sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
	except Exception as e:
		logging.error(f"Failed to authenticate with Spotify: {e}")
		sys.exit(1)

	# Extract playlist ID from URL
	import re
	match = re.search(r'playlist/([a-zA-Z0-9]+)', args.playlist_url)
	if not match:
		logging.error("Invalid playlist URL.")
		sys.exit(1)
	playlist_id = match.group(1)

	logging.info(f"Fetching tracks for playlist ID: {playlist_id}")
	try:
		results = sp.playlist_tracks(playlist_id)
	except Exception as e:
		logging.error(f"Failed to fetch playlist tracks: {e}")
		sys.exit(1)

	tracks = results['items']
	while results['next']:
		try:
			results = sp.next(results)
			tracks.extend(results['items'])
		except Exception as e:
			logging.warning(f"Failed to fetch next page of tracks: {e}")
			break


	def clean_name(s):
		# Remove commas and dashes, and extra spaces
		s = s.replace(',', '')
		s = s.replace('-', '')
		s = ' '.join(s.split())
		return s

	logging.info(f"Found {len(tracks)} tracks. Printing cleaned track list:")
	for idx, item in enumerate(tracks, 1):
		track = item['track']
		if not track:
			logging.warning(f"Track {idx} is missing track data. Skipping.")
			continue
		artists = ' '.join([clean_name(artist['name']) for artist in track['artists']])
		name = clean_name(track['name'])
		print(f"{artists} {name}")


if __name__ == "__main__":
	main()
