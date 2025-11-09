"""

Spotify playlist scraper module.

This module provides functionality to extract track information from Spotify
playlists using the Spotify Web API. It handles authentication, pagination,
and name normalization for downstream processing.
"""


import argparse
import os
import re
from typing import List, Tuple
from logs_utils import write_log

import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials

load_dotenv()


def clean_name(name: str) -> str:
    """
    Normalize track and artist names for search consistency.
    
    Removes common punctuation and extra whitespace that may interfere
    with Soulseek searches.
    
    Args:
        name: Original track or artist name
    
    Returns:
        Cleaned name with normalized spacing and removed punctuation
    
    Example:
        >>> clean_name("DC Breaks, InsideInfo - Remix")
        "DC Breaks InsideInfo Remix"
    """
    # Remove problematic punctuation
    name = name.replace(",", "")
    name = name.replace(" - ", " ")
    name = name.replace("&", "")
    
    # Normalize whitespace
    name = " ".join(name.split())
    
    return name

def get_tracks_from_playlist(playlist_url: str) -> List[Tuple[str, str, str]]:
    """
    Extract track information and playlist name from a Spotify playlist.
    
    This function authenticates with the Spotify API, extracts the playlist ID
    from the URL, fetches the playlist name and all tracks (handling pagination),
    and returns the playlist name and cleaned track metadata.
    
    Args:
        playlist_url: Full Spotify playlist URL 
                     (e.g., "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M")
    
    Returns:
        Tuple containing:
            - playlist_name: The name of the playlist (str)
            - tracks: List of tuples (spotify_id, artists, track_name) for each track.
        Artist names are concatenated with spaces, and names are cleaned for consistency.
    
    Raises:
        ValueError: If API credentials are missing or playlist URL is invalid
        spotipy.SpotifyException: If API requests fail
    
    Example:
        >>> playlist_name, tracks = get_tracks_from_playlist("https://open.spotify.com/playlist/...")
        >>> print(playlist_name)
        "My Playlist Name"
        >>> print(tracks[0])
        ("5ms8IkagrFWObtzSOahVrx", "MASTER BOOT RECORD", "Skynet")
    """
    # Validate API credentials
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        write_log.error("SPOTIFY_CREDENTIALS_MISSING", "Spotify API credentials are not set.")
        raise ValueError("Missing Spotify API credentials (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET).")

    # Authenticate with Spotify API
    write_log.info("SPOTIFY_AUTH", "Authenticating with Spotify API.")
    try:
        auth_manager = SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
        sp = spotipy.Spotify(auth_manager=auth_manager)
    except Exception as e:
        write_log.error("SPOTIFY_AUTH_FAIL", "Failed to authenticate with Spotify.", {"error": str(e)})
        raise

    # Extract playlist ID from URL
    match = re.search(r"playlist/([a-zA-Z0-9]+)", playlist_url)
    if not match:
        write_log.error("SPOTIFY_URL_INVALID", "Invalid playlist URL format.", {"playlist_url": playlist_url})
        raise ValueError("Invalid playlist URL. Expected format: https://open.spotify.com/playlist/...")
    
    playlist_id = match.group(1)
    write_log.info("SPOTIFY_FETCH", "Fetching playlist metadata and tracks.", {"playlist_id": playlist_id})

    # Fetch playlist metadata (name) and all tracks with pagination
    try:
        playlist_obj = sp.playlist(playlist_id)
        playlist_name = playlist_obj.get("name", "")
        results = playlist_obj["tracks"]
    except Exception as e:
        write_log.error("SPOTIFY_FETCH_FAIL", "Failed to fetch playlist metadata or tracks.", {"playlist_id": playlist_id, "error": str(e)})
        raise

    tracks = results["items"]
    
    # Handle pagination for large playlists
    while results["next"]:
        try:
            results = sp.next(results)
            tracks.extend(results["items"])
        except Exception as e:
            write_log.warn("SPOTIFY_PAGINATION_FAIL", "Failed to fetch next page of tracks.", {"playlist_id": playlist_id, "error": str(e)})
            break

    # Process and clean track data
    cleaned_tracks = []
    for idx, item in enumerate(tracks, 1):
        track = item.get("track")
        
        if not track:
            write_log.warn("SPOTIFY_TRACK_MISSING", "Track is missing track data. Skipping.", {"index": idx})
            continue
        
        spotify_id = track.get("id")
        if not spotify_id:
            write_log.warn("SPOTIFY_ID_MISSING", "Track is missing Spotify ID. Skipping.", {"index": idx})
            continue
        
        # Concatenate and clean artist names
        artists = " ".join([clean_name(artist["name"]) for artist in track.get("artists", [])])
        track_name = clean_name(track.get("name", ""))
        
        cleaned_tracks.append((spotify_id, artists, track_name))

    return playlist_name, cleaned_tracks

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract track information from a Spotify playlist URL."
    )
    parser.add_argument(
        "playlist_url",
        help="Spotify playlist URL (e.g., https://open.spotify.com/playlist/...)"
    )
    args = parser.parse_args()

    try:
        playlist_name, tracks = get_tracks_from_playlist(args.playlist_url)
        print(f"Playlist name: {playlist_name}")
        for spotify_id, artists, track_name in tracks:
            print(f"{spotify_id}\t{artists}\t{track_name}")
    except Exception as e:
        write_log.error("SPOTIFY_MAIN_ERROR", "Error in main execution.", {"error": str(e)})
        exit(1)
