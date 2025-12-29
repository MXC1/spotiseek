"""Spotify playlist scraper module.

This module provides functionality to extract track information from Spotify
playlists using the Spotify Web API. It handles authentication, pagination,
and name normalization for downstream processing.

Key Features:
- Spotify API authentication via client credentials
- Automatic pagination for large playlists
- Name cleaning/normalization for improved search results
- Comprehensive error handling and logging

Public API:
- get_tracks_from_playlist(): Main function to fetch playlist tracks
- clean_name(): Utility to normalize track/artist names
"""

import os
import re

import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials

from scripts.logs_utils import write_log

load_dotenv()


def clean_name(name: str) -> str:
    """Normalize track and artist names for improved search consistency.

    Removes common punctuation and normalizes whitespace that may interfere
    with Soulseek search queries. This improves match rates.

    Args:
        name: Original track or artist name from Spotify

    Returns:
        Cleaned name with normalized spacing and removed punctuation

    Example:
        >>> clean_name("DC Breaks, InsideInfo - Remix")
        "DC Breaks InsideInfo Remix"
        >>> clean_name("Track  Name   (feat.  Artist)")
        "Track Name feat Artist"

    """
    # Remove problematic punctuation that interferes with searches
    name = name.replace(",", "")
    name = name.replace(" - ", " ")
    name = name.replace("&", "")

    # Normalize whitespace (collapse multiple spaces)
    name = " ".join(name.split())

    return name


def get_tracks_from_playlist(playlist_url: str) -> tuple[str, list[tuple[str, str, str]]]:
    """Extract track information and playlist name from a Spotify playlist.

    This function:
    1. Authenticates with the Spotify API using client credentials
    2. Extracts the playlist ID from the URL
    3. Fetches playlist metadata (name) and all tracks with pagination
    4. Cleans artist and track names for improved search results
    5. Returns structured track data

    Args:
        playlist_url: Full Spotify playlist URL
                     (e.g., "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M")

    Returns:
        Tuple containing:
            - playlist_name (str): The name of the playlist
            - tracks (List[Tuple]): List of (track_id, artists, track_name) tuples.
              Track ID is the Spotify track ID.
              Artist names are space-concatenated and cleaned.

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
            client_secret=client_secret,
        )
        sp = spotipy.Spotify(auth_manager=auth_manager)
    except Exception as e:
        write_log.error("SPOTIFY_AUTH_FAIL", "Failed to authenticate with Spotify.", {"error": str(e)})
        raise

    # Extract playlist ID from URL using regex
    match = re.search(r"playlist/([a-zA-Z0-9]+)", playlist_url)
    if not match:
        write_log.error("SPOTIFY_URL_INVALID", "Invalid playlist URL format.", {"playlist_url": playlist_url})
        raise ValueError("Invalid playlist URL. Expected format: https://open.spotify.com/playlist/...")

    playlist_id = match.group(1)
    write_log.debug("SPOTIFY_FETCH", "Fetching playlist metadata and tracks.", {"playlist_id": playlist_id})

    # Fetch playlist metadata and initial batch of tracks
    try:
        playlist_obj = sp.playlist(playlist_id)
        playlist_name = playlist_obj.get("name", "")
        results = playlist_obj["tracks"]
    except Exception as e:
        write_log.error("SPOTIFY_FETCH_FAIL", "Failed to fetch playlist metadata or tracks.",
                       {"playlist_id": playlist_id, "error": str(e)})
        raise

    tracks = results["items"]

    # Handle pagination for large playlists (Spotify API limits to 100 per request)
    while results["next"]:
        try:
            results = sp.next(results)
            tracks.extend(results["items"])
        except Exception as e:
            write_log.warn("SPOTIFY_PAGINATION_FAIL", "Failed to fetch next page of tracks. Partial results returned.",
                          {"playlist_id": playlist_id, "error": str(e)})
            break

    # Process and clean track data
    cleaned_tracks = []
    for idx, item in enumerate(tracks, 1):
        track = item.get("track")

        # Skip null tracks (removed/unavailable)
        if not track:
            write_log.warn("SPOTIFY_TRACK_MISSING", "Track data is null. Skipping.", {"index": idx})
            continue

        track_id = track.get("id")
        if not track_id:
            write_log.warn("SPOTIFY_ID_MISSING", "Track is missing Spotify ID. Skipping.",
                          {"index": idx, "track_name": track.get("name")})
            continue

        # Concatenate and clean artist names (multiple artists separated by spaces)
        artists = " ".join([clean_name(artist["name"]) for artist in track.get("artists", [])])
        track_name = clean_name(track.get("name", ""))

        cleaned_tracks.append((track_id, artists, track_name))

    write_log.info("SPOTIFY_FETCH_SUCCESS", "Successfully fetched and cleaned tracks.",
                  {"playlist_name": playlist_name, "track_count": len(cleaned_tracks)})

    return playlist_name, cleaned_tracks
