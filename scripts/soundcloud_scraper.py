"""SoundCloud playlist scraper module.

This module provides functionality to extract track information from SoundCloud
playlists by parsing the embedded hydration data in the page HTML and fetching
additional track details via the SoundCloud API.

Since SoundCloud does not accept new API key requests, this module:
1. Parses embedded __sc_hydration JSON for initial playlist data
2. Extracts the client_id from SoundCloud's JavaScript bundles
3. Uses the client_id to fetch full details for stub tracks via the API

Key Features:
- No API key required (dynamically extracts client_id from page)
- Handles playlists with more than 5 tracks (SoundCloud only hydrates first 5)
- Name cleaning/normalization for improved search results
- Comprehensive error handling and logging

Public API:
- get_tracks_from_playlist(): Main function to fetch playlist tracks
"""

import json
import re

import requests

from scripts.logs_utils import write_log
from scripts.playlist_scraper import clean_name

# User agent to mimic a real browser
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Request timeout in seconds
REQUEST_TIMEOUT = 30

# SoundCloud API base URL
SOUNDCLOUD_API_BASE = "https://api-v2.soundcloud.com"

# Maximum tracks per API request (SoundCloud limit)
API_BATCH_SIZE = 50


def _extract_client_id(html_content: str) -> str | None:
    """Extract the SoundCloud client_id from JavaScript bundles.

    SoundCloud's client_id is embedded in their JS bundles. This function:
    1. Finds all JS bundle URLs in the page
    2. Fetches each bundle until a client_id is found
    3. Returns the 32-character alphanumeric client_id

    Args:
        html_content: Raw HTML response from SoundCloud

    Returns:
        Client ID string, or None if not found

    """
    # Find all script URLs
    script_urls = re.findall(r'<script crossorigin src="([^"]+)"', html_content)

    if not script_urls:
        write_log.warn(
            "SOUNDCLOUD_NO_SCRIPTS",
            "No script URLs found in SoundCloud page.",
        )
        return None

    headers = {"User-Agent": USER_AGENT}

    # Try each script URL (start from the end, as client_id is often in later bundles)
    for script_url in reversed(script_urls):
        try:
            response = requests.get(script_url, headers=headers, timeout=REQUEST_TIMEOUT)
            if not response.ok:
                continue

            # Look for client_id pattern (32 alphanumeric characters)
            client_ids = re.findall(r'client_id[=:]["\'\s]*([a-zA-Z0-9]{32})', response.text)
            if client_ids:
                client_id = client_ids[0]
                write_log.debug(
                    "SOUNDCLOUD_CLIENT_ID_FOUND",
                    "Extracted client_id from JS bundle.",
                    {"client_id": client_id[:8] + "..."},  # Log partial for security
                )
                return client_id

        except requests.RequestException:
            continue

    write_log.warn(
        "SOUNDCLOUD_CLIENT_ID_NOT_FOUND",
        "Could not extract client_id from any JS bundle.",
    )
    return None


def _fetch_tracks_by_ids(track_ids: list[int], client_id: str) -> list[dict]:
    """Fetch full track details from SoundCloud API for given track IDs.

    Args:
        track_ids: List of SoundCloud track IDs (integers)
        client_id: SoundCloud API client_id

    Returns:
        List of track data dictionaries with full details

    """
    if not track_ids or not client_id:
        return []

    all_tracks = []
    headers = {"User-Agent": USER_AGENT}

    # Process in batches to avoid API limits
    for i in range(0, len(track_ids), API_BATCH_SIZE):
        batch_ids = track_ids[i:i + API_BATCH_SIZE]
        ids_param = ",".join(str(tid) for tid in batch_ids)

        url = f"{SOUNDCLOUD_API_BASE}/tracks?ids={ids_param}&client_id={client_id}"

        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.ok:
                tracks = response.json()
                all_tracks.extend(tracks)
                write_log.debug(
                    "SOUNDCLOUD_API_FETCH",
                    "Fetched track batch from API.",
                    {"batch_size": len(batch_ids), "fetched": len(tracks)},
                )
            else:
                write_log.warn(
                    "SOUNDCLOUD_API_FAIL",
                    "API request failed for track batch.",
                    {"status": response.status_code, "batch_start": i},
                )

        except requests.RequestException as e:
            write_log.warn(
                "SOUNDCLOUD_API_ERROR",
                "Error fetching tracks from API.",
                {"error": str(e), "batch_start": i},
            )

    return all_tracks


def _extract_track_slug(permalink_url: str) -> str:
    """Extract the track slug (user/track-name) from a SoundCloud permalink URL.

    Args:
        permalink_url: Full SoundCloud track URL
            (e.g., "https://soundcloud.com/lobsta-b/7th-element-vip")

    Returns:
        Track slug in format "user/track-name"

    Example:
        >>> _extract_track_slug("https://soundcloud.com/lobsta-b/7th-element-vip")
        "lobsta-b/7th-element-vip"

    """
    # Remove the base URL and extract user/track path
    path = permalink_url.replace("https://soundcloud.com/", "")
    path = path.replace("http://soundcloud.com/", "")

    # Get the first two path segments (user/track)
    parts = path.split("/")
    if len(parts) >= 2:  # noqa: PLR2004
        return f"{parts[0]}/{parts[1]}"
    if len(parts) == 1:
        return parts[0]
    return path


def _parse_hydration_data(html_content: str) -> dict | None:
    """Extract and parse the __sc_hydration JSON data from HTML content.

    SoundCloud embeds playlist data as JSON in a script tag with the format:
    <script>window.__sc_hydration = [...JSON...];</script>

    Args:
        html_content: Raw HTML response from SoundCloud

    Returns:
        Parsed playlist data dict, or None if not found

    """
    # Pattern to match the hydration script
    pattern = r"<script>window\.__sc_hydration = (.+?);</script>"
    match = re.search(pattern, html_content)

    if not match:
        write_log.error(
            "SOUNDCLOUD_HYDRATION_NOT_FOUND",
            "Could not find __sc_hydration data in page.",
        )
        return None

    try:
        hydration_data = json.loads(match.group(1))

        # Find the playlist hydratable
        for item in hydration_data:
            if item.get("hydratable") == "playlist":
                return item.get("data")

        write_log.error(
            "SOUNDCLOUD_PLAYLIST_NOT_FOUND",
            "Playlist hydratable not found in hydration data.",
        )
        return None

    except Exception as e:
        write_log.error(
            "SOUNDCLOUD_PARSE_FAIL",
            "Failed to parse hydration JSON.",
            {"error": str(e)},
        )
        return None


def get_tracks_from_playlist(  # noqa: PLR0915
    playlist_url: str,
) -> tuple[str, list[tuple[str, str, str, str | None]]]:
    """Extract track information and playlist name from a SoundCloud playlist.

    This function:
    1. Fetches the playlist page HTML
    2. Extracts the embedded __sc_hydration JSON data
    3. Parses playlist metadata (name) and all tracks
    4. Extracts genre information from track data
    5. Cleans artist and track names for improved search results
    6. Returns structured track data

    Args:
        playlist_url: Full SoundCloud playlist URL
                     (e.g., "https://soundcloud.com/courtjester-uk/sets/donk-and-bits")

    Returns:
        Tuple containing:
            - playlist_name (str): The name of the playlist
            - tracks (List[Tuple]): List of (track_id, artists, track_name, genre) tuples.
              Track ID is the URL slug (e.g., "artist/track-name").
              Artist names are cleaned for search optimization.
              Genre is extracted from the track data, or None if not available.

    Raises:
        ValueError: If playlist URL is invalid or page cannot be parsed
        requests.RequestException: If HTTP request fails

    Example:
        >>> playlist_name, tracks = get_tracks_from_playlist(
        ...     "https://soundcloud.com/courtjester-uk/sets/donk-and-bits"
        ... )
        >>> print(playlist_name)
        "donk and hard dance edits"
        >>> print(tracks[0])
        ("lobsta-b/7th-element-vip", "LOBSTA B", "7TH ELEMENT VIP", "HARD HOUSE")

    """
    write_log.info(
        "SOUNDCLOUD_FETCH",
        "Fetching SoundCloud playlist.",
        {"playlist_url": playlist_url},
    )

    # Validate URL format
    if "/sets/" not in playlist_url or "soundcloud.com" not in playlist_url:
        write_log.error(
            "SOUNDCLOUD_URL_INVALID",
            "Invalid SoundCloud playlist URL format.",
            {"playlist_url": playlist_url},
        )
        raise ValueError(
            "Invalid SoundCloud playlist URL. "
            "Expected format: https://soundcloud.com/user/sets/playlist-name",
        )

    # Fetch the playlist page
    try:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(playlist_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        write_log.error(
            "SOUNDCLOUD_REQUEST_FAIL",
            "Failed to fetch SoundCloud playlist page.",
            {"playlist_url": playlist_url, "error": str(e)},
        )
        raise

    html_content = response.text

    # Parse hydration data
    playlist_data = _parse_hydration_data(html_content)
    if not playlist_data:
        raise ValueError(
            f"Could not parse playlist data from SoundCloud page: {playlist_url}",
        )

    # Extract playlist metadata
    playlist_name = playlist_data.get("title", "Unknown Playlist")
    track_count = playlist_data.get("track_count", 0)
    raw_tracks = playlist_data.get("tracks", [])

    write_log.debug(
        "SOUNDCLOUD_PLAYLIST_META",
        "Extracted playlist metadata.",
        {
            "playlist_name": playlist_name,
            "track_count": track_count,
            "tracks_in_data": len(raw_tracks),
        },
    )

    # Separate full tracks from stubs
    # Full tracks have 'permalink_url' and 'user', stubs only have 'id'
    full_tracks = []
    stub_track_ids = []

    for track in raw_tracks:
        if track.get("permalink_url") and track.get("user"):
            full_tracks.append(track)
        elif track.get("id"):
            stub_track_ids.append(track.get("id"))

    write_log.debug(
        "SOUNDCLOUD_TRACK_SPLIT",
        "Separated full tracks from stubs.",
        {"full_tracks": len(full_tracks), "stub_tracks": len(stub_track_ids)},
    )

    # Fetch stub track details from API if needed
    if stub_track_ids:
        client_id = _extract_client_id(html_content)
        if client_id:
            api_tracks = _fetch_tracks_by_ids(stub_track_ids, client_id)
            full_tracks.extend(api_tracks)
            write_log.info(
                "SOUNDCLOUD_STUBS_FETCHED",
                "Fetched stub track details from API.",
                {"requested": len(stub_track_ids), "received": len(api_tracks)},
            )
        else:
            write_log.warn(
                "SOUNDCLOUD_NO_CLIENT_ID",
                "Could not fetch stub tracks - no client_id available.",
                {"stub_count": len(stub_track_ids)},
            )

    # Process all tracks
    cleaned_tracks = []
    for idx, track in enumerate(full_tracks, 1):
        try:
            # Extract track info
            permalink_url = track.get("permalink_url", "")
            track_title = track.get("title", "")
            user_data = track.get("user", {})
            artist_name = user_data.get("username", "")

            # Extract genre from track data
            genre = track.get("genre") or None

            # Generate track ID from URL slug
            track_id = _extract_track_slug(permalink_url)

            if not track_id:
                write_log.warn(
                    "SOUNDCLOUD_TRACK_NO_ID",
                    "Track missing permalink URL, skipping.",
                    {"index": idx, "track_title": track_title},
                )
                continue

            # Clean names for search optimization
            cleaned_artist = clean_name(artist_name)
            cleaned_title = clean_name(track_title)

            cleaned_tracks.append((track_id, cleaned_artist, cleaned_title, genre))

        except Exception as e:
            write_log.warn(
                "SOUNDCLOUD_TRACK_PARSE_FAIL",
                "Failed to parse track, skipping.",
                {"index": idx, "error": str(e)},
            )
            continue

    write_log.info(
        "SOUNDCLOUD_FETCH_SUCCESS",
        "Successfully fetched and cleaned tracks from SoundCloud.",
        {"playlist_name": playlist_name, "track_count": len(cleaned_tracks)},
    )

    return playlist_name, cleaned_tracks
