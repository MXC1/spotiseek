"""
Soulseek downloader module for interfacing with slskd API.

This module handles searching for tracks on the Soulseek network via the slskd
daemon API and managing download requests. It integrates with the database to
track download status and maintain mappings between Soulseek and Spotify IDs.
"""

from logs_utils import write_log
import os
import time
import uuid
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv

from database_management import TrackDB

load_dotenv()

# slskd API configuration
SLSKD_BASE_URL = os.getenv("SLSKD_BASE_URL", "http://localhost:5030")
SLSKD_URL = f"{SLSKD_BASE_URL}/api/v0"
TOKEN = os.getenv("TOKEN")

# Search and download constants
MAX_SEARCH_ATTEMPTS = 100
SEARCH_POLL_INTERVAL = 2  # seconds

# Database instance
track_db = TrackDB()

def create_search(search_text: str) -> str:
    """
    Initiate a search on the Soulseek network via slskd API.
    
    Args:
        search_text: Query string to search for (e.g., "Artist - Track Name")
    
    Returns:
        Unique search ID (UUID) for tracking this search
    
    Raises:
        requests.HTTPError: If the API request fails
    """
    search_id = str(uuid.uuid4())
    write_log.debug("SLSKD_SEARCH_CREATE", "Creating search.", {"search_id": search_id, "search_text": search_text})
    
    try:
        resp = requests.post(
            f"{SLSKD_URL}/searches",
            json={"id": search_id, "searchText": search_text},
            headers={"X-API-Key": TOKEN}
        )
        write_log.debug("SLSKD_SEARCH_POST", "Search POST.", {"status_code": resp.status_code, "response": resp.text})
        resp.raise_for_status()
    except Exception as e:
        write_log.error("SLSKD_SEARCH_CREATE_FAIL", "Failed to create search.", {"error": str(e)})
        raise
    
    return search_id


def get_search_responses(search_id: str) -> List[Dict[str, Any]]:
    """
    Poll for search results from the Soulseek network.
    
    This function polls the slskd API repeatedly until search results are found
    or the maximum number of attempts is reached.
    
    Args:
        search_id: UUID of the search to retrieve results for
    
    Returns:
        List of search response objects, each containing username and files.
        Returns empty list if no results are found after polling.
    """
    for attempt in range(1, MAX_SEARCH_ATTEMPTS + 1):
        write_log.debug("SLSKD_SEARCH_POLL", "Polling for search responses.", {"attempt": attempt, "max_attempts": MAX_SEARCH_ATTEMPTS, "search_id": search_id})
        try:
            resp = requests.get(
                f"{SLSKD_URL}/searches/{search_id}/responses",
                headers={"X-API-Key": TOKEN}
            )
            write_log.debug("SLSKD_RESPONSES_GET", "Responses GET.", {"status_code": resp.status_code, "response": resp.text})
            resp.raise_for_status()
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                write_log.debug("SLSKD_RESPONSES_FOUND", "Found search responses.", {"count": len(data)})
                return data
        except Exception as e:
            write_log.error("SLSKD_SEARCH_POLL_FAIL", "Error during response polling.", {"error": str(e)})
        time.sleep(SEARCH_POLL_INTERVAL)
    write_log.debug("SLSKD_SEARCH_POLL_NONE", "No search responses found after maximum polling attempts.", {"search_id": search_id})
    return []

def enqueue_download(
    search_id: str,
    fileinfo: Dict[str, Any],
    username: str,
    spotify_id: str
) -> Dict[str, Any]:
    """
    Queue a file for download from a Soulseek user and track the mapping.
    
    Args:
        search_id: UUID of the search that found this file
        fileinfo: Dictionary containing 'filename' and 'size' of the file
        username: Soulseek username to download from
        spotify_id: Spotify track ID to associate with this download
    
    Returns:
        API response dictionary containing enqueued download information
    
    Raises:
        requests.HTTPError: If the API request fails
    """
    write_log.debug("SLSKD_ENQUEUE", "Enqueuing download.", {"search_id": search_id, "username": username, "fileinfo": fileinfo})
    
    try:
        url = f"{SLSKD_URL}/transfers/downloads/{username}"
        payload = [{**fileinfo, "username": username}]
        resp = requests.post(
            url,
            json=payload,
            headers={"X-API-Key": TOKEN}
        )
        write_log.debug("SLSKD_DOWNLOAD_POST", "Download POST.", {"status_code": resp.status_code, "response": resp.text})
        resp.raise_for_status()
        download_response = resp.json()
        # Validate and extract enqueued download information
        enqueued = download_response.get("enqueued", [])
        if not enqueued:
            write_log.error("SLSKD_ENQUEUE_NONE", "No downloads were enqueued.", {"response": download_response})
            return download_response
        slskd_uuid = enqueued[0].get("id")
        if not slskd_uuid:
            write_log.error("SLSKD_ENQUEUE_UUID_MISSING", "No slskd UUID in enqueued response.", {"response": enqueued[0]})
            return download_response
        # Store mapping between Soulseek UUID and Spotify ID
        write_log.info("SLSKD_ENQUEUE_SUCCESS", "Enqueued download.", {"slskd_uuid": slskd_uuid, "spotify_id": spotify_id})
        track_db.add_slskd_mapping(slskd_uuid, spotify_id)
        return download_response
    except Exception as e:
        write_log.error("SLSKD_ENQUEUE_FAIL", "Failed to enqueue download.", {"error": str(e)})
        raise

def download_track(artist: str, track: str, spotify_id: str) -> None:
    """
    Search for and initiate download of a track on the Soulseek network.
    
    This function:
    1. Checks if the track is already downloading or completed
    2. Creates a search query combining artist and track name
    3. Polls for search results
    4. Selects the first available file from the first response
    5. Enqueues the file for download
    6. Updates the database with download status
    
    Args:
        artist: Artist name(s)
        track: Track name
        spotify_id: Spotify track identifier for database tracking
    
    Note:
        Skips download if track status is already in a terminal or active state
        (completed, queued, downloading, requested, inprogress).
    """
    # Skip if already downloaded or in progress
    current_status = track_db.get_track_status(spotify_id)
    skip_statuses = {"completed", "queued", "downloading", "requested", "inprogress"}
    
    if current_status in skip_statuses:
        write_log.debug("SLSKD_SKIP", "Skipping download.", {"artist": artist, "track": track, "current_status": current_status})
        return

    search_text = f"{artist} {track}"
    write_log.info("SLSKD_SEARCH", "Searching for track.", {"search_text": search_text})
    track_db.update_track_status(spotify_id, "searching")
    

    def select_best_file(responses, search_text):
        """Select the best file from responses, filtering out remixes/edits/etc unless requested."""
        excluded_keywords = [
            'remix', 'edit', 'bootleg', 'mashup', 'mix', 'acapella',
            'instrumental', 'sped up', 'slowed', 'cover', 'karaoke',
            'tribute', 'demo', 'live', 'acoustic', 'version', 'remaster',
            'flip'
        ]

        search_text_lower = search_text.lower()
        # If user is searching for an alternative version, do not filter
        allow_alternatives = any(kw in search_text_lower for kw in excluded_keywords)

        def is_original(filename):
            fname_lower = filename.lower()
            for keyword in excluded_keywords:
                if keyword in fname_lower:
                    return False
            return True

        candidates = []
        for response in responses:
            username = response.get("username")
            files = response.get("files", [])
            for file in files:
                candidates.append((file, username))

        if allow_alternatives:
            search_pool = candidates
        else:
            original_candidates = [(f, u) for f, u in candidates if is_original(f.get("filename", ""))]
            search_pool = original_candidates if original_candidates else candidates

        # 1. WAV files
        for file, username in search_pool:
            ext = (file.get("extension") or "").lower()
            fname = file.get("filename", "").lower()
            if ext == "wav" or fname.endswith(".wav"):
                return file, username

        # 2. MP3 320kbps
        for file, username in search_pool:
            ext = (file.get("extension") or "").lower()
            fname = file.get("filename", "").lower()
            if (ext == "mp3" or fname.endswith(".mp3")) and file.get("bitRate") == 320:
                return file, username

        # 3. Fallback: first available file
        if search_pool:
            return search_pool[0]
        return None, None

    try:
        # Perform search on Soulseek network
        search_id = create_search(search_text)
        responses = get_search_responses(search_id)
        if not responses:
            write_log.info("SLSKD_NO_RESULTS", "No search results found.", {"artist": artist, "track": track})
            track_db.update_track_status(spotify_id, "not_found")
            return
        # Select best file according to rules
        best_file, username = select_best_file(responses, search_text)
        if not best_file:
            write_log.info("SLSKD_NO_FILES", "No files in search results.", {"artist": artist, "track": track})
            track_db.update_track_status(spotify_id, "failed")
            return
        filename = best_file.get("filename")
        size = best_file.get("size")
        extension = best_file.get("extension")
        bitrate = best_file.get("bitRate") or best_file.get("bitrate")
        # If extension is empty, extract from filename
        if not extension and filename:
            if "." in filename:
                extension = filename.rsplit(".", 1)[-1].lower()
            else:
                extension = None
        fileinfo = {"filename": filename, "size": size}
        # Enqueue download and update database
        write_log.info("SLSKD_DOWNLOAD", "Downloading file.", {"filename": filename})
        download_resp = enqueue_download(search_id, fileinfo, username, spotify_id)
        write_log.debug("SLSKD_DOWNLOAD_INITIATED", "Download initiated.", {"download_resp": download_resp})
        # Pass the full filename (may include subdirectories) so TrackDB can trim as needed
        track_db.update_track_status(spotify_id, "downloading")
        track_db.update_slskd_file_name(spotify_id, filename)
        # Update extension and bitrate in DB if available
        track_db.update_extension_bitrate(spotify_id, extension, bitrate)
    except Exception as e:
        write_log.error("SLSKD_DOWNLOAD_FAIL", f"Failed to download track.", {"artist": artist, "track": track, "error": str(e)})
        track_db.update_track_status(spotify_id, "failed")


def query_download_status() -> List[Dict[str, Any]]:
    """
    Query the status of all active downloads from slskd API.
    
    Returns:
        List of download status objects containing directories, files, and states.
        Returns empty list if the query fails.
    """
    write_log.info("SLSKD_QUERY_STATUS", "Querying download status for all transfers...")
    
    try:
        resp = requests.get(
            f"{SLSKD_URL}/transfers/downloads",
            headers={"X-API-Key": TOKEN}
        )
        write_log.debug("SLSKD_QUERY_STATUS_RESP", "Download status response.", {"response": resp.text})
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        write_log.error("SLSKD_QUERY_STATUS_FAIL", "Failed to query download status.", {"error": str(e)})
        return []

if __name__ == "__main__":
    # Example usage for testing
    test_tracks = [
        ("5ms8IkagrFWObtzSOahVrx", "MASTER BOOT RECORD", "Skynet")
    ]
    
    for spotify_id, artist, track_name in test_tracks:
        track_db.add_track(spotify_id=spotify_id, track_name=track_name, artist=artist)
        download_track(artist, track_name, spotify_id)

    track_db.close()
