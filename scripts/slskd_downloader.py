"""
Soulseek downloader module for interfacing with slskd API.

This module handles searching for tracks on the Soulseek network via the slskd
daemon API and managing download requests. It integrates with the database to
track download status and maintain mappings between Soulseek and Spotify IDs.
"""

import logging
import os
import time
import uuid
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv

from logs_utils import setup_logging
from database_management import TrackDB

# Initialize logging and environment
setup_logging(log_name_prefix="slskd_downloader")
load_dotenv()

# slskd API configuration
SLSKD_URL = "http://localhost:5030/api/v0"
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
    logging.debug(f"Creating search: id={search_id}, text='{search_text}'")
    
    try:
        resp = requests.post(
            f"{SLSKD_URL}/searches",
            json={"id": search_id, "searchText": search_text},
            headers={"X-API-Key": TOKEN}
        )
        logging.debug(f"Search POST status: {resp.status_code}")
        logging.debug(f"Search POST response: {resp.text}")
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to create search: {e}")
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
        logging.debug(f"Polling for search responses (attempt {attempt}/{MAX_SEARCH_ATTEMPTS})...")
        
        try:
            resp = requests.get(
                f"{SLSKD_URL}/searches/{search_id}/responses",
                headers={"X-API-Key": TOKEN}
            )
            logging.debug(f"Responses GET status: {resp.status_code}")
            logging.debug(f"Responses GET response: {resp.text}")
            resp.raise_for_status()
            
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                logging.debug(f"Found {len(data)} search responses.")
                return data
                
        except Exception as e:
            logging.error(f"Error during response polling: {e}")
        
        time.sleep(SEARCH_POLL_INTERVAL)
    
    logging.debug("No search responses found after maximum polling attempts.")
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
    logging.debug(
        f"Enqueuing download for search_id={search_id}, username={username}, "
        f"fileinfo={fileinfo}"
    )
    
    try:
        url = f"{SLSKD_URL}/transfers/downloads/{username}"
        payload = [{**fileinfo, "username": username}]
        
        resp = requests.post(
            url,
            json=payload,
            headers={"X-API-Key": TOKEN}
        )
        logging.debug(f"Download POST status: {resp.status_code}")
        logging.debug(f"Download POST response: {resp.text}")
        resp.raise_for_status()
        
        download_response = resp.json()

        # Validate and extract enqueued download information
        enqueued = download_response.get("enqueued", [])
        if not enqueued:
            logging.error(f"No downloads were enqueued. Response: {download_response}")
            return download_response

        slskd_uuid = enqueued[0].get("id")
        if not slskd_uuid:
            logging.error(f"No slskd UUID in enqueued response. Response: {enqueued[0]}")
            return download_response

        # Store mapping between Soulseek UUID and Spotify ID
        logging.info(f"Enqueued download with slskd_uuid={slskd_uuid} for spotify_id={spotify_id}")
        track_db.add_slskd_mapping(slskd_uuid, spotify_id)
        
        return download_response
        
    except Exception as e:
        logging.error(f"Failed to enqueue download: {e}")
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
        logging.info(
            f"Skipping download for '{artist} - {track}' "
            f"(current status: '{current_status}')"
        )
        return

    search_text = f"{artist} {track}"
    logging.info(f"Searching for: {search_text}")
    track_db.update_track_status(spotify_id, "searching")
    
    try:
        # Perform search on Soulseek network
        search_id = create_search(search_text)
        responses = get_search_responses(search_id)
        
        if not responses:
            logging.info(f"No search results found for '{artist} - {track}'")
            track_db.update_track_status(spotify_id, "not_found")
            return
        
        # Extract first available file from first response
        first_response = responses[0]
        files = first_response.get("files", [])
        username = first_response.get("username")
        
        if not files:
            logging.info(f"No files in search results for '{artist} - {track}'")
            track_db.update_track_status(spotify_id, "failed")
            return
        
        # Prepare file information for download
        first_file = files[0]
        filename = first_file.get("filename")
        size = first_file.get("size")
        fileinfo = {"filename": filename, "size": size}
        
        # Enqueue download and update database
        logging.info(f"Downloading: {filename}")
        download_resp = enqueue_download(search_id, fileinfo, username, spotify_id)
        logging.debug(f"Download initiated: {download_resp}")
        
        # Store only the basename 
        basename = os.path.basename(filename) if filename else filename
        track_db.update_track_status(spotify_id, "downloading")
        track_db.update_slskd_file_name(spotify_id, basename)
        
    except Exception as e:
        logging.error(f"Failed to download track '{artist} - {track}': {e}")
        track_db.update_track_status(spotify_id, "failed")


def query_download_status() -> List[Dict[str, Any]]:
    """
    Query the status of all active downloads from slskd API.
    
    Returns:
        List of download status objects containing directories, files, and states.
        Returns empty list if the query fails.
    """
    logging.info("Querying download status for all transfers...")
    
    try:
        resp = requests.get(
            f"{SLSKD_URL}/transfers/downloads",
            headers={"X-API-Key": TOKEN}
        )
        logging.debug(f"Download status response: {resp.text}")
        resp.raise_for_status()
        return resp.json()
        
    except Exception as e:
        logging.error(f"Failed to query download status: {e}")
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
