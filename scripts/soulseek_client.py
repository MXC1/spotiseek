"""
Soulseek download client module for interfacing with slskd API.

This module handles searching for tracks on the Soulseek network via the slskd
daemon API and managing download requests. It integrates with the database to
track download status and maintain mappings between Soulseek and Spotify IDs.

Key Features:
- Intelligent file quality selection (WAV > FLAC > MP3 320 > others)
- Automatic filtering of remixes/edits unless explicitly requested
- Quality upgrade system for existing downloads
- Download status polling and database synchronization
- Thread-safe API communication

Quality Priority:
1. WAV files (lossless, uncompressed)
2. FLAC files (lossless, compressed)
3. MP3 320kbps (high quality lossy)
4. MP3 lower bitrates
5. Other formats (OGG, M4A, etc.)

Public API:
- download_track(): Main entry point for track downloads
- query_download_status(): Poll slskd for active download states
- process_redownload_queue(): Handle quality upgrade requests
"""

import os
import time
import uuid
import requests
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv

from logs_utils import write_log
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


# Quality Assessment Functions

def extract_file_quality(file: Dict[str, Any]) -> Tuple[str, Optional[int]]:
    """
    Extract file extension and bitrate from slskd file object.
    
    Args:
        file: File object from slskd API response
        
    Returns:
        Tuple of (extension, bitrate). Extension is lowercase, bitrate in kbps.
        
    Example:
        >>> extract_file_quality({"extension": "MP3", "bitRate": 320})
        ("mp3", 320)
    """
    ext = (file.get("extension") or '').lower()
    filename = file.get("filename", "")
    
    # Fallback: extract extension from filename if not in metadata
    if not ext and filename and "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()
    
    # Parse bitrate
    bitrate = file.get("bitRate") or file.get("bitrate")
    try:
        bitrate = int(bitrate) if bitrate is not None else None
    except (ValueError, TypeError):
        bitrate = None
    
    return ext, bitrate


def is_better_quality(file: Dict[str, Any], current_extension: str, current_bitrate: Optional[int]) -> bool:
    """
    Determine if a file has better quality than the current one.
    
    Quality hierarchy: WAV > FLAC > MP3 (by bitrate) > others
    
    Args:
        file: New file object to evaluate
        current_extension: Extension of current file
        current_bitrate: Bitrate of current file (kbps)
        
    Returns:
        True if the new file is higher quality
    """
    ext, bitrate = extract_file_quality(file)

    # WAV is always preferred over anything else
    if ext == "wav" and current_extension != "wav":
        return True

    # FLAC is NOT considered an upgrade over MP3 320, since we remux to MP3 320
    # Only upgrade to FLAC if current is lower than MP3 (not 320), and not if current is MP3 320 or better
    if ext == "flac":
        if current_extension in ("wav", "flac"):
            return False
        if current_extension == "mp3":
            # Only upgrade if current MP3 is less than 320kbps
            if current_bitrate is not None and current_bitrate < 320:
                return True
            return False
        # If current is lower quality (e.g., ogg, m4a, etc.), allow upgrade
        return current_extension not in ("wav", "flac", "mp3")

    # Among MP3 files, prefer higher bitrate
    if ext == "mp3" and current_extension == "mp3":
        if bitrate and current_bitrate and bitrate > current_bitrate:
            return True

    # MP3 is preferred over lower quality formats
    if ext == "mp3" and current_extension not in ("mp3", "wav", "flac"):
        return True

    return False


def quality_sort_key(item: Tuple[Dict[str, Any], str]) -> Tuple[int, int]:
    """
    Generate a sort key for file quality prioritization.
    
    Args:
        item: Tuple of (file_object, username)
        
    Returns:
        Tuple of (format_priority, bitrate) for sorting.
        Higher values = better quality.
    """
    file, _ = item
    ext, bitrate = extract_file_quality(file)
    
    # Format priority: WAV (3) > FLAC (2) > MP3 (1) > others (0)
    if ext == "wav":
        return (3, 0)
    if ext == "flac":
        return (2, 0)
    if ext == "mp3":
        return (1, bitrate if bitrate is not None else 0)
    
    return (0, 0)


# File Selection Functions

def is_original_version(filename: str, allow_alternatives: bool) -> bool:
    """
    Determine if a filename represents an original version (not remix/edit/etc).
    
    Args:
        filename: Name of the file to check
        allow_alternatives: If True, always returns True
        
    Returns:
        True if file appears to be original version
    """
    if allow_alternatives:
        return True
    
    excluded_keywords = [
        'remix', 'edit', 'bootleg', 'mashup', 'mix', 'acapella',
        'instrumental', 'sped up', 'slowed', 'cover', 'karaoke',
        'tribute', 'demo', 'live', 'acoustic', 'version', 'remaster',
        'flip', 'extended', 'rework', 're-edit', 'dub', 'radio'
    ]
    
    filename_lower = filename.lower()
    for keyword in excluded_keywords:
        if keyword in filename_lower:
            return False
    
    return True


def select_best_file(responses: List[Dict[str, Any]], search_text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Select the best quality file from search responses.
    
    Selection process:
    1. Filter out remixes/edits unless search text includes such terms
    2. Prioritize by quality: WAV > FLAC > MP3 320 > other MP3 > others
    3. Return best match or None if no suitable files found
    
    Args:
        responses: List of search response objects from slskd
        search_text: Original search query
        
    Returns:
        Tuple of (best_file_object, username) or (None, None) if no suitable files
    """
    # Determine if user is explicitly searching for alternatives
    excluded_keywords = [
        'remix', 'edit', 'bootleg', 'mashup', 'mix', 'acapella',
        'instrumental', 'sped up', 'slowed', 'cover', 'karaoke',
        'tribute', 'demo', 'live', 'acoustic', 'version', 'remaster',
        'flip', 'extended', 'rework', 're-edit', 'dub', 'radio'
    ]
    search_text_lower = search_text.lower()
    allow_alternatives = any(keyword in search_text_lower for keyword in excluded_keywords)
    
    # Collect all candidate files, skipping blacklisted slskd_uuids
    candidates = []
    for response in responses:
        username = response.get("username")
        files = response.get("files", [])
        for file in files:
            slskd_uuid = file.get("id")
            if slskd_uuid and track_db.is_slskd_blacklisted(slskd_uuid):
                write_log.info("SLSKD_BLACKLIST_SKIP", "Skipping blacklisted slskd_uuid in file selection.", {"slskd_uuid": slskd_uuid})
                continue
            candidates.append((file, username))
    
    if not candidates:
        return None, None
    
    # Filter by originality if not explicitly looking for alternatives
    if allow_alternatives:
        search_pool = candidates
    else:
        original_candidates = [
            (f, u) for f, u in candidates 
            if is_original_version(f.get("filename", ""), allow_alternatives=False)
        ]
        search_pool = original_candidates if original_candidates else candidates
    
    # Sort by quality (best first)
    search_pool.sort(key=quality_sort_key, reverse=True)
    
    # Return best match
    return search_pool[0] if search_pool else (None, None)


# API Communication Functions

def create_search(search_text: str) -> str:
    """
    Initiate a search on the Soulseek network via slskd API.
    
    Args:
        search_text: Query string to search for (e.g., "Artist Track Name")
    
    Returns:
        Unique search ID (UUID) for tracking this search
    
    Raises:
        requests.HTTPError: If the API request fails
    """
    search_id = str(uuid.uuid4())
    write_log.debug("SLSKD_SEARCH_CREATE", "Creating Soulseek search.", 
                   {"search_id": search_id, "search_text": search_text})
    
    try:
        resp = requests.post(
            f"{SLSKD_URL}/searches",
            json={"id": search_id, "searchText": search_text},
            headers={"X-API-Key": TOKEN},
            timeout=10
        )
        write_log.debug("SLSKD_SEARCH_RESPONSE", "Search POST response.", 
                       {"status_code": resp.status_code, "response_preview": resp.text[:200]})
        resp.raise_for_status()
    except requests.RequestException as e:
        write_log.error("SLSKD_SEARCH_CREATE_FAIL", "Failed to create search.", 
                       {"error": str(e), "search_text": search_text})
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
        write_log.debug("SLSKD_SEARCH_POLL", "Polling for search responses.", 
                       {"attempt": attempt, "max_attempts": MAX_SEARCH_ATTEMPTS, "search_id": search_id})
        try:
            # Get search responses
            resp = requests.get(
                f"{SLSKD_URL}/searches/{search_id}/responses",
                headers={"X-API-Key": TOKEN},
                timeout=10
            )
            resp.raise_for_status()
            responses = resp.json()
            
            # Check completion status separately
            is_complete = False
            try:
                status_resp = requests.get(
                    f"{SLSKD_URL}/searches/{search_id}",
                    headers={"X-API-Key": TOKEN},
                    timeout=10
                )
                status_resp.raise_for_status()
                status_data = status_resp.json()
                is_complete = status_data.get("isComplete", False) or status_data.get("state") == "Completed"
            except requests.RequestException as e:
                write_log.debug("SLSKD_SEARCH_STATUS_CHECK", "Could not check search completion status.", 
                               {"error": str(e)})
            
            # Return responses if any found
            if responses and isinstance(responses, list) and len(responses) > 0:
                write_log.info("SLSKD_SEARCH_RESULTS", "Found search results.", 
                              {"search_id": search_id, "response_count": len(responses)})
                return responses
            
            # Exit polling loop if search is marked complete with no results
            if is_complete:
                write_log.debug("SLSKD_SEARCH_COMPLETE", "Search marked complete with no results.", 
                               {"search_id": search_id})
                break
            
        except requests.RequestException as e:
            write_log.warn("SLSKD_SEARCH_POLL_ERROR", "Error polling for search results.", 
                          {"attempt": attempt, "error": str(e)})
        
        time.sleep(SEARCH_POLL_INTERVAL)
    
    write_log.warn("SLSKD_SEARCH_NO_RESULTS", "No search responses found after polling attempts.", 
                  {"search_id": search_id})
    return []


def enqueue_download(search_id: str, file: Dict[str, Any], username: str, spotify_id: str) -> Dict[str, Any]:
    """
    Queue a file for download from a Soulseek user and track the mapping.
    
    Args:
        search_id: UUID of the search that found this file
        file: File object containing 'filename' and 'size'
        username: Soulseek username to download from
        spotify_id: Spotify track ID to associate with this download
    
    Returns:
        API response dictionary containing enqueued download information
    
    Raises:
        requests.HTTPError: If the API request fails
        ValueError: If response doesn't contain expected data
    """
    filename = file.get("filename")
    size = file.get("size")
    extension, bitrate = extract_file_quality(file)
    
    write_log.info("SLSKD_DOWNLOAD_ENQUEUE", "Enqueuing download.", 
                  {"filename": filename, "username": username, "extension": extension, "bitrate": bitrate})
    
    try:
        url = f"{SLSKD_URL}/transfers/downloads/{username}"
        payload = [{"filename": filename, "size": size, "username": username}]
        
        resp = requests.post(
            url,
            json=payload,
            headers={"X-API-Key": TOKEN},
            timeout=10
        )
        write_log.debug("SLSKD_DOWNLOAD_RESPONSE", "Download POST response.", 
                       {"status_code": resp.status_code, "response_preview": resp.text[:200]})
        resp.raise_for_status()
        
        download_response = resp.json()
        
        # Validate and extract enqueued download information
        enqueued = download_response.get("enqueued", [])
        if not enqueued:
            raise ValueError("No downloads were enqueued in response.")
        
        slskd_uuid = enqueued[0].get("id")
        if not slskd_uuid:
            raise ValueError("Enqueued download missing UUID.")
        
        # Store mapping between Soulseek UUID and Spotify ID
        write_log.info("SLSKD_ENQUEUE_SUCCESS", "Successfully enqueued download.", 
                      {"slskd_uuid": slskd_uuid, "spotify_id": spotify_id})
        
        track_db.add_slskd_mapping(slskd_uuid, spotify_id)
        track_db.update_track_status(spotify_id, "downloading")
        track_db.update_slskd_file_name(spotify_id, filename)
        track_db.update_extension_bitrate(spotify_id, extension, bitrate)
        
        return download_response
        
    except requests.RequestException as e:
        write_log.error("SLSKD_ENQUEUE_FAIL", "Failed to enqueue download.", 
                       {"error": str(e), "filename": filename})
        raise
    except ValueError as e:
        write_log.error("SLSKD_ENQUEUE_INVALID", "Invalid download response.", {"error": str(e)})
        raise


# Main Download Functions

def download_track(artist: str, track: str, spotify_id: str) -> None:
    """
    Search for and initiate download of a track on the Soulseek network.
    
    This function:
    1. Checks if the track is already downloading or completed
    2. Creates a search query combining artist and track name
    3. Polls for search results
    4. Selects the best quality file from results
    5. Enqueues the file for download
    6. Updates the database with download status
    
    Quality Upgrade Logic:
    - If track exists but is not WAV, marks for redownload
    - Completed tracks are skipped unless they need quality upgrades
    
    Args:
        artist: Artist name(s)
        track: Track name
        spotify_id: Spotify track identifier for database tracking
    """
    # Check current status
    current_status = track_db.get_track_status(spotify_id)
    skip_statuses = {"completed", "queued", "downloading", "requested", "inprogress"}
    
    if current_status in skip_statuses:
        # Check if track needs quality upgrade (non-WAV files)
        current_extension = track_db.get_track_extension(spotify_id)
        if current_extension and current_extension.lower() != "wav":
            write_log.info("TRACK_QUALITY_UPGRADE", "Marking track for quality upgrade.", 
                          {"spotify_id": spotify_id, "current_extension": current_extension})
            track_db.update_track_status(spotify_id, "redownload_pending")
            return
        
        write_log.debug("SLSKD_SKIP", "Skipping download (already in progress or completed).", 
                       {"artist": artist, "track": track, "current_status": current_status})
        return

    search_text = f"{artist} {track}"
    write_log.info("SLSKD_SEARCH", "Searching for track on Soulseek.", {"search_text": search_text})
    track_db.update_track_status(spotify_id, "searching")

    try:
        # Perform search on Soulseek network
        search_id = create_search(search_text)
        responses = get_search_responses(search_id)
        
        if not responses:
            write_log.warn("SLSKD_NO_RESULTS", "No search results found.", {"search_text": search_text})
            track_db.update_track_status(spotify_id, "failed")
            return
        
        # Select best file according to quality rules
        best_file, username = select_best_file(responses, search_text)
        
        if not best_file:
            write_log.warn("SLSKD_NO_SUITABLE_FILE", "No suitable file found in results.", 
                          {"search_text": search_text})
            track_db.update_track_status(spotify_id, "failed")
            return
        
        # Enqueue download
        enqueue_download(search_id, best_file, username, spotify_id)
        
    except Exception as e:
        write_log.error("SLSKD_DOWNLOAD_FAIL", "Failed to download track.", 
                       {"artist": artist, "track": track, "error": str(e)})
        track_db.update_track_status(spotify_id, "failed")


def query_download_status() -> List[Dict[str, Any]]:
    """
    Query the status of all active downloads from slskd API.
    
    Returns:
        List of download status objects containing directories, files, and states.
        Returns empty list if the query fails.
    """
    write_log.info("SLSKD_QUERY_STATUS", "Querying download status for all transfers.")
    
    try:
        resp = requests.get(
            f"{SLSKD_URL}/transfers/downloads",
            headers={"X-API-Key": TOKEN},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        write_log.error("SLSKD_QUERY_STATUS_FAIL", "Failed to query download status.", {"error": str(e)})
        return []


def process_redownload_queue() -> None:
    """
    Process tracks marked for redownload (quality upgrade).
    
    This function should be called after all new tracks have been processed
    to upgrade existing tracks that don't have WAV quality.
    """
    write_log.info("SLSKD_REDOWNLOAD_QUEUE", "Processing redownload queue for quality upgrades.")
    
    # Get all tracks marked for redownload
    redownload_tracks = track_db.get_tracks_by_status("redownload_pending")
    
    if not redownload_tracks:
        write_log.info("SLSKD_REDOWNLOAD_EMPTY", "No tracks in redownload queue.")
        return
    
    write_log.info("SLSKD_REDOWNLOAD_COUNT", f"Found {len(redownload_tracks)} tracks for quality upgrade.")
    
    for track_row in redownload_tracks:
        spotify_id, track_name, artist = track_row[0], track_row[1], track_row[2]
        current_extension = track_db.get_track_extension(spotify_id)
        current_bitrate = get_track_bitrate(spotify_id)

        # Search for new candidates
        search_text = f"{artist} {track_name}"
        search_id = create_search(search_text)
        responses = get_search_responses(search_id)
        best_file, username = select_best_file(responses, search_text)

        if not best_file:
            write_log.warn("SLSKD_NO_SUITABLE_FILE", "No suitable file found for upgrade.", {"spotify_id": spotify_id})
            continue

        # Only proceed if the new file is truly better
        if is_better_quality(best_file, current_extension, current_bitrate):
            write_log.info("SLSKD_REDOWNLOAD_PROCESS", "Processing quality upgrade.", 
                          {"spotify_id": spotify_id, "track": track_name, "artist": artist, "upgrade": True})
            track_db.update_track_status(spotify_id, "pending")
            enqueue_download(search_id, best_file, username, spotify_id)
        else:
            write_log.info("SLSKD_REDOWNLOAD_SKIP", "No better quality file found for upgrade.", 
                          {"spotify_id": spotify_id, "track": track_name, "artist": artist, "upgrade": False})


def get_track_bitrate(spotify_id: str) -> Optional[int]:
    """
    Helper to get the bitrate for a track using TrackDB, not inline SQL.
    """
    try:
        cursor = track_db.conn.cursor()
        cursor.execute("SELECT bitrate FROM tracks WHERE spotify_id = ?", (spotify_id,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return int(result[0])
    except Exception:
        pass
    return None


if __name__ == "__main__":
    # Example usage for testing
    test_tracks = [
        ("5ms8IkagrFWObtzSOahVrx", "MASTER BOOT RECORD", "Skynet"),
    ]
    
    from logs_utils import setup_logging
    setup_logging(log_name_prefix="slskd_test")
    
    for spotify_id, artist, track_name in test_tracks:
        track_db.add_track(spotify_id, track_name, artist)
        download_track(artist, track_name, spotify_id)
    
    track_db.close()