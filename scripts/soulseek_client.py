"""
Soulseek download client module for interfacing with slskd API.

This module handles searching for tracks on the Soulseek network via the slskd
daemon API and managing download requests. It integrates with the database to
track download status and maintain mappings between Soulseek and Spotify IDs.

Key Features:
- Asynchronous batch searching for improved performance
- Intelligent file quality selection prioritizing lossless formats
- Automatic filtering of remixes/edits unless explicitly requested
- Quality upgrade system for existing downloads (upgrades lossy to lossless)
- Download status polling and database synchronization
- Thread-safe API communication

Quality Priority (all lossless formats will be remuxed to WAV):
1. Lossless formats: WAV, FLAC, ALAC, APE (all remuxed to WAV)
2. MP3 320kbps (remuxed from any lossy format)
3. Lower bitrate lossy formats
4. Other formats (OGG, M4A, etc.)

Note: All downloads are automatically remuxed to preferred formats:
- Lossless (FLAC, ALAC, APE) -> WAV
- Lossy (OGG, M4A, AAC, WMA, OPUS) -> MP3 320kbps

Public API:
- download_tracks_async(): Batch async downloads (recommended for multiple tracks)
- download_track(): Single track download (legacy, uses async internally)
- initiate_track_search(): Start search without waiting (for custom workflows)
- process_search_results(): Complete a search initiated earlier
- query_download_status(): Poll slskd for active download states
- process_redownload_queue(): Handle quality upgrade requests (async)
"""

import os
import time
import uuid
from typing import Any

import requests
from dotenv import load_dotenv

from scripts.database_management import TrackDB
from scripts.logs_utils import write_log

load_dotenv()

# Remuxing mode configuration from environment
PREFER_MP3 = os.getenv("PREFER_MP3", "false").lower() in ("true", "1", "yes")

# slskd API configuration
SLSKD_BASE_URL = os.getenv("SLSKD_BASE_URL", "http://localhost:5030")
SLSKD_URL = f"{SLSKD_BASE_URL}/api/v0"
TOKEN = os.getenv("TOKEN")

# Search and download constants
MAX_SEARCH_ATTEMPTS = 50
SEARCH_POLL_INTERVAL = 2  # seconds

# HTTP status codes
HTTP_OK = 200
HTTP_NOT_FOUND = 404
HTTP_SERVER_ERROR = 500

# Quality thresholds
MIN_BITRATE_KBPS = 320

# Database instance
track_db = TrackDB()


# Health Check Functions

def wait_for_slskd_ready(max_wait_seconds: int = 60, poll_interval: int = 2) -> bool:
    """
    Wait for slskd to be connected and authenticated before proceeding.

    This function polls the slskd server connection state endpoint until it reports
    that the server is connected and logged in. This prevents connection errors when
    the workflow starts before slskd is fully initialized.

    Args:
        max_wait_seconds: Maximum time to wait for slskd (default: 60 seconds)
        poll_interval: Time between status checks in seconds (default: 2 seconds)

    Returns:
        True if slskd is ready, False if timeout reached
    """
    write_log.info("SLSKD_HEALTH_CHECK", "Waiting for slskd to be ready.",
                  {"max_wait": max_wait_seconds, "poll_interval": poll_interval})

    start_time = time.time()
    attempts = 0

    while time.time() - start_time < max_wait_seconds:
        attempts += 1

        try:
            # Check the server connection state
            resp = requests.get(
                f"{SLSKD_URL}/server",
                headers={"X-API-Key": TOKEN} if TOKEN else {},
                timeout=5
            )

            if resp.status_code == HTTP_OK:
                try:
                    data = resp.json()
                    state = data.get("state", "Unknown")
                    is_connected = data.get("isConnected", False)
                    is_logged_in = data.get("isLoggedIn", False)

                    # Check if both connected and logged in using boolean flags
                    if is_connected and is_logged_in:
                        write_log.info("SLSKD_READY",
                                     "slskd is ready and authenticated.",
                                     {"attempts": attempts,
                                      "state": state,
                                      "wait_time": round(time.time() - start_time, 2)})
                        return True

                except (ValueError, KeyError):
                    pass  # Silently retry on parse errors
        except (requests.ConnectionError, requests.RequestException):
            pass  # Silently retry on connection errors

        # Wait before next attempt
        time.sleep(poll_interval)

    # Timeout reached
    write_log.error("SLSKD_TIMEOUT",
                   "Timeout waiting for slskd to be ready.",
                   {"max_wait": max_wait_seconds, "attempts": attempts})
    return False


# Quality Assessment Functions

def extract_file_quality(file: dict[str, Any]) -> tuple[str, int | None]:
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


def is_better_quality(file: dict[str, Any], current_extension: str, current_bitrate: int | None) -> bool:
    """
    Determine if a file has better quality than the current one.

    Quality hierarchy depends on PREFER_MP3 mode:

    Mode 1 (PREFER_MP3=True): All formats -> MP3 320kbps
    - No quality upgrades possible (all end up as MP3 320kbps)

    Mode 2 (PREFER_MP3=False): Lossless -> WAV, Lossy -> MP3 320kbps
    - Lossless formats > Lossy formats

    Args:
        file: New file object to evaluate
        current_extension: Extension of current file
        current_bitrate: Bitrate of current file (kbps)

    Returns:
        True if the new file is higher quality
    """
    ext, bitrate = extract_file_quality(file)

    # Define format categories (matching workflow.py remuxing logic)
    lossless_formats = {'wav', 'flac', 'alac', 'ape'}
    current_is_lossless = current_extension in lossless_formats

    # Mode 1: All files remux to MP3 320kbps - no quality upgrades possible
    if PREFER_MP3:
        return False

    # Mode 2: Lossless -> WAV, Lossy -> MP3 320kbps
    if ext in lossless_formats:
        return not current_is_lossless

    if current_is_lossless:
        return False

    if ext == "mp3":
        if current_extension == "mp3" and bitrate and current_bitrate:
            return bitrate > current_bitrate
        return bool(bitrate and bitrate >= MIN_BITRATE_KBPS and current_extension not in ("mp3", *lossless_formats))

    return False


def quality_sort_key(item: tuple[dict[str, Any], str]) -> tuple[int, int]:
    """
    Generate a sort key for file quality prioritization.

    Priority depends on PREFER_MP3 mode:

    Mode 1 (PREFER_MP3=True): All formats -> MP3 320kbps
    - Prioritize MP3 files (already in target format)
    - Then other formats by bitrate

    Mode 2 (PREFER_MP3=False): Lossless -> WAV, Lossy -> MP3 320kbps
    - Prioritize lossless formats (will become WAV)
    - Then lossy formats by bitrate

    Args:
        item: Tuple of (file_object, username)

    Returns:
        Tuple of (format_priority, bitrate) for sorting.
        Higher values = better quality.
    """
    file, _ = item
    ext, bitrate = extract_file_quality(file)

    lossless_formats = {'wav', 'flac', 'alac', 'ape'}

    if PREFER_MP3:
        # Mode 1: All files remux to MP3 320kbps
        # Prioritize MP3 files (no conversion needed), then by bitrate
        if ext == "mp3":
            return (2, bitrate if bitrate is not None else 0)
        # Lossless and other lossy formats need conversion
        return (1, bitrate if bitrate is not None else 0)
    else:
        # Mode 2: Lossless -> WAV, Lossy -> MP3 320kbps
        # Lossless formats: WAV, FLAC, ALAC, APE (all will become WAV, priority 3)
        if ext in lossless_formats:
            return (3, 0)

        # Lossy formats prioritized by bitrate (all will become MP3 320kbps, priority 1)
        if ext == "mp3":
            return (1, bitrate if bitrate is not None else 0)

        # Other lossy formats (OGG, M4A, AAC, WMA, OPUS) - lower priority
        return (0, bitrate if bitrate is not None else 0)


# File Selection Functions

def is_audio_file(file: dict[str, Any]) -> bool:
    """
    Check if a file is a supported audio format.

    Supported formats: WAV, FLAC, MP3, OGG, M4A, AAC, ALAC, APE, WMA, OPUS

    Args:
        file: File object from slskd API response

    Returns:
        True if file is a supported audio format
    """
    ext, _ = extract_file_quality(file)

    # List of supported audio extensions
    supported_audio_formats = {
        'wav', 'flac', 'mp3', 'ogg', 'm4a', 'aac',
        'alac', 'ape', 'wma', 'opus'
    }

    is_supported = ext in supported_audio_formats

    return is_supported


def meets_bitrate_requirements(file: dict[str, Any]) -> bool:
    """
    Check if a file meets minimum bitrate requirements.

    Requirements:
    - WAV: Always accepted (lossless)
    - FLAC: Always accepted (lossless)
    - MP3: Minimum 320kbps
    - Other lossy formats: Minimum 320kbps

    Args:
        file: File object from slskd API response

    Returns:
        True if file meets bitrate requirements
    """
    ext, bitrate = extract_file_quality(file)

    # Lossless formats always meet requirements
    lossless_formats = {'wav', 'flac', 'alac', 'ape'}
    if ext in lossless_formats:
        return True

    # For lossy formats, require 320kbps minimum
    minimum_bitrate = 320

    if bitrate is None:
        # If bitrate is unknown, reject the file
        return False

    meets_requirement = bitrate >= minimum_bitrate

    if not meets_requirement:
        file.get('filename', '')

    return meets_requirement


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
    return all(keyword not in filename_lower for keyword in excluded_keywords)


def select_best_file(responses: list[dict[str, Any]], search_text: str) -> tuple[dict[str, Any] | None, str | None]:
    """
    Select the best quality file from search responses.

    Selection process:
    1. Filter out blacklisted files
    2. Filter out non-audio files
    3. Filter out low-bitrate files (< 320kbps for lossy formats)
    4. Filter out remixes/edits unless search text includes such terms
    5. Prioritize by quality: WAV > FLAC > MP3 320 > other MP3 > others
    6. Return best match or None if no suitable files found

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

    write_log.debug("SLSKD_FILE_SELECTION_START", "Starting file selection process.",
                   {"response_count": len(responses), "allow_alternatives": allow_alternatives})

    # Collect all candidate files, skipping blacklisted slskd_uuids
    candidates = []
    non_audio_count = 0
    low_bitrate_count = 0
    total_files = 0

    for response in responses:
        username = response.get("username")
        files = response.get("files", [])
        total_files += len(files)

        for file in files:
            slskd_uuid = file.get("id")

            if slskd_uuid and track_db.is_slskd_blacklisted(slskd_uuid):
                continue  # Blacklisted file skipped

            # Filter out non-audio files
            if not is_audio_file(file):
                non_audio_count += 1
                continue

            # Filter out low-bitrate files
            if not meets_bitrate_requirements(file):
                low_bitrate_count += 1
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

        search_pool = original_candidates or candidates

    # Sort by quality (best first)
    search_pool.sort(key=quality_sort_key, reverse=True)

    if search_pool:
        return search_pool[0]

    return None, None


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


def check_search_status(search_id: str) -> tuple[bool, list[dict[str, Any]]]:
    """
    Check if a search is complete and retrieve its responses (single check, no polling).

    Args:
        search_id: UUID of the search to check

    Returns:
        Tuple of (is_complete, responses)
        - is_complete: True if search finished (with or without results)
        - responses: List of response objects if any found, empty list otherwise
        - is_complete: None if search UUID not found in slskd (e.g., data wiped)
    """

    try:
        # Get search responses
        resp = requests.get(
            f"{SLSKD_URL}/searches/{search_id}/responses",
            headers={"X-API-Key": TOKEN},
            timeout=10
        )

        # If search UUID not found (HTTP_NOT_FOUND), return None to indicate missing search
        if resp.status_code == HTTP_NOT_FOUND:
            write_log.warn("SLSKD_SEARCH_NOT_FOUND", "Search UUID not found in slskd",
                          {"search_id": search_id})
            return (None, [])

        resp.raise_for_status()
        responses = resp.json()

        # Check completion status
        is_complete = False
        try:
            status_resp = requests.get(
                f"{SLSKD_URL}/searches/{search_id}",
                headers={"X-API-Key": TOKEN},
                timeout=10
            )

            # If search UUID not found (HTTP_NOT_FOUND), return None to indicate missing search
            if status_resp.status_code == HTTP_NOT_FOUND:
                write_log.warn("SLSKD_SEARCH_NOT_FOUND", "Search UUID not found in slskd",
                              {"search_id": search_id})
                return (None, [])

            status_resp.raise_for_status()
            status_data = status_resp.json()
            is_complete = status_data.get("isComplete", False) or status_data.get("state") == "Completed"
        except requests.RequestException:
            pass  # Silently continue on error checking status

        # Return responses if any found
        if responses and isinstance(responses, list) and len(responses) > 0:
            return (True, responses)  # Consider search complete if it has results

        # Return completion status even if no results
        if is_complete:
            return (True, [])

        # Search still in progress
        return (False, [])

    except Exception:
        return (False, [])

def _make_download_request(
    url: str, payload: list[dict], attempt: int  # noqa: ARG001
) -> dict[str, Any]:
    """Make the download request to slskd API."""
    resp = requests.post(
        url,
        json=payload,
        headers={"X-API-Key": TOKEN},
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def _validate_download_response(download_response: dict[str, Any]) -> str:
    """Validate download response and extract the slskd UUID."""
    enqueued = download_response.get("enqueued", [])
    if not enqueued:
        raise ValueError("No downloads were enqueued in response.")

    slskd_uuid = enqueued[0].get("id")
    if not slskd_uuid:
        raise ValueError("Enqueued download missing UUID.")

    return slskd_uuid


def enqueue_download(
    file: dict[str, Any], username: str, track_id: str, max_retries: int = 3
) -> dict[str, Any]:
    """
    Queue a file for download from a Soulseek user and track the mapping.

    Implements retry logic with exponential backoff for handling temporary
    slskd server issues (500 errors, timeouts).

    Args:
        file: File object containing 'filename' and 'size'
        username: Soulseek username to download from
        track_id: Track ID to associate with this download
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        API response dictionary containing enqueued download information

    Raises:
        requests.HTTPError: If the API request fails after all retries
        ValueError: If response doesn't contain expected data
    """
    filename = file.get("filename")
    size = file.get("size")
    extension, bitrate = extract_file_quality(file)

    write_log.debug("SLSKD_DOWNLOAD_ENQUEUE", "Enqueuing download.",
                  {"filename": filename, "username": username, "extension": extension, "bitrate": bitrate})

    url = f"{SLSKD_URL}/transfers/downloads/{username}"
    payload = [{"filename": filename, "size": size, "username": username}]
    last_error = None

    for attempt in range(max_retries):
        try:
            download_response = _make_download_request(url, payload, attempt)
            slskd_uuid = _validate_download_response(download_response)

            # Update database after successful download enqueue
            write_log.debug("SLSKD_ENQUEUE_SUCCESS", "Successfully enqueued download.",
                          {"slskd_uuid": slskd_uuid, "track_id": track_id, "attempt": attempt + 1})
            track_db.set_download_uuid(track_id, slskd_uuid, username)
            track_db.update_track_status(track_id, "downloading")
            track_db.update_slskd_file_name(track_id, filename)
            track_db.update_extension_bitrate(track_id, extension, bitrate)

            return download_response

        except (requests.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                write_log.info("SLSKD_ENQUEUE_RETRY", "Download enqueue failed, retrying.",
                              {"error": str(e), "attempt": attempt + 1, "max_retries": max_retries,
                               "wait_time": wait_time, "filename": filename})
                time.sleep(wait_time)
            else:
                write_log.warn("SLSKD_ENQUEUE_FAIL", "Failed to enqueue download after all retries.",
                               {"error": str(e), "filename": filename, "attempts": max_retries})
                track_db.update_track_status(track_id, "failed", failed_reason=str(e))
                raise

        except requests.HTTPError as e:
            last_error = e
            if e.response.status_code >= HTTP_SERVER_ERROR and attempt < max_retries - 1:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
            else:
                write_log.warn("SLSKD_ENQUEUE_FAIL", "Failed to enqueue download.",
                               {"error": str(e), "filename": filename})
                track_db.update_track_status(track_id, "failed", failed_reason=str(e))
                raise

        except requests.RequestException as e:
            last_error = e
            write_log.warn("SLSKD_ENQUEUE_FAIL", "Failed to enqueue download.",
                           {"error": str(e), "filename": filename})
            track_db.update_track_status(track_id, "failed", failed_reason=str(e))
            raise

        except ValueError as e:
            last_error = e
            write_log.warn("SLSKD_ENQUEUE_INVALID", "Invalid download response.", {"error": str(e)})
            track_db.update_track_status(track_id, "failed", failed_reason=str(e))
            raise

    # This should not be reached due to raise in the loop, but just in case
    if last_error:
        raise last_error


# Main Download Functions

def initiate_track_search(artist: str, track: str, track_id: str) -> tuple[str, str, str] | None:
    """
    Initiate an asynchronous search for a track on the Soulseek network.

    This function only creates the search without waiting for results.
    Call process_search_results() later to complete the download process.

    Args:
        artist: Artist name(s)
        track: Track name
        track_id: Track identifier for database tracking

    Returns:
        Tuple of (search_id, search_text, track_id) if search was initiated,
        None if track should be skipped
    """
    # Check current status
    current_status = track_db.get_track_status(track_id)
    skip_statuses = {"completed", "queued", "downloading", "requested", "inprogress"}

    if current_status in skip_statuses:
        return None

    search_text = f"{artist} {track}"

    try:
        # Create search without waiting for results
        search_id = create_search(search_text)

        # Store the search mapping immediately so we can find it later
        track_db.set_search_uuid(track_id, search_id)

        # Update status to searching after mapping is stored
        track_db.update_track_status(track_id, "searching")

        return (search_id, search_text, track_id)

    except Exception as e:
        write_log.warn("SLSKD_SEARCH_INITIATE_FAIL", "Failed to initiate search.",
                       {"artist": artist, "track": track, "error": str(e)})
        track_db.update_track_status(track_id, "failed", failed_reason=str(e))
        return None


def process_search_results(
    search_id: str, search_text: str, track_id: str, check_quality_upgrade: bool = False
) -> bool:
    """
    Check search results once and enqueue download if suitable file is found.

    This function does NOT poll - it checks the search status exactly once.
    For fire-and-forget workflow, call this after searches have had time to complete.

    Args:
        search_id: UUID of the search to retrieve results for
        search_text: Original search query text
        track_id: Track identifier for database tracking
        check_quality_upgrade: If True, only download if quality is better than current file

    Returns:
        True if search was completed and processed, False if still in progress
    """
    write_log.debug("SLSKD_SEARCH_PROCESS", "Processing search results.",
                  {"search_id": search_id, "track_id": track_id, "search_text": search_text,
                   "check_quality": check_quality_upgrade})

    try:
        # Check search status once (no polling)
        is_complete, responses = check_search_status(search_id)

        # If search UUID not found in slskd (data wiped), reset track status
        if is_complete is None:
            write_log.warn("SLSKD_SEARCH_UUID_LOST", "Search UUID not found in slskd, resetting track.",
                          {"search_id": search_id, "track_id": track_id})
            # Clear the slskd_search_uuid and reset status to allow re-searching
            track_db.set_search_uuid(track_id, None)
            track_db.update_track_status(track_id, "not_found")
            return True

        # If search is not complete, leave status as 'searching'
        if not is_complete:
            return False

        # Search is complete but no results
        if not responses:
            write_log.info("SLSKD_NO_RESULTS", "No search results found.",
                          {"search_text": search_text, "track_id": track_id})
            # If this was a quality upgrade attempt, revert to completed status
            if check_quality_upgrade:
                track_db.update_track_status(track_id, "completed")
            else:
                track_db.update_track_status(track_id, "not_found")
            remove_search_from_slskd(search_id, track_id)
            track_db.set_search_uuid(track_id, None)
            return True

        # Select best file according to quality rules
        best_file, username = select_best_file(responses, search_text)

        if not best_file:
            write_log.info("SLSKD_NO_SUITABLE_FILE", "No suitable file found in results.",
                          {"search_text": search_text, "track_id": track_id})
            # If this was a quality upgrade attempt, revert to completed status
            if check_quality_upgrade:
                track_db.update_track_status(track_id, "completed")
            else:
                track_db.update_track_status(track_id, "no_suitable_file")
            remove_search_from_slskd(search_id, track_id)
            track_db.set_search_uuid(track_id, None)
            return True

        # If checking for quality upgrade, verify new file is actually better
        if check_quality_upgrade:
            current_extension = track_db.get_track_extension(track_id)
            current_bitrate = get_track_bitrate(track_id)

            if not is_better_quality(best_file, current_extension, current_bitrate):
                write_log.info("SLSKD_REDOWNLOAD_SKIP", "No better quality file found for upgrade.",
                              {"track_id": track_id, "current_extension": current_extension,
                               "current_bitrate": current_bitrate})
                track_db.update_track_status(track_id, "completed")
                remove_search_from_slskd(search_id, track_id)
                track_db.set_search_uuid(track_id, None)
                return True

            write_log.info("SLSKD_REDOWNLOAD_PROCESS", "Found better quality file for upgrade.",
                          {"track_id": track_id})

        # Enqueue download (will update status to pending/queued)
        enqueue_download(best_file, username, track_id)
        remove_search_from_slskd(search_id, track_id)
        track_db.set_search_uuid(track_id, None)
        return True

    except Exception as e:
        write_log.warn("SLSKD_SEARCH_PROCESS_FAIL", "Failed to process search results.",
                       {"search_id": search_id, "track_id": track_id, "error": str(e)})
        track_db.update_track_status(track_id, "failed", failed_reason=str(e))


def download_tracks_async(tracks: list[tuple[str, str, str]]) -> None:
    """
    Initiate searches for multiple tracks without waiting for results.

    This function uses a fire-and-forget approach: it creates all search requests
    in slskd but does NOT wait for them to complete. Searches will continue running
    in slskd even after this function returns.

    To process completed searches, call process_pending_searches() later.

    Args:
        tracks: List of tuples containing (track_id, artist, track_name)
    """
    if not tracks:
        return

    # Initiate all searches without waiting for results
    initiated_count = 0
    for track_id, artist, track_name in tracks:
        search_info = initiate_track_search(artist, track_name, track_id)
        if search_info:
            initiated_count += 1

    if initiated_count > 0:
        write_log.info("ASYNC_DOWNLOAD_START", "Initiated searches for tracks.",
                      {"initiated": initiated_count, "total": len(tracks)})


def process_pending_searches() -> None:
    """
    Process all tracks with 'searching' status by checking their search results.

    This function should be called periodically or at workflow start to process
    searches that were initiated but not yet completed. It's restart-safe:
    - Tracks marked 'searching' may have completed searches in slskd
    - Checks each search once without polling
    - Updates status based on results or leaves as 'searching' if incomplete
    - Handles both new downloads and quality upgrades
    """
    write_log.info("PROCESS_PENDING_SEARCHES", "Checking for completed searches.")

    # Get all tracks currently in 'searching' status
    searching_tracks = track_db.get_tracks_by_status("searching")

    if not searching_tracks:
        return

    # Process each track's search
    processed_count = 0
    still_searching_count = 0

    for track_row in searching_tracks:
        track_id = track_row[0]  # First column is track_id
        track_name = track_row[1]
        artist = track_row[2]
        # Column 6 is local_file_path
        local_file_path = track_row[6] if len(track_row) > 6 else None  # noqa: PLR2004

        # Try to get the slskd search UUID for this track
        slskd_uuid = track_db.get_search_uuid_by_track_id(track_id)

        if not slskd_uuid:
            # No search UUID means search was never properly initiated
            track_db.update_track_status(track_id, "pending")
            continue

        # Construct search text to pass to processor
        search_text = f"{artist} {track_name}"

        # Determine if this is a quality upgrade search (track has existing file)
        is_quality_upgrade = bool(local_file_path)

        # Process the search results (checks once, no polling)
        was_completed = process_search_results(slskd_uuid, search_text, track_id,
                                               check_quality_upgrade=is_quality_upgrade)

        if was_completed:
            processed_count += 1
        else:
            still_searching_count += 1

    if processed_count > 0 or still_searching_count > 0:
        write_log.info("PENDING_SEARCHES_PROCESSED", "Checked pending searches.",
                      {"processed": processed_count, "still_searching": still_searching_count})


def remove_search_from_slskd(search_id: str, track_id: str | None = None, max_retries: int = 3) -> bool:
    """
    Remove a completed search from slskd so it no longer appears in status queries.

    Args:
        search_id: slskd search UUID to remove
        track_id: Optional Track ID to clear local mapping
        max_retries: Maximum retry attempts for transient failures

    Returns:
        True if the search was removed (or already absent), False otherwise
    """
    for attempt in range(max_retries):
        try:
            resp = requests.delete(
                f"{SLSKD_URL}/searches/{search_id}",
                headers={"X-API-Key": TOKEN},
                timeout=10
            )

            if resp.status_code in (200, 204, 404):
                write_log.debug(
                    "SLSKD_SEARCH_REMOVE_SUCCESS",
                    "Search removed from slskd.",
                    {"search_id": search_id, "track_id": track_id}
                )
                if track_id:
                    track_db.set_search_uuid(track_id, None)
                return True

            resp.raise_for_status()

        except (requests.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                write_log.warn(
                    "SLSKD_SEARCH_REMOVE_RETRY",
                    "Network error removing search, retrying.",
                    {"attempt": attempt + 1, "max_retries": max_retries, "wait_time": wait_time, "error": str(e)}
                )
                time.sleep(wait_time)
            else:
                write_log.warn(
                    "SLSKD_SEARCH_REMOVE_FAIL",
                    "Failed to remove search after retries.",
                    {"search_id": search_id, "track_id": track_id, "error": str(e)}
                )
                return False

        except requests.HTTPError as e:
            write_log.warn(
                "SLSKD_SEARCH_REMOVE_FAIL",
                "HTTP error removing search.",
                {"search_id": search_id, "track_id": track_id,
                 "status_code": e.response.status_code if e.response else None, "error": str(e)}
            )
            return False

        except requests.RequestException as e:
            write_log.warn(
                "SLSKD_SEARCH_REMOVE_FAIL",
                "Failed to remove search.",
                {"search_id": search_id, "track_id": track_id, "error": str(e)}
            )
            return False

    return False


def remove_download_from_slskd(username: str, slskd_uuid: str, max_retries: int = 3) -> bool:
    """
    Remove a download from slskd to prevent it from appearing in future status queries.

    This is useful for cleaning up failed downloads so they don't produce duplicate
    log entries on subsequent workflow runs.

    Args:
        username: Soulseek username the download is from
        slskd_uuid: UUID of the download to remove
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        True if the download was successfully removed, False otherwise
    """

    for attempt in range(max_retries):
        try:
            url = f"{SLSKD_URL}/transfers/downloads/{username}/{slskd_uuid}?remove=true"
            resp = requests.delete(
                url,
                headers={"X-API-Key": TOKEN},
                timeout=10
            )

            if resp.status_code in (200, 204, 404):
                # 200/204 = successfully removed, 404 = already gone
                write_log.info("SLSKD_REMOVE_SUCCESS", "Successfully removed download from slskd.",
                              {"username": username, "slskd_uuid": slskd_uuid})
                # Also remove from our database mapping
                track_db.delete_slskd_mapping(slskd_uuid)
                return True

            resp.raise_for_status()

        except (requests.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                write_log.warn("SLSKD_REMOVE_RETRY", "Network error removing download, retrying.",
                              {"attempt": attempt + 1, "max_retries": max_retries,
                               "wait_time": wait_time, "error": str(e)})
                time.sleep(wait_time)
            else:
                write_log.warn("SLSKD_REMOVE_FAIL", "Failed to remove download after all retries.",
                              {"username": username, "slskd_uuid": slskd_uuid, "error": str(e)})
                return False

        except requests.HTTPError as e:
            write_log.warn("SLSKD_REMOVE_FAIL", "HTTP error removing download.",
                          {"username": username, "slskd_uuid": slskd_uuid,
                           "status_code": e.response.status_code if e.response else None,
                           "error": str(e)})
            return False

        except requests.RequestException as e:
            write_log.warn("SLSKD_REMOVE_FAIL", "Failed to remove download.",
                          {"username": username, "slskd_uuid": slskd_uuid, "error": str(e)})
            return False

    return False


def query_download_status(max_retries: int = 3) -> list[dict[str, Any]]:
    """
    Query the status of all active downloads from slskd API.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        List of download status objects containing directories, files, and states.
        Returns empty list if the query fails.
    """
    for attempt in range(max_retries):
        try:
            resp = requests.get(
                f"{SLSKD_URL}/transfers/downloads",
                headers={"X-API-Key": TOKEN},
                timeout=10
            )
            resp.raise_for_status()
            return resp.json()

        except (requests.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries - 1:
                backoff_time = 2 ** attempt
                time.sleep(backoff_time)
            else:
                write_log.warn(
                    "SLSKD_QUERY_STATUS_FAIL",
                    "Failed to query download status after retries.",
                    {"error": str(e)}
                )

        except requests.HTTPError as e:
            if e.response and e.response.status_code >= HTTP_SERVER_ERROR:
                if attempt < max_retries - 1:
                    backoff_time = 2 ** attempt
                    time.sleep(backoff_time)
                else:
                    write_log.warn(
                        "SLSKD_QUERY_STATUS_FAIL",
                        "Server error querying download status.",
                        {"error": str(e)}
                    )
            else:
                write_log.warn(
                    "SLSKD_QUERY_STATUS_FAIL",
                    "HTTP error querying download status.",
                    {"error": str(e)}
                )
                return []

        except requests.RequestException as e:
            write_log.warn("SLSKD_QUERY_STATUS_FAIL", "Failed to query download status.", {"error": str(e)})
            return []

    # All retries exhausted
    return []


def process_redownload_queue() -> None:
    """
    Initiate searches for tracks marked for redownload (quality upgrade).

    This function uses a fire-and-forget approach: it changes the status from
    'redownload_pending' to 'searching' and initiates searches without waiting.
    The searches will be processed on the next workflow run.

    Note: Unlike new tracks, quality upgrades need special handling to compare
    file quality before downloading. This is done in process_pending_searches().
    """
    # Get all tracks marked for redownload
    redownload_tracks = track_db.get_tracks_by_status("redownload_pending")

    if not redownload_tracks:
        return

    # Initiate all searches without waiting
    initiated_count = 0

    for track_row in redownload_tracks:
        track_id, track_name, artist = track_row[0], track_row[1], track_row[2]

        # Create search and update status
        search_text = f"{artist} {track_name}"
        try:
            search_id = create_search(search_text)
            track_db.set_search_uuid(track_id, search_id)
            track_db.update_track_status(track_id, "searching")
            initiated_count += 1
        except Exception as e:
            write_log.warn("SLSKD_REDOWNLOAD_SEARCH_FAIL", "Failed to create upgrade search.",
                          {"error": str(e)})
            track_db.update_track_status(track_id, "failed", failed_reason=str(e))

    if initiated_count > 0:
        write_log.info("SLSKD_REDOWNLOAD_SEARCHES_INITIATED", "Initiated upgrade searches.",
                      {"initiated": initiated_count})


def get_track_bitrate(track_id: str) -> int | None:
    """
    Helper to get the bitrate for a track using the TrackDB abstraction layer.
    """
    try:
        return track_db.get_track_bitrate(track_id)
    except Exception:
        # Preserve existing behavior of returning None on any error
        return None
