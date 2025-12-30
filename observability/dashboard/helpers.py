"""
Shared helper functions for the dashboard.

Contains utility functions used across multiple tabs.
"""

import os
import shutil
from pathlib import Path
from typing import Dict, Optional, Tuple

import streamlit as st
from mutagen import File as MutagenFile

from scripts.constants import LOSSLESS_FORMATS, MIN_BITRATE_KBPS
from scripts.logs_utils import write_log
from scripts.m3u8_manager import update_track_in_m3u8
from scripts.soulseek_client import remove_search_from_slskd, remove_download_from_slskd

from .config import DB_PATH, IS_DOCKER, IMPORTED_DIR, track_db


def require_database(db_path: str = None, error_msg: str = None) -> bool:
    """
    Check if database exists and show appropriate message if not.
    
    Args:
        db_path: Path to database file. Defaults to DB_PATH.
        error_msg: Custom error message. Defaults to generic message.
    
    Returns:
        True if database exists, False otherwise (also displays UI message).
    """
    path = db_path or DB_PATH
    if not os.path.exists(path):
        if error_msg:
            st.error(error_msg)
        else:
            st.info("Database file not found.")
        return False
    return True


def sanitize_filename(artist: str, track_name: str, extension: str) -> str:
    """
    Create a safe filename from artist and track name.
    
    Args:
        artist: Artist name
        track_name: Track name
        extension: File extension (with or without leading dot)
    
    Returns:
        Sanitized filename safe for filesystem use.
    """
    # Ensure extension has leading dot
    if not extension.startswith('.'):
        extension = f'.{extension}'
    
    raw = f"{artist}_{track_name}{extension}".replace(' ', '_')
    # Remove invalid filename characters, keep only alphanumeric, underscore, dot, hyphen
    return "".join(c for c in raw if c.isalnum() or c in ('_', '.', '-'))


def normalize_docker_path(path: str) -> str:
    """
    Normalize a file path for Docker environment.
    
    If running in Docker and path doesn't start with /app/, converts it.
    
    Args:
        path: Absolute file path
    
    Returns:
        Path normalized for Docker (or unchanged if not in Docker).
    """
    if IS_DOCKER and not path.startswith('/app/'):
        # Get the base directory (two levels up from config.py location)
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        return path.replace(base_dir, '/app')
    return path


def is_quality_worse_than_mp3_320(file_path: str, extension: str, bitrate: Optional[int]) -> Tuple[bool, str]:
    """
    Check if an audio file is worse quality than MP3 320kbps.
    
    Args:
        file_path: Path to the audio file
        extension: File extension (e.g., 'mp3', 'flac', 'wav')
        bitrate: Bitrate in kbps (None if unavailable)
    
    Returns:
        Tuple of (is_worse_quality: bool, reason: str)
    """
    # Lossless formats are always considered acceptable quality
    if extension.lower() in LOSSLESS_FORMATS:
        return False, ""
    
    # For lossy formats, check bitrate
    if bitrate is None:
        return True, "Could not determine bitrate"
    
    if bitrate < MIN_BITRATE_KBPS:
        return True, f"{extension.upper()} {bitrate}kbps is lower quality than MP3 {MIN_BITRATE_KBPS}kbps"
    
    return False, ""


def extract_metadata_from_file(file_path: str) -> Dict[str, Optional[any]]:
    """
    Extract extension and bitrate from an audio file using mutagen.
    
    Args:
        file_path: Path to the audio file
    
    Returns:
        Dictionary with 'extension' and 'bitrate' keys
    """
    metadata = {
        'extension': None,
        'bitrate': None
    }
    
    try:
        # Get extension from filename
        extension = Path(file_path).suffix.lstrip('.').lower()
        metadata['extension'] = extension
        
        # Extract bitrate using mutagen
        audio = MutagenFile(file_path, easy=False)
        if audio and hasattr(audio.info, 'bitrate') and audio.info.bitrate:
            metadata['bitrate'] = int(audio.info.bitrate / 1000)  # Convert to kbps
        
        write_log.debug("IMPORT_METADATA_EXTRACT", "Extracted metadata from file.", 
                       {"file_path": file_path, "metadata": metadata})
    
    except Exception as e:
        write_log.error("IMPORT_METADATA_FAIL", "Failed to extract metadata.", 
                       {"file_path": file_path, "error": str(e)})
    
    return metadata


def compute_effective_bitrate_kbps(file_path: str) -> Optional[int]:
    """
    Compute an effective bitrate (kbps) from file size and duration.
    Returns None if duration cannot be determined.
    """
    try:
        if not file_path or not os.path.exists(file_path):
            return None
        size_bytes = os.path.getsize(file_path)
        audio = MutagenFile(file_path, easy=False)
        duration = getattr(getattr(audio, "info", None), "length", None)
        if not duration or duration <= 0:
            return None
        kbps = int(round((size_bytes * 8) / duration / 1000))
        return kbps
    except Exception as e:
        write_log.debug("BITRATE_EFFECTIVE_FAIL", "Failed to compute effective bitrate.", {
            "file_path": file_path,
            "error": str(e)
        })
        return None


def do_track_import(
    track_id: str,
    source_path: str,
    artist: str,
    track_name: str,
    is_upload: bool = False,
    uploaded_file=None
) -> Tuple[bool, str]:
    """
    Shared import logic for both manual and auto import.
    
    Args:
        track_id: Track identifier
        source_path: Source file path (for auto import) or temp path (for upload)
        artist: Artist name for filename
        track_name: Track name for filename
        is_upload: If True, source is an uploaded file buffer
        uploaded_file: Streamlit UploadedFile object (required if is_upload=True)
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Determine file extension
        if is_upload and uploaded_file:
            file_extension = Path(uploaded_file.name).suffix
        else:
            file_extension = os.path.splitext(source_path)[1]
        
        # Generate safe destination filename
        safe_filename = sanitize_filename(artist, track_name, file_extension)
        
        # Use absolute path and normalize for Docker
        destination_path = os.path.abspath(os.path.join(IMPORTED_DIR, safe_filename))
        destination_path = normalize_docker_path(destination_path)
        
        # Save/copy file to destination
        if is_upload and uploaded_file:
            with open(destination_path, 'wb') as f:
                f.write(uploaded_file.getbuffer())
            write_log.info("IMPORT_FILE_SAVED", "Saved imported file.", 
                          {"track_id": track_id, "destination": destination_path})
        else:
            shutil.copy2(source_path, destination_path)
            write_log.info("AUTO_IMPORT_FILE_COPIED", "Copied file for auto-import.", 
                          {"track_id": track_id, "source": source_path, "destination": destination_path})
        
        # Clean up any ongoing searches and downloads in slskd
        search_uuid = track_db.get_search_uuid_by_track_id(track_id)
        if search_uuid:
            remove_search_from_slskd(search_uuid, track_id)
            write_log.debug("IMPORT_SEARCH_REMOVED", "Removed ongoing search from slskd.", 
                          {"track_id": track_id, "search_uuid": search_uuid})
        
        download_uuid = track_db.get_download_uuid_by_track_id(track_id)
        if download_uuid:
            username = track_db.get_username_by_slskd_uuid(download_uuid)
            if username:
                remove_download_from_slskd(username, download_uuid)
                write_log.debug("IMPORT_DOWNLOAD_REMOVED", "Removed download from slskd.", 
                              {"track_id": track_id, "download_uuid": download_uuid, "username": username})
        
        # Extract metadata
        metadata = extract_metadata_from_file(destination_path)
        
        # Check quality and log warning if below threshold
        is_worse, reason = is_quality_worse_than_mp3_320(
            destination_path,
            metadata.get('extension', ''),
            metadata.get('bitrate')
        )
        if is_worse:
            write_log.warn("IMPORT_LOW_QUALITY", "Imported file has lower quality than target.",
                            {"track_id": track_id, "reason": reason})
        
        # Update database
        track_db.update_local_file_path(track_id, destination_path)
        track_db.update_extension_bitrate(
            track_id, 
            extension=metadata['extension'], 
            bitrate=metadata['bitrate']
        )
        track_db.update_track_status(track_id, "completed")
        
        write_log.info("IMPORT_DB_UPDATED", "Updated database for imported track.", 
                      {"track_id": track_id, "extension": metadata['extension'], 
                       "bitrate": metadata['bitrate']})
        
        # Update M3U8 files
        playlist_urls = track_db.get_playlists_for_track(track_id)
        for playlist_url in playlist_urls:
            m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
            if m3u8_path:
                update_track_in_m3u8(m3u8_path, track_id, destination_path)
                write_log.debug("IMPORT_M3U8_UPDATED", "Updated M3U8 file.", 
                              {"m3u8_path": m3u8_path, "track_id": track_id})
        
        return True, f"Successfully imported: {artist} - {track_name}"
    
    except Exception as e:
        error_msg = f"Failed to import track: {str(e)}"
        write_log.error("IMPORT_TRACK_FAIL", "Failed to import track.", 
                       {"track_id": track_id, "error": str(e)})
        return False, error_msg
