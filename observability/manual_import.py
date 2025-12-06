"""
Manual Track Import Tool for Spotiseek.

This Streamlit web application allows users to manually import tracks that have 
failed to download automatically. It provides:
- Track browser grouped by playlist (non-completed tracks only)
- Native file picker for selecting local files
- Automatic metadata extraction (extension, bitrate) using mutagen
- Database and M3U8 file updates
- iTunes XML library re-export

Usage:
    streamlit run observability/manual_import.py
"""

import os
import sys
import shutil
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv
from mutagen import File as MutagenFile

# Disable .pyc file generation
sys.dont_write_bytecode = True

# Load environment configuration
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

from scripts.logs_utils import setup_logging, write_log
from scripts.database_management import TrackDB
from scripts.m3u8_manager import update_track_in_m3u8
from scripts.xml_exporter import export_itunes_xml, extract_file_metadata

# Initialize logging
setup_logging(log_name_prefix="manual_import")

# Get environment configuration
ENV = os.getenv("APP_ENV")
if not ENV:
    st.error("⚠️ APP_ENV environment variable is not set. Please set it to 'test', 'stage', or 'prod'.")
    st.stop()

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "database", ENV, f"database_{ENV}.db")
IMPORTED_DIR = os.path.join(BASE_DIR, "slskd_docker_data", ENV, "imported")
DOWNLOADS_ROOT = os.path.join(BASE_DIR, "slskd_docker_data", ENV, "downloads")
XML_DIR = os.path.join(BASE_DIR, "database", "xml", ENV)
M3U8_DIR = os.path.join(BASE_DIR, "database", "m3u8s", ENV)

# Check if running in Docker
IS_DOCKER = os.path.exists("/.dockerenv")

# Ensure directories exist
os.makedirs(IMPORTED_DIR, exist_ok=True)
os.makedirs(XML_DIR, exist_ok=True)

# Initialize database
track_db = TrackDB(db_path=DB_PATH)


def get_non_completed_tracks_by_playlist() -> Dict[str, List[Tuple]]:
    """
    Retrieve all tracks missing a local_file_path, grouped by playlist.
    
    Returns:
        Dictionary mapping playlist names to lists of track dicts:
        {
            "Playlist Name": [
                {
                    'spotify_id': ..., 'track_name': ..., 'artist': ..., 'status': ..., 'playlist_url': ...
                },
                ...
            ],
            ...
        }
    """
    write_log.info("IMPORT_UI_QUERY", "Querying tracks missing local_file_path grouped by playlist.")
    
    cursor = track_db.conn.cursor()
    
    # Query tracks with their playlist associations, only those missing local_file_path
    query = """
        SELECT 
            p.playlist_name,
            p.playlist_url,
            t.spotify_id,
            t.track_name,
            t.artist,
            t.download_status
        FROM tracks t
        JOIN playlist_tracks pt ON t.spotify_id = pt.spotify_id
        JOIN playlists p ON pt.playlist_url = p.playlist_url
        WHERE t.local_file_path IS NULL OR t.local_file_path = ''
        ORDER BY p.playlist_name, t.track_name
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Group by playlist
    grouped_tracks = {}
    for playlist_name, playlist_url, spotify_id, track_name, artist, status in rows:
        if playlist_name not in grouped_tracks:
            grouped_tracks[playlist_name] = []
        
        grouped_tracks[playlist_name].append({
            'spotify_id': spotify_id,
            'track_name': track_name,
            'artist': artist,
            'status': status,
            'playlist_url': playlist_url
        })
    
    write_log.debug("IMPORT_UI_QUERY_RESULT", "Retrieved tracks missing local_file_path.", 
                   {"playlist_count": len(grouped_tracks)})
    
    return grouped_tracks


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


def import_track(spotify_id: str, uploaded_file, track_info: dict) -> Tuple[bool, str]:
    """
    Import a track file and update the database.
    
    Args:
        spotify_id: Spotify track identifier
        uploaded_file: Streamlit UploadedFile object
        track_info: Dictionary with track metadata
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Generate destination path
        file_extension = Path(uploaded_file.name).suffix
        safe_filename = f"{track_info['artist']}_{track_info['track_name']}{file_extension}".replace(' ', '_')
        # Remove invalid filename characters
        safe_filename = "".join(c for c in safe_filename if c.isalnum() or c in ('_', '.', '-'))
        
        # Use absolute path (normalize for Docker environment)
        destination_path = os.path.abspath(os.path.join(IMPORTED_DIR, safe_filename))
        
        # If in Docker, ensure path starts with /app/
        if IS_DOCKER and not destination_path.startswith('/app/'):
            destination_path = destination_path.replace(os.path.dirname(os.path.dirname(__file__)), '/app')
        
        # Save uploaded file
        with open(destination_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())
        
        write_log.info("IMPORT_FILE_SAVED", "Saved imported file.", 
                      {"spotify_id": spotify_id, "destination": destination_path})
        
        # Extract metadata
        metadata = extract_metadata_from_file(destination_path)
        
        # Update database
        track_db.update_local_file_path(spotify_id, destination_path)
        track_db.update_extension_bitrate(
            spotify_id, 
            extension=metadata['extension'], 
            bitrate=metadata['bitrate']
        )
        track_db.update_track_status(spotify_id, "completed")
        
        write_log.info("IMPORT_DB_UPDATED", "Updated database for imported track.", 
                      {"spotify_id": spotify_id, "extension": metadata['extension'], 
                       "bitrate": metadata['bitrate']})
        
        # Update M3U8 files
        playlist_urls = track_db.get_playlists_for_track(spotify_id)
        for playlist_url in playlist_urls:
            m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
            if m3u8_path:
                update_track_in_m3u8(m3u8_path, spotify_id, destination_path)
                write_log.debug("IMPORT_M3U8_UPDATED", "Updated M3U8 file.", 
                              {"m3u8_path": m3u8_path, "spotify_id": spotify_id})
        
        # Re-export iTunes XML
        xml_path = os.path.join(XML_DIR, "spotiseek_library.xml")
        
        # Calculate music folder URL (handle Docker to host path conversion)
        downloads_path = DOWNLOADS_ROOT
        if IS_DOCKER:
            host_base_path = os.getenv("HOST_BASE_PATH")
            if host_base_path and downloads_path.startswith("/app/"):
                downloads_path = downloads_path.replace("/app/", f"{host_base_path}/", 1)
        
        music_folder_url = f"file://localhost/{downloads_path.replace(os.sep, '/')}/"
        export_itunes_xml(xml_path, music_folder_url)
        
        write_log.info("IMPORT_XML_EXPORTED", "Re-exported iTunes XML.", {"xml_path": xml_path})
        
        return True, f"✅ Successfully imported **{track_info['artist']} - {track_info['track_name']}**"
    
    except Exception as e:
        error_msg = f"❌ Failed to import track: {str(e)}"
        write_log.error("IMPORT_TRACK_FAIL", "Failed to import track.", 
                       {"spotify_id": spotify_id, "error": str(e)})
        return False, error_msg


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Spotiseek Manual Import",
        page_icon="🎵",
        layout="wide"
    )
    
    st.title("🎵 Spotiseek Manual Track Import")
    st.markdown(f"**Environment:** `{ENV}`")
    st.markdown("---")
    
    # Fetch non-completed tracks
    with st.spinner("Loading tracks..."):
        grouped_tracks = get_non_completed_tracks_by_playlist()
    
    if not grouped_tracks:
        st.success("✨ All tracks have been successfully downloaded!")
        st.info("No tracks require manual import.")
        return
    
    # Statistics
    total_tracks = sum(len(tracks) for tracks in grouped_tracks.values())
    st.metric("Total Tracks Needing Import", total_tracks)
    st.markdown("---")
    
    # Playlist selection
    st.subheader("📋 Select Playlist")
    selected_playlist = st.selectbox(
        "Choose a playlist to view its incomplete tracks:",
        options=list(grouped_tracks.keys()),
        format_func=lambda x: f"{x} ({len(grouped_tracks[x])} tracks)"
    )
    
    if selected_playlist:
        st.markdown("---")
        st.subheader(f"🎶 Tracks in: **{selected_playlist}**")
        
        tracks = grouped_tracks[selected_playlist]
        
        # Display tracks with import functionality
        for idx, track in enumerate(tracks):
            with st.expander(
                f"**{track['artist']} - {track['track_name']}**  •  Status: `{track['status']}`",
                expanded=False
            ):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown(f"**Artist:** {track['artist']}")
                    st.markdown(f"**Track:** {track['track_name']}")
                    st.markdown(f"**Status:** `{track['status']}`")
                    st.markdown(f"**Spotify ID:** `{track['spotify_id']}`")
                
                with col2:
                    # File uploader for this track
                    uploaded_file = st.file_uploader(
                        "Select audio file",
                        type=['mp3', 'flac', 'wav', 'm4a', 'ogg', 'wma'],
                        key=f"upload_{track['spotify_id']}",
                        help="Upload the audio file for this track"
                    )
                    
                    if uploaded_file is not None:
                        st.info(f"📁 Selected: `{uploaded_file.name}`")
                        
                        if st.button(
                            "Import Track",
                            key=f"import_{track['spotify_id']}",
                            type="primary"
                        ):
                            with st.spinner("Importing..."):
                                success, message = import_track(
                                    track['spotify_id'],
                                    uploaded_file,
                                    track
                                )
                            
                            if success:
                                st.success(message)
                                st.balloons()
                                # Refresh the page after successful import
                                st.rerun()
                            else:
                                st.error(message)
    
    # Footer
    st.markdown("---")
    st.markdown("💡 **Tip:** Files will be saved to `slskd_docker_data/{ENV}/imported/`")
    st.markdown("🔄 After import, M3U8 playlists and iTunes XML are automatically updated.")


if __name__ == "__main__":
    main()
