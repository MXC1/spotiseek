"""
iTunes-compatible XML library export module.

This module generates iTunes Music Library.xml format files from the Spotiseek
database, enabling integration with MusicBee and other players that support
the iTunes library format.

Key Features:
- iTunes Music Library.xml format compatibility
- Path conversion for Docker/host environments
- URL encoding for file paths
- Playlist and track metadata export
- Automatic metadata extraction from audio files (using mutagen)
- Rich track information including dates, bitrate, sample rate, album, genre, etc.

Extracted Metadata:
- Date Modified: File modification timestamp
- Date Added: File creation/add timestamp
- Bit Rate: Audio bitrate in kbps
- Sample Rate: Audio sample rate in Hz
- Album: Album name from ID3/Vorbis tags
- Genre: Genre from ID3/Vorbis tags
- Year: Release year from tags
- File Size: File size in bytes
- Total Time: Duration in milliseconds

Public API:
- export_itunes_xml(): Main export function
- extract_file_metadata(): Extract metadata from an audio file
"""

import os
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any
from urllib.parse import quote

from mutagen import File as MutagenFile
from mutagen.flac import FLAC
from mutagen.mp3 import MP3

from scripts.database_management import TrackDB
from scripts.logs_utils import write_log


def convert_to_windows_path(container_path: str) -> str:
    """
    Convert a Docker container path to a Windows host path.

    If HOST_BASE_PATH environment variable is set, replaces /app/ prefix
    with the Windows host path. Otherwise, returns the path unchanged.

    Args:
        container_path: File path as stored in database (may be container path)

    Returns:
        Windows host path suitable for file:// URLs

    Example:
        >>> os.environ['HOST_BASE_PATH'] = 'E:/Projects/spotiseek'
        >>> convert_to_windows_path('/app/downloads/file.mp3')
        'E:/Projects/spotiseek/downloads/file.mp3'
    """
    host_base_path = os.getenv("HOST_BASE_PATH")

    # Convert Docker container paths to host paths
    if host_base_path and container_path.startswith("/app/"):
        container_path = container_path.replace("/app/", f"{host_base_path}/", 1)

    return container_path


def format_file_location_url(local_file_path: str) -> str:
    """
    Format a local file path as a file:// URL with proper encoding.

    Converts backslashes to forward slashes, URL-encodes each path component,
    and prefixes with file://localhost/.

    Args:
        local_file_path: Absolute path to local file

    Returns:
        Properly encoded file:// URL

    Example:
        >>> format_file_location_url('E:\\Music\\Artist\\Track Name.mp3')
        'file://localhost/E:/Music/Artist/Track%20Name.mp3'
    """
    # Convert to Windows host path if running in Docker
    windows_path = convert_to_windows_path(local_file_path)

    # Normalize path separators to forward slashes
    normalized_path = windows_path.replace("\\", "/")

    # Split into components and URL-encode each one
    path_parts = normalized_path.split("/")
    encoded_parts = [quote(part, safe='') for part in path_parts if part]
    encoded_path = "/".join(encoded_parts)

    return f'file://localhost/{encoded_path}'


def extract_file_metadata(local_file_path: str) -> dict[str, Any]:  # noqa: PLR0912, PLR0915
    """
    Extract metadata from an audio file using mutagen.

    Args:
        local_file_path: Absolute path to the audio file

    Returns:
        Dictionary containing file metadata:
        - file_size: File size in bytes
        - date_modified: ISO 8601 formatted modification date
        - date_added: ISO 8601 formatted creation/add date
        - bitrate: Bitrate in kbps (integer)
        - sample_rate: Sample rate in Hz (integer)
        - duration_ms: Duration in milliseconds (integer)
        - album: Album name (string)
        - genre: Genre (string)
        - year: Year (integer)
    """
    metadata = {
        'file_size': None,
        'date_modified': None,
        'date_added': None,
        'bitrate': None,
        'sample_rate': None,
        'duration_ms': None,
        'album': None,
        'genre': None,
        'year': None
    }

    try:
        # Use the path as-is for file access (works inside Docker container)
        # Don't convert to Windows path here - that's only for XML URLs
        file_path = local_file_path

        # Check if file exists
        if not os.path.exists(file_path):
            write_log.warn("FILE_NOT_FOUND", "File not found for metadata extraction.",
                          {"file_path": file_path})
            return metadata

        # Get file system metadata
        stat_info = os.stat(file_path)
        metadata['file_size'] = stat_info.st_size
        metadata['date_modified'] = datetime.fromtimestamp(stat_info.st_mtime).isoformat() + 'Z'
        metadata['date_added'] = datetime.fromtimestamp(stat_info.st_ctime).isoformat() + 'Z'

        # Get audio metadata using mutagen
        audio = MutagenFile(file_path, easy=False)

        if audio is None:
            write_log.warn("MUTAGEN_PARSE_FAIL", "Failed to parse audio file.",
                          {"file_path": file_path})
            return metadata

        # Extract bitrate (convert to kbps)
        if hasattr(audio.info, 'bitrate') and audio.info.bitrate:
            metadata['bitrate'] = int(audio.info.bitrate / 1000)

        # Extract sample rate
        if hasattr(audio.info, 'sample_rate') and audio.info.sample_rate:
            metadata['sample_rate'] = int(audio.info.sample_rate)

        # Extract duration (convert to milliseconds)
        if hasattr(audio.info, 'length') and audio.info.length:
            metadata['duration_ms'] = int(audio.info.length * 1000)

        # Extract tags based on file format
        if isinstance(audio, MP3):
            # MP3 ID3 tags
            if audio.tags:
                # Album
                    if 'TALB' in audio.tags and hasattr(audio.tags['TALB'], 'text') and audio.tags['TALB'].text:
                        metadata['album'] = str(audio.tags['TALB'].text[0])
                # Genre
                    if 'TCON' in audio.tags and hasattr(audio.tags['TCON'], 'text') and audio.tags['TCON'].text:
                        metadata['genre'] = str(audio.tags['TCON'].text[0])
                # Year
                    if 'TDRC' in audio.tags and hasattr(audio.tags['TDRC'], 'text') and audio.tags['TDRC'].text:
                        year_str = str(audio.tags['TDRC'].text[0])[:4]
                        if year_str and year_str.isdigit():
                            metadata['year'] = int(year_str)
                    elif 'TYER' in audio.tags and hasattr(audio.tags['TYER'], 'text') and audio.tags['TYER'].text:
                        year_str = str(audio.tags['TYER'].text[0])
                        if year_str and year_str.isdigit():
                            metadata['year'] = int(year_str)

        elif isinstance(audio, FLAC):
            # FLAC Vorbis comments
            if audio.tags:
                    if audio.tags.get('album'):
                        metadata['album'] = audio.tags['album'][0]
                    if audio.tags.get('genre'):
                        metadata['genre'] = audio.tags['genre'][0]
                    if audio.tags.get('date'):
                        date_str = audio.tags['date'][0]
                        year_str = date_str[:4]
                        if year_str and year_str.isdigit():
                            metadata['year'] = int(year_str)

        # Generic tag handling for other formats
        elif hasattr(audio, 'tags') and audio.tags:
            # Try common tag keys
            for album_key in ['album', 'ALBUM', 'Album']:
                    if audio.tags.get(album_key):
                        tag_val = audio.tags[album_key]
                        metadata['album'] = str(tag_val[0]) if isinstance(tag_val, list) else str(tag_val)
                        break

            for genre_key in ['genre', 'GENRE', 'Genre']:
                    if audio.tags.get(genre_key):
                        tag_val = audio.tags[genre_key]
                        metadata['genre'] = str(tag_val[0]) if isinstance(tag_val, list) else str(tag_val)
                        break
            for year_key in ['date', 'DATE', 'year', 'YEAR']:
                    if audio.tags.get(year_key):
                        tag_val = audio.tags[year_key]
                        year_val = str(tag_val[0]) if isinstance(tag_val, list) else str(tag_val)
                        year_str = year_val[:4]
                        if year_str and year_str.isdigit():
                            metadata['year'] = int(year_str)
                        break

        write_log.debug("METADATA_EXTRACTED", "Successfully extracted file metadata.",
                       {"file_path": file_path, "metadata": metadata})

    except Exception as e:
        write_log.warn("METADATA_EXTRACT_FAIL", "Failed to extract file metadata.",
                       {"file_path": local_file_path, "error": str(e)})

    return metadata


def export_itunes_xml(xml_path: str, music_folder_url: str | None = None) -> None:
    """
    Export all playlists and tracks from database to iTunes Music Library.xml format.

    This function generates an XML file compatible with iTunes and MusicBee,
    containing all completed tracks and their playlist associations.

    Args:
        xml_path: Output path for the XML file
        music_folder_url: Optional base URL for <Music Folder> key. If None,
                         derives from downloads directory.

    Raises:
        Exception: If database queries or file writing fails

    Example:
        >>> export_itunes_xml(
        ...     'database/test/library.xml',
        ...     'file://localhost/E:/Downloads/'
        ... )
    """
    write_log.info("XML_EXPORT_START", "Starting iTunes XML export.", {"xml_path": xml_path})

    db = TrackDB()
    conn = db.conn
    cursor = conn.cursor()

    # Fetch all tracks from database
    cursor.execute("""
        SELECT spotify_id, track_name, artist, download_status,
               slskd_file_name, local_file_path, added_at
        FROM tracks
    """)
    tracks = cursor.fetchall()
    write_log.debug("XML_TRACKS_FETCHED", "Fetched tracks from database.", {"count": len(tracks)})

    # Fetch all playlists
    cursor.execute("SELECT playlist_url, playlist_name FROM playlists")
    playlists = cursor.fetchall()
    write_log.debug("XML_PLAYLISTS_FETCHED", "Fetched playlists from database.", {"count": len(playlists)})

    # Fetch playlist-track associations
    cursor.execute("SELECT playlist_url, spotify_id FROM playlist_tracks")
    playlist_tracks_raw = cursor.fetchall()
    playlist_tracks = {}
    for playlist_url, spotify_id in playlist_tracks_raw:
        playlist_tracks.setdefault(playlist_url, []).append(spotify_id)
    write_log.debug("XML_ASSOCIATIONS_FETCHED", "Fetched playlist-track associations.",
                   {"count": len(playlist_tracks_raw)})

    # Build XML structure
    plist = ET.Element('plist', version="1.0")
    dict_root = ET.SubElement(plist, 'dict')

    # Add top-level metadata
    _add_xml_key_value(dict_root, 'Major Version', '1', 'integer')
    _add_xml_key_value(dict_root, 'Minor Version', '1', 'integer')
    _add_xml_key_value(dict_root, 'Application Version', '3.5.8698.34385', 'string')
    _add_xml_key_value(dict_root, 'Music Folder', music_folder_url or "", 'string')
    _add_xml_key_value(dict_root, 'Library Persistent ID', 'SPOTISEEKLIB0000001', 'string')

    # Build tracks dictionary
    ET.SubElement(dict_root, 'key').text = 'Tracks'
    tracks_dict = ET.SubElement(dict_root, 'dict')

    # Map spotify_id to track integer ID (only for downloaded tracks)
    spotify_id_to_track_id = {}
    downloaded_tracks = [t for t in tracks if t[5]]  # Filter by local_file_path
    write_log.info("XML_DOWNLOADED_TRACKS", "Filtered downloaded tracks.",
                   {"total_tracks": len(tracks), "downloaded_tracks": len(downloaded_tracks)})

    for idx, (spotify_id, track_name, artist, _, _, local_file_path, _) in enumerate(downloaded_tracks, 1):
        try:
            _add_track_to_xml(tracks_dict, idx, track_name, artist, spotify_id, local_file_path)
            spotify_id_to_track_id[spotify_id] = idx
        except Exception as e:
            write_log.error("XML_TRACK_ADD_FAIL", "Failed to add track to XML.",
                           {"track_id": idx, "spotify_id": spotify_id, "error": str(e)})

    # Build playlists array
    ET.SubElement(dict_root, 'key').text = 'Playlists'
    playlists_array = ET.SubElement(dict_root, 'array')

    for playlist_idx, (playlist_url, playlist_name) in enumerate(playlists, 1):
        _add_playlist_to_xml(
            playlists_array,
            playlist_idx,
            playlist_name or playlist_url,
            playlist_tracks.get(playlist_url, []),
            spotify_id_to_track_id
        )

    # Write XML to file with proper formatting
    tree = ET.ElementTree(plist)
    ET.indent(tree, space="\t", level=0)

    # Generate XML string
    import io  # noqa: PLC0415
    xml_io = io.BytesIO()
    tree.write(xml_io, encoding="utf-8", xml_declaration=False)
    xml_content = xml_io.getvalue().decode("utf-8")

    # Write with proper DOCTYPE and header
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN" ')
        f.write('"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n')
        f.write(xml_content.lstrip())

    write_log.info("XML_EXPORT_SUCCESS", "Exported iTunes XML successfully.", {"xml_path": xml_path})


# Helper functions for XML construction

def _add_xml_key_value(parent: ET.Element, key: str, value: str, value_type: str) -> None:
    """Add a key-value pair to XML dict element."""
    ET.SubElement(parent, 'key').text = key
    ET.SubElement(parent, value_type).text = value


def _add_track_to_xml(  # noqa: PLR0913
    tracks_dict: ET.Element, track_id: int, track_name: str,
    artist: str, spotify_id: str, local_file_path: str
) -> None:
    """Add a track entry to the tracks dictionary with file metadata."""
    track_key = ET.SubElement(tracks_dict, 'key')
    track_key.text = str(track_id)
    track_dict = ET.SubElement(tracks_dict, 'dict')

    # Extract metadata from the actual file
    file_metadata = extract_file_metadata(local_file_path)

    # Basic track information
    _add_xml_key_value(track_dict, 'Track ID', str(track_id), 'integer')
    _add_xml_key_value(track_dict, 'Name', track_name or '', 'string')
    _add_xml_key_value(track_dict, 'Artist', artist or '', 'string')

    # Add album if available
    if file_metadata.get('album'):
        _add_xml_key_value(track_dict, 'Album', file_metadata['album'], 'string')

    # Add year if available
    if file_metadata.get('year'):
        _add_xml_key_value(track_dict, 'Year', str(file_metadata['year']), 'integer')

    # Add genre if available
    if file_metadata.get('genre'):
        _add_xml_key_value(track_dict, 'Genre', file_metadata['genre'], 'string')

    # File type and format
    _add_xml_key_value(track_dict, 'Kind', 'MPEG audio file', 'string')

    # Add file size if available
    if file_metadata.get('file_size'):
        _add_xml_key_value(track_dict, 'Size', str(file_metadata['file_size']), 'integer')

    # Add duration if available
    if file_metadata.get('duration_ms'):
        _add_xml_key_value(track_dict, 'Total Time', str(file_metadata['duration_ms']), 'integer')

    # Add dates
    if file_metadata.get('date_modified'):
        ET.SubElement(track_dict, 'key').text = 'Date Modified'
        ET.SubElement(track_dict, 'date').text = file_metadata['date_modified']

    if file_metadata.get('date_added'):
        ET.SubElement(track_dict, 'key').text = 'Date Added'
        ET.SubElement(track_dict, 'date').text = file_metadata['date_added']

    # Add bitrate if available
    if file_metadata.get('bitrate'):
        _add_xml_key_value(track_dict, 'Bit Rate', str(file_metadata['bitrate']), 'integer')

    # Add sample rate if available
    if file_metadata.get('sample_rate'):
        _add_xml_key_value(track_dict, 'Sample Rate', str(file_metadata['sample_rate']), 'integer')

    # Track identification
    _add_xml_key_value(track_dict, 'Persistent ID', spotify_id or '', 'string')
    _add_xml_key_value(track_dict, 'Track Type', 'File', 'string')
    _add_xml_key_value(track_dict, 'Location', format_file_location_url(local_file_path), 'string')


def _add_playlist_to_xml(playlists_array: ET.Element, playlist_id: int,
                        playlist_name: str, spotify_ids: list,
                        spotify_id_to_track_id: dict) -> None:
    """Add a playlist entry to the playlists array."""
    playlist_dict = ET.SubElement(playlists_array, 'dict')

    _add_xml_key_value(playlist_dict, 'Playlist ID', str(playlist_id), 'integer')

    # Generate persistent ID
    persistent_id = f"PL{playlist_id:014X}"
    _add_xml_key_value(playlist_dict, 'Playlist Persistent ID', persistent_id, 'string')

    _add_xml_key_value(playlist_dict, 'All Items', '', 'true')
    _add_xml_key_value(playlist_dict, 'Name', playlist_name.replace(' ', '_'), 'string')

    # Add playlist items
    ET.SubElement(playlist_dict, 'key').text = 'Playlist Items'
    items_array = ET.SubElement(playlist_dict, 'array')

    for spotify_id in spotify_ids:
        if spotify_id in spotify_id_to_track_id:
            item_dict = ET.SubElement(items_array, 'dict')
            _add_xml_key_value(item_dict, 'Track ID', str(spotify_id_to_track_id[spotify_id]), 'integer')
