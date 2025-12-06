"""
M3U8 playlist file management module.

This module provides utilities for creating and updating M3U8 playlist files
that map Spotify tracks to local file paths. M3U8 files use a comment-based
system to track pending downloads, which are replaced with actual paths once complete.

Key Features:
- Create M3U8 files with track metadata as comments
- Update comments to file paths when downloads complete
- Bulk deletion of M3U8 files for database resets

File Format:
    #EXTM3U
    # spotify_id - artist - track_name
    # spotify_id - artist - track_name
    E:\path\to\downloaded\file.mp3
    # spotify_id - artist - track_name
    ...

Public API:
- write_playlist_m3u8(): Create new M3U8 file with track list
- update_track_in_m3u8(): Replace comment with file path
- delete_all_m3u8_files(): Remove all M3U8 files in directory
"""

import os
import glob
from typing import List, Tuple

from scripts.logs_utils import write_log


def write_playlist_m3u8(m3u8_path: str, tracks: List[Tuple[str, str, str]]) -> None:
    """
    Create a new M3U8 playlist file with commented track entries.
    
    Each track is written as a comment line in the format:
    # spotify_id - artist - track_name
    
    These comments are later replaced with actual file paths as downloads complete.
    
    Args:
        m3u8_path: Absolute path where the M3U8 file should be created
        tracks: List of tuples (spotify_id, artist, track_name)
    
    Example:
        >>> tracks = [("abc123", "Artist Name", "Track Title")]
        >>> write_playlist_m3u8("playlists/my_playlist.m3u8", tracks)
    """
    write_log.info("M3U8_WRITE", "Writing M3U8 playlist file.", 
                  {"m3u8_path": m3u8_path, "track_count": len(tracks)})
    
    try:
        with open(m3u8_path, 'w', encoding='utf-8') as m3u8_file:
            # Write M3U8 header
            m3u8_file.write('#EXTM3U\n')
            
            # Write each track as a comment
            for spotify_id, artist, track_name in tracks:
                comment = f"# {spotify_id} - {artist} - {track_name}\n"
                m3u8_file.write(comment)
        
        write_log.debug("M3U8_WRITE_SUCCESS", "M3U8 file written successfully.", {"m3u8_path": m3u8_path})
    except Exception as e:
        write_log.error("M3U8_WRITE_FAIL", "Failed to write M3U8 file.", 
                       {"m3u8_path": m3u8_path, "error": str(e)})
        raise


def update_track_in_m3u8(m3u8_path: str, spotify_id: str, local_file_path: str) -> None:
    """
    Replace a track comment with the actual file path in an M3U8 file.
    
    Searches for the comment line matching the spotify_id and replaces it with
    the local file path. Only replaces the first matching comment.
    
    Args:
        m3u8_path: Path to the M3U8 file to update
        spotify_id: Spotify track ID to search for
        local_file_path: Absolute path to the downloaded file
    
    Note:
        If the M3U8 file doesn't exist or the track isn't found, the operation
        is skipped silently. Only the first matching comment is replaced.
    
    Example:
        >>> update_track_in_m3u8("playlists/my_playlist.m3u8", "abc123", "E:\\downloads\\track.mp3")
    """
    if not os.path.exists(m3u8_path):
        write_log.warn("M3U8_NOT_FOUND", "M3U8 file not found for update.", {"m3u8_path": m3u8_path})
        return
    
    write_log.debug("M3U8_UPDATE", "Updating track in M3U8 file.", 
                   {"m3u8_path": m3u8_path, "spotify_id": spotify_id})
    
    try:
        # Read all lines
        with open(m3u8_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Find and replace the matching comment
        comment_prefix = f"# {spotify_id} - "
        new_lines = []
        replaced = False
        
        for line in lines:
            if line.startswith(comment_prefix) and not replaced:
                new_lines.append(local_file_path + '\n')
                replaced = True
            else:
                new_lines.append(line)
        
        # Write back if replacement occurred
        if replaced:
            with open(m3u8_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            write_log.debug("M3U8_UPDATE_SUCCESS", "Track updated in M3U8 file.", 
                           {"m3u8_path": m3u8_path, "spotify_id": spotify_id})
        else:
            write_log.debug("M3U8_TRACK_NOT_FOUND", "Track comment not found in M3U8 file.", 
                           {"m3u8_path": m3u8_path, "spotify_id": spotify_id})
    
    except Exception as e:
        write_log.error("M3U8_UPDATE_FAIL", "Failed to update M3U8 file.", 
                       {"m3u8_path": m3u8_path, "error": str(e)})


def delete_all_m3u8_files(m3u8_dir: str) -> None:
    """
    Recursively delete all M3U8 files in a directory tree.
    
    Useful for database resets where playlist files need to be regenerated.
    Individual file deletion failures are logged but don't stop the process.
    
    Args:
        m3u8_dir: Root directory to search for M3U8 files
    
    Example:
        >>> delete_all_m3u8_files("database/m3u8s/test")
    """
    write_log.info("M3U8_DELETE_ALL", "Deleting all M3U8 files in directory.", {"m3u8_dir": m3u8_dir})
    
    pattern = os.path.join(m3u8_dir, '**', '*.m3u8')
    files = glob.glob(pattern, recursive=True)
    
    deleted_count = 0
    failed_count = 0
    
    for file_path in files:
        try:
            os.remove(file_path)
            deleted_count += 1
            write_log.debug("M3U8_FILE_DELETED", "Deleted M3U8 file.", {"file_path": file_path})
        except Exception as e:
            failed_count += 1
            write_log.warn("M3U8_DELETE_FAIL", "Failed to delete M3U8 file.", 
                          {"file_path": file_path, "error": str(e)})
    
    write_log.info("M3U8_DELETE_COMPLETE", "M3U8 deletion complete.", 
                  {"deleted": deleted_count, "failed": failed_count, "total": len(files)})