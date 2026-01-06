"""
Blacklist tab for the dashboard.

Contains functions for rendering the manual track blacklist interface.
Allows users to search for tracks by artist/track name and blacklist them.
"""

import os
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st

from observability.dashboard.config import (
    ENV,
    DB_PATH,
    track_db,
    CACHE_TTL_MEDIUM,
)
from observability.dashboard.helpers import require_database
from scripts.logs_utils import write_log
from scripts.m3u8_manager import update_track_in_m3u8


# ============================================================================
# CACHED DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_MEDIUM)
def _search_completed_tracks_cached(
    db_path: str,
    search: str,
    offset: int,
    limit: int,
    cache_nonce: int,
) -> Tuple[List[dict], int]:
    """
    Search for completed tracks (those with local_file_path) by artist or track name.

    Returns: (rows, total_count)
    Each row is a dict with keys: track_id, track_name, artist, local_file_path, 
                                  extension, bitrate, username, slskd_file_name
    cache_nonce is used to bust cache after blacklist operations.
    """
    _ = cache_nonce  # used only to vary cache key
    if not os.path.exists(db_path):
        return [], 0
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    where_search = ""
    params: List[str] = []
    if search:
        where_search = " AND (LOWER(t.track_name) LIKE ? OR LOWER(t.artist) LIKE ?)"
        like = f"%{search.lower()}%"
        params.extend([like, like])

    # Total count first
    count_sql = (
        """
        SELECT COUNT(*)
        FROM tracks t
        WHERE t.local_file_path IS NOT NULL 
          AND TRIM(t.local_file_path) != ''
        """ + where_search
    )
    cursor.execute(count_sql, params)
    row = cursor.fetchone()
    total = row[0] if row is not None else 0

    # Data page
    data_sql = (
        """
        SELECT 
            t.track_id,
            t.track_name,
            t.artist,
            t.local_file_path,
            t.extension,
            t.bitrate,
            t.username,
            t.slskd_file_name
        FROM tracks t
        WHERE t.local_file_path IS NOT NULL 
          AND TRIM(t.local_file_path) != ''
        """ + where_search + " ORDER BY t.artist, t.track_name LIMIT ? OFFSET ?"
    )

    page_params = params + [limit, offset]
    cursor.execute(data_sql, page_params)
    rows = cursor.fetchall()
    conn.close()

    result = [
        {
            "track_id": r[0],
            "track_name": r[1],
            "artist": r[2],
            "local_file_path": r[3],
            "extension": r[4],
            "bitrate": r[5],
            "username": r[6],
            "slskd_file_name": r[7],
        }
        for r in rows
    ]
    return result, int(total)


# ============================================================================
# BLACKLIST FUNCTIONS
# ============================================================================

def blacklist_track(track: dict) -> Tuple[bool, str]:
    """
    Blacklist a track by:
    1. Adding username + slskd_file_name to blacklist table
    2. Deleting the local file
    3. Setting local_file_path, bitrate, and extension to NULL
    4. Setting status to 'blacklisted'
    
    Args:
        track: Dictionary with track information
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    track_id = track['track_id']
    artist = track['artist']
    track_name = track['track_name']
    local_file_path = track['local_file_path']
    username = track['username']
    slskd_file_name = track['slskd_file_name']
    
    try:
        # Step 1: Add to blacklist if we have username and slskd_file_name
        if username and slskd_file_name:
            track_db.add_slskd_blacklist(
                username=username,
                slskd_file_name=slskd_file_name,
                reason="manual_blacklist"
            )
            write_log.info(
                "BLACKLIST_ADDED",
                "Added track to blacklist via dashboard.",
                {
                    "track_id": track_id,
                    "username": username,
                    "slskd_file_name": slskd_file_name,
                }
            )
        else:
            write_log.warn(
                "BLACKLIST_MISSING_INFO",
                "Cannot blacklist track - missing username or slskd_file_name.",
                {
                    "track_id": track_id,
                    "username": username,
                    "slskd_file_name": slskd_file_name,
                }
            )
        
        # Step 2: Delete the local file if it exists
        if local_file_path and os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                write_log.info(
                    "BLACKLIST_FILE_DELETED",
                    "Deleted blacklisted track file.",
                    {"track_id": track_id, "file_path": local_file_path}
                )
            except Exception as e:
                write_log.error(
                    "BLACKLIST_FILE_DELETE_FAIL",
                    "Failed to delete blacklisted track file.",
                    {"track_id": track_id, "file_path": local_file_path, "error": str(e)}
                )
                return False, f"Failed to delete file: {str(e)}"
        
        # Step 3: Update database - set local_file_path, bitrate, extension to NULL
        cursor = track_db.conn.cursor()
        cursor.execute(
            """
            UPDATE tracks 
            SET local_file_path = NULL, 
                bitrate = NULL, 
                extension = NULL
            WHERE track_id = ?
            """,
            (track_id,)
        )
        track_db.conn.commit()
        
        write_log.info(
            "BLACKLIST_DB_CLEARED",
            "Cleared file metadata for blacklisted track.",
            {"track_id": track_id}
        )
        
        # Step 4: Set status to 'blacklisted'
        track_db.update_track_status(track_id, "blacklisted")
        write_log.info(
            "BLACKLIST_STATUS_SET",
            "Set track status to blacklisted.",
            {"track_id": track_id}
        )
        
        # Step 5: Update M3U8 playlists to remove the track reference
        playlist_urls = track_db.get_playlists_for_track(track_id)
        for playlist_url in playlist_urls:
            m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
            if m3u8_path:
                # Update M3U8 to remove the path (update_track_in_m3u8 with None path)
                update_track_in_m3u8(m3u8_path, track_id, None)
                write_log.debug(
                    "BLACKLIST_M3U8_UPDATED",
                    "Updated M3U8 file to remove blacklisted track.",
                    {"m3u8_path": m3u8_path, "track_id": track_id}
                )
        
        return True, f"Successfully blacklisted: {artist} - {track_name}"
    
    except Exception as e:
        error_msg = f"Failed to blacklist track: {str(e)}"
        write_log.error(
            "BLACKLIST_TRACK_FAIL",
            "Failed to blacklist track.",
            {"track_id": track_id, "error": str(e)}
        )
        return False, error_msg


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def render_blacklist_section():
    """Render the complete blacklist interface with search and track selection."""
    st.markdown(f"**Environment:** `{ENV}`")
    
    st.markdown("""
    ### 🚫 Manual Track Blacklist
    
    Use this tab to blacklist tracks that are:
    - Wrong version (e.g., remix instead of original)
    - Corrupted or have audio issues
    - Otherwise unsuitable
    
    **What happens when you blacklist a track:**
    1. The username + file combination is added to the blacklist
    2. The local file is deleted from disk
    3. Track status is set to `blacklisted` for re-searching
    4. The track will be searched again in future search runs
    """)
    
    # Check if database exists
    if not require_database(
        error_msg=f"❌ Database file not found: `{DB_PATH}`"
    ):
        st.info("💡 The database will be created when the workflow runs for the first time. Please run the workflow first.")
        return
    
    # Lightweight cache-busting nonce for blacklist-only queries
    if "blacklist_nonce" not in st.session_state:
        st.session_state["blacklist_nonce"] = 0
    
    st.markdown("---")
    st.subheader("🔍 Search for Tracks")
    
    # Search + paging controls
    search = st.text_input(
        "Search by artist or track name:",
        value="",
        help="Search for tracks that have been successfully downloaded"
    )
    
    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        page_size = st.selectbox("Rows per page", options=[10, 25, 50, 100], index=1)
    with col_b:
        page_number = st.number_input("Page", min_value=1, step=1, value=1)
    
    # Fetch page
    offset = (int(page_number) - 1) * int(page_size)
    with st.spinner("Loading tracks..."):
        rows, total = _search_completed_tracks_cached(
            DB_PATH, search, offset, int(page_size), st.session_state["blacklist_nonce"]
        )
    
    if offset >= max(total, 1):
        offset = 0
        page_number = 1
        rows, total = _search_completed_tracks_cached(
            DB_PATH, search, offset, int(page_size), st.session_state["blacklist_nonce"]
        )
    
    st.markdown(f"**Found {total} track(s) matching your search**")
    
    # Table view
    if rows:
        # Prepare dataframe for display
        display_df = pd.DataFrame(rows)[["artist", "track_name", "extension", "bitrate", "track_id"]]
        display_df = display_df.rename(columns={
            "artist": "Artist",
            "track_name": "Track Name",
            "extension": "Format",
            "bitrate": "Bitrate (kbps)",
            "track_id": "Track ID"
        })
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        st.subheader("⚠️ Blacklist Selected Track")
        
        # Track selection
        labels_by_id = {
            r["track_id"]: f"{r['artist']} - {r['track_name']} • {r['extension']} {r['bitrate']}kbps [{r['track_id']}]"
            for r in rows
        }
        tracks_by_id = {r["track_id"]: r for r in rows}
        
        selected_track_id = st.selectbox(
            "Select a track to blacklist:",
            options=list(labels_by_id.keys()),
            format_func=lambda track_id: labels_by_id.get(track_id, str(track_id)),
        )
        
        track = tracks_by_id[selected_track_id]
        
        # Show track details
        st.info(f"**Selected:** {track['artist']} - {track['track_name']}")
        st.text(f"File: {track['local_file_path']}")
        st.text(f"Format: {track['extension']} @ {track['bitrate']} kbps")
        
        # Warning and confirmation
        st.warning("""
        **⚠️ Warning:** This action will:
        - Delete the file from disk permanently
        - Add this file to the blacklist
        - Mark the track for re-download
        
        This action cannot be undone!
        """)
        
        # Blacklist button
        if st.button("🚫 Blacklist This Track", key=f"blacklist_{selected_track_id}", type="primary"):
            with st.spinner("Blacklisting track..."):
                success, message = blacklist_track(track)
            
            if success:
                st.success(message)
                # Bust cache to reflect the change
                st.session_state["blacklist_nonce"] += 1
                # Small delay before rerun
                import time
                time.sleep(0.5)
                st.rerun()
            else:
                st.error(message)
    
    else:
        st.info("No tracks found matching your search. Try a different search term.")
    
    # Footer
    st.markdown("---")
    st.markdown("💡 **Tip:** Blacklisted tracks will be searched again in the next search cycle.")
    st.markdown("🔄 The workflow will automatically attempt to find a better version of the track.")


def render_blacklist_tab():
    """Render the complete Blacklist tab content."""
    render_blacklist_section()
