"""
Blacklist tab for the dashboard.

Contains functions for rendering the manual track blacklist interface.
Allows users to search for tracks by artist/track name and blacklist them.
"""

import os
import sqlite3
import time

import pandas as pd
import streamlit as st

from observability.dashboard.config import (
    CACHE_TTL_MEDIUM,
    DB_PATH,
    ENV,
    track_db,
)
from observability.dashboard.helpers import require_database
from scripts.logs_utils import write_log

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
) -> tuple[list[dict], int]:
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
    params: list[str] = []
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

    page_params = [*params, limit, offset]
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

def _revert_track_to_comment_in_m3u8(
    m3u8_path: str, track_id: str, artist: str, track_name: str, local_file_path: str
) -> None:
    """
    Revert a track entry in an M3U8 file back to a comment.

    This is used when blacklisting a track - the file path is replaced with
    a comment so it shows as incomplete in the playlist.

    Based on how update_track_in_m3u8 works: when a track is completed,
    the comment line "# track_id - artist - track_name" is REPLACED with
    the file path line. So we need to find the file path and replace it
    back with the comment.

    Args:
        m3u8_path: Path to the M3U8 file to update
        track_id: Track ID to search for
        artist: Artist name for the comment
        track_name: Track name for the comment
        local_file_path: The file path to search for and replace
    """
    try:
        # Read all lines
        with open(m3u8_path, encoding="utf-8") as f:
            lines = f.readlines()

        # Find and replace the file path line with a comment
        new_lines = []
        track_found = False

        for line in lines:
            # Check if this line is the file path for this track
            # The file path line will be the exact path (with or without trailing newline)
            if line.strip() == local_file_path.strip() and not track_found:
                # Replace the file path with the comment
                new_lines.append(f"# {track_id} - {artist} - {track_name}\n")
                track_found = True
            else:
                # Keep all other lines as-is
                new_lines.append(line)

        # If we didn't find the track path, it might already be a comment
        # Check if comment already exists
        if not track_found:
            comment_line = f"# {track_id} - {artist} - {track_name}\n"
            if comment_line not in new_lines:
                # Add comment if it doesn't exist
                new_lines.append(comment_line)

        # Write back
        with open(m3u8_path, "w", encoding="utf-8") as f:
            f.writelines(new_lines)

        write_log.debug(
            "M3U8_REVERT_SUCCESS",
            "Reverted track to comment in M3U8 file.",
            {"m3u8_path": m3u8_path, "track_id": track_id, "local_file_path": local_file_path}
        )

    except Exception as e:
        write_log.error(
            "M3U8_REVERT_FAIL",
            "Failed to revert track to comment in M3U8 file.",
            {"m3u8_path": m3u8_path, "track_id": track_id, "error": str(e)}
        )


def blacklist_track(track: dict) -> tuple[bool, str]:
    """
    Blacklist a track by:
    1. Adding username + slskd_file_name to blacklist table
    2. Deleting the local file
    3. Setting local_file_path, bitrate, and extension to NULL
    4. Setting status to 'blacklisted'
    5. Reverting M3U8 playlist entries to comments

    Args:
        track: Dictionary with track information containing:
            - track_id: Track identifier
            - track_name: Name of the track
            - artist: Artist name
            - local_file_path: Current file path on disk
            - username: Soulseek username
            - slskd_file_name: Normalized filename from slskd
            - extension: File extension (optional)
            - bitrate: Bitrate in kbps (optional)

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
            # Abort blacklist operation when required Soulseek metadata is missing
            # to avoid re-downloading the same file from the same user without
            # a corresponding blacklist entry.
            return False, (
                "Cannot blacklist track because Soulseek metadata is incomplete "
                "(missing username or file name). The file and track status were "
                "left unchanged. Try again after a new download attempt."
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
                return False, f"Failed to delete file: {e!s}"

        # Capture previous DB state for potential rollback
        previous_state = {
            "local_file_path": local_file_path,
            "bitrate": track.get("bitrate"),
            "extension": track.get("extension"),
            "download_status": "completed",  # Assume completed since track was searchable
        }

        try:
            # Step 3: Update database - set local_file_path, bitrate, extension to NULL
            # Use a short-lived SQLite connection instead of accessing track_db.conn directly
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE tracks
                    SET local_file_path = NULL,
                        bitrate = NULL,
                        extension = NULL
                    WHERE track_id = ?
                    """,
                    (track_id,),
                )
                conn.commit()

            write_log.info(
                "BLACKLIST_DB_CLEARED",
                "Cleared file metadata for blacklisted track.",
                {"track_id": track_id}
            )

            # Step 4: Set status to 'blacklisted'
            # Use another connection to update status
            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE tracks SET download_status = ?, failed_reason = NULL WHERE track_id = ?",
                    ("blacklisted", track_id),
                )
                conn.commit()

            write_log.info(
                "BLACKLIST_STATUS_SET",
                "Set track status to blacklisted.",
                {"track_id": track_id}
            )

            # Step 5: Update M3U8 playlists to revert track back to comment
            # When a track is blacklisted, we need to revert the M3U8 entry back to a comment
            # so it shows as incomplete in the playlist
            playlist_urls = track_db.get_playlists_for_track(track_id)
            for playlist_url in playlist_urls:
                m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
                if m3u8_path and os.path.exists(m3u8_path):
                    try:
                        _revert_track_to_comment_in_m3u8(
                            m3u8_path,
                            track_id,
                            artist,
                            track_name,
                            local_file_path,
                        )
                        write_log.debug(
                            "BLACKLIST_M3U8_UPDATED",
                            "Reverted M3U8 entry back to comment for blacklisted track.",
                            {"m3u8_path": m3u8_path, "track_id": track_id}
                        )
                    except Exception as m3u8_error:
                        write_log.error(
                            "BLACKLIST_M3U8_UPDATE_FAIL",
                            "Failed to update M3U8 for blacklisted track.",
                            {
                                "m3u8_path": m3u8_path,
                                "track_id": track_id,
                                "error": str(m3u8_error),
                            },
                        )
                        # Propagate to trigger rollback of DB changes
                        raise

        except Exception as step_error:
            # Best-effort rollback of DB fields if anything after file deletion fails
            try:
                with sqlite3.connect(DB_PATH) as rollback_conn:
                    rollback_cursor = rollback_conn.cursor()
                    rollback_cursor.execute(
                        """
                        UPDATE tracks
                        SET local_file_path = ?,
                            bitrate = ?,
                            extension = ?,
                            download_status = ?
                        WHERE track_id = ?
                        """,
                        (
                            previous_state.get("local_file_path"),
                            previous_state.get("bitrate"),
                            previous_state.get("extension"),
                            previous_state.get("download_status"),
                            track_id,
                        ),
                    )
                    rollback_conn.commit()
                write_log.warn(
                    "BLACKLIST_ROLLBACK_SUCCESS",
                    "Rolled back DB changes after blacklist failure.",
                    {"track_id": track_id},
                )
            except Exception as rollback_error:
                write_log.error(
                    "BLACKLIST_ROLLBACK_FAIL",
                    "Failed to roll back DB changes after blacklist failure.",
                    {
                        "track_id": track_id,
                        "original_error": str(step_error),
                        "rollback_error": str(rollback_error),
                    },
                )
            # Re-raise so outer handler can report failure
            raise

        return True, f"Successfully blacklisted: {artist} - {track_name}"

    except Exception as e:
        error_msg = f"Failed to blacklist track: {e!s}"
        write_log.error(
            "BLACKLIST_TRACK_FAIL",
            "Failed to blacklist track.",
            {"track_id": track_id, "error": str(e)}
        )
        return False, error_msg


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def _render_track_selection_and_blacklist(rows: list[dict]) -> None:
    """Render track selection dropdown and blacklist action button."""
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
            time.sleep(0.5)
            st.rerun()
        else:
            st.error(message)


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
        st.info(
            "💡 The database will be created when the workflow runs for the first time. "
            "Please run the workflow first."
        )
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

    col_a, col_b, _ = st.columns([1, 1, 2])
    with col_a:
        page_size = st.selectbox(
            "Rows per page",
            options=[10, 25, 50, 100],
            index=1,
            key="blacklist_page_size",
        )
    with col_b:
        page_number = st.number_input("Page", min_value=1, step=1, value=1, key="blacklist_page_num")

    # Fetch page
    offset = (int(page_number) - 1) * int(page_size)
    with st.spinner("Loading tracks..."):
        rows, total = _search_completed_tracks_cached(
            DB_PATH, search, offset, int(page_size), st.session_state["blacklist_nonce"]
        )

    if offset >= max(total, 1):
        offset = 0
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
        st.dataframe(display_df, width="stretch", hide_index=True)

        _render_track_selection_and_blacklist(rows)

    else:
        st.info("No tracks found matching your search. Try a different search term.")

    # Footer
    st.markdown("---")
    st.markdown("💡 **Tip:** Blacklisted tracks will be searched again in the next search cycle.")
    st.markdown("🔄 The workflow will automatically attempt to find a better version of the track.")


def render_blacklist_tab():
    """Render the complete Blacklist tab content."""
    render_blacklist_section()
