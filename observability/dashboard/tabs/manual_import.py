"""
Manual Import tab for the dashboard.

Contains functions for rendering the manual track import interface.
"""

import os
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from observability.dashboard.config import (
    ENV,
    DB_PATH,
    DOWNLOADS_ROOT,
    XML_DIR,
    IS_DOCKER,
    track_db,
    CACHE_TTL_MEDIUM,
)
from observability.dashboard.helpers import (
    require_database,
    is_quality_worse_than_mp3_320,
    extract_metadata_from_file,
    do_track_import,
)
from scripts.logs_utils import write_log
from scripts.xml_exporter import export_itunes_xml


# ============================================================================
# CACHED DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_MEDIUM)
def _get_non_completed_tracks_cached(db_path: str) -> Dict[str, List[dict]]:
    """
    Cached helper to retrieve all tracks missing a local_file_path, grouped by playlist.
    
    Returns:
        Dictionary mapping playlist names to lists of track dicts
    """
    write_log.info("IMPORT_UI_QUERY", "Querying tracks missing local_file_path grouped by playlist.")
    
    if not os.path.exists(db_path):
        write_log.warn("IMPORT_UI_DB_NOT_FOUND", "Database file does not exist.", {"db_path": db_path})
        return {}
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Query tracks with their playlist associations, only those missing local_file_path
    query = """
        SELECT 
            p.playlist_name,
            p.playlist_url,
            t.track_id,
            t.track_name,
            t.artist,
            t.download_status
        FROM tracks t
        JOIN playlist_tracks pt ON t.track_id = pt.track_id
        JOIN playlists p ON pt.playlist_url = p.playlist_url
        WHERE t.local_file_path IS NULL OR t.local_file_path = ''
        ORDER BY p.playlist_name, t.track_name
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Group by playlist
    grouped_tracks = {}
    for playlist_name, playlist_url, track_id, track_name, artist, status in rows:
        if playlist_name not in grouped_tracks:
            grouped_tracks[playlist_name] = []
        
        grouped_tracks[playlist_name].append({
            'track_id': track_id,
            'track_name': track_name,
            'artist': artist,
            'status': status,
            'playlist_url': playlist_url
        })
    
    write_log.debug("IMPORT_UI_QUERY_RESULT", "Retrieved tracks missing local_file_path.", 
                   {"playlist_count": len(grouped_tracks)})
    
    conn.close()
    return grouped_tracks


@st.cache_data(ttl=CACHE_TTL_MEDIUM)
def _get_playlists_with_incomplete_counts_cached(db_path: str) -> pd.DataFrame:
    """
    Return a DataFrame of playlists with counts of tracks missing local files.

    Columns: playlist_name, playlist_url, incomplete_count
    """
    if not os.path.exists(db_path):
        return pd.DataFrame(columns=['playlist_name', 'playlist_url', 'incomplete_count'])
    conn = sqlite3.connect(db_path)
    query = """
        SELECT 
            p.playlist_name,
            p.playlist_url,
            COUNT(*) AS incomplete_count
        FROM playlists p
        JOIN playlist_tracks pt ON p.playlist_url = pt.playlist_url
        JOIN tracks t ON t.track_id = pt.track_id
        WHERE t.local_file_path IS NULL OR TRIM(t.local_file_path) = ''
        GROUP BY p.playlist_name, p.playlist_url
        ORDER BY incomplete_count DESC, p.playlist_name
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=CACHE_TTL_MEDIUM)
def _get_incomplete_tracks_for_playlist_cached(
    db_path: str,
    playlist_url: str,
    search: Optional[str],
    offset: int,
    limit: int,
    cache_nonce: int,
) -> Tuple[List[dict], int]:
    """
    Paginated query of tracks missing local_file_path for a given playlist.

    Returns: (rows, total_count)
    Each row is a dict with keys: track_id, track_name, artist, status, playlist_url
    cache_nonce is used to bust cache after imports without clearing global cache.
    """
    _ = cache_nonce  # used only to vary cache key
    if not os.path.exists(db_path):
        return [], 0
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    where_search = ""
    params: List[str] = [playlist_url]
    if search:
        where_search = " AND (LOWER(t.track_name) LIKE ? OR LOWER(t.artist) LIKE ?)"
        like = f"%{search.lower()}%"
        params.extend([like, like])

    # Total count first
    count_sql = (
        """
        SELECT COUNT(*)
        FROM playlist_tracks pt
        JOIN tracks t ON t.track_id = pt.track_id
        WHERE pt.playlist_url = ?
          AND (t.local_file_path IS NULL OR TRIM(t.local_file_path) = '')
        """ + where_search
    )
    cursor.execute(count_sql, params)
    row = cursor.fetchone()
    total = row[0] if row is not None else 0

    # Data page
    data_sql = (
        """
        SELECT 
            pt.playlist_url,
            t.track_id,
            t.track_name,
            t.artist,
            t.download_status
        FROM playlist_tracks pt
        JOIN tracks t ON t.track_id = pt.track_id
        WHERE pt.playlist_url = ?
          AND (t.local_file_path IS NULL OR TRIM(t.local_file_path) = '')
        """ + where_search + " ORDER BY t.track_name LIMIT ? OFFSET ?"
    )

    page_params = params + [limit, offset]
    cursor.execute(data_sql, page_params)
    rows = cursor.fetchall()
    conn.close()

    result = [
        {
            "playlist_url": r[0],
            "track_id": r[1],
            "track_name": r[2],
            "artist": r[3],
            "status": r[4],
        }
        for r in rows
    ]
    return result, int(total)


# ============================================================================
# IMPORT FUNCTIONS
# ============================================================================

def import_track(track_id: str, uploaded_file, track_info: dict) -> Tuple[bool, str]:
    """
    Import a track file and update the database (wrapper for manual import).
    
    Args:
        track_id: Track identifier
        uploaded_file: Streamlit UploadedFile object
        track_info: Dictionary with track metadata
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    success, message = do_track_import(
        track_id=track_id,
        source_path="",  # Not used for uploads
        artist=track_info['artist'],
        track_name=track_info['track_name'],
        is_upload=True,
        uploaded_file=uploaded_file
    )
    # Add markdown formatting for manual import UI
    if success:
        return True, f"✅ **{message}**"
    else:
        return False, f"❌ {message}"


def export_itunes_xml_for_manual_import() -> Tuple[bool, str]:
    """
    Export the iTunes XML library on demand from the manual import tab.
    """
    try:
        xml_path = os.path.join(XML_DIR, f"library_{ENV}.xml")

        downloads_path = DOWNLOADS_ROOT
        if IS_DOCKER:
            host_base_path = os.getenv("HOST_BASE_PATH")
            if host_base_path and downloads_path.startswith("/app/"):
                downloads_path = downloads_path.replace("/app/", f"{host_base_path}/", 1)

        music_folder_url = f"file://localhost/{downloads_path.replace(os.sep, '/')}/"
        export_itunes_xml(xml_path, music_folder_url)

        write_log.info(
            "MANUAL_IMPORT_XML_EXPORTED",
            "Exported iTunes XML from manual import tab.",
            {"xml_path": xml_path, "music_folder_url": music_folder_url},
        )
        return True, f"iTunes XML exported to {xml_path}"
    except Exception as e:
        write_log.error(
            "MANUAL_IMPORT_XML_EXPORT_FAIL",
            "Failed to export iTunes XML from manual import tab.",
            {"error": str(e)},
        )
        return False, f"Failed to export iTunes XML: {e}"


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def render_manual_import_section():
    """Render the complete manual import interface with pagination and single-uploader flow."""
    st.markdown(f"**Environment:** `{ENV}`")

    # Check if database exists
    if not require_database(
        error_msg=f"❌ Database file not found: `{DB_PATH}`"
    ):
        st.info("💡 The database will be created when the workflow runs for the first time. Please run the workflow first.")
        return

    # Lightweight cache-busting nonce for manual-import-only queries
    if "import_nonce" not in st.session_state:
        st.session_state["import_nonce"] = 0

    # Fast overview of playlists with incomplete counts
    playlists_df = _get_playlists_with_incomplete_counts_cached(DB_PATH)

    if playlists_df is None or playlists_df.empty:
        st.success("✨ All tracks have been successfully downloaded!")
        st.info("No tracks require manual import.")
        return

    total_tracks = int(playlists_df["incomplete_count"].sum())
    st.metric("Total Tracks Needing Import", total_tracks)
    st.markdown("---")

    # Playlist selection (store URL as value, show name + count)
    st.subheader("📋 Select Playlist")
    options = [
        f"{row.playlist_name} ({row.incomplete_count} tracks)" for _, row in playlists_df.iterrows()
    ]
    url_map = {options[i]: playlists_df.iloc[i].playlist_url for i in range(len(options))}
    selected_label = st.selectbox(
        "Choose a playlist to view its incomplete tracks:",
        options=options,
        index=0,
    )
    selected_playlist_url = url_map.get(selected_label)

    if not selected_playlist_url:
        return

    st.markdown("---")
    st.subheader(f"🎶 Tracks in: **{selected_label}**")

    # Search + paging controls
    search = st.text_input("Search (artist or track contains):", value="")
    col_a, col_b, col_c = st.columns([1, 1, 2])
    with col_a:
        page_size = st.selectbox("Rows per page", options=[10, 25, 50, 100], index=1)
    with col_b:
        page_number = st.number_input("Page", min_value=1, step=1, value=1)

    # Fetch page
    offset = (int(page_number) - 1) * int(page_size)
    with st.spinner("Loading tracks..."):
        rows, total_for_playlist = _get_incomplete_tracks_for_playlist_cached(
            DB_PATH, selected_playlist_url, search, offset, int(page_size), st.session_state["import_nonce"]
        )

    if offset >= max(total_for_playlist, 1):
        offset = 0
        page_number = 1
        rows, total_for_playlist = _get_incomplete_tracks_for_playlist_cached(
            DB_PATH, selected_playlist_url, search, offset, int(page_size), st.session_state["import_nonce"]
        )

    # Table view (lightweight)
    if rows:
        df = pd.DataFrame(rows)[["artist", "track_name", "status", "track_id"]]
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No matching tracks on this page.")

    labels_by_id = {
        r["track_id"]: f"{r['artist']} - {r['track_name']} • {r['status']} [{r['track_id']}]"
        for r in rows
    }
    tracks_by_id = {r["track_id"]: r for r in rows}
    if labels_by_id:
        selected_track_id = st.selectbox(
            "Select a track to import:",
            options=list(labels_by_id.keys()),
            format_func=lambda track_id: labels_by_id.get(track_id, str(track_id)),
        )
        track = tracks_by_id[selected_track_id]

        uploaded_file = st.file_uploader(
            "Select audio file",
            type=["mp3", "flac", "wav", "m4a", "ogg", "wma"],
            key=f"upload_{track['track_id']}",
            help="Upload the audio file for this track",
        )

        if uploaded_file is not None:
            st.info(f"📁 Selected: `{uploaded_file.name}`")
            
            # Check file quality and show warning if worse than MP3 320kbps
            try:
                # Save to temp file for metadata extraction
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                    tmp_file.write(uploaded_file.getbuffer())
                    tmp_path = tmp_file.name
                
                # Reset file pointer for later use
                uploaded_file.seek(0)
                
                # Extract metadata
                temp_metadata = extract_metadata_from_file(tmp_path)
                
                # Clean up temp file
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
                
                # Check quality
                is_worse, reason = is_quality_worse_than_mp3_320(
                    tmp_path,
                    temp_metadata.get('extension', ''),
                    temp_metadata.get('bitrate')
                )
                
                if is_worse:
                    st.warning(f"⚠️ **Quality Warning:** {reason}. Consider uploading a higher quality version for better audio fidelity.")
            
            except Exception as e:
                write_log.debug("QUALITY_CHECK_FAIL", "Failed to check file quality.", {"error": str(e)})
            
            if st.button("Import Track", key=f"import_{track['track_id']}", type="primary"):
                with st.spinner("Importing..."):
                    success, message = import_track(track["track_id"], uploaded_file, track)
                if success:
                    st.success(message)
                    # Bust only manual-import caches
                    st.session_state["import_nonce"] += 1
                    # Refresh the view to reflect the import
                    time.sleep(0.3)
                    st.rerun()
                else:
                    st.error(message)

    # Footer
    st.markdown("---")
    st.markdown("💡 **Tip:** Files will be saved to `slskd_docker_data/{ENV}/imported/`")
    st.markdown("🔄 M3U8 playlists update automatically after import. Export iTunes XML manually when needed.")

    if st.button("Export iTunes XML now", key="export_itunes_xml_manual", type="secondary"):
        with st.spinner("Exporting iTunes XML..."):
            success, message = export_itunes_xml_for_manual_import()
        if success:
            st.success(f"✅ {message}")
        else:
            st.error(f"❌ {message}")


def render_manual_import_tab():
    """Render the complete Manual Import tab content."""
    render_manual_import_section()
