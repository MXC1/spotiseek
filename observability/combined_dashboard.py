"""
Combined Spotiseek Dashboard.

Unified Streamlit application combining:
- Overall statistics dashboard
- Workflow execution inspection
- Manual track import tool

All served on the same port with tabbed navigation.

Usage:
    streamlit run observability/combined_dashboard.py
"""

import os
import sys
import json
import time
import sqlite3
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from mutagen import File as MutagenFile

# Disable .pyc file generation
sys.dont_write_bytecode = True

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

# Add parent directory to path to import from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import helper functions from scripts/
from scripts.logs_utils import (
    get_log_files,
    parse_logs,
    filter_warning_error_logs,
    logs_to_dataframe,
    prepare_log_summary,
    get_workflow_runs,
    get_task_scheduler_logs,
    analyze_workflow_run,
    setup_logging,
    write_log
)
from scripts.database_management import (
    get_playlists,
    get_track_status_breakdown,
    get_failed_reason_breakdown,
    TrackDB
)
from scripts.m3u8_manager import update_track_in_m3u8
from scripts.xml_exporter import export_itunes_xml, extract_file_metadata
from scripts.task_scheduler import get_task_registry, TaskStatus

# Get environment from environment variable
ENV = os.getenv("APP_ENV")

if not ENV:
    st.error("⚠️ APP_ENV environment variable is not set. Please set it to 'test', 'stage', or 'prod'.")
    st.stop()

# Initialize logging
setup_logging(log_name_prefix="combined_dashboard")

# Page configuration
st.set_page_config(
    page_title=f"Spotiseek Dashboard ({ENV.upper()})",
    page_icon="🎵",
    layout="wide"
)

st.title(f"🎵 Spotiseek Dashboard - {ENV.upper()} Environment")

# Environment-specific constants
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs", ENV)
DB_PATH = os.path.join(BASE_DIR, 'database', ENV, f'database_{ENV}.db')
IMPORTED_DIR = os.path.join(BASE_DIR, "slskd_docker_data", ENV, "imported")
DOWNLOADS_ROOT = os.path.join(BASE_DIR, "slskd_docker_data", ENV, "downloads")
XML_DIR = os.path.join(BASE_DIR, "database", "xml", ENV)
M3U8_DIR = os.path.join(BASE_DIR, "database", "m3u8s", ENV)

# Check if running in Docker
IS_DOCKER = os.path.exists("/.dockerenv")

# Ensure directories exist
os.makedirs(IMPORTED_DIR, exist_ok=True)
os.makedirs(XML_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Check if database exists
DB_EXISTS = os.path.exists(DB_PATH)

# Initialize database (will create if doesn't exist)
try:
    track_db = TrackDB()
    if not DB_EXISTS and os.path.exists(DB_PATH):
        write_log.info("DASHBOARD_DB_CREATED", "Database file was created during initialization.", {"db_path": DB_PATH})
except Exception as e:
    write_log.error("DASHBOARD_DB_INIT_FAIL", "Failed to initialize database.", {"db_path": DB_PATH, "error": str(e)})
    track_db = None


# ============================================================================
# CACHING & PERFORMANCE CONFIGURATION
# ============================================================================

# Cache TTL values for different operation types
CACHE_TTL_SHORT = 300   # 5 minutes for stats queries
CACHE_TTL_LONG = 600    # 10 minutes for expensive log analysis
CACHE_TTL_MEDIUM = 180  # 3 minutes for import data


# ============================================================================
# OVERALL STATS TAB FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_SHORT)
def get_extension_bitrate_breakdown(db_path):
    """
    Returns three DataFrames: extension breakdown, bitrate breakdown, and download status breakdown from the tracks table.
    Extension and bitrate breakdowns only include tracks with local_file_path.
    Handles both NULL and empty string as 'Not Downloaded'.
    """
    if not os.path.exists(db_path):
        return None, None, None, "Database file does not exist"
    try:
        conn = sqlite3.connect(db_path)
        # Extension breakdown - only tracks with local_file_path
        ext_df = pd.read_sql_query(
            "SELECT extension, COUNT(*) as count FROM tracks WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != '' GROUP BY extension ORDER BY count DESC", conn)
        # Bitrate breakdown - only tracks with local_file_path
        br_df = pd.read_sql_query(
            "SELECT bitrate, COUNT(*) as count FROM tracks WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != '' GROUP BY bitrate ORDER BY count DESC", conn)
        # Downloaded/not downloaded breakdown (treat NULL and empty string as Not Downloaded)
        dl_df = pd.read_sql_query(
            """
            SELECT 
                CASE 
                    WHEN local_file_path IS NOT NULL AND TRIM(local_file_path) != '' THEN 'Downloaded'
                    ELSE 'Not Downloaded'
                END AS download_status,
                COUNT(*) as count
            FROM tracks
            GROUP BY 
                CASE 
                    WHEN local_file_path IS NOT NULL AND TRIM(local_file_path) != '' THEN 'Downloaded'
                    ELSE 'Not Downloaded'
                END
            ORDER BY count DESC
            """, conn)
        conn.close()
        return ext_df, br_df, dl_df, None
    except Exception as e:
        return None, None, None, str(e)


@st.cache_data(ttl=CACHE_TTL_SHORT)
def get_failed_reason_breakdown_cached(db_path: str):
    """Cached helper for failed reason breakdown."""
    return get_failed_reason_breakdown(db_path)


@st.cache_data(ttl=CACHE_TTL_SHORT)
def _get_warning_error_logs(logs_dir: str) -> Tuple[pd.DataFrame, List[dict]]:
    """Cached helper to load and parse warning/error logs."""
    log_files = get_log_files(logs_dir)
    log_entries = parse_logs(log_files)
    warn_err_logs = filter_warning_error_logs(log_entries)
    df_logs = logs_to_dataframe(warn_err_logs)
    return df_logs, warn_err_logs


def render_log_breakdown_section():
    """Render the warning and error log breakdown section."""
    df_logs, warn_err_logs = _get_warning_error_logs(LOGS_DIR)
    
    st.subheader("WARNING and ERROR Log Summary")
    
    if not df_logs.empty:
        summary = prepare_log_summary(df_logs, warn_err_logs)
        render_log_summary_table(summary)
    else:
        st.info("No WARNING or ERROR logs found.")


def render_log_summary_table(summary: pd.DataFrame):
    """
    Render interactive log summary table with expandable sample logs.
    
    Args:
        summary: DataFrame containing log summary with sample logs
    """
    # Render table header
    header_cols = st.columns([2, 3, 4, 1, 2, 2])
    header_cols[0].markdown("**Level**")
    header_cols[1].markdown("**Event ID**")
    header_cols[2].markdown("**Message**")
    header_cols[3].markdown("**Count**")
    header_cols[4].markdown("**Latest**")
    header_cols[5].markdown("**Action**")
    
    # Initialize session state for selected sample
    if 'selected_sample_idx' not in st.session_state:
        st.session_state['selected_sample_idx'] = None
    
    # Render each row with expandable sample
    for i, row in summary.iterrows():
        cols = st.columns([2, 3, 4, 1, 2, 2])
        cols[0].markdown(f"{row['level']}")
        cols[1].markdown(f"{row['event_id']}")
        # Show message (truncate if too long)
        message = row.get('message', '')
        if isinstance(message, str) and len(message) > 120:
            display_message = message[:117] + '...'
        else:
            display_message = message
        cols[2].markdown(f"{display_message}")
        cols[3].markdown(f"{row['count']}")
        cols[4].markdown(f"{row.get('latest', '')}")
        if cols[5].button("View Sample", key=f"view_sample_{i}"):
            st.session_state['selected_sample_idx'] = (
                None if st.session_state['selected_sample_idx'] == i else i
            )
        # Show sample log if this row is selected
        if st.session_state['selected_sample_idx'] == i:
            st.code(row['sample_log'], language='json', wrap_lines=True)


def render_playlists_section():
    """Render the playlists table section."""
    st.subheader("Unique Playlists")
    
    if not os.path.exists(DB_PATH):
        st.info("Database file not found.")
        return
    
    df, error = get_playlists(DB_PATH)
    
    if error:
        st.error(f"Error querying database: {error}")
    elif df is not None and not df.empty:
        st.dataframe(df)
    else:
        st.info("No playlists found in the database.")


def render_track_status_section():
    """Render the track download status breakdown section."""
    st.subheader("Track Download Status Breakdown")
    
    if not os.path.exists(DB_PATH):
        st.info("Database file not found.")
        return
    
    status_df, error = get_track_status_breakdown(DB_PATH)
    
    if error:
        st.error(f"Error querying track statuses: {error}")
        return
    
    if status_df is None or status_df.empty:
        st.info("No track status data found in the database.")
        return
    
    # Render chart (excluding completed tracks)
    render_status_chart(status_df)
    
    # Render status table with total
    render_status_table(status_df)


def render_status_chart(status_df: pd.DataFrame):
    """
    Render bar chart of track statuses (excluding completed).
    
    Args:
        status_df: DataFrame with download status breakdown
    """
    graph_df = status_df[status_df['download_status'].str.lower() != 'completed']
    
    if graph_df.empty:
        st.info("No non-completed track statuses to display in the graph.")
        return
    
    fig = px.bar(
        graph_df,
        x='download_status',
        y='count',
        title='Track Download Status (excluding completed)',
    )
    
    fig.update_layout(
        xaxis_title='Download Status',
        yaxis_title='Count',
        dragmode=False,
        hovermode='x',
        autosize=True,
        margin=dict(l=40, r=40, t=40, b=40),
        showlegend=False
    )
    
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    
    st.plotly_chart(fig, width='stretch')


def render_status_table(status_df: pd.DataFrame):
    """
    Render status table with total row.
    
    Args:
        status_df: DataFrame with download status breakdown
    """
    total = status_df['count'].sum()
    total_row = pd.DataFrame({'download_status': ['Total'], 'count': [total]})
    status_df_with_total = pd.concat([status_df, total_row], ignore_index=True)
    st.dataframe(status_df_with_total, hide_index=True)


def render_extension_bitrate_section():
    """Render the extension, bitrate, and download status breakdown section."""
    st.subheader("Track Extension, Bitrate, and Download Status Breakdown")
    if not os.path.exists(DB_PATH):
        st.info("Database file not found.")
        return
    ext_df, br_df, dl_df, error = get_extension_bitrate_breakdown(DB_PATH)
    if error:
        st.error(f"Error querying extension/bitrate breakdown: {error}")
        return
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**File Extension Breakdown**")
        if ext_df is not None and not ext_df.empty:
            st.dataframe(ext_df)
        else:
            st.info("No extension data found.")
    with col2:
        st.markdown("**Bitrate Breakdown**")
        if br_df is not None and not br_df.empty:
            st.dataframe(br_df)
        else:
            st.info("No bitrate data found.")
    with col3:
        st.markdown("**Download Status Breakdown**")
        if dl_df is not None and not dl_df.empty:
            st.dataframe(dl_df)
        else:
            st.info("No download status data found.")


def render_failed_reason_section():
    """Render breakdown of reasons for tracks without local files."""
    st.subheader("Tracks Without Local Files")
    st.caption("Breakdown by status and reason for tracks that haven't been downloaded")

    if not os.path.exists(DB_PATH):
        st.info("Database file not found.")
        return

    df, error = get_failed_reason_breakdown_cached(DB_PATH)

    if error:
        st.error(f"Error querying reasons: {error}")
        return

    if df is None or df.empty:
        st.info("All tracks have local files!")
        return

    st.dataframe(df, hide_index=True)


# ============================================================================
# EXECUTION INSPECTION TAB FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_SHORT)
def _get_cached_workflow_runs(logs_dir: str) -> List[dict]:
    """Cached helper to load workflow runs."""
    return get_workflow_runs(logs_dir)


@st.cache_data(ttl=CACHE_TTL_LONG)
def _analyze_workflow_run_cached(log_file: str) -> dict:
    """Cached helper to analyze workflow run."""
    return analyze_workflow_run(log_file)


def render_workflow_runs_section():
    """Render workflow run selection and detailed inspection section."""
    st.subheader("Workflow Run Inspection")
    
    # Get all workflow runs (cached)
    runs = _get_cached_workflow_runs(LOGS_DIR)
    
    if not runs:
        st.info("No workflow runs found.")
        return
    
    # Create dropdown options
    run_options = {run['display_name']: run for run in runs}
    
    # Run selection dropdown
    selected_display = st.selectbox(
        "Select a workflow run to inspect:",
        options=list(run_options.keys()),
        key="workflow_run_selector"
    )
    
    if not selected_display:
        return
    
    selected_run = run_options[selected_display]
    
    # Analyze the selected run (cached)
    with st.spinner("Analyzing workflow run..."):
        analysis = _analyze_workflow_run_cached(selected_run['log_file'])
    
    # Display run summary
    render_run_summary(selected_run, analysis)


def render_run_summary(run: dict, analysis: dict):
    """
    Render detailed summary of a workflow run.
    
    Args:
        run: Run metadata dictionary
        analysis: Analysis results from analyze_workflow_run
    """
    # Status badge
    status = analysis['workflow_status']
    status_colors = {
        'completed': '🟢',
        'failed': '🔴',
        'incomplete': '🟡',
        'unknown': '⚪'
    }
    status_icon = status_colors.get(status, '⚪')
    
    # Get the actual log filename from the path
    log_filename = os.path.basename(run['log_file'])
    
    st.markdown(f"### {status_icon} Run: {run['display_name']}")
    st.markdown(f"**Status:** {status.upper()} | **Log File:** `{log_filename}`")
    
    # Key metrics in columns
    st.markdown("#### Summary Statistics")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Logs", analysis['total_logs'])
        st.metric("Errors", len(analysis['errors']))
    
    with col2:
        st.metric("Warnings", len(analysis['warnings']))
        st.metric("Tracks Added", analysis['tracks_added'])
    
    with col3:
        st.metric("Playlists Added", analysis['playlists_added'])
        st.metric("Quality Upgrades", analysis['tracks_upgraded'])
    
    with col4:
        st.metric("Searches (New)", analysis['new_searches'])
        st.metric("Searches (Upgrade)", analysis['upgrade_searches'])
    
    # Split downloads completed into new vs upgrades
    downloads_new = analysis.get('downloads_completed_new', 0)
    downloads_upgrade = analysis.get('downloads_completed_upgrade', 0)
    with col5:
        st.metric("Downloads Completed (New)", downloads_new)
        st.metric("Downloads Completed (Upgrade)", downloads_upgrade)
        st.metric("Downloads Failed", analysis['downloads_failed'])
    
    # Timeline
    if analysis['timeline']:
        st.markdown("#### Workflow Timeline")
        timeline_df = pd.DataFrame([
            {
                'Time': item['display_time'],
                'Event': item['event_id'],
                'Message': item['message']
            }
            for item in analysis['timeline']
        ])
        st.dataframe(timeline_df, use_container_width=True, hide_index=True)
    
    # Errors section
    if analysis['errors']:
        with st.expander(f"❌ Errors ({len(analysis['errors'])})", expanded=False):
            for error in analysis['errors']:
                st.code(
                    f"Event: {error.get('event_id', 'N/A')}\n"
                    f"Message: {error.get('message', 'N/A')}\n"
                    f"Context: {json.dumps(error.get('context', {}), indent=2)}",
                    language='json'
                )
    
    # Warnings section
    if analysis['warnings']:
        with st.expander(f"⚠️ Warnings ({len(analysis['warnings'])})", expanded=False):
            for warning in analysis['warnings']:
                st.code(
                    f"Event: {warning.get('event_id', 'N/A')}\n"
                    f"Message: {warning.get('message', 'N/A')}\n"
                    f"Context: {json.dumps(warning.get('context', {}), indent=2)}",
                    language='json'
                )


# ============================================================================
# MANUAL IMPORT TAB FUNCTIONS
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
        write_log.warning("IMPORT_UI_DB_NOT_FOUND", "Database file does not exist.", {"db_path": db_path})
        return {}
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
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
    
    conn.close()
    return grouped_tracks


def get_non_completed_tracks_by_playlist() -> Dict[str, List[Tuple]]:
    """
    Retrieve all tracks missing a local_file_path, grouped by playlist.
    Uses cached version for better performance.
    
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
    return _get_non_completed_tracks_cached(DB_PATH)


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
        JOIN tracks t ON t.spotify_id = pt.spotify_id
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
    Each row is a dict with keys: spotify_id, track_name, artist, status, playlist_url
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
        JOIN tracks t ON t.spotify_id = pt.spotify_id
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
            t.spotify_id,
            t.track_name,
            t.artist,
            t.download_status
        FROM playlist_tracks pt
        JOIN tracks t ON t.spotify_id = pt.spotify_id
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
            "spotify_id": r[1],
            "track_name": r[2],
            "artist": r[3],
            "status": r[4],
        }
        for r in rows
    ]
    return result, int(total)


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


def render_manual_import_section():
    """Render the complete manual import interface with pagination and single-uploader flow."""
    st.subheader("Manual Import Tool")
    st.markdown(f"**Environment:** `{ENV}`")

    # Check if database exists
    if not os.path.exists(DB_PATH):
        st.error(f"❌ Database file not found: `{DB_PATH}`")
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
        df = pd.DataFrame(rows)[["artist", "track_name", "status", "spotify_id"]]
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No matching tracks on this page.")

    labels_by_id = {
        r["spotify_id"]: f"{r['artist']} - {r['track_name']} • {r['status']} [{r['spotify_id']}]"
        for r in rows
    }
    tracks_by_id = {r["spotify_id"]: r for r in rows}
    if labels_by_id:
        selected_spotify_id = st.selectbox(
            "Select a track to import:",
            options=list(labels_by_id.keys()),
            format_func=lambda spotify_id: labels_by_id.get(spotify_id, str(spotify_id)),
        )
        track = tracks_by_id[selected_spotify_id]

        uploaded_file = st.file_uploader(
            "Select audio file",
            type=["mp3", "flac", "wav", "m4a", "ogg", "wma"],
            key=f"upload_{track['spotify_id']}",
            help="Upload the audio file for this track",
        )

        if uploaded_file is not None:
            st.info(f"📁 Selected: `{uploaded_file.name}`")
            if st.button("Import Track", key=f"import_{track['spotify_id']}", type="primary"):
                with st.spinner("Importing..."):
                    success, message = import_track(track["spotify_id"], uploaded_file, track)
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
    st.markdown("🔄 After import, M3U8 playlists and iTunes XML are automatically updated.")


# ============================================================================
# TASKS TAB FUNCTIONS
# ============================================================================

def get_status_emoji(status: str) -> str:
    """Get emoji for task status."""
    status_map = {
        'idle': '⚪',
        'running': '🔵',
        'completed': '🟢',
        'failed': '🔴',
        'skipped': '🟡',
    }
    return status_map.get(status.lower() if status else 'idle', '⚪')


def format_datetime(dt_str: str) -> str:
    """Format ISO datetime string for display."""
    if not dt_str:
        return "Never"
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return dt_str or "Never"


def format_interval(minutes: int) -> str:
    """Format interval in minutes to human-readable string."""
    if minutes < 60:
        return f"{minutes} min"
    elif minutes < 1440:
        hours = minutes / 60
        return f"{hours:.1f} hr" if hours != int(hours) else f"{int(hours)} hr"
    else:
        days = minutes / 1440
        return f"{days:.1f} days" if days != int(days) else f"{int(days)} day"


def render_tasks_section():
    """Render the task scheduler management interface."""
    st.subheader("⏱️ Scheduled Tasks")
    st.markdown("Manage and monitor automated workflow tasks (like Radarr).")
    
    try:
        registry = get_task_registry()
    except Exception as e:
        st.error(f"Failed to initialize task registry: {e}")
        return
    
    # Run All Tasks button
    st.markdown("### 🚀 Quick Actions")
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("▶️ Run All Tasks", type="primary", use_container_width=True):
            registry.run_all_tasks()
            st.success("✅ All tasks have been started! Check task history below for progress.")
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()
    
    with col2:
        if st.button("🔄 Refresh Status", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    st.markdown("---")
    
    # Task list
    st.markdown("### 📋 Task Overview")
    
    task_states = registry.get_all_task_states()
    
    if not task_states:
        st.info("No tasks registered.")
        return
    
    # Display tasks in a table-like format
    for state in task_states:
        with st.container():
            # Task header row
            cols = st.columns([3, 2, 2, 2, 2, 1])
            
            status_emoji = get_status_emoji(state.get('last_status'))
            is_running = state.get('is_running', False)
            
            if is_running:
                status_emoji = "🔵"
                status_text = "Running"
            else:
                status_text = (state.get('last_status') or 'Never run').capitalize()
            
            with cols[0]:
                st.markdown(f"**{status_emoji} {state['display_name']}**")
                if state.get('dependencies'):
                    deps = ", ".join(state['dependencies'])
                    st.caption(f"Depends on: {deps}")
            
            with cols[1]:
                st.markdown("**Interval**")
                st.markdown(format_interval(state['interval_minutes']))
            
            with cols[2]:
                st.markdown("**Last Run**")
                st.markdown(format_datetime(state.get('last_run_at')))
            
            with cols[3]:
                st.markdown("**Next Run**")
                st.markdown(format_datetime(state.get('next_run_at')))
            
            with cols[4]:
                st.markdown("**Status**")
                st.markdown(status_text)
            
            with cols[5]:
                # Run Now button
                if st.button("▶️", key=f"run_{state['task_name']}", 
                           help=f"Run {state['display_name']} now",
                           disabled=is_running):
                    with st.spinner(f"Running {state['display_name']}..."):
                        success, message = registry.run_task(state['task_name'], force=True)
                    
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                    
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
            
            st.markdown("---")
    
    # Task History section
    st.markdown("### 📜 Recent Task History")
    render_task_history_section(registry)
    
    # Task Scheduler Logs section
    st.markdown("---")
    render_task_scheduler_logs_section()


def render_task_history_section(registry):
    """Render the task execution history section."""
    # Task filter
    task_names = ["All Tasks"] + list(registry.tasks.keys())
    selected_task = st.selectbox(
        "Filter by task:",
        options=task_names,
        key="task_history_filter"
    )
    
    # Get history
    if selected_task == "All Tasks":
        history = registry.get_recent_runs(limit=50)
    else:
        history = registry.get_task_history(selected_task, limit=50)
    
    if not history:
        st.info("No task execution history found.")
        return
    
    # Convert to DataFrame for display
    history_df = pd.DataFrame(history)
    
    # Format columns
    if 'started_at' in history_df.columns:
        history_df['started_at'] = history_df['started_at'].apply(format_datetime)
    if 'completed_at' in history_df.columns:
        history_df['completed_at'] = history_df['completed_at'].apply(format_datetime)
    
    # Add status emoji
    if 'status' in history_df.columns:
        history_df['status'] = history_df['status'].apply(
            lambda s: f"{get_status_emoji(s)} {s.capitalize()}" if s else "⚪ Unknown"
        )
    
    # Select columns to display
    display_cols = ['task_name', 'status', 'started_at', 'completed_at', 'tracks_processed']
    display_cols = [c for c in display_cols if c in history_df.columns]
    
    # Rename columns for display
    column_names = {
        'task_name': 'Task',
        'status': 'Status',
        'started_at': 'Started',
        'completed_at': 'Completed',
        'tracks_processed': 'Tracks',
        'error_message': 'Error'
    }
    
    display_df = history_df[display_cols].rename(columns=column_names)
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Show errors if any failed runs
    failed_runs = [h for h in history if h.get('status') == 'failed' and h.get('error_message')]
    if failed_runs:
        with st.expander(f"❌ Failed Runs ({len(failed_runs)})", expanded=False):
            for run in failed_runs[:10]:  # Show last 10 failures
                st.code(
                    f"Task: {run['task_name']}\n"
                    f"Time: {format_datetime(run['started_at'])}\n"
                    f"Error: {run['error_message']}",
                    language='text'
                )


def render_task_scheduler_logs_section():
    """Render the task scheduler log viewer section."""
    st.markdown("### 📝 Task Scheduler Logs")
    
    # Get task scheduler log files
    log_files = get_task_scheduler_logs(LOGS_DIR)
    
    if not log_files:
        st.info("No task scheduler logs found.")
        return
    
    # Create dropdown options
    log_options = {log['display_name']: log for log in log_files}
    
    # Log selection dropdown
    selected_display = st.selectbox(
        "Select a log file to view:",
        options=list(log_options.keys()),
        key="task_scheduler_log_selector"
    )
    
    if not selected_display:
        return
    
    selected_log = log_options[selected_display]
    
    # Parse and display logs
    log_entries = parse_logs([selected_log['log_file']])
    
    if not log_entries:
        st.info("No log entries found in selected file.")
        return
    
    # Filter options
    col1, col2 = st.columns([1, 3])
    with col1:
        level_filter = st.multiselect(
            "Filter by level:",
            options=['INFO', 'WARNING', 'ERROR', 'DEBUG'],
            default=['INFO', 'WARNING', 'ERROR'],
            key="task_log_level_filter"
        )
    
    # Apply filter
    if level_filter:
        filtered_entries = [e for e in log_entries if e.get('level') in level_filter]
    else:
        filtered_entries = log_entries
    
    st.markdown(f"**Showing {len(filtered_entries)} of {len(log_entries)} entries**")
    
    # Display recent logs (most recent first)
    filtered_entries.reverse()
    
    # Limit display to avoid overwhelming the UI
    max_display = 100
    if len(filtered_entries) > max_display:
        st.warning(f"Showing only the most recent {max_display} entries.")
        filtered_entries = filtered_entries[:max_display]
    
    for entry in filtered_entries:
        level = entry.get('level', 'INFO')
        timestamp = entry.get('timestamp', '')
        event_id = entry.get('event_id', '')
        message = entry.get('message', '')
        context = entry.get('context', {})
        
        # Format timestamp for display
        try:
            from datetime import datetime
            ts = datetime.strptime(timestamp, '%Y%m%d_%H%M%S_%f')
            display_time = ts.strftime('%H:%M:%S')
        except (ValueError, TypeError):
            display_time = timestamp[:8] if timestamp else ''
        
        # Color based on level
        level_colors = {
            'ERROR': '🔴',
            'WARNING': '🟡',
            'INFO': '🔵',
            'DEBUG': '⚪'
        }
        level_emoji = level_colors.get(level, '⚪')
        
        # Display log entry
        with st.container():
            st.markdown(f"{level_emoji} **{display_time}** [{event_id}] {message}")
            if context and context != {}:
                with st.expander("Context", expanded=False):
                    st.json(context)


# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application entry point with tabbed interface."""
    
    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overall Stats", "⏱️ Tasks", "🔍 Execution Inspection", "📥 Manual Import"])
    
    with tab1:
        st.header("Overall Statistics")
        
        st.markdown("---")
        
        # Two-column layout for playlists and track status
        col1, col2 = st.columns(2)
        
        with col1:
            render_playlists_section()
            render_extension_bitrate_section()
        
        with col2:
            render_track_status_section()
            render_failed_reason_section()
            
        # Workflow run inspection section (full width)
        render_log_breakdown_section()
    
    with tab2:
        st.header("Task Scheduler")
        render_tasks_section()
    
    with tab3:
        st.header("Workflow Execution Inspection")
        render_workflow_runs_section()
    
    with tab4:
        st.header("Manual Track Import")
        render_manual_import_section()


if __name__ == "__main__":
    main()
