"""
Overall Statistics tab for the dashboard.

Contains functions for rendering playlists, track status, extension/bitrate breakdown,
and log summary sections.
"""

import os
import sqlite3
from typing import List, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st

from observability.dashboard.config import (
    DB_PATH,
    CACHE_TTL_SHORT,
)
from observability.dashboard.helpers import (
    require_database,
    compute_effective_bitrate_kbps,
)
from scripts.database_management import (
    get_playlists,
    get_track_status_breakdown,
    get_failed_reason_breakdown,
)
from scripts.constants import LOSSLESS_FORMATS


# ============================================================================
# CACHED DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_SHORT)
def get_extension_bitrate_breakdown(db_path: str):
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
def get_enhanced_bitrate_breakdown(db_path: str):
    """
    Build an enhanced bitrate breakdown with the following categories:
    - Known numeric bitrates (e.g., 320)
    - Lossless (extensions: wav, flac, alac)
    - Unknown (Effective) <kbps> for files without stored bitrate, computed from size/duration

    Returns: (DataFrame, error_str)
    DataFrame columns: bitrate, count
    """
    if not os.path.exists(db_path):
        return pd.DataFrame(columns=["bitrate", "count"]), "Database file does not exist"
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT extension, bitrate, local_file_path
            FROM tracks
            WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != ''
            """
        )
        rows = cursor.fetchall()
        conn.close()

        lossless_count = 0
        known_counts = {}
        unknown_effective_counts = {}
        unknown_unmeasured = 0

        for ext, br, path in rows:
            ext_norm = (ext or "").lower()
            if ext_norm in LOSSLESS_FORMATS:
                lossless_count += 1
                continue

            # Known stored bitrate
            if br is not None and str(br).strip() != "":
                try:
                    br_int = int(br)
                except Exception:
                    # If non-integer stored, skip to effective computation
                    br_int = None
                if br_int is not None:
                    known_counts[br_int] = known_counts.get(br_int, 0) + 1
                    continue

            # Unknown: try computing effective bitrate
            eff = compute_effective_bitrate_kbps(path)
            if eff is not None:
                unknown_effective_counts[eff] = unknown_effective_counts.get(eff, 0) + 1
            else:
                unknown_unmeasured += 1

        # Build display rows
        display_rows = []

        # Known bitrates, sorted by count desc then bitrate desc
        for br_val, cnt in sorted(known_counts.items(), key=lambda x: (x[1], x[0]), reverse=True):
            display_rows.append({"bitrate": str(br_val), "count": cnt})

        # Lossless aggregate
        if lossless_count > 0:
            display_rows.append({"bitrate": "Lossless", "count": lossless_count})

        # Unknown effective buckets, sorted by count desc then bitrate desc
        for eff_val, cnt in sorted(unknown_effective_counts.items(), key=lambda x: (x[1], x[0]), reverse=True):
            display_rows.append({"bitrate": f"Unknown (Effective) {eff_val}", "count": cnt})

        # Unknown unmeasured bucket if any remain
        if unknown_unmeasured > 0:
            display_rows.append({"bitrate": "Unknown (Unmeasured)", "count": unknown_unmeasured})

        df = pd.DataFrame(display_rows, columns=["bitrate", "count"])
        return df, None
    except Exception as e:
        return pd.DataFrame(columns=["bitrate", "count"]), str(e)


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def render_playlists_section():
    """Render the playlists table section."""
    st.subheader("Unique Playlists")
    
    if not require_database():
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
    
    if not require_database():
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
    if not require_database():
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
        st.markdown("**Bitrate Breakdown (Enhanced)**")
        enhanced_df, enh_error = get_enhanced_bitrate_breakdown(DB_PATH)
        if enh_error:
            st.error(f"Error computing enhanced bitrate breakdown: {enh_error}")
        elif enhanced_df is not None and not enhanced_df.empty:
            st.dataframe(enhanced_df, hide_index=True)
            st.caption("Known numeric bitrates, aggregated Lossless, and Unknown files with computed effective bitrate.")
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

    if not require_database():
        return

    df, error = get_failed_reason_breakdown_cached(DB_PATH)

    if error:
        st.error(f"Error querying reasons: {error}")
        return

    if df is None or df.empty:
        st.info("All tracks have local files!")
        return
    # Normalize noisy 500 error reasons (collapse per-URL variants)
    try:
        def _normalize_failed_reason(reason: str) -> str:
            if isinstance(reason, str) and reason.startswith("500 Server Error: Internal Server Error"):
                return "500 Server Error: Internal Server Error"
            return reason

        df["failed_reason"] = df["failed_reason"].apply(_normalize_failed_reason)

        # Re-aggregate after normalization to combine duplicates
        df = (
            df.groupby(["download_status", "failed_reason"], as_index=False)["count"].sum()
              .sort_values("count", ascending=False)
        )
    except Exception:
        # If normalization fails for any reason, show original df
        pass

    st.dataframe(df, hide_index=True)


def render_overall_stats_tab():
    """Render the complete Overall Stats tab content."""
    
    # Two-column layout for playlists and track status
    col1, col2 = st.columns(2)
    
    with col1:
        render_playlists_section()
        render_extension_bitrate_section()
    
    with col2:
        render_track_status_section()
        render_failed_reason_section()
