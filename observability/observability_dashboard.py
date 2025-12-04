import os
import sys
import json
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import sqlite3

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

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
    analyze_workflow_run
)
from scripts.database_management import get_playlists, get_track_status_breakdown


# Get environment from environment variable
ENV = os.getenv("APP_ENV")

# Page configuration
st.set_page_config(page_title=f"Spotiseek Observability ({ENV.upper()})", layout="wide")
st.title(f"Spotiseek Observability Dashboard - {ENV.upper()} Environment")


# Environment-specific constants
LOGS_DIR = os.path.join(os.path.dirname(__file__), "logs", ENV)
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', ENV, f'database_{ENV}.db')

def get_extension_bitrate_breakdown(db_path):
    """
    Returns three DataFrames: extension breakdown, bitrate breakdown, and download status breakdown from the tracks table.
    Handles both NULL and empty string as 'Not Downloaded'.
    """
    try:
        conn = sqlite3.connect(db_path)
        ext_df = pd.read_sql_query(
            "SELECT extension, COUNT(*) as count FROM tracks GROUP BY extension ORDER BY count DESC", conn)
        br_df = pd.read_sql_query(
            "SELECT bitrate, COUNT(*) as count FROM tracks GROUP BY bitrate ORDER BY count DESC", conn)
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
            GROUP BY download_status
            ORDER BY count DESC
            """, conn)
        conn.close()
        return ext_df, br_df, dl_df, None
    except Exception as e:
        return None, None, None, str(e)

def render_log_breakdown_section():
    """Render the warning and error log breakdown section."""
    log_files = get_log_files(LOGS_DIR)
    log_entries = parse_logs(log_files)
    warn_err_logs = filter_warning_error_logs(log_entries)
    df_logs = logs_to_dataframe(warn_err_logs)
    
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
    st.dataframe(status_df_with_total)


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


def render_workflow_runs_section():
    """Render workflow run selection and detailed inspection section."""
    st.subheader("Workflow Run Inspection")
    
    # Get all workflow runs
    runs = get_workflow_runs(LOGS_DIR)
    
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
    
    # Analyze the selected run
    with st.spinner("Analyzing workflow run..."):
        analysis = analyze_workflow_run(selected_run['log_file'])
    
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
    
    st.markdown(f"### {status_icon} Run: {run['display_name']}")
    st.markdown(f"**Status:** {status.upper()} | **Log File:** `{run['run_id']}.log`")
    
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
    
    with col5:
        st.metric("Downloads Completed", analysis['downloads_completed'])
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

def main():
    """Main application entry point."""
    # Workflow run inspection section (full width)
    render_workflow_runs_section()
    
    st.markdown("---")
    
    # Log breakdown section (full width)
    render_log_breakdown_section()

    # Two-column layout for playlists and track status
    col1, col2 = st.columns(2)

    with col1:
        render_playlists_section()
        render_extension_bitrate_section()

    with col2:
        render_track_status_section()



if __name__ == "__main__":
    main()