"""Spotiseek Observability Dashboard - Main application file."""
import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv

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
    prepare_log_summary
)
from scripts.database_management import get_playlists, get_track_status_breakdown


# Get environment from environment variable
ENV = os.getenv("APP_ENV", "test")

# Page configuration
st.set_page_config(page_title=f"Spotiseek Observability ({ENV.upper()})", layout="wide")
st.title(f"Spotiseek Observability Dashboard - {ENV.upper()} Environment")


# Environment-specific constants
LOGS_DIR = os.path.join(os.path.dirname(__file__), f'{ENV}_logs')
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'database', f'database_{ENV}.db')


def render_log_breakdown_section():
    """Render the warning and error log breakdown section."""
    with st.expander("Warning and Error Log Breakdown", expanded=True):
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
    header_cols = st.columns([2, 3, 4, 1, 2])
    header_cols[0].markdown("**Level**")
    header_cols[1].markdown("**Event ID**")
    header_cols[2].markdown("**Message**")
    header_cols[3].markdown("**Count**")
    header_cols[4].markdown("**Action**")
    
    # Initialize session state for selected sample
    if 'selected_sample_idx' not in st.session_state:
        st.session_state['selected_sample_idx'] = None
    
    # Render each row with expandable sample
    for i, row in summary.iterrows():
        cols = st.columns([2, 3, 4, 1, 2])
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
        
        if cols[4].button("View Sample", key=f"view_sample_{i}"):
            st.session_state['selected_sample_idx'] = (
                None if st.session_state['selected_sample_idx'] == i else i
            )
        
        # Show sample log if this row is selected
        if st.session_state['selected_sample_idx'] == i:
            st.code(row['sample_log'], language='json')


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


def main():
    """Main application entry point."""
    # Log breakdown section (full width)
    render_log_breakdown_section()
    
    # Two-column layout for playlists and track status
    col1, col2 = st.columns(2)
    
    with col1:
        render_playlists_section()
    
    with col2:
        render_track_status_section()


if __name__ == "__main__":
    main()