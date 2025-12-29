"""
Execution Inspection tab for the dashboard.

Contains functions for rendering workflow run selection and detailed inspection.
"""

import json
import os
from typing import List

import pandas as pd
import streamlit as st

from observability.dashboard.config import (
    LOGS_DIR,
    CACHE_TTL_SHORT,
    CACHE_TTL_LONG,
)
from scripts.logs_utils import get_workflow_runs, analyze_workflow_run


# ============================================================================
# CACHED DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_SHORT)
def _get_cached_workflow_runs(logs_dir: str) -> List[dict]:
    """Cached helper to load workflow runs."""
    return get_workflow_runs(logs_dir)


@st.cache_data(ttl=CACHE_TTL_LONG)
def _analyze_workflow_run_cached(log_file: str) -> dict:
    """Cached helper to analyze workflow run."""
    return analyze_workflow_run(log_file)


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def render_workflow_runs_section():
    """Render workflow run selection and detailed inspection section."""
    
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
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Total Logs", analysis['total_logs'])
        st.metric("Errors", len(analysis['errors']))
        st.metric("Warnings", len(analysis['warnings']))

    with col2:
        st.metric("Searches (New)", analysis['new_searches'])
        st.metric("Searches (Upgrade)", analysis['upgrade_searches'])

    with col3:
        st.metric("Playlists Added", analysis['playlists_added'])
        st.metric("Playlists Removed", analysis.get('playlists_removed', 0))

    with col4:
        st.metric("Tracks Added", analysis['tracks_added'])
        st.metric("Tracks Removed", analysis.get('tracks_removed', 0))

    with col5:
        st.metric("Quality Upgrades", analysis['tracks_upgraded'])

    # Split downloads completed into new vs upgrades
    downloads_new = analysis.get('downloads_completed_new', 0)
    downloads_upgrade = analysis.get('downloads_completed_upgrade', 0)
    with col6:
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


def render_execution_inspection_tab():
    """Render the complete Execution Inspection tab content."""
    render_workflow_runs_section()
