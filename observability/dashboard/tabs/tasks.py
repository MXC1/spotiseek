"""
Tasks tab for the dashboard.

Contains functions for rendering the task scheduler management interface.
"""

import time
from datetime import datetime
from typing import List

import pandas as pd
import streamlit as st

from observability.dashboard.config import LOGS_DIR
from scripts.logs_utils import get_task_scheduler_logs, parse_logs
from scripts.task_scheduler import get_task_registry


# ============================================================================
# HELPER FUNCTIONS
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


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def render_tasks_section():
    """Render the task scheduler management interface."""
    
    try:
        registry = get_task_registry()
    except Exception as e:
        st.error(f"Failed to initialize task registry: {e}")
        return
    
    # Run All Tasks button
    st.markdown("### 🚀 Quick Actions")
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("▶️ Run All Tasks", type="primary", width="stretch"):
            registry.run_all_tasks()
            st.success("✅ All tasks have been started! Check task history below for progress.")
            st.cache_data.clear()
            time.sleep(1)
            st.rerun()
    
    with col2:
        if st.button("🔄 Refresh Status", width="stretch"):
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
    
    st.dataframe(display_df, width="stretch", hide_index=True)
    
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


def render_tasks_tab():
    """Render the complete Tasks tab content."""
    render_tasks_section()
