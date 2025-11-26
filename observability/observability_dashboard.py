import streamlit as st
import os
import plotly.express as px
import sqlite3
import pandas as pd

import glob
import json
from datetime import datetime

st.set_page_config(page_title="Spotiseek Observability", layout="wide")
st.title("Spotiseek Observability Dashboard")

# --- Log Breakdown Section ---

def get_log_files(logs_dir):
    # Recursively find all .log files in the logs directory
    pattern = os.path.join(logs_dir, '**', '*.log')
    return glob.glob(pattern, recursive=True)

def parse_logs(log_files):
    log_entries = []
    for file in log_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        entry = json.loads(line.strip())
                        log_entries.append(entry)
                    except Exception:
                        continue
        except Exception:
            continue
    return log_entries

def filter_warning_error_logs(log_entries):
    return [entry for entry in log_entries if entry.get('level') in ('WARNING', 'ERROR')]

def logs_to_dataframe(log_entries):
    # Only keep relevant columns
    rows = []
    for entry in log_entries:
        rows.append({
            'timestamp': entry.get('timestamp'),
            'level': entry.get('level'),
            'event_id': entry.get('event_id'),
            'message': entry.get('message'),
        })
    df = pd.DataFrame(rows)
    # Try to sort by timestamp if possible
    if not df.empty:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d_%H%M%S_%f', errors='coerce')
            df = df.sort_values('timestamp', ascending=False)
        except Exception:
            pass
    return df

with st.expander("Warning and Error Log Breakdown", expanded=True):
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    log_files = get_log_files(logs_dir)
    log_entries = parse_logs(log_files)
    warn_err_logs = filter_warning_error_logs(log_entries)
    df_logs = logs_to_dataframe(warn_err_logs)
    st.subheader("WARNING and ERROR Log Summary")
    if not df_logs.empty:
        # Prepare summary and sample logs
        summary = df_logs.groupby(['level', 'event_id']).size().reset_index(name='count')
        samples = []
        for _, row in summary.iterrows():
            level = row['level']
            event_id = row['event_id']
            sample_row = df_logs[(df_logs['level'] == level) & (df_logs['event_id'] == event_id)].iloc[0]
            context_obj = warn_err_logs[df_logs.index[(df_logs['level'] == level) & (df_logs['event_id'] == event_id)][0]].get('context', {})
            sample_str = f"Timestamp: {sample_row['timestamp']}\nMessage: {sample_row['message']}\nContext: {json.dumps(context_obj, indent=2)}"
            samples.append(sample_str)
        summary['sample_log'] = samples

        # Render table header
        header_cols = st.columns([2, 3, 1, 2])
        header_cols[0].markdown("**Level**")
        header_cols[1].markdown("**Event ID**")
        header_cols[2].markdown("**Count**")
        header_cols[3].markdown("**Action**")

        if 'selected_sample_idx' not in st.session_state:
            st.session_state['selected_sample_idx'] = None
        for i, row in summary.iterrows():
            cols = st.columns([2, 3, 1, 2])
            cols[0].markdown(f"{row['level']}")
            cols[1].markdown(f"{row['event_id']}")
            cols[2].markdown(f"{row['count']}")
            if cols[3].button("View Sample", key=f"view_sample_{i}"):
                if st.session_state['selected_sample_idx'] == i:
                    st.session_state['selected_sample_idx'] = None
                else:
                    st.session_state['selected_sample_idx'] = i
            # Show the sample log directly below the row if selected
            if st.session_state['selected_sample_idx'] == i:
                st.code(row['sample_log'], language='json')
    else:
        st.info("No WARNING or ERROR logs found.")

# --- Playlists Table ---

db_path = os.path.join(os.path.dirname(__file__), '../database/database_test.db')
col1, col2 = st.columns(2)

with col1:
    st.subheader("Unique Playlists")
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            query = "SELECT playlist_name, playlist_url FROM playlists"
            df = pd.read_sql_query(query, conn)
            conn.close()
            if not df.empty:
                st.dataframe(df)
            else:
                st.info("No playlists found in the database.")
        except Exception as e:
            st.error(f"Error querying database: {e}")
    else:
        st.info("Database file not found.")

# --- Track Status Breakdown ---

with col2:
    st.subheader("Track Download Status Breakdown")
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            status_query = "SELECT download_status, COUNT(*) as count FROM tracks GROUP BY download_status"
            status_df = pd.read_sql_query(status_query, conn)
            conn.close()
            if not status_df.empty:
                # Exclude 'completed' from the graph
                graph_df = status_df[status_df['download_status'].str.lower() != 'completed']
                if not graph_df.empty:
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
                    )
                    fig.update_xaxes(fixedrange=True)
                    fig.update_yaxes(fixedrange=True)
                    fig.update_layout(
                        autosize=True,
                        margin=dict(l=40, r=40, t=40, b=40),
                        showlegend=False
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No non-completed track statuses to display in the graph.")

                # Add total row to table
                total = status_df['count'].sum()
                total_row = pd.DataFrame({'download_status': ['Total'], 'count': [total]})
                status_df_with_total = pd.concat([status_df, total_row], ignore_index=True)
                st.dataframe(status_df_with_total)
            else:
                st.info("No track status data found in the database.")
        except Exception as e:
            st.error(f"Error querying track statuses: {e}")