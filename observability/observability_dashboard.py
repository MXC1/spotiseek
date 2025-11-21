import streamlit as st
import os
import plotly.express as px
import sqlite3
import pandas as pd

st.set_page_config(page_title="Spotiseek Observability", layout="wide")
st.title("Spotiseek Observability Dashboard")

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