"""
Combined Spotiseek Dashboard.

Unified Streamlit application combining:
- Overall statistics dashboard
- Task scheduler management
- Workflow execution inspection
- Manual track import tool
- Automatic track import tool
- Documentation viewer

All served on the same port with tabbed navigation.

Usage:
    streamlit run observability/combined_dashboard.py
"""

import os
import sys

import streamlit as st
from dotenv import load_dotenv

# Disable .pyc file generation
sys.dont_write_bytecode = True

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

# Add parent directory to path to import from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import configuration (initializes logging, track_db, directories)
from observability.dashboard.config import ENV

# Import tab render functions
from observability.dashboard.tabs import (
    render_overall_stats_tab,
    render_tasks_tab,
    render_execution_inspection_tab,
    render_manual_import_tab,
    render_auto_import_tab,
    render_docs_tab,
)


def main():
    """Main application entry point with tabbed interface."""
    
    # Page configuration
    st.set_page_config(
        page_title=f"Spotiseek Dashboard ({ENV.upper()})",
        page_icon="🎵",
        layout="wide"
    )
    
    st.title(f"🎵 Spotiseek Dashboard - {ENV.upper()} Environment")
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Overall Stats",
        "⏱️ Tasks",
        "🔍 Execution Inspection",
        "📥 Manual Import",
        "🤖 Auto Import",
        "📖 Docs"
    ])
    
    with tab1:
        render_overall_stats_tab()
    
    with tab2:
        render_tasks_tab()
    
    with tab3:
        render_execution_inspection_tab()
    
    with tab4:
        render_manual_import_tab()
    
    with tab5:
        render_auto_import_tab()
    
    with tab6:
        render_docs_tab()


if __name__ == "__main__":
    main()
