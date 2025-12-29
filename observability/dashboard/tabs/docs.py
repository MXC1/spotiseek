"""
Documentation tab for the dashboard.

Contains functions for rendering the documentation viewer interface.
"""

import os
from typing import Optional, Tuple

import streamlit as st

from observability.dashboard.config import BASE_DIR, CACHE_TTL_LONG


# ============================================================================
# CONFIGURATION
# ============================================================================

DOCS_DIR = os.path.join(BASE_DIR, "docs")
DOC_FILES = {
    "Overview": os.path.join(BASE_DIR, "README.md"),
    "Dashboard Guide": os.path.join(DOCS_DIR, "DASHBOARD.md"),
    "Configuration": os.path.join(DOCS_DIR, "CONFIGURATION.md"),
    "Troubleshooting": os.path.join(DOCS_DIR, "TROUBLESHOOTING.md"),
}


# ============================================================================
# CACHED DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_LONG)
def _load_markdown_file(file_path: str) -> Tuple[str, Optional[str]]:
    """
    Load and return contents of a markdown file.
    
    Args:
        file_path: Path to the markdown file
    
    Returns:
        Tuple of (content, error_message)
    """
    try:
        if not os.path.exists(file_path):
            return "", f"File not found: {file_path}"
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return content, None
    except Exception as e:
        return "", f"Error reading file: {str(e)}"


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def render_documentation_section():
    """Render the documentation viewer interface."""
    
    # Check if docs directory exists
    if not os.path.isdir(DOCS_DIR):
        st.error(f"❌ Documentation directory not found: `{DOCS_DIR}`")
        return
    
    # Document selector
    doc_names = list(DOC_FILES.keys())
    
    # Use columns for a cleaner layout
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.markdown("### 📄 Select Document")
        selected_doc = st.radio(
            "Choose a document:",
            options=doc_names,
            key="doc_selector",
            label_visibility="collapsed"
        )
    
    with col2:
        if selected_doc:
            doc_path = DOC_FILES[selected_doc]
            
            # Load the markdown content
            content, error = _load_markdown_file(doc_path)
            
            if error:
                st.error(f"❌ {error}")
            elif content:
                # Render the markdown
                st.markdown(content, unsafe_allow_html=False)
            else:
                st.info("Document is empty.")
    
    # Footer with file location
    st.markdown("---")
    st.caption(f"📁 Documentation files are located in: `{DOCS_DIR}`")


def render_docs_tab():
    """Render the complete Documentation tab content."""
    render_documentation_section()
