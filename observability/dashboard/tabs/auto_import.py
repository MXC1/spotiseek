"""
Auto Import tab for the dashboard.

Contains functions for rendering the automatic track import interface with fuzzy matching.
"""

import os
import sqlite3
import time
from typing import Dict, List, Tuple

import streamlit as st
from mutagen import File as MutagenFile
from rapidfuzz import fuzz

from observability.dashboard.config import (
    ENV,
    DB_PATH,
    IS_DOCKER,
    CACHE_TTL_MEDIUM,
)
from observability.dashboard.helpers import (
    require_database,
    is_quality_worse_than_mp3_320,
    do_track_import,
)
from scripts.logs_utils import write_log
from scripts.constants import SUPPORTED_AUDIO_FORMATS


# ============================================================================
# CONFIGURATION
# ============================================================================

# Supported audio file extensions for scanning (with leading dots for os.path.splitext)
AUDIO_EXTENSIONS = {f'.{ext}' for ext in SUPPORTED_AUDIO_FORMATS}


# ============================================================================
# CACHED DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=CACHE_TTL_MEDIUM)
def _get_all_incomplete_tracks_cached(db_path: str, cache_nonce: int) -> List[Dict]:
    """
    Get all tracks missing local_file_path for auto-matching.
    
    Args:
        db_path: Path to database
        cache_nonce: Cache busting nonce
    
    Returns:
        List of track dicts with track_id, track_name, artist, playlist_name
    """
    _ = cache_nonce
    if not os.path.exists(db_path):
        return []
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = """
        SELECT DISTINCT
            t.track_id,
            t.track_name,
            t.artist,
            t.download_status,
            GROUP_CONCAT(p.playlist_name, ', ') as playlists
        FROM tracks t
        LEFT JOIN playlist_tracks pt ON t.track_id = pt.track_id
        LEFT JOIN playlists p ON pt.playlist_url = p.playlist_url
        WHERE t.local_file_path IS NULL OR TRIM(t.local_file_path) = ''
        GROUP BY t.track_id, t.track_name, t.artist, t.download_status
        ORDER BY t.artist, t.track_name
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()
    
    return [
        {
            'track_id': r[0],
            'track_name': r[1],
            'artist': r[2],
            'status': r[3],
            'playlists': r[4] or 'Unknown'
        }
        for r in rows
    ]


# ============================================================================
# SCANNING & MATCHING FUNCTIONS
# ============================================================================

def scan_directory_for_audio_files(directory: str) -> List[Dict]:
    """
    Recursively scan a directory for audio files and extract metadata.
    
    Args:
        directory: Path to the directory to scan
    
    Returns:
        List of dicts with keys: file_path, filename, metadata_artist, metadata_title, 
                                  parsed_artist, parsed_title, extension
    """
    audio_files = []
    
    if not os.path.isdir(directory):
        write_log.warning("AUTO_IMPORT_INVALID_DIR", "Invalid directory path.", {"directory": directory})
        return audio_files
    
    write_log.info("AUTO_IMPORT_SCAN_START", "Starting directory scan.", {"directory": directory})
    
    for root, _, files in os.walk(directory):
        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in AUDIO_EXTENSIONS:
                continue
            
            file_path = os.path.join(root, filename)
            file_info = {
                'file_path': file_path,
                'filename': filename,
                'extension': ext.lstrip('.'),
                'metadata_artist': None,
                'metadata_title': None,
                'parsed_artist': None,
                'parsed_title': None,
                'bitrate': None,
                'is_low_quality': False,
                'quality_warning': None,
            }
            
            # Extract metadata using mutagen
            try:
                audio = MutagenFile(file_path, easy=True)
                if audio:
                    # Get artist - try multiple tag names
                    artist_tags = ['artist', 'albumartist', 'performer']
                    for tag in artist_tags:
                        if tag in audio and audio[tag]:
                            file_info['metadata_artist'] = audio[tag][0]
                            break
                    
                    # Get title
                    if 'title' in audio and audio['title']:
                        file_info['metadata_title'] = audio['title'][0]
                
                # Extract bitrate (need non-easy mode for this)
                audio_full = MutagenFile(file_path, easy=False)
                if audio_full and hasattr(audio_full.info, 'bitrate') and audio_full.info.bitrate:
                    file_info['bitrate'] = int(audio_full.info.bitrate / 1000)  # Convert to kbps
                
                # Check quality
                is_worse, reason = is_quality_worse_than_mp3_320(
                    file_path,
                    file_info['extension'],
                    file_info['bitrate']
                )
                file_info['is_low_quality'] = is_worse
                file_info['quality_warning'] = reason if is_worse else None
                
            except Exception as e:
                write_log.debug("AUTO_IMPORT_METADATA_FAIL", "Failed to extract metadata.", 
                              {"file_path": file_path, "error": str(e)})
            
            # Parse filename as fallback (try common patterns: "Artist - Title", "Title")
            name_without_ext = os.path.splitext(filename)[0]
            if ' - ' in name_without_ext:
                parts = name_without_ext.split(' - ', 1)
                file_info['parsed_artist'] = parts[0].strip()
                file_info['parsed_title'] = parts[1].strip()
            else:
                # Just use the filename as title
                file_info['parsed_title'] = name_without_ext.strip()
            
            audio_files.append(file_info)
    
    write_log.info("AUTO_IMPORT_SCAN_COMPLETE", "Directory scan complete.", 
                  {"directory": directory, "files_found": len(audio_files)})
    
    return audio_files


def get_best_artist_title(file_info: Dict) -> Tuple[str, str]:
    """
    Get the best artist and title from file info, preferring metadata over filename parsing.
    
    Args:
        file_info: Dictionary with metadata_artist, metadata_title, parsed_artist, parsed_title
    
    Returns:
        Tuple of (artist, title)
    """
    artist = file_info.get('metadata_artist') or file_info.get('parsed_artist') or ''
    title = file_info.get('metadata_title') or file_info.get('parsed_title') or ''
    return artist, title


def calculate_match_score(file_info: Dict, track: Dict) -> Dict:
    """
    Calculate fuzzy match score between a source file and a track from the database.
    
    Uses strict matching strategies that require BOTH title and artist to match independently.
    Prevents false positives from artist-only or title-only matches.
    
    Args:
        file_info: Source file info dict
        track: Track dict with track_name, artist keys
    
    Returns:
        Dict with score, match_type, and details
    """
    file_artist, file_title = get_best_artist_title(file_info)
    track_artist = track.get('artist', '') or ''
    track_title = track.get('track_name', '') or ''
    
    # Strict thresholds - BOTH title and artist must independently meet these
    MIN_TITLE_SCORE = 80  # Increased from 75
    MIN_ARTIST_SCORE = 70  # Increased from 65
    
    scores = []
    
    # Strategy 1: Direct title+artist comparison (primary strategy)
    # Both title and artist are compared independently and must both pass thresholds
    if file_title and file_artist and track_artist:
        title_score = fuzz.token_sort_ratio(file_title.lower(), track_title.lower())
        artist_score = fuzz.token_sort_ratio(file_artist.lower(), track_artist.lower())
        
        # BOTH must independently meet minimum thresholds
        if title_score >= MIN_TITLE_SCORE and artist_score >= MIN_ARTIST_SCORE:
            # Equal weighting - both are equally important
            combined = (title_score * 0.5) + (artist_score * 0.5)
            scores.append({
                'score': combined,
                'match_type': 'artist+title',
                'title_score': title_score,
                'artist_score': artist_score
            })
    
    # Strategy 2: Combined string match with independent validation
    # Must verify BOTH title and artist separately, not just the combined string
    if file_artist and file_title and track_artist:
        # Check title and artist independently FIRST
        title_score = fuzz.token_sort_ratio(file_title.lower(), track_title.lower())
        artist_score = fuzz.token_sort_ratio(file_artist.lower(), track_artist.lower())
        
        # Only proceed if both meet thresholds independently
        if title_score >= MIN_TITLE_SCORE and artist_score >= MIN_ARTIST_SCORE:
            file_combined = f"{file_artist} - {file_title}"
            track_combined = f"{track_artist} - {track_title}"
            combined_score = fuzz.token_sort_ratio(file_combined.lower(), track_combined.lower())
            
            scores.append({
                'score': combined_score,
                'match_type': 'combined_string',
                'title_score': title_score,
                'artist_score': artist_score
            })
    
    # Strategy 3: Filename matching with independent validation
    # Verify that filename contains good matches for BOTH artist and title
    if track_artist and track_title:
        filename_lower = file_info['filename'].lower()
        
        # Check if both artist and title appear in filename independently
        title_in_filename = fuzz.partial_ratio(track_title.lower(), filename_lower)
        artist_in_filename = fuzz.partial_ratio(track_artist.lower(), filename_lower)
        
        # Both must be present in the filename with good scores
        if title_in_filename >= MIN_TITLE_SCORE and artist_in_filename >= MIN_ARTIST_SCORE:
            # Also check overall filename match
            track_combined = f"{track_artist} - {track_title}"
            filename_score = fuzz.token_sort_ratio(filename_lower, track_combined.lower())
            
            scores.append({
                'score': filename_score,
                'match_type': 'filename',
                'title_score': title_in_filename,
                'artist_score': artist_in_filename
            })
    
    # If no valid scores, return a very low score to indicate poor match
    if not scores:
        return {
            'score': 0,
            'match_type': 'no_match',
            'title_score': 0,
            'artist_score': 0
        }
    
    # Return best score
    best = max(scores, key=lambda x: x['score'])
    return best


def find_matches_for_tracks(audio_files: List[Dict], tracks: List[Dict]) -> List[Dict]:
    """
    Find all potential matches between source audio files and incomplete tracks.
    
    Args:
        audio_files: List of scanned audio file info dicts
        tracks: List of incomplete track dicts from database
    
    Returns:
        List of match dicts sorted by score descending
    """
    matches = []
    
    for track in tracks:
        for file_info in audio_files:
            score_info = calculate_match_score(file_info, track)
            
            file_artist, file_title = get_best_artist_title(file_info)
            
            matches.append({
                'track_id': track['track_id'],
                'track_name': track['track_name'],
                'track_artist': track['artist'],
                'track_playlists': track['playlists'],
                'file_path': file_info['file_path'],
                'file_name': file_info['filename'],
                'file_artist': file_artist,
                'file_title': file_title,
                'file_extension': file_info['extension'],
                'file_bitrate': file_info.get('bitrate'),
                'is_low_quality': file_info.get('is_low_quality', False),
                'quality_warning': file_info.get('quality_warning'),
                'score': score_info['score'],
                'match_type': score_info['match_type'],
            })
    
    # Sort by score descending
    matches.sort(key=lambda x: x['score'], reverse=True)
    
    return matches


def auto_import_track(track_id: str, source_file: str, track_info: Dict) -> Tuple[bool, str]:
    """
    Import a track by copying from source location to imported directory (wrapper for auto import).
    
    Args:
        track_id: Track identifier
        source_file: Full path to source audio file
        track_info: Dictionary with track metadata (track_artist, track_name)
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    return do_track_import(
        track_id=track_id,
        source_path=source_file,
        artist=track_info['track_artist'],
        track_name=track_info['track_name'],
        is_upload=False,
        uploaded_file=None
    )


def get_score_color(score: float) -> str:
    """Get color for score display based on match quality."""
    if score >= 90:
        return "🟢"  # Excellent match
    elif score >= 70:
        return "🟡"  # Good match
    elif score >= 50:
        return "🟠"  # Fair match
    else:
        return "🔴"  # Poor match


# ============================================================================
# RENDER FUNCTIONS
# ============================================================================

def render_auto_import_section():
    """Render the automatic import interface."""
    st.markdown(f"**Environment:** `{ENV}`")
    st.markdown("""
    This tool scans a directory for audio files and attempts to match them 
    with tracks that are missing from your library. Matches are ranked by 
    fuzzy matching score.
    """)
    
    # Check if database exists
    if not require_database(
        error_msg=f"❌ Database file not found: `{DB_PATH}`"
    ):
        st.info("💡 The database will be created when the workflow runs for the first time.")
        return
    
    # Initialize session state
    if "auto_import_nonce" not in st.session_state:
        st.session_state["auto_import_nonce"] = 0
    if "auto_import_matches" not in st.session_state:
        st.session_state["auto_import_matches"] = None
    if "auto_import_scanned_dir" not in st.session_state:
        st.session_state["auto_import_scanned_dir"] = None
    
    st.markdown("---")
    
    # Show Docker warning if applicable
    if IS_DOCKER:
        with st.expander("📖 How to mount a directory in Docker", expanded=False):
            st.markdown("""            
            **Add a volume mount to docker-compose.yml**
            
            Add your music directory to the `dashboard` service volumes:
            ```yaml
            dashboard:
              volumes:
                # ... existing volumes ...
                - E:/Folder:/mnt/music:ro  # Mount your music folder
            ```
            
            Then use `/mnt/music` as the path.
            
            After editing, run `invoke up` to rebuild.
            """)
    
    # Directory input
    st.markdown("### 📁 Source Directory")
    if IS_DOCKER:
        st.caption("Enter the **mounted path** inside the container (e.g., `/mnt/music/...`).")
    else:
        st.caption("Enter the path to a directory containing audio files to match.")
    
    source_dir = st.text_input(
        "Directory path:",
        placeholder="/mnt/music/folder" if IS_DOCKER else "e.g., E:\\Music\\MyFolder",
        key="auto_import_source_dir"
    )
    
    col1, col2 = st.columns([1, 3])
    with col1:
        scan_button = st.button("🔍 Scan Directory", type="primary", use_container_width=True)
    
    if scan_button and source_dir:
        if not os.path.isdir(source_dir):
            st.error(f"❌ Directory not found: `{source_dir}`")
            if IS_DOCKER:
                # Check if it looks like a Windows path
                if '\\' in source_dir or (len(source_dir) > 1 and source_dir[1] == ':'):
                    st.error(
                        "🐳 **This looks like a Windows path.** Docker containers cannot access Windows paths directly. "
                        "Either run the dashboard locally, or mount the directory and use the container path."
                    )
                else:
                    st.info("💡 Make sure the directory is mounted in docker-compose.yml and the path is correct.")
            else:
                st.info("💡 Make sure the path exists and is accessible.")
        else:
            with st.spinner("Scanning directory for audio files..."):
                audio_files = scan_directory_for_audio_files(source_dir)
            
            if not audio_files:
                st.warning("No audio files found in the specified directory.")
                st.session_state["auto_import_matches"] = None
            else:
                st.success(f"Found {len(audio_files)} audio files.")
                
                # Get incomplete tracks
                with st.spinner("Loading incomplete tracks from database..."):
                    tracks = _get_all_incomplete_tracks_cached(
                        DB_PATH, st.session_state["auto_import_nonce"]
                    )
                
                if not tracks:
                    st.success("✨ All tracks have been downloaded! Nothing to match.")
                    st.session_state["auto_import_matches"] = None
                else:
                    st.info(f"Found {len(tracks)} tracks missing from library.")
                    
                    # Find matches
                    with st.spinner("Calculating matches (this may take a moment)..."):
                        matches = find_matches_for_tracks(audio_files, tracks)
                    
                    st.session_state["auto_import_matches"] = matches
                    st.session_state["auto_import_scanned_dir"] = source_dir
                    st.success(f"Found {len(matches)} potential matches.")
    
    # Display matches if available
    matches = st.session_state.get("auto_import_matches")
    
    if matches:
        st.markdown("---")
        st.markdown("### 🎯 Potential Matches")
        st.caption(f"Showing matches for directory: `{st.session_state.get('auto_import_scanned_dir', '')}`")
        
        # Filter controls
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            min_score = st.slider("Minimum score:", 0, 100, 70, key="auto_min_score")
        with col2:
            page_size = st.selectbox("Matches per page:", [25, 50, 100, 200], index=0, key="auto_page_size")
        with col3:
            search_filter = st.text_input("Filter by track/artist:", key="auto_search_filter")
        
        # Filter matches
        filtered_matches = [
            m for m in matches 
            if m['score'] >= min_score
            and (not search_filter or 
                 search_filter.lower() in m['track_name'].lower() or
                 search_filter.lower() in m['track_artist'].lower() or
                 search_filter.lower() in m['file_name'].lower())
        ]
        
        if not filtered_matches:
            st.info("No matches found with current filters.")
            return
        
        # Pagination
        total_matches = len(filtered_matches)
        total_pages = (total_matches + page_size - 1) // page_size
        
        page_col1, page_col2 = st.columns([1, 3])
        with page_col1:
            current_page = st.number_input(
                f"Page (1-{total_pages}):", 
                min_value=1, 
                max_value=max(1, total_pages), 
                value=1,
                key="auto_page_num"
            )
        
        start_idx = (current_page - 1) * page_size
        end_idx = min(start_idx + page_size, total_matches)
        page_matches = filtered_matches[start_idx:end_idx]
        
        st.markdown(f"**Showing {start_idx + 1}-{end_idx} of {total_matches} matches**")
        
        # Initialize selection state
        if "auto_selected_matches" not in st.session_state:
            st.session_state["auto_selected_matches"] = set()
        
        # Display matches with checkboxes
        st.markdown("---")
        
        # Header row
        header_cols = st.columns([0.5, 0.8, 2.5, 2.5, 2, 1.2, 1])
        header_cols[0].markdown("**Select**")
        header_cols[1].markdown("**Score**")
        header_cols[2].markdown("**Track Name - Artist Name (DB)**")
        header_cols[3].markdown("**Track Name - Artist Name (Candidate)**")
        header_cols[4].markdown("**Filename (Candidate)**")
        header_cols[5].markdown("**Quality**")
        header_cols[6].markdown("**Match Type**")
        
        for i, match in enumerate(page_matches):
            match_key = f"{match['track_id']}::{match['file_path']}"
            idx = start_idx + i
            
            cols = st.columns([0.5, 0.8, 2.5, 2.5, 2, 1.2, 1])
            
            # Checkbox
            is_selected = cols[0].checkbox(
                "Select",
                key=f"auto_check_{idx}",
                value=match_key in st.session_state["auto_selected_matches"],
                label_visibility="collapsed"
            )
            
            # Update selection state
            if is_selected:
                st.session_state["auto_selected_matches"].add(match_key)
            else:
                st.session_state["auto_selected_matches"].discard(match_key)
            
            # Score with color
            score_color = get_score_color(match['score'])
            cols[1].markdown(f"{score_color} **{match['score']:.0f}%**")
            
            # Track Name - Artist Name (DB) - combined
            db_combined = f"{match['track_name']} - {match['track_artist']}"
            cols[2].markdown(db_combined)
            
            # Track Name - Artist Name (Candidate) - combined
            file_artist = match.get('file_artist') or ''
            file_title = match.get('file_title') or ''
            candidate_combined = f"{file_title} - {file_artist}" if file_artist else file_title
            cols[3].markdown(candidate_combined)
            
            # Filename (Candidate) - no truncation, use word wrap
            cols[4].markdown(f"`{match['file_name']}`")
            
            # Quality info with warning indicator
            bitrate = match.get('file_bitrate')
            ext = match['file_extension'].upper()
            if match.get('is_low_quality'):
                quality_str = f"⚠️ {ext}"
                if bitrate:
                    quality_str += f" {bitrate}k"
            else:
                quality_str = f"✅ {ext}"
                if bitrate:
                    quality_str += f" {bitrate}k"
                elif ext in ('FLAC', 'WAV', 'AIFF', 'ALAC'):
                    quality_str = f"✅ {ext} (lossless)"
            cols[5].markdown(quality_str)
            
            cols[6].markdown(match['match_type'])
        
        st.markdown("---")
        
        # Import selected button
        selected_count = len(st.session_state["auto_selected_matches"])
        
        # Check for low-quality files in selection
        match_lookup = {f"{m['track_id']}::{m['file_path']}": m for m in matches}
        low_quality_selected = [
            match_lookup[key] for key in st.session_state["auto_selected_matches"]
            if key in match_lookup and match_lookup[key].get('is_low_quality')
        ]
        
        if low_quality_selected:
            st.warning(
                f"⚠️ **Quality Warning:** {len(low_quality_selected)} of your selected files are below MP3 320kbps quality. "
                "Consider finding higher quality versions for better audio fidelity."
            )
            with st.expander(f"View {len(low_quality_selected)} low-quality files", expanded=False):
                for m in low_quality_selected[:20]:  # Show max 20
                    reason = m.get('quality_warning', 'Unknown quality issue')
                    st.markdown(f"- **{m['file_name']}**: {reason}")
                if len(low_quality_selected) > 20:
                    st.markdown(f"_...and {len(low_quality_selected) - 20} more_")
        
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            st.markdown(f"**{selected_count} matches selected**")
        
        with col2:
            if st.button("❌ Clear Selection", use_container_width=True):
                st.session_state["auto_selected_matches"] = set()
                st.rerun()
        
        with col3:
            import_button = st.button(
                f"✅ Import {selected_count} Selected Tracks",
                type="primary",
                disabled=selected_count == 0,
                use_container_width=True
            )
        
        if import_button and selected_count > 0:
            # Build lookup for matches
            match_lookup = {f"{m['track_id']}::{m['file_path']}": m for m in matches}
            
            success_count = 0
            fail_count = 0
            imported_track_ids = set()
            
            progress_bar = st.progress(0, text="Importing tracks...")
            
            selected_list = list(st.session_state["auto_selected_matches"])
            for i, match_key in enumerate(selected_list):
                match = match_lookup.get(match_key)
                if not match:
                    continue
                
                # Skip if this track was already imported in this batch
                if match['track_id'] in imported_track_ids:
                    continue
                
                track_info = {
                    'track_artist': match['track_artist'],
                    'track_name': match['track_name']
                }
                
                success, message = auto_import_track(
                    match['track_id'],
                    match['file_path'],
                    track_info
                )
                
                if success:
                    success_count += 1
                    imported_track_ids.add(match['track_id'])
                else:
                    fail_count += 1
                    st.warning(f"⚠️ {message}")
                
                progress_bar.progress((i + 1) / len(selected_list), text=f"Importing... ({i + 1}/{len(selected_list)})")
            
            progress_bar.empty()
            
            if success_count > 0:
                st.success(f"✅ Successfully imported {success_count} tracks!")
            if fail_count > 0:
                st.error(f"❌ Failed to import {fail_count} tracks.")
            
            # Clear selection and refresh
            st.session_state["auto_selected_matches"] = set()
            st.session_state["auto_import_nonce"] += 1
            st.session_state["auto_import_matches"] = None  # Force re-scan
            
            time.sleep(0.5)
            st.rerun()
        
        # Tips section
        st.markdown("---")
        st.markdown("""        
        💡 **Tips:**
        - 🟢 Scores ≥90% are excellent matches (high confidence)
        - 🟡 Scores 70-89% are good matches (review recommended)
        - 🟠 Scores 50-69% are fair matches (careful review needed)
        - 🔴 Scores <50% are poor matches (likely incorrect)
        - Files are **copied** (originals remain in place)
        - Each track can only be imported once per batch
        """)


def render_auto_import_tab():
    """Render the complete Auto Import tab content."""
    render_auto_import_section()
