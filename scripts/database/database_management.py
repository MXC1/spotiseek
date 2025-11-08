"""
Database management module for Spotiseek application.

This module provides a thread-safe singleton interface for managing the SQLite database
that tracks Spotify playlists, tracks, download statuses, and mappings to Soulseek downloads.
"""

import logging
import os
import sqlite3
import threading
from typing import Optional, List, Tuple

from logs_utils import setup_logging
# Initialize logging for database operations
setup_logging(log_name_prefix="database_management")

# Validate environment configuration
ENV = os.getenv("APP_ENV")
if not ENV:
    raise EnvironmentError(
        "APP_ENV environment variable is not set. Database interaction is disabled."
    )

# Construct database path based on environment
DB_PATH = os.path.join(os.path.dirname(__file__), f"database_{ENV}.db")


class TrackDB:
    """
    Thread-safe singleton database manager for track and playlist management.
    
    This class implements the Singleton pattern to ensure only one database connection
    exists throughout the application lifecycle. It manages:
    - Track metadata and download status
    - Playlist information and track associations
    - Mappings between Spotify IDs and Soulseek download UUIDs
    
    Attributes:
        conn: SQLite database connection
    """
    
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Ensure only one instance of TrackDB exists (thread-safe)."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TrackDB, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_path: str = DB_PATH):
        """
        Initialize the database connection and create tables if needed.
        
        Args:
            db_path: Path to the SQLite database file. Defaults to environment-specific path.
        
        Note:
            Due to singleton pattern, initialization only happens once per application run.
        """
        if self._initialized:
            return
        
        self._initialized = True
        logging.info(f"Connecting to database at {db_path}")
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_tables()

    def clear_database(self) -> None:
        """
        Delete the database file and reinitialize with empty tables.
        
        This method includes safeguards for production environments, requiring
        explicit user confirmation before proceeding with deletion.
        
        Raises:
            RuntimeError: If running in production and no input is available for confirmation.
        
        Warning:
            This operation is destructive and cannot be undone. All track, playlist,
            and download mapping data will be permanently lost.
        """
        # Production environment safeguard
        if ENV == "prod":
            try:
                confirm = input(
                    f"APP_ENV is: {ENV}.\n"
                    "Are you sure you want to delete the database? "
                    "This action cannot be undone. Type 'yes' to continue: "
                )
            except EOFError:
                logging.error("No input available for confirmation prompt. Aborting clear_database().")
                raise RuntimeError(
                    "No input available for confirmation prompt. Aborting clear_database()."
                )
            
            if confirm.strip().lower() != "yes":
                logging.info("clear_database() aborted by user.")
                return

        # Get database path and close connection
        db_path = getattr(self.conn, "database", DB_PATH)
        logging.info(f"Attempting to delete database file at {db_path}")
        self.close()
        
        # Delete database file if it exists
        if os.path.exists(db_path):
            os.remove(db_path)
            logging.info("Database file deleted.")
        else:
            logging.warning("Database file does not exist.")
        
        # Reconnect and recreate tables
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._create_tables()

    def _create_tables(self) -> None:
        """
        Create database schema if it doesn't already exist.
        
        Schema includes:
        - tracks: Spotify track metadata and download status
        - playlists: Playlist names and IDs
        - playlist_tracks: Many-to-many relationship between playlists and tracks
        - slskd_mapping: Links Soulseek download UUIDs to Spotify track IDs
        """
        logging.info("Creating database tables if they don't exist.")
        cursor = self.conn.cursor()
        
        # Tracks table: stores track metadata and download state
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                spotify_id TEXT PRIMARY KEY,
                track_name TEXT NOT NULL,
                artist TEXT NOT NULL,
                download_status TEXT NOT NULL,
                slskd_file_name TEXT,
                local_file_path TEXT,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Playlists table: stores playlist information
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlists (
                playlist_url TEXT PRIMARY KEY NOT NULL
            )
        """)
        
        # Junction table: many-to-many relationship between playlists and tracks
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist_tracks (
                playlist_url TEXT,
                spotify_id TEXT,
                FOREIGN KEY (playlist_url) REFERENCES playlists(playlist_url),
                FOREIGN KEY (spotify_id) REFERENCES tracks(spotify_id),
                PRIMARY KEY (playlist_url, spotify_id)
            )
        """)
        
        # Mapping table: links Soulseek download UUIDs to Spotify IDs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slskd_mapping (
                slskd_uuid TEXT PRIMARY KEY,
                spotify_id TEXT NOT NULL,
                FOREIGN KEY (spotify_id) REFERENCES tracks(spotify_id)
            )
        """)
        
        self.conn.commit()

    def add_track(
        self,
        spotify_id: str,
        track_name: str,
        artist: str,
        download_status: str = "pending",
        slskd_file_name: Optional[str] = None
    ) -> None:
        """
        Add a track to the database if it doesn't already exist.
        
        Args:
            spotify_id: Unique Spotify track identifier
            track_name: Name of the track
            artist: Artist name(s)
            download_status: Initial download status (default: "pending")
            slskd_file_name: Optional filename from Soulseek download
        
        Note:
            Uses INSERT OR IGNORE to prevent duplicate entries. If the track
            already exists, this operation has no effect.
        """
        logging.info(
            f"Adding track: spotify_id={spotify_id}, track_name={track_name}, "
            f"artist={artist}, status={download_status}"
        )
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO tracks 
            (spotify_id, track_name, artist, download_status, slskd_file_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            (spotify_id, track_name, artist, download_status, slskd_file_name)
        )
        self.conn.commit()

    def add_playlist(self, playlist_url: str) -> int:
        """
        Add a new playlist to the database if it doesn't already exist.
        
        Args:
            playlist_url: Name of the playlist
        
        Returns:
            The database ID of the playlist (existing or newly created)
        """
        logging.info(f"Adding playlist: {playlist_url}")
        cursor = self.conn.cursor()

        # Check if the playlist already exists
        cursor.execute(
            "SELECT rowid FROM playlists WHERE playlist_url = ?",
            (playlist_url,)
        )
        result = cursor.fetchone()

        if result:
            logging.info(f"Playlist already exists: {playlist_url}")
            return result[0]  # Return the existing playlist ID

        # Insert the new playlist
        cursor.execute(
            "INSERT INTO playlists (playlist_url) VALUES (?)",
            (playlist_url,)
        )
        self.conn.commit()
        return cursor.lastrowid

    def link_track_to_playlist(self, spotify_id: str, playlist_url: str) -> None:
        """
        Create an association between a track and a playlist.
        
        Args:
            spotify_id: Spotify track identifier
            playlist_url: Database playlist URL
        
        Note:
            Uses INSERT OR IGNORE to prevent duplicate associations.
            A track can be linked to multiple playlists.
        """
        logging.info(f"Linking track {spotify_id} to playlist {playlist_url}")
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO playlist_tracks (playlist_url, spotify_id) VALUES (?, ?)",
            (playlist_url, spotify_id)
        )
        self.conn.commit()

    def update_track_status(
        self,
        spotify_id: str,
        status: str
    ) -> None:
        """
        Update the download status for a track.
        
        Args:
            spotify_id: Spotify track identifier
            status: New download status (e.g., "pending", "downloading", "completed", "failed")
        """
        logging.info(
            f"Updating track {spotify_id} status to {status}"
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET download_status = ? WHERE spotify_id = ?",
            (status, spotify_id)
        )
        self.conn.commit()

    def update_slskd_file_name(
        self,
        spotify_id: str,
        slskd_file_name: str
    ) -> None:
        """
        Update the Soulseek file name for a track.
        
        Args:
            spotify_id: Spotify track identifier
            slskd_file_name: Soulseek filename to update
        """
        logging.info(
            f"Updating Soulseek file name for track {spotify_id} to {slskd_file_name}"
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_file_name = ? WHERE spotify_id = ?",
            (slskd_file_name, spotify_id)
        )
        self.conn.commit()

    def get_tracks_by_status(self, status: str) -> List[Tuple]:
        """
        Retrieve all tracks with a specific download status.
        
        Args:
            status: Download status to filter by
        
        Returns:
            List of tuples containing all track fields for matching tracks
        """
        logging.info(f"Querying tracks by status: {status}")
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM tracks WHERE download_status = ?",
            (status,)
        )
        return cursor.fetchall()

    def add_slskd_mapping(self, slskd_uuid: str, spotify_id: str) -> None:
        """
        Create a mapping between a Soulseek download UUID and a Spotify track ID.
        
        Args:
            slskd_uuid: Unique identifier from Soulseek download system
            spotify_id: Spotify track identifier
        
        Note:
            Uses INSERT OR IGNORE to prevent duplicate mappings.
        """
        logging.info(f"Adding slskd mapping: slskd_uuid={slskd_uuid}, spotify_id={spotify_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO slskd_mapping (slskd_uuid, spotify_id) VALUES (?, ?)",
            (slskd_uuid, spotify_id)
        )
        self.conn.commit()

    def get_spotify_id_by_slskd_uuid(self, slskd_uuid: str) -> Optional[str]:
        """
        Retrieve the Spotify ID associated with a Soulseek download UUID.
        
        Args:
            slskd_uuid: Soulseek download UUID
        
        Returns:
            Spotify track ID if found, None otherwise
        """
        logging.info(f"Querying Spotify ID for slskd_uuid={slskd_uuid}")
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT spotify_id FROM slskd_mapping WHERE slskd_uuid = ?",
            (slskd_uuid,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_status(self, spotify_id: str) -> Optional[str]:
        """
        Retrieve the download status of a track.
        
        Args:
            spotify_id: Spotify track identifier
        
        Returns:
            Download status string if track exists, None otherwise
        """
        logging.debug(f"Querying track status for spotify_id={spotify_id}")
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT download_status FROM tracks WHERE spotify_id = ?",
            (spotify_id,)
        )
        result = cursor.fetchone()
        status = result[0] if result else None
        logging.info(f"Track status for spotify_id={spotify_id} is {status}")
        return status
    
    def update_local_file_path(self, spotify_id: str, local_file_path: str) -> None:
        """
        Update the local filesystem path for a downloaded track.
        
        Args:
            spotify_id: Spotify track identifier
            local_file_path: Absolute path to the downloaded file
        """
        logging.info(f"Updating local_file_path for {spotify_id} to {local_file_path}")
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET local_file_path = ? WHERE spotify_id = ?",
            (local_file_path, spotify_id)
        )
        self.conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        logging.info("Closing database connection.")
        self.conn.close()
