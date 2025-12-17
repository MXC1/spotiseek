"""
Database management module for Spotiseek application.

This module provides a thread-safe singleton interface for managing the SQLite database
that tracks Spotify playlists, tracks, download statuses, and mappings to Soulseek downloads.
"""

import os
import sqlite3
import threading
from typing import TYPE_CHECKING, ClassVar, Optional

if TYPE_CHECKING:
    import pandas as pd

# Handle both relative and absolute imports for flexibility
try:
    from scripts.logs_utils import write_log
except ImportError:
    from scripts.logs_utils import write_log

# Get environment configuration (used by TrackDB class)
# Note: Avoid hard-binding to ENV/DB_PATH at import time for long-lived processes.
_IMPORT_ENV = os.getenv("APP_ENV")
_BASE_DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'database')
_IMPORT_DB_PATH = (
    os.path.join(_BASE_DB_DIR, _IMPORT_ENV, f"database_{_IMPORT_ENV}.db")
    if _IMPORT_ENV else None
)


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

    # Maintain one instance per absolute db_path
    _instances: ClassVar[dict] = {}
    _lock = threading.Lock()

    def __new__(cls, db_path: str | None = None, *args, **kwargs):
        """Return a singleton instance keyed by absolute db_path."""
        # Resolve db_path deterministically at construction time
        if db_path is None:
            # Build path from current environment each time, not at import
            env_now = os.getenv("APP_ENV")
            if not env_now:
                raise OSError(
                    "APP_ENV environment variable is not set. Database interaction is disabled."
                )
            db_dir_now = os.path.join(_BASE_DB_DIR, env_now)
            resolved_db_path = os.path.join(db_dir_now, f"database_{env_now}.db")
        else:
            resolved_db_path = os.path.abspath(db_path)

        with cls._lock:
            inst = cls._instances.get(resolved_db_path)
            if inst is None:
                inst = super().__new__(cls)
                inst._initialized = False
                inst.db_path = resolved_db_path
                cls._instances[resolved_db_path] = inst
        return inst

    def __init__(self, db_path: str | None = None):
        """
        Initialize the database connection and create tables if needed.

        Args:
            db_path: Optional path to the SQLite database file. If not provided, constructed from current APP_ENV.

        Note:
            Due to singleton pattern, initialization only happens once per application run.
        """
        if self._initialized:
            return

        # self.db_path is set in __new__; ensure directory exists
        db_dir = os.path.dirname(self.db_path)
        write_log.info("DB_MKDIR", "Creating database directory.", {"db_dir": db_dir})
        os.makedirs(db_dir, exist_ok=True)

        self._initialized = True
        write_log.info("DB_CONNECT", "Connecting to database.", {"db_path": self.db_path})
        # Optimize SQLite connection for performance
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        # Enable write-ahead logging for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        # Optimize query performance
        self.conn.execute("PRAGMA synchronous=NORMAL")
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
        # Production environment safeguard (evaluate at runtime)
        env_now = os.getenv("APP_ENV")
        if env_now == "prod":
            try:
                confirm = input(
                    f"APP_ENV is: {env_now}.\n"
                    "Are you sure you want to delete the database? "
                    "This action cannot be undone. Type 'yes' to continue: "
                )
            except EOFError:
                write_log.error("DB_CLEAR_CONFIRM_FAIL", "No input available for confirmation prompt. Aborting clear_database().", {"ENV": env_now})
                raise RuntimeError(
                    "No input available for confirmation prompt. Aborting clear_database()."
                ) from None

            if confirm.strip().lower() != "yes":
                write_log.info("DB_CLEAR_ABORTED", "clear_database() aborted by user.", {"ENV": env_now})
                return

        # Get database path and close connection
        db_path = self.db_path
        write_log.info("DB_DELETE_ATTEMPT", "Attempting to delete database file.", {"db_path": db_path})
        self.close()

        # Delete database file if it exists
        if os.path.exists(db_path):
            os.remove(db_path)
            write_log.info("DB_DELETED", "Database file deleted.", {"db_path": db_path})
        else:
            write_log.warn("DB_DELETE_MISSING", "Database file does not exist.", {"db_path": db_path})

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
        write_log.info("DB_CREATE_TABLES", "Creating database tables if they don't exist.")
        cursor = self.conn.cursor()

        # Tracks table: stores track metadata, download state, and Soulseek mappings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                spotify_id TEXT PRIMARY KEY,
                track_name TEXT NOT NULL,
                artist TEXT NOT NULL,
                download_status TEXT NOT NULL,
                slskd_file_name TEXT,
                local_file_path TEXT,
                extension TEXT,
                bitrate INTEGER,
                slskd_search_uuid TEXT,
                slskd_download_uuid TEXT,
                username TEXT,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Add extension, bitrate, and slskd columns if they do not exist (migration for existing DBs)
        cursor.execute("PRAGMA table_info(tracks)")
        columns = [row[1] for row in cursor.fetchall()]
        if "extension" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN extension TEXT")
        if "bitrate" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN bitrate INTEGER")
        if "slskd_search_uuid" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN slskd_search_uuid TEXT")
        if "slskd_download_uuid" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN slskd_download_uuid TEXT")
        if "username" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN username TEXT")


        # Playlists table: stores playlist information, m3u8 path, and playlist name
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlists (
                playlist_url TEXT PRIMARY KEY NOT NULL,
                playlist_name TEXT,
                m3u8_path TEXT
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

        # Blacklist table: stores blacklisted slskd_uuids
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slskd_blacklist (
                slskd_uuid TEXT PRIMARY KEY,
                reason TEXT,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes for frequently queried columns (performance optimization)
        # These help queries that filter on local_file_path, download_status, etc.
        indexes = [
            ("idx_tracks_local_file_path", "tracks", "local_file_path"),
            ("idx_tracks_download_status", "tracks", "download_status"),
            ("idx_tracks_spotify_id", "tracks", "spotify_id"),
            ("idx_tracks_search_uuid", "tracks", "slskd_search_uuid"),
            ("idx_tracks_download_uuid", "tracks", "slskd_download_uuid"),
            ("idx_playlist_tracks_playlist_url", "playlist_tracks", "playlist_url"),
            ("idx_playlist_tracks_spotify_id", "playlist_tracks", "spotify_id"),
        ]

        for index_name, table_name, column_name in indexes:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_name})")
            except sqlite3.OperationalError:
                # Index might already exist, which is fine
                pass

        self.conn.commit()

    def add_slskd_blacklist(self, slskd_uuid: str, reason: str | None = None) -> None:
        """
        Add a slskd_uuid to the blacklist table.
        Args:
            slskd_uuid: The Soulseek download UUID to blacklist
            reason: Optional reason for blacklisting
        """
        write_log.info("SLSKD_BLACKLIST_ADD", "Adding slskd_uuid to blacklist.", {"slskd_uuid": slskd_uuid, "reason": reason})
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO slskd_blacklist (slskd_uuid, reason) VALUES (?, ?)",
            (slskd_uuid, reason)
        )
        self.conn.commit()

    def is_slskd_blacklisted(self, slskd_uuid: str) -> bool:
        """
        Check if a slskd_uuid is blacklisted.
        Args:
            slskd_uuid: The Soulseek download UUID to check
        Returns:
            True if blacklisted, False otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM slskd_blacklist WHERE slskd_uuid = ?",
            (slskd_uuid,)
        )
        return cursor.fetchone() is not None

    def add_track(
        self,
        spotify_id: str,
        track_name: str,
        artist: str,
        download_status: str = "pending",
        slskd_file_name: str | None = None,
        extension: str | None = None,
        bitrate: int | None = None
    ) -> None:
        """
        Add a track to the database if it doesn't already exist.
        Args:
            spotify_id: Unique Spotify track identifier
            track_name: Name of the track
            artist: Artist name(s)
            download_status: Initial download status (default: "pending")
            slskd_file_name: Optional filename from Soulseek download
            extension: File extension (e.g., 'mp3', 'wav')
            bitrate: Bitrate in kbps (e.g., 320)
        Note:
            Uses INSERT OR IGNORE to prevent duplicate entries. If the track
            already exists, this operation has no effect.
        """
        cursor = self.conn.cursor()

        # Check if track already exists
        cursor.execute("SELECT 1 FROM tracks WHERE spotify_id = ?", (spotify_id,))
        already_exists = cursor.fetchone() is not None

        if not already_exists:
            write_log.debug(
                "TRACK_ADD", "Adding track.", {
                    "spotify_id": spotify_id,
                    "track_name": track_name,
                    "artist": artist,
                    "status": download_status,
                    "extension": extension,
                    "bitrate": bitrate
                }
            )

        cursor.execute(
            """
            INSERT OR IGNORE INTO tracks
            (spotify_id, track_name, artist, download_status, slskd_file_name, extension, bitrate)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (spotify_id, track_name, artist, download_status, slskd_file_name, extension, bitrate)
        )
        self.conn.commit()

    def add_playlist(self, playlist_url: str, m3u8_path: str | None = None, playlist_name: str | None = None) -> int:
        """
        Add a new playlist to the database if it doesn't already exist.

        Args:
            playlist_url: Name of the playlist
            m3u8_path: Path to the m3u8 file for this playlist
            playlist_name: Name of the playlist from Spotify

        Returns:
            The database ID of the playlist (existing or newly created)
        """
        cursor = self.conn.cursor()

        # Check if the playlist already exists
        cursor.execute(
            "SELECT rowid FROM playlists WHERE playlist_url = ?",
            (playlist_url,)
        )
        result = cursor.fetchone()

        if result:
            cursor.execute(
                "UPDATE playlists SET m3u8_path = ?, playlist_name = ? WHERE playlist_url = ?",
                (m3u8_path, playlist_name, playlist_url)
            )
            self.conn.commit()
            return result[0]  # Return the existing playlist ID

        # Insert the new playlist - only log when actually adding
        write_log.info("PLAYLIST_ADD", "Adding playlist.", {"playlist_url": playlist_url})
        cursor.execute(
            "INSERT INTO playlists (playlist_url, m3u8_path, playlist_name) VALUES (?, ?, ?)",
            (playlist_url, m3u8_path, playlist_name)
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_playlist_m3u8_path(self, playlist_url: str, m3u8_path: str) -> None:
        """
        Update the m3u8_path for a playlist.
        Args:
            playlist_url: Playlist URL
            m3u8_path: Path to the m3u8 file
        """
        write_log.debug("PLAYLIST_M3U8_UPDATE", "Updating m3u8_path for playlist.", {"playlist_url": playlist_url, "m3u8_path": m3u8_path})
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE playlists SET m3u8_path = ? WHERE playlist_url = ?",
            (m3u8_path, playlist_url)
        )
        self.conn.commit()

    def update_playlist_name(self, playlist_url: str, playlist_name: str) -> None:
        """
        Update the playlist_name for a playlist.
        Args:
            playlist_url: Playlist URL
            playlist_name: Name of the playlist from Spotify
        """
        write_log.debug("PLAYLIST_NAME_UPDATE", "Updating playlist_name for playlist.", {"playlist_url": playlist_url, "playlist_name": playlist_name})
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE playlists SET playlist_name = ? WHERE playlist_url = ?",
            (playlist_name, playlist_url)
        )
        self.conn.commit()

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
        write_log.debug("TRACK_LINK_PLAYLIST", "Linking track to playlist.", {"spotify_id": spotify_id, "playlist_url": playlist_url})
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
        write_log.debug(
            "TRACK_STATUS_UPDATE", "Updating track status.", {
                "spotify_id": spotify_id,
                "status": status
            }
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
        Only store the last subdirectory and filename (e.g., 'folder/filename.ext' or 'folder\\filename.ext').

        Args:
            spotify_id: Spotify track identifier
            slskd_file_name: Soulseek filename to update (may be a full or partial path)
        """
        # Normalize path separators
        norm_path = slskd_file_name.replace("/", "\\")
        parts = norm_path.split("\\")
        # Only keep the last two components (subfolder and filename), or just filename if only one
        if len(parts) >= 2:
            trimmed = parts[-2] + "\\" + parts[-1]
        elif len(parts) == 1:
            trimmed = parts[0]
        else:
            trimmed = slskd_file_name
        write_log.info(
            "TRACK_SLSKD_FILENAME_UPDATE", "Updating Soulseek file name for track.", {
                "spotify_id": spotify_id,
                "slskd_file_name": trimmed
            }
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_file_name = ? WHERE spotify_id = ?",
            (trimmed, spotify_id)
        )
        self.conn.commit()

    def update_extension_bitrate(self, spotify_id: str, extension: str | None = None, bitrate: int | None = None) -> None:
        """
        Update the extension and bitrate for a track.
        Args:
            spotify_id: Spotify track identifier
            extension: File extension (e.g., 'mp3', 'wav')
            bitrate: Bitrate in kbps (e.g., 320)
        """
        write_log.debug("TRACK_UPDATE_EXT_BITRATE", "Updating extension and bitrate for track.", {"spotify_id": spotify_id, "extension": extension, "bitrate": bitrate})
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET extension = ?, bitrate = ? WHERE spotify_id = ?",
            (extension, bitrate, spotify_id)
        )
        self.conn.commit()

    def get_tracks_by_status(self, status: str) -> list[tuple]:
        """
        Retrieve all tracks with a specific download status.

        Args:
            status: Download status to filter by

        Returns:
            List of tuples containing all track fields for matching tracks
        """
        write_log.info("TRACKS_QUERY_STATUS", "Querying tracks by status.", {"status": status})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM tracks WHERE download_status = ?",
            (status,)
        )
        return cursor.fetchall()

    def set_search_uuid(self, spotify_id: str, slskd_search_uuid: str | None) -> None:
        """
        Set or update the search UUID for a given Spotify track.
        """
        write_log.debug("SLSKD_SEARCH_UUID_SET", "Setting search UUID for track.", {"spotify_id": spotify_id, "slskd_search_uuid": slskd_search_uuid})
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_search_uuid = ? WHERE spotify_id = ?",
            (slskd_search_uuid, spotify_id)
        )
        self.conn.commit()

    def set_download_uuid(self, spotify_id: str, slskd_download_uuid: str | None, username: str | None = None) -> None:
        """
        Set or update the download UUID (and optionally username) for a given Spotify track.
        Username is updated only if provided (non-None).
        """
        write_log.debug("SLSKD_DOWNLOAD_UUID_SET", "Setting download UUID for track.", {"spotify_id": spotify_id, "slskd_download_uuid": slskd_download_uuid, "username": username})
        cursor = self.conn.cursor()
        if username is not None:
            cursor.execute(
                "UPDATE tracks SET slskd_download_uuid = ?, username = ? WHERE spotify_id = ?",
                (slskd_download_uuid, username, spotify_id)
            )
        else:
            cursor.execute(
                "UPDATE tracks SET slskd_download_uuid = ? WHERE spotify_id = ?",
                (slskd_download_uuid, spotify_id)
            )
        self.conn.commit()

    def get_username_by_slskd_uuid(self, slskd_uuid: str) -> str | None:
        """
        Retrieve the Soulseek username associated with a download UUID.

        Args:
            slskd_uuid: Soulseek download UUID

        Returns:
            Username if found, None otherwise
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT username FROM tracks WHERE slskd_download_uuid = ?",
            (slskd_uuid,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def delete_slskd_mapping(self, slskd_uuid: str) -> None:
        """
        Clear the Soulseek download UUID mapping for a track.

        Args:
            slskd_uuid: Soulseek download UUID to remove
        """
        write_log.debug("SLSKD_MAPPING_DELETE", "Clearing slskd download UUID.", {"slskd_uuid": slskd_uuid})
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_download_uuid = NULL WHERE slskd_download_uuid = ?",
            (slskd_uuid,)
        )
        self.conn.commit()

    def get_spotify_id_by_slskd_search_uuid(self, slskd_uuid: str) -> str | None:
        """
        Retrieve the Spotify ID associated with a Soulseek search UUID.

        Args:
            slskd_uuid: Soulseek search UUID

        Returns:
            Spotify track ID if found, None otherwise
        """
        write_log.debug("SLSKD_QUERY_SPOTIFY_ID", "Querying Spotify ID for slskd_search_uuid.", {"slskd_uuid": slskd_uuid})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT spotify_id FROM tracks WHERE slskd_search_uuid = ?",
            (slskd_uuid,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_spotify_id_by_slskd_download_uuid(self, slskd_uuid: str) -> str | None:
        """
        Retrieve the Spotify ID associated with a Soulseek download UUID.

        Args:
            slskd_uuid: Soulseek download UUID

        Returns:
            Spotify track ID if found, None otherwise
        """
        write_log.debug("SLSKD_QUERY_SPOTIFY_ID_DOWNLOAD", "Querying Spotify ID for slskd_download_uuid.", {"slskd_uuid": slskd_uuid})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT spotify_id FROM tracks WHERE slskd_download_uuid = ?",
            (slskd_uuid,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_download_uuid_by_spotify_id(self, spotify_id: str) -> str | None:
        """
        Retrieve the Soulseek download UUID associated with a Spotify track ID.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT slskd_download_uuid FROM tracks WHERE spotify_id = ?",
            (spotify_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_search_uuid_by_spotify_id(self, spotify_id: str) -> str | None:
        """
        Retrieve the Soulseek search UUID associated with a Spotify track ID.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT slskd_search_uuid FROM tracks WHERE spotify_id = ?",
            (spotify_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_status(self, spotify_id: str) -> str | None:
        """
        Retrieve the download status of a track.

        Args:
            spotify_id: Spotify track identifier

        Returns:
            Download status string if track exists, None otherwise
        """
        write_log.debug("TRACK_STATUS_QUERY", "Querying track status.", {"spotify_id": spotify_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT download_status FROM tracks WHERE spotify_id = ?",
            (spotify_id,)
        )
        result = cursor.fetchone()
        status = result[0] if result else None
        write_log.debug("TRACK_STATUS_RESULT", "Track status result.", {"spotify_id": spotify_id, "status": status})
        return status

    def get_track_extension(self, spotify_id: str) -> str | None:
        """
        Retrieve the file extension of a track.

        Args:
            spotify_id: Spotify track identifier

        Returns:
            File extension string if track exists, None otherwise
        """
        write_log.debug("TRACK_EXTENSION_QUERY", "Querying track extension.", {"spotify_id": spotify_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT extension FROM tracks WHERE spotify_id = ?",
            (spotify_id,)
        )
        result = cursor.fetchone()
        extension = result[0] if result else None
        write_log.debug("TRACK_EXTENSION_RESULT", "Track extension result.", {"spotify_id": spotify_id, "extension": extension})
        return extension

    def get_local_file_path(self, spotify_id: str) -> str | None:
        """
        Retrieve the local file path of a track.

        Args:
            spotify_id: Spotify track identifier

        Returns:
            Local file path string if track exists and has one, None otherwise
        """
        write_log.debug("TRACK_LOCAL_PATH_QUERY", "Querying local_file_path for track.", {"spotify_id": spotify_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT local_file_path FROM tracks WHERE spotify_id = ?",
            (spotify_id,)
        )
        result = cursor.fetchone()
        local_path = result[0] if result else None
        write_log.debug("TRACK_LOCAL_PATH_RESULT", "Track local_file_path result.", {"spotify_id": spotify_id, "local_file_path": local_path})
        return local_path

    def update_local_file_path(self, spotify_id: str, local_file_path: str) -> None:
        """
        Update the local filesystem path for a downloaded track.

        Args:
            spotify_id: Spotify track identifier
            local_file_path: Absolute path to the downloaded file
        """
        write_log.debug("TRACK_LOCAL_PATH_UPDATE", "Updating local_file_path for track.", {"spotify_id": spotify_id, "local_file_path": local_file_path})
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET local_file_path = ? WHERE spotify_id = ?",
            (local_file_path, spotify_id)
        )
        self.conn.commit()

    def get_playlists_for_track(self, spotify_id: str) -> list:
        """
        Return a list of playlist URLs for a given spotify_id.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT playlist_url FROM playlist_tracks WHERE spotify_id = ?", (spotify_id,))
        return [row[0] for row in cursor.fetchall()]

    def get_m3u8_path_for_playlist(self, playlist_url: str) -> str:
        """
        Return the m3u8_path for a given playlist_url, or None if not found.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT m3u8_path FROM playlists WHERE playlist_url = ?", (playlist_url,))
        result = cursor.fetchone()
        return result[0] if result else None

    def close(self) -> None:
        """Close the database connection."""
        write_log.info("DB_CLOSE", "Closing database connection.")
        self.conn.close()

# --- Dashboard Helper Functions ---


def get_playlists(db_path: str) -> tuple[Optional['pd.DataFrame'], str | None]:
    """
    Retrieve all playlists from the database.
    Args:
        db_path: Path to the SQLite database file
    Returns:
        Tuple of (DataFrame with playlists, error message if any)
    """
    try:
        import pandas as pd
        conn = sqlite3.connect(db_path)
        query = "SELECT playlist_name, playlist_url FROM playlists"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)

def get_track_status_breakdown(db_path: str) -> tuple[Optional['pd.DataFrame'], str | None]:
    """
    Retrieve track download status breakdown from the database.
    Args:
        db_path: Path to the SQLite database file
    Returns:
        Tuple of (DataFrame with status breakdown, error message if any)
    """
    try:
        import pandas as pd
        conn = sqlite3.connect(db_path)
        query = "SELECT download_status, COUNT(*) as count FROM tracks GROUP BY download_status"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)
