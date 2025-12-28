"""
Database management module for Spotiseek application.

This module provides a thread-safe singleton interface for managing the SQLite database
that tracks music playlists (Spotify, SoundCloud), tracks, download statuses, and mappings
to Soulseek downloads.
"""

import os
import sqlite3
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar, Optional

if TYPE_CHECKING:
    import pandas as pd

from scripts.logs_utils import write_log

# Get environment configuration (used by TrackDB class)
# Note: Avoid hard-binding to ENV/DB_PATH at import time for long-lived processes.
_IMPORT_ENV = os.getenv("APP_ENV")
_BASE_DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')
_IMPORT_DB_PATH = (
    os.path.join(_BASE_DB_DIR, _IMPORT_ENV, f"database_{_IMPORT_ENV}.db")
    if _IMPORT_ENV else None
)


@dataclass
class TrackData:
    """Data class for track information to reduce function parameters."""
    track_id: str
    track_name: str
    artist: str
    source: str = "spotify"  # 'spotify' or 'soundcloud'
    download_status: str = "pending"
    failed_reason: str | None = None
    slskd_file_name: str | None = None
    extension: str | None = None
    bitrate: int | None = None


class TrackDB:
    """
    Thread-safe singleton database manager for track and playlist management.

    This class implements the Singleton pattern to ensure only one database connection
    exists throughout the application lifecycle. It manages:
    - Track metadata and download status (from Spotify, SoundCloud, etc.)
    - Playlist information and track associations
    - Mappings between track IDs and Soulseek download UUIDs

    Attributes:
        conn: SQLite database connection
    """

    # Maintain one instance per absolute db_path
    _instances: ClassVar[dict] = {}
    _lock = threading.Lock()

    def __new__(cls, db_path: str | None = None):
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

    def __init__(self):
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
        self.conn.execute("PRAGMA journal_mode=WAL").fetchone()
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
                write_log.error(
                    "DB_CLEAR_CONFIRM_FAIL",
                    "No input available for confirmation prompt. Aborting clear_database().",
                    {"ENV": env_now}
                )
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
        - tracks: Track metadata and download status (supports Spotify, SoundCloud, etc.)
        - playlists: Playlist names and IDs
        - playlist_tracks: Many-to-many relationship between playlists and tracks
        - slskd_blacklist: Blacklisted Soulseek download UUIDs
        """
        write_log.info("DB_CREATE_TABLES", "Creating database tables if they don't exist.")
        cursor = self.conn.cursor()

        # Tracks table: stores track metadata, download state, and Soulseek mappings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                track_id TEXT PRIMARY KEY,
                track_name TEXT NOT NULL,
                artist TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'spotify',
                download_status TEXT NOT NULL,
                failed_reason TEXT,
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

        # Add columns if they do not exist (migration for existing DBs)
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
        if "failed_reason" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN failed_reason TEXT")
        if "source" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN source TEXT NOT NULL DEFAULT 'spotify'")


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
                track_id TEXT,
                FOREIGN KEY (playlist_url) REFERENCES playlists(playlist_url),
                FOREIGN KEY (track_id) REFERENCES tracks(track_id),
                PRIMARY KEY (playlist_url, track_id)
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
            ("idx_tracks_track_id", "tracks", "track_id"),
            ("idx_tracks_source", "tracks", "source"),
            ("idx_tracks_search_uuid", "tracks", "slskd_search_uuid"),
            ("idx_tracks_download_uuid", "tracks", "slskd_download_uuid"),
            ("idx_playlist_tracks_playlist_url", "playlist_tracks", "playlist_url"),
            ("idx_playlist_tracks_track_id", "playlist_tracks", "track_id"),
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
        write_log.info(
            "SLSKD_BLACKLIST_ADD",
            "Adding slskd_uuid to blacklist.",
            {"slskd_uuid": slskd_uuid, "reason": reason}
        )
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

    def add_track(self, track_data: TrackData) -> None:
        """
        Add a track to the database if it doesn't already exist.

        Args:
            track_data: TrackData object containing track information

        Note:
            Uses INSERT OR IGNORE to prevent duplicate entries. If the track
            already exists, this operation has no effect.
        """
        cursor = self.conn.cursor()

        # Check if track already exists
        cursor.execute("SELECT 1 FROM tracks WHERE track_id = ?", (track_data.track_id,))
        already_exists = cursor.fetchone() is not None

        if not already_exists:
            write_log.debug(
                "TRACK_ADD", "Adding track.", {
                    "track_id": track_data.track_id,
                    "track_name": track_data.track_name,
                    "artist": track_data.artist,
                    "source": track_data.source,
                    "status": track_data.download_status,
                    "extension": track_data.extension,
                    "bitrate": track_data.bitrate
                }
            )

        cursor.execute(
            """
            INSERT OR IGNORE INTO tracks
              (track_id, track_name, artist, source, download_status,
               failed_reason, slskd_file_name, extension, bitrate)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (track_data.track_id, track_data.track_name, track_data.artist,
               track_data.source, track_data.download_status, track_data.failed_reason,
               track_data.slskd_file_name, track_data.extension, track_data.bitrate)
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
        write_log.debug("PLAYLIST_ADD", "Adding playlist.", {"playlist_url": playlist_url})
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
        write_log.debug(
            "PLAYLIST_M3U8_UPDATE",
            "Updating m3u8_path for playlist.",
            {"playlist_url": playlist_url, "m3u8_path": m3u8_path}
        )
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
        write_log.debug(
            "PLAYLIST_NAME_UPDATE",
            "Updating playlist_name for playlist.",
            {"playlist_url": playlist_url, "playlist_name": playlist_name}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE playlists SET playlist_name = ? WHERE playlist_url = ?",
            (playlist_name, playlist_url)
        )
        self.conn.commit()

    def link_track_to_playlist(self, track_id: str, playlist_url: str) -> None:
        """
        Create an association between a track and a playlist.

        Args:
            track_id: Track identifier
            playlist_url: Database playlist URL

        Note:
            Uses INSERT OR IGNORE to prevent duplicate associations.
            A track can be linked to multiple playlists.
        """
        write_log.debug(
            "TRACK_LINK_PLAYLIST",
            "Linking track to playlist.",
            {"track_id": track_id, "playlist_url": playlist_url}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO playlist_tracks (playlist_url, track_id) VALUES (?, ?)",
            (playlist_url, track_id)
        )
        self.conn.commit()

    def update_track_status(
        self,
        track_id: str,
        status: str,
        failed_reason: str | None = None
    ) -> None:
        """
        Update the download status for a track.

        Args:
            track_id: Track identifier
            status: New download status (e.g., "pending", "downloading", "completed", "failed")
            failed_reason: Optional reason when status is set to "failed"
        """
        context = {"track_id": track_id, "status": status}
        if failed_reason:
            context["failed_reason"] = failed_reason
        write_log.debug("TRACK_STATUS_UPDATE", "Updating track status.", context)
        cursor = self.conn.cursor()
        if status == "failed":
            cursor.execute(
                "UPDATE tracks SET download_status = ?, failed_reason = ? WHERE track_id = ?",
                (status, failed_reason, track_id)
            )
        else:
            cursor.execute(
                "UPDATE tracks SET download_status = ?, failed_reason = NULL WHERE track_id = ?",
                (status, track_id)
            )
        self.conn.commit()

    def update_slskd_file_name(
        self,
        track_id: str,
        slskd_file_name: str
    ) -> None:
        """
        Update the Soulseek file name for a track.
        Only store the last subdirectory and filename (e.g., 'folder/filename.ext' or 'folder\\filename.ext').

        Args:
            track_id: Track identifier
            slskd_file_name: Soulseek filename to update (may be a full or partial path)
        """
        # Normalize path separators
        norm_path = slskd_file_name.replace("/", "\\")
        parts = norm_path.split("\\")
        # Only keep the last two components (subfolder and filename), or just filename if only one
        if len(parts) >= 2:  # noqa: PLR2004
            trimmed = parts[-2] + "\\" + parts[-1]
        elif len(parts) == 1:
            trimmed = parts[0]
        else:
            trimmed = slskd_file_name
        write_log.debug(
            "TRACK_SLSKD_FILENAME_UPDATE", "Updating Soulseek file name for track.", {
                "track_id": track_id,
                "slskd_file_name": trimmed
            }
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_file_name = ? WHERE track_id = ?",
            (trimmed, track_id)
        )
        self.conn.commit()

    def update_extension_bitrate(
        self, track_id: str, extension: str | None = None, bitrate: int | None = None
    ) -> None:
        """
        Update the extension and bitrate for a track.
        Args:
            track_id: Track identifier
            extension: File extension (e.g., 'mp3', 'wav')
            bitrate: Bitrate in kbps (e.g., 320)
        """
        write_log.debug(
            "TRACK_UPDATE_EXT_BITRATE",
            "Updating extension and bitrate for track.",
            {"track_id": track_id, "extension": extension, "bitrate": bitrate}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET extension = ?, bitrate = ? WHERE track_id = ?",
            (extension, bitrate, track_id)
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

    def set_search_uuid(self, track_id: str, slskd_search_uuid: str | None) -> None:
        """
        Set or update the search UUID for a given Spotify track.
        """
        write_log.debug(
            "SLSKD_SEARCH_UUID_SET",
            "Setting search UUID for track.",
            {"track_id": track_id, "slskd_search_uuid": slskd_search_uuid}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_search_uuid = ? WHERE track_id = ?",
            (slskd_search_uuid, track_id)
        )
        self.conn.commit()

    def set_download_uuid(self, track_id: str, slskd_download_uuid: str | None, username: str | None = None) -> None:
        """
        Set or update the download UUID (and optionally username) for a given Spotify track.
        Username is updated only if provided (non-None).
        """
        write_log.debug(
            "SLSKD_DOWNLOAD_UUID_SET",
            "Setting download UUID for track.",
            {"track_id": track_id, "slskd_download_uuid": slskd_download_uuid, "username": username}
        )
        cursor = self.conn.cursor()
        if username is not None:
            cursor.execute(
                "UPDATE tracks SET slskd_download_uuid = ?, username = ? WHERE track_id = ?",
                (slskd_download_uuid, username, track_id)
            )
        else:
            cursor.execute(
                "UPDATE tracks SET slskd_download_uuid = ? WHERE track_id = ?",
                (slskd_download_uuid, track_id)
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

    def get_track_id_by_slskd_search_uuid(self, slskd_uuid: str) -> str | None:
        """
        Retrieve the Spotify ID associated with a Soulseek search UUID.

        Args:
            slskd_uuid: Soulseek search UUID

        Returns:
            Spotify track ID if found, None otherwise
        """
        write_log.debug(
            "SLSKD_QUERY_TRACK_ID",
            "Querying Spotify ID for slskd_search_uuid.",
            {"slskd_uuid": slskd_uuid}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_id FROM tracks WHERE slskd_search_uuid = ?",
            (slskd_uuid,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_id_by_slskd_download_uuid(self, slskd_uuid: str) -> str | None:
        """
        Retrieve the Spotify ID associated with a Soulseek download UUID.

        Args:
            slskd_uuid: Soulseek download UUID

        Returns:
            Spotify track ID if found, None otherwise
        """
        write_log.debug(
            "SLSKD_QUERY_TRACK_ID_DOWNLOAD",
            "Querying Spotify ID for slskd_download_uuid.",
            {"slskd_uuid": slskd_uuid}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_id FROM tracks WHERE slskd_download_uuid = ?",
            (slskd_uuid,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_download_uuid_by_track_id(self, track_id: str) -> str | None:
        """
        Retrieve the Soulseek download UUID associated with a Spotify track ID.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT slskd_download_uuid FROM tracks WHERE track_id = ?",
            (track_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_search_uuid_by_track_id(self, track_id: str) -> str | None:
        """
        Retrieve the Soulseek search UUID associated with a Spotify track ID.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT slskd_search_uuid FROM tracks WHERE track_id = ?",
            (track_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_status(self, track_id: str) -> str | None:
        """
        Retrieve the download status of a track.

        Args:
            track_id: Track identifier

        Returns:
            Download status string if track exists, None otherwise
        """
        write_log.debug("TRACK_STATUS_QUERY", "Querying track status.", {"track_id": track_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT download_status FROM tracks WHERE track_id = ?",
            (track_id,)
        )
        result = cursor.fetchone()
        status = result[0] if result else None
        write_log.debug("TRACK_STATUS_RESULT", "Track status result.", {"track_id": track_id, "status": status})
        return status

    def get_track_extension(self, track_id: str) -> str | None:
        """
        Retrieve the file extension of a track.

        Args:
            track_id: Track identifier

        Returns:
            File extension string if track exists, None otherwise
        """
        write_log.debug("TRACK_EXTENSION_QUERY", "Querying track extension.", {"track_id": track_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT extension FROM tracks WHERE track_id = ?",
            (track_id,)
        )
        result = cursor.fetchone()
        extension = result[0] if result else None
        write_log.debug(
            "TRACK_EXTENSION_RESULT",
            "Track extension result.",
            {"track_id": track_id, "extension": extension}
        )
        return extension

    def get_local_file_path(self, track_id: str) -> str | None:
        """
        Retrieve the local file path of a track.

        Args:
            track_id: Track identifier

        Returns:
            Local file path string if track exists and has one, None otherwise
        """
        write_log.debug("TRACK_LOCAL_PATH_QUERY", "Querying local_file_path for track.", {"track_id": track_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT local_file_path FROM tracks WHERE track_id = ?",
            (track_id,)
        )
        result = cursor.fetchone()
        local_path = result[0] if result else None
        write_log.debug(
            "TRACK_LOCAL_PATH_RESULT",
            "Track local_file_path result.",
            {"track_id": track_id, "local_file_path": local_path}
        )
        return local_path

    def update_local_file_path(self, track_id: str, local_file_path: str) -> None:
        """
        Update the local filesystem path for a downloaded track.

        Args:
            track_id: Track identifier
            local_file_path: Absolute path to the downloaded file
        """
        write_log.debug(
            "TRACK_LOCAL_PATH_UPDATE",
            "Updating local_file_path for track.",
            {"track_id": track_id, "local_file_path": local_file_path}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET local_file_path = ? WHERE track_id = ?",
            (local_file_path, track_id)
        )
        self.conn.commit()

    def get_playlists_for_track(self, track_id: str) -> list:
        """
        Return a list of playlist URLs for a given track_id.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT playlist_url FROM playlist_tracks WHERE track_id = ?", (track_id,))
        return [row[0] for row in cursor.fetchall()]

    def get_all_playlist_urls(self) -> list[str]:
        """Return all playlist URLs currently stored."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT playlist_url FROM playlists")
        return [row[0] for row in cursor.fetchall()]

    def get_track_ids_for_playlist(self, playlist_url: str) -> list[str]:
        """Return track_ids linked to a playlist."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_id FROM playlist_tracks WHERE playlist_url = ?",
            (playlist_url,)
        )
        return [row[0] for row in cursor.fetchall()]

    def unlink_track_from_playlist(self, track_id: str, playlist_url: str) -> None:
        """Remove a track→playlist association."""
        write_log.debug(
            "TRACK_UNLINK_PLAYLIST",
            "Unlinking track from playlist.",
            {"track_id": track_id, "playlist_url": playlist_url}
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM playlist_tracks WHERE playlist_url = ? AND track_id = ?",
            (playlist_url, track_id)
        )
        self.conn.commit()

    def delete_playlist(self, playlist_url: str) -> None:
        """Delete a playlist and all its associations."""
        write_log.info(
            "PLAYLIST_DELETE",
            "Deleting playlist and associations.",
            {"playlist_url": playlist_url}
        )
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM playlist_tracks WHERE playlist_url = ?", (playlist_url,))
        cursor.execute("DELETE FROM playlists WHERE playlist_url = ?", (playlist_url,))
        self.conn.commit()

    def get_playlist_usage_count(self, track_id: str) -> int:
        """Return how many playlists reference a track."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM playlist_tracks WHERE track_id = ?",
            (track_id,)
        )
        result = cursor.fetchone()
        return int(result[0]) if result and result[0] is not None else 0

    def delete_track(self, track_id: str) -> None:
        """Delete a track and its playlist links."""
        write_log.info(
            "TRACK_DELETE",
            "Deleting track and associations.",
            {"track_id": track_id}
        )
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM playlist_tracks WHERE track_id = ?", (track_id,))
        cursor.execute("DELETE FROM tracks WHERE track_id = ?", (track_id,))
        self.conn.commit()

    def get_playlist_tracks_with_metadata(self, playlist_url: str) -> list[tuple[str, str, str, str | None]]:
        """Return track_id, artist, track_name, local_file_path for tracks in a playlist."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT pt.track_id, t.artist, t.track_name, t.local_file_path
            FROM playlist_tracks pt
            JOIN tracks t ON pt.track_id = t.track_id
            WHERE pt.playlist_url = ?
            """,
            (playlist_url,)
        )
        return cursor.fetchall()

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
        import pandas as pd  # noqa: PLC0415
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
        import pandas as pd  # noqa: PLC0415
        conn = sqlite3.connect(db_path)
        query = "SELECT download_status, COUNT(*) as count FROM tracks GROUP BY download_status"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)


def get_failed_reason_breakdown(db_path: str) -> tuple[Optional['pd.DataFrame'], str | None]:
    """
    Retrieve breakdown of reasons why tracks don't have a local_file_path.

    Includes all tracks without a local file path, grouped by download_status and failed_reason.

    Args:
        db_path: Path to the SQLite database file

    Returns:
        Tuple of (DataFrame with download_status, failed_reason, and counts, error message if any)
    """
    try:
        import pandas as pd  # noqa: PLC0415
        conn = sqlite3.connect(db_path)
        query = (
            "SELECT download_status, "
            "COALESCE(NULLIF(failed_reason, ''), 'N/A') AS failed_reason, "
            "COUNT(*) AS count FROM tracks "
            "WHERE local_file_path IS NULL OR TRIM(local_file_path) = '' "
            "GROUP BY download_status, COALESCE(NULLIF(failed_reason, ''), 'N/A') "
            "ORDER BY count DESC"
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)
