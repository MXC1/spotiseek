"""Database management module for Spotiseek application.

This module provides a thread-safe singleton interface for managing the SQLite database
that tracks music playlists (Spotify, SoundCloud), tracks, download statuses, and mappings
to Soulseek downloads.
"""

import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, ClassVar, Optional

if TYPE_CHECKING:
    import pandas as pd

from scripts.audit_checks import AUDIT_CHECKS_BY_ID
from scripts.logs_utils import write_log

# Get environment configuration (used by TrackDB class)
# Note: Avoid hard-binding to ENV/DB_PATH at import time for long-lived processes.
_IMPORT_ENV = os.getenv("APP_ENV")
_BASE_DB_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
_IMPORT_DB_PATH = (
    os.path.join(_BASE_DB_DIR, _IMPORT_ENV, f"database_{_IMPORT_ENV}.db")
    if _IMPORT_ENV else None
)


def normalize_slskd_filename(slskd_file_name: str) -> str:
    """Normalize a slskd filename to a consistent format for storage and comparison.

    This function:
    1. Replaces forward slashes with backslashes
    2. Keeps only the last two path components (subfolder + filename)
       or just the filename if there's only one component

    Args:
        slskd_file_name: The raw filename from slskd (may be a full or partial path)

    Returns:
        Normalized filename string

    """
    # Normalize path separators
    norm_path = slskd_file_name.replace("/", "\\")
    parts = norm_path.split("\\")
    # Only keep the last two components (subfolder and filename), or just filename if only one
    if len(parts) >= 2:  # noqa: PLR2004
        return parts[-2] + "\\" + parts[-1]
    if len(parts) == 1:
        return parts[0]
    return slskd_file_name


# --- Database explorer helpers (docs/adr/0008-dashboard-database-explorer.md) -----------------

# Tables the explorer lists first, in this order; any other table follows alphabetically.
_EXPLORER_TABLE_ORDER = (
    "tracks", "playlists", "playlist_tracks", "playlist_folder_memberships",
    "slskd_blacklist", "task_runs", "task_state",
)

# Hard ceiling on rows returned by one browse/audit read, whatever the caller asks for: every
# explorer read is a single bounded fetchall() so it can never hold the shared connection's
# read lock (and so block the workflow's writes) for long.
EXPLORER_MAX_PAGE_SIZE = 100

_SYNCHRONOUS_NAMES = {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}


def _quote_ident(name: str) -> str:
    """Quote an SQL identifier. Only ever called with a name that came out of sqlite_master
    or PRAGMA table_info -- never with raw request input."""
    return '"' + name.replace('"', '""') + '"'


def _escape_like(text: str) -> str:
    """Escape LIKE wildcards so a user's filter text is matched literally (ESCAPE '\\')."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def status_age_seconds(status_changed_at: str | None) -> int | None:
    """Seconds since a status_changed_at value (SQLite CURRENT_TIMESTAMP text, UTC), or None
    if it is missing or unparseable."""
    if not status_changed_at:
        return None
    try:
        changed = datetime.strptime(status_changed_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return max(0, int((datetime.now(timezone.utc) - changed).total_seconds()))


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
    genre: str | None = None


class TrackDB:
    """Thread-safe singleton database manager for track and playlist management.

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
                    "APP_ENV environment variable is not set. Database interaction is disabled.",
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
        """Initialize the database connection and create tables if needed.

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
        # Deliberately NOT WAL: this file lives on a Docker Desktop/WSL2 bind mount
        # (a Windows host directory mounted into Linux containers), which behaves like
        # a network filesystem to SQLite. WAL's cross-process coordination depends on a
        # shared-memory (-shm) file, whose locking isn't reliably supported over mounts
        # like this one - SQLite's own docs warn against WAL there - and it has caused
        # real database corruption when workflow/dashboard/backup (each a separate
        # process/container) touch the same db file concurrently. The classic rollback
        # journal uses plain file locks instead, at the cost of writers briefly
        # blocking readers (the timeout= above lets a blocked connection wait it out
        # instead of erroring immediately).
        self.conn.execute("PRAGMA journal_mode=DELETE").fetchone()
        # FULL (not NORMAL) because DELETE-mode journaling needs the extra fsync to
        # stay crash-safe; NORMAL's relaxed sync is only safe to pair with WAL.
        self.conn.execute("PRAGMA synchronous=FULL")
        self._create_tables()

    def clear_database(self) -> None:
        """Delete the database file and reinitialize with empty tables.

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
                    "This action cannot be undone. Type 'yes' to continue: ",
                )
            except EOFError:
                write_log.error(
                    "DB_CLEAR_CONFIRM_FAIL",
                    "No input available for confirmation prompt. Aborting clear_database().",
                    {"ENV": env_now},
                )
                raise RuntimeError(
                    "No input available for confirmation prompt. Aborting clear_database().",
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

    def _migrate_status_changed_at(self, cursor: sqlite3.Cursor, columns: list[str]) -> None:
        """Add and backfill tracks.status_changed_at: when download_status last changed
        VALUE (see docs/adr/0009-track-status-changed-at.md).

        Idempotent under a concurrent first start: workflow and dashboard both run this at
        startup and `invoke up` starts them together, so both can see the column missing.
        The loser's ALTER fails with "duplicate column name", which only means the winner
        already added it.
        """
        if "status_changed_at" not in columns:
            try:
                cursor.execute("ALTER TABLE tracks ADD COLUMN status_changed_at DATETIME")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    raise
        # ADD COLUMN can't take a CURRENT_TIMESTAMP default, so existing rows (and any row
        # written by pre-migration code during a rollback) are backfilled with "now". Only
        # NULL rows are touched, so re-running this on every start is a no-op.
        cursor.execute("SELECT 1 FROM tracks WHERE status_changed_at IS NULL LIMIT 1")
        if cursor.fetchone():
            cursor.execute("UPDATE tracks SET status_changed_at = CURRENT_TIMESTAMP WHERE status_changed_at IS NULL")
            self.conn.commit()

    def _create_tables(self) -> None:
        """Create database schema if it doesn't already exist.

        Schema includes:
        - tracks: Track metadata and download status (supports Spotify, SoundCloud, etc.)
        - playlists: Playlist names and IDs
        - playlist_tracks: Many-to-many relationship between playlists and tracks
        - slskd_blacklist: Blacklisted username + file name combinations
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
        if "genre" not in columns:
            cursor.execute("ALTER TABLE tracks ADD COLUMN genre TEXT")
        self._migrate_status_changed_at(cursor, columns)


        # Playlists table: stores playlist information, m3u8 path, and playlist name
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlists (
                playlist_url TEXT PRIMARY KEY NOT NULL,
                playlist_name TEXT,
                m3u8_path TEXT
            )
        """)

        # Ensure playlists table has display_order column for ordering by CSV
        cursor.execute("PRAGMA table_info(playlists)")
        playlist_columns = [row[1] for row in cursor.fetchall()]
        if "display_order" not in playlist_columns:
            cursor.execute("ALTER TABLE playlists ADD COLUMN display_order INTEGER")
            self.conn.commit()

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

        # Folder memberships: which folder(s) each playlist sits under in the
        # exported iTunes tree. folder_name = '' means the playlist is at the
        # root. A playlist can have several rows (one per folder + optionally
        # root). Fully derived from the Playlists CSV and rebuilt on each scrape.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist_folder_memberships (
                playlist_url TEXT NOT NULL,
                folder_name TEXT NOT NULL,
                csv_sequence INTEGER NOT NULL,
                PRIMARY KEY (playlist_url, folder_name)
            )
        """)

        # Blacklist table: stores blacklisted username + slskd_file_name combinations
        # Migration: Handle old blacklist schema (slskd_uuid -> username + slskd_file_name)
        cursor.execute("PRAGMA table_info(slskd_blacklist)")
        blacklist_columns = [row[1] for row in cursor.fetchall()]

        if blacklist_columns and "slskd_uuid" in blacklist_columns:
            # Old schema detected - dropping table since UUID-based data cannot be migrated.
            write_log.info(
                "DB_MIGRATE_BLACKLIST",
                "Old blacklist schema detected - dropping table; UUID-based data cannot be migrated.",
            )
            cursor.execute("DROP TABLE slskd_blacklist")

        # Create or recreate blacklist table with new schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slskd_blacklist (
                username TEXT NOT NULL,
                slskd_file_name TEXT NOT NULL,
                reason TEXT,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (username, slskd_file_name)
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
            ("idx_pfm_folder_name", "playlist_folder_memberships", "folder_name"),
        ]

        for index_name, table_name, column_name in indexes:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_name})")
            except sqlite3.OperationalError:
                # Index might already exist, which is fine
                pass

        self.conn.commit()

    def add_slskd_blacklist(self, username: str, slskd_file_name: str, reason: str | None = None) -> None:
        """Add a username + slskd_file_name combination to the blacklist table.

        Args:
            username: The Soulseek username
            slskd_file_name: The file name from slskd (will be normalized before storage)
            reason: Optional reason for blacklisting

        """
        # Normalize filename to ensure consistency
        normalized_filename = normalize_slskd_filename(slskd_file_name)
        write_log.info(
            "SLSKD_BLACKLIST_ADD",
            "Adding username + file to blacklist.",
            {"username": username, "slskd_file_name": normalized_filename, "reason": reason},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO slskd_blacklist (username, slskd_file_name, reason) VALUES (?, ?, ?)",
            (username, normalized_filename, reason),
        )
        self.conn.commit()

    def is_slskd_blacklisted(self, username: str, slskd_file_name: str) -> bool:
        """Check if a username + slskd_file_name combination is blacklisted.

        Args:
            username: The Soulseek username
            slskd_file_name: The file name from slskd (will be normalized before checking)
        Returns:
            True if blacklisted, False otherwise

        """
        # Normalize filename to ensure consistency with stored values
        normalized_filename = normalize_slskd_filename(slskd_file_name)
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM slskd_blacklist WHERE username = ? AND slskd_file_name = ?",
            (username, normalized_filename),
        )
        return cursor.fetchone() is not None

    def add_track(self, track_data: TrackData) -> None:
        """Add a track to the database if it doesn't already exist.

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
                    "bitrate": track_data.bitrate,
                    "genre": track_data.genre,
                },
            )

        cursor.execute(
            """
            INSERT OR IGNORE INTO tracks
              (track_id, track_name, artist, source, download_status,
               failed_reason, slskd_file_name, extension, bitrate, genre, status_changed_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (track_data.track_id, track_data.track_name, track_data.artist,
               track_data.source, track_data.download_status, track_data.failed_reason,
               track_data.slskd_file_name, track_data.extension, track_data.bitrate,
               track_data.genre),
        )
        self.conn.commit()

    def add_playlist(self, playlist_url: str, m3u8_path: str | None = None, playlist_name: str | None = None) -> int:
        """Add a new playlist to the database if it doesn't already exist.

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
            (playlist_url,),
        )
        result = cursor.fetchone()

        if result:
            cursor.execute(
                "UPDATE playlists SET m3u8_path = ?, playlist_name = ? WHERE playlist_url = ?",
                (m3u8_path, playlist_name, playlist_url),
            )
            self.conn.commit()
            return result[0]  # Return the existing playlist ID

        # Insert the new playlist - only log when actually adding
        write_log.debug("PLAYLIST_ADD", "Adding playlist.", {"playlist_url": playlist_url})
        cursor.execute(
            "INSERT INTO playlists (playlist_url, m3u8_path, playlist_name) VALUES (?, ?, ?)",
            (playlist_url, m3u8_path, playlist_name),
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_playlist_m3u8_path(self, playlist_url: str, m3u8_path: str) -> None:
        """Update the m3u8_path for a playlist.

        Args:
            playlist_url: Playlist URL
            m3u8_path: Path to the m3u8 file

        """
        write_log.debug(
            "PLAYLIST_M3U8_UPDATE",
            "Updating m3u8_path for playlist.",
            {"playlist_url": playlist_url, "m3u8_path": m3u8_path},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE playlists SET m3u8_path = ? WHERE playlist_url = ?",
            (m3u8_path, playlist_url),
        )
        self.conn.commit()

    def update_playlist_name(self, playlist_url: str, playlist_name: str) -> None:
        """Update the playlist_name for a playlist.

        Args:
            playlist_url: Playlist URL
            playlist_name: Name of the playlist from Spotify

        """
        write_log.debug(
            "PLAYLIST_NAME_UPDATE",
            "Updating playlist_name for playlist.",
            {"playlist_url": playlist_url, "playlist_name": playlist_name},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE playlists SET playlist_name = ? WHERE playlist_url = ?",
            (playlist_name, playlist_url),
        )
        self.conn.commit()

    def set_playlist_display_order(self, playlist_url: str, display_order: int) -> None:
        """Set or update the display order for a playlist, creating it if needed.

        Args:
            playlist_url: Playlist URL
            display_order: Zero-based order from CSV

        """
        write_log.debug(
            "PLAYLIST_ORDER_SET",
            "Setting display order for playlist.",
            {"playlist_url": playlist_url, "display_order": display_order},
        )
        cursor = self.conn.cursor()
        # Ensure playlist row exists
        cursor.execute(
            "INSERT OR IGNORE INTO playlists (playlist_url) VALUES (?)",
            (playlist_url,),
        )
        # Update display_order
        cursor.execute(
            "UPDATE playlists SET display_order = ? WHERE playlist_url = ?",
            (display_order, playlist_url),
        )
        self.conn.commit()

    def replace_playlist_folder_memberships(
        self, memberships: list[tuple[str, str, int]],
    ) -> None:
        """Replace every playlist -> folder membership row with the given set.

        Args:
            memberships: ``(playlist_url, folder_name, csv_sequence)`` tuples.
                ``folder_name == ""`` places the playlist at the root of the
                exported tree. Memberships are fully derived from the Playlists
                CSV, so the whole table is rebuilt on every call. Repeated
                ``(playlist_url, folder_name)`` pairs collapse to the first seen.

        """
        write_log.info(
            "PLAYLIST_FOLDERS_REPLACE",
            "Rebuilding playlist folder memberships.",
            {"count": len(memberships)},
        )
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM playlist_folder_memberships")
        cursor.executemany(
            "INSERT OR IGNORE INTO playlist_folder_memberships "
            "(playlist_url, folder_name, csv_sequence) VALUES (?, ?, ?)",
            memberships,
        )
        self.conn.commit()

    def get_playlist_folder_memberships(self) -> list[tuple[str, str, int]]:
        """Return ``(playlist_url, folder_name, csv_sequence)`` rows in CSV order.

        ``folder_name == ""`` marks a root-level occurrence. Ordered by
        ``csv_sequence`` so callers can reproduce the CSV's playlist order.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT playlist_url, folder_name, csv_sequence FROM playlist_folder_memberships "
            "ORDER BY csv_sequence, folder_name, playlist_url",
        )
        return cursor.fetchall()

    def link_track_to_playlist(self, track_id: str, playlist_url: str) -> None:
        """Create an association between a track and a playlist.

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
            {"track_id": track_id, "playlist_url": playlist_url},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO playlist_tracks (playlist_url, track_id) VALUES (?, ?)",
            (playlist_url, track_id),
        )
        self.conn.commit()

    def update_track_status(
        self,
        track_id: str,
        status: str,
        failed_reason: str | None = None,
    ) -> None:
        """Update the download status for a track.

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
        # status_changed_at moves only when the status VALUE changes (ADR 0009): the search
        # retry loops re-set the status a track already has, and bumping on those would keep
        # resetting the age of a track that is really stuck. SQLite evaluates every SET
        # expression against the row as it was BEFORE the update, so the CASE below compares
        # the new status with the old one.
        if status == "failed":
            cursor.execute(
                "UPDATE tracks SET "
                "status_changed_at = CASE WHEN download_status IS NOT ? THEN CURRENT_TIMESTAMP "
                "ELSE status_changed_at END, "
                "download_status = ?, failed_reason = ? WHERE track_id = ?",
                (status, status, failed_reason, track_id),
            )
        else:
            cursor.execute(
                "UPDATE tracks SET "
                "status_changed_at = CASE WHEN download_status IS NOT ? THEN CURRENT_TIMESTAMP "
                "ELSE status_changed_at END, "
                "download_status = ?, failed_reason = NULL WHERE track_id = ?",
                (status, status, track_id),
            )
        self.conn.commit()

    def update_slskd_file_name(
        self,
        track_id: str,
        slskd_file_name: str,
    ) -> None:
        """Update the Soulseek file name for a track.
        Only store the last subdirectory and filename (e.g., 'folder/filename.ext' or 'folder\\filename.ext').

        Args:
            track_id: Track identifier
            slskd_file_name: Soulseek filename to update (may be a full or partial path)

        """
        trimmed = normalize_slskd_filename(slskd_file_name)
        write_log.debug(
            "TRACK_SLSKD_FILENAME_UPDATE", "Updating Soulseek file name for track.", {
                "track_id": track_id,
                "slskd_file_name": trimmed,
            },
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_file_name = ? WHERE track_id = ?",
            (trimmed, track_id),
        )
        self.conn.commit()

    def update_extension_bitrate(
        self, track_id: str, extension: str | None = None, bitrate: int | None = None,
    ) -> None:
        """Update the extension and bitrate for a track.

        Args:
            track_id: Track identifier
            extension: File extension (e.g., 'mp3', 'wav')
            bitrate: Bitrate in kbps (e.g., 320)

        """
        write_log.debug(
            "TRACK_UPDATE_EXT_BITRATE",
            "Updating extension and bitrate for track.",
            {"track_id": track_id, "extension": extension, "bitrate": bitrate},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET extension = ?, bitrate = ? WHERE track_id = ?",
            (extension, bitrate, track_id),
        )
        self.conn.commit()

    def get_tracks_by_status(self, status: str) -> list[tuple]:
        """Retrieve all tracks with a specific download status.

        Args:
            status: Download status to filter by

        Returns:
            List of tuples containing all track fields for matching tracks

        """
        write_log.info("TRACKS_QUERY_STATUS", "Querying tracks by status.", {"status": status})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM tracks WHERE download_status = ?",
            (status,),
        )
        return cursor.fetchall()

    def set_search_uuid(self, track_id: str, slskd_search_uuid: str | None) -> None:
        """Set or update the search UUID for a given Spotify track.
        """
        write_log.debug(
            "SLSKD_SEARCH_UUID_SET",
            "Setting search UUID for track.",
            {"track_id": track_id, "slskd_search_uuid": slskd_search_uuid},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_search_uuid = ? WHERE track_id = ?",
            (slskd_search_uuid, track_id),
        )
        self.conn.commit()

    def set_download_uuid(self, track_id: str, slskd_download_uuid: str | None, username: str | None = None) -> None:
        """Set or update the download UUID (and optionally username) for a given Spotify track.
        Username is updated only if provided (non-None).
        """
        write_log.debug(
            "SLSKD_DOWNLOAD_UUID_SET",
            "Setting download UUID for track.",
            {"track_id": track_id, "slskd_download_uuid": slskd_download_uuid, "username": username},
        )
        cursor = self.conn.cursor()
        if username is not None:
            cursor.execute(
                "UPDATE tracks SET slskd_download_uuid = ?, username = ? WHERE track_id = ?",
                (slskd_download_uuid, username, track_id),
            )
        else:
            cursor.execute(
                "UPDATE tracks SET slskd_download_uuid = ? WHERE track_id = ?",
                (slskd_download_uuid, track_id),
            )
        self.conn.commit()

    def get_username_by_slskd_uuid(self, slskd_uuid: str) -> str | None:
        """Retrieve the Soulseek username associated with a download UUID.

        Args:
            slskd_uuid: Soulseek download UUID

        Returns:
            Username if found, None otherwise

        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT username FROM tracks WHERE slskd_download_uuid = ?",
            (slskd_uuid,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def delete_slskd_mapping(self, slskd_uuid: str) -> None:
        """Clear the Soulseek download UUID mapping for a track.

        Args:
            slskd_uuid: Soulseek download UUID to remove

        """
        write_log.debug("SLSKD_MAPPING_DELETE", "Clearing slskd download UUID.", {"slskd_uuid": slskd_uuid})
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET slskd_download_uuid = NULL WHERE slskd_download_uuid = ?",
            (slskd_uuid,),
        )
        self.conn.commit()

    def get_track_id_by_slskd_search_uuid(self, slskd_uuid: str) -> str | None:
        """Retrieve the Spotify ID associated with a Soulseek search UUID.

        Args:
            slskd_uuid: Soulseek search UUID

        Returns:
            Spotify track ID if found, None otherwise

        """
        write_log.debug(
            "SLSKD_QUERY_TRACK_ID",
            "Querying Spotify ID for slskd_search_uuid.",
            {"slskd_uuid": slskd_uuid},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_id FROM tracks WHERE slskd_search_uuid = ?",
            (slskd_uuid,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_id_by_slskd_download_uuid(self, slskd_uuid: str) -> str | None:
        """Retrieve the Spotify ID associated with a Soulseek download UUID.

        Args:
            slskd_uuid: Soulseek download UUID

        Returns:
            Spotify track ID if found, None otherwise

        """
        write_log.debug(
            "SLSKD_QUERY_TRACK_ID_DOWNLOAD",
            "Querying Spotify ID for slskd_download_uuid.",
            {"slskd_uuid": slskd_uuid},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_id FROM tracks WHERE slskd_download_uuid = ?",
            (slskd_uuid,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_download_uuid_by_track_id(self, track_id: str) -> str | None:
        """Retrieve the Soulseek download UUID associated with a Spotify track ID.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT slskd_download_uuid FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_search_uuid_by_track_id(self, track_id: str) -> str | None:
        """Retrieve the Soulseek search UUID associated with a Spotify track ID.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT slskd_search_uuid FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_status(self, track_id: str) -> str | None:
        """Retrieve the download status of a track.

        Args:
            track_id: Track identifier

        Returns:
            Download status string if track exists, None otherwise

        """
        write_log.debug("TRACK_STATUS_QUERY", "Querying track status.", {"track_id": track_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT download_status FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        status = result[0] if result else None
        write_log.debug("TRACK_STATUS_RESULT", "Track status result.", {"track_id": track_id, "status": status})
        return status

    def get_track_extension(self, track_id: str) -> str | None:
        """Retrieve the file extension of a track.

        Args:
            track_id: Track identifier

        Returns:
            File extension string if track exists, None otherwise

        """
        write_log.debug("TRACK_EXTENSION_QUERY", "Querying track extension.", {"track_id": track_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT extension FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        extension = result[0] if result else None
        write_log.debug(
            "TRACK_EXTENSION_RESULT",
            "Track extension result.",
            {"track_id": track_id, "extension": extension},
        )
        return extension

    def get_track_bitrate(self, track_id: str) -> int | None:
        """Retrieve the bitrate of a track in kbps.

        Args:
            track_id: Track identifier

        Returns:
            Bitrate in kbps if track exists, None otherwise

        """
        write_log.debug("TRACK_BITRATE_QUERY", "Querying track bitrate.", {"track_id": track_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT bitrate FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        bitrate = result[0] if result else None
        write_log.debug(
            "TRACK_BITRATE_RESULT",
            "Track bitrate result.",
            {"track_id": track_id, "bitrate": bitrate},
        )
        return bitrate

    def get_track_genre(self, track_id: str) -> str | None:
        """Retrieve the genre of a track.

        Args:
            track_id: Track identifier

        Returns:
            Genre string if track exists and has one, None otherwise

        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT genre FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_artist(self, track_id: str) -> str | None:
        """Retrieve the artist name of a track.

        Args:
            track_id: Track identifier

        Returns:
            Artist name if track exists, None otherwise

        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT artist FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_track_name(self, track_id: str) -> str | None:
        """Retrieve the track name.

        Args:
            track_id: Track identifier

        Returns:
            Track name if track exists, None otherwise

        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_name FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_local_file_path(self, track_id: str) -> str | None:
        """Retrieve the local file path of a track.

        Args:
            track_id: Track identifier

        Returns:
            Local file path string if track exists and has one, None otherwise

        """
        write_log.debug("TRACK_LOCAL_PATH_QUERY", "Querying local_file_path for track.", {"track_id": track_id})
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT local_file_path FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        local_path = result[0] if result else None
        write_log.debug(
            "TRACK_LOCAL_PATH_RESULT",
            "Track local_file_path result.",
            {"track_id": track_id, "local_file_path": local_path},
        )
        return local_path

    def get_username_by_track_id(self, track_id: str) -> str | None:
        """Retrieve the Soulseek username associated with a track.

        Args:
            track_id: Track identifier

        Returns:
            Username if found, None otherwise

        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT username FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def get_slskd_file_name_by_track_id(self, track_id: str) -> str | None:
        """Retrieve the slskd file name associated with a track.

        Args:
            track_id: Track identifier

        Returns:
            slskd file name if found, None otherwise

        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT slskd_file_name FROM tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return result[0] if result else None

    def update_local_file_path(self, track_id: str, local_file_path: str) -> None:
        """Update the local filesystem path for a downloaded track.

        Args:
            track_id: Track identifier
            local_file_path: Absolute path to the downloaded file

        """
        write_log.debug(
            "TRACK_LOCAL_PATH_UPDATE",
            "Updating local_file_path for track.",
            {"track_id": track_id, "local_file_path": local_file_path},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE tracks SET local_file_path = ? WHERE track_id = ?",
            (local_file_path, track_id),
        )
        self.conn.commit()

    def get_playlists_for_track(self, track_id: str) -> list:
        """Return a list of playlist URLs for a given track_id.
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
            (playlist_url,),
        )
        return [row[0] for row in cursor.fetchall()]

    def unlink_track_from_playlist(self, track_id: str, playlist_url: str) -> None:
        """Remove a track→playlist association."""
        write_log.debug(
            "TRACK_UNLINK_PLAYLIST",
            "Unlinking track from playlist.",
            {"track_id": track_id, "playlist_url": playlist_url},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM playlist_tracks WHERE playlist_url = ? AND track_id = ?",
            (playlist_url, track_id),
        )
        self.conn.commit()

    def delete_playlist(self, playlist_url: str) -> None:
        """Delete a playlist and all its associations."""
        write_log.info(
            "PLAYLIST_DELETE",
            "Deleting playlist and associations.",
            {"playlist_url": playlist_url},
        )
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM playlist_tracks WHERE playlist_url = ?", (playlist_url,))
        cursor.execute("DELETE FROM playlist_folder_memberships WHERE playlist_url = ?", (playlist_url,))
        cursor.execute("DELETE FROM playlists WHERE playlist_url = ?", (playlist_url,))
        self.conn.commit()

    def get_playlist_usage_count(self, track_id: str) -> int:
        """Return how many playlists reference a track."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM playlist_tracks WHERE track_id = ?",
            (track_id,),
        )
        result = cursor.fetchone()
        return int(result[0]) if result and result[0] is not None else 0

    def delete_track(self, track_id: str) -> None:
        """Delete a track and its playlist links."""
        write_log.info(
            "TRACK_DELETE",
            "Deleting track and associations.",
            {"track_id": track_id},
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
            (playlist_url,),
        )
        return cursor.fetchall()

    def get_m3u8_path_for_playlist(self, playlist_url: str) -> str:
        """Return the m3u8_path for a given playlist_url, or None if not found.
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT m3u8_path FROM playlists WHERE playlist_url = ?", (playlist_url,))
        result = cursor.fetchone()
        return result[0] if result else None

    # --- dashboard Overall Stats queries ---
    # Routed through the singleton connection instead of an ad-hoc sqlite3.connect,
    # per docs/adr/0003-dashboard-rewrite-fastapi-htmx.md.

    def get_playlists(self) -> list[tuple[str, str]]:
        """Return (playlist_name, playlist_url) for every playlist, in CSV/display order."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT playlist_name, playlist_url FROM playlists "
            "ORDER BY display_order IS NULL, display_order",
        )
        return cursor.fetchall()

    def get_track_status_breakdown(self) -> list[tuple[str, int]]:
        """Return (download_status, count) for every status value in the tracks table."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT download_status, COUNT(*) FROM tracks GROUP BY download_status")
        return cursor.fetchall()

    def get_download_status_breakdown(self) -> list[tuple[str, int]]:
        """Return ("Downloaded"|"Not Downloaded", count); NULL/blank paths count as not downloaded."""
        # GROUP BY repeats the CASE expression rather than referencing the "local_file_status"
        # alias: aliasing it "download_status" once collided with the tracks table's own
        # download_status column, so SQLite grouped by the granular per-track status instead
        # of this collapsed label.
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                CASE WHEN local_file_path IS NOT NULL AND TRIM(local_file_path) != ''
                     THEN 'Downloaded' ELSE 'Not Downloaded' END AS local_file_status,
                COUNT(*)
            FROM tracks
            GROUP BY
                CASE WHEN local_file_path IS NOT NULL AND TRIM(local_file_path) != ''
                     THEN 'Downloaded' ELSE 'Not Downloaded' END
            ORDER BY COUNT(*) DESC
            """,
        )
        return cursor.fetchall()

    def get_extension_breakdown(self) -> list[tuple[str, int]]:
        """Return (extension, count) for tracks that have a local file, most common first."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT extension, COUNT(*) FROM tracks "
            "WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != '' "
            "GROUP BY extension ORDER BY COUNT(*) DESC",
        )
        return cursor.fetchall()

    def get_tracks_with_local_files(self) -> list[tuple[str | None, int | None, str]]:
        """Return (extension, bitrate, local_file_path) for every track with a local file.

        Raw rows, not a GROUP BY: the enhanced bitrate breakdown needs a per-file
        effective-bitrate computation (from file size/duration) that SQL can't express.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT extension, bitrate, local_file_path FROM tracks "
            "WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != ''",
        )
        return cursor.fetchall()

    def get_failed_reason_breakdown(self) -> list[tuple[str, str, int]]:
        """Return (download_status, failed_reason, count) for tracks with no local file."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT download_status, COALESCE(NULLIF(failed_reason, ''), 'N/A'), COUNT(*) "
            "FROM tracks WHERE local_file_path IS NULL OR TRIM(local_file_path) = '' "
            "GROUP BY download_status, COALESCE(NULLIF(failed_reason, ''), 'N/A') "
            "ORDER BY COUNT(*) DESC",
        )
        return cursor.fetchall()

    def get_playlists_with_incomplete_counts(self) -> list[tuple[str, str, int]]:
        """Return (playlist_name, playlist_url, incomplete_count) for playlists that
        have at least one track missing a local file, most-incomplete first."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT p.playlist_name, p.playlist_url, COUNT(*) AS incomplete_count
            FROM playlists p
            JOIN playlist_tracks pt ON p.playlist_url = pt.playlist_url
            JOIN tracks t ON t.track_id = pt.track_id
            WHERE t.local_file_path IS NULL OR TRIM(t.local_file_path) = ''
            GROUP BY p.playlist_name, p.playlist_url
            ORDER BY incomplete_count DESC, p.playlist_name
            """,
        )
        return cursor.fetchall()

    def get_folders_with_incomplete_counts(self) -> list[tuple[str, int]]:
        """Return (folder_name, incomplete_count) for folders with at least one member
        playlist track missing a local file, most-incomplete first.

        The count is unique tracks across the folder's member playlists (the folder's
        union, like its master playlist), so a track in two member playlists counts once.
        Root-level memberships (folder_name = '') are not folders and are excluded.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT pfm.folder_name, COUNT(DISTINCT t.track_id) AS incomplete_count
            FROM playlist_folder_memberships pfm
            JOIN playlist_tracks pt ON pt.playlist_url = pfm.playlist_url
            JOIN tracks t ON t.track_id = pt.track_id
            WHERE pfm.folder_name != ''
              AND (t.local_file_path IS NULL OR TRIM(t.local_file_path) = '')
            GROUP BY pfm.folder_name
            ORDER BY incomplete_count DESC, pfm.folder_name
            """,
        )
        return cursor.fetchall()

    def get_total_incomplete_tracks(self) -> int:
        """Return the count of tracks without a local file (unique tracks, not playlist rows)."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM tracks WHERE local_file_path IS NULL OR TRIM(local_file_path) = ''",
        )
        return cursor.fetchone()[0]

    def _get_incomplete_tracks_page(
        self, membership_filter: str, membership_params: list[str],
        search: str | None, offset: int, limit: int,
    ) -> tuple[list[tuple[str, str, str, str]], int]:
        """Paginated (track_id, track_name, artist, status) rows missing a local file.

        ``membership_filter`` is an optional ``AND ...`` SQL fragment over ``t`` that
        narrows the tracks to a playlist/folder (empty for every track); it is matched
        with ``IN (subquery)`` so a track reachable through several playlists is one row.
        Returns (rows, total_count_before_pagination).
        """
        where = "(t.local_file_path IS NULL OR TRIM(t.local_file_path) = '')" + membership_filter
        params = list(membership_params)
        if search:
            where += " AND (LOWER(t.track_name) LIKE ? OR LOWER(t.artist) LIKE ?)"
            like = f"%{search.lower()}%"
            params.extend([like, like])

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM tracks t WHERE {where}", params)
        total = cursor.fetchall()[0][0]

        cursor.execute(
            "SELECT t.track_id, t.track_name, t.artist, t.download_status "
            f"FROM tracks t WHERE {where} ORDER BY t.track_name LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        return cursor.fetchall(), total

    def get_incomplete_tracks(
        self, search: str | None, offset: int, limit: int,
    ) -> tuple[list[tuple[str, str, str, str]], int]:
        """Paginated (track_id, track_name, artist, status) rows for every track missing a
        local file, whether or not it belongs to a playlist, optionally filtered by
        artist/track substring. Returns (rows, total_count_before_pagination).
        """
        return self._get_incomplete_tracks_page("", [], search, offset, limit)

    def get_incomplete_tracks_for_playlist(
        self, playlist_url: str, search: str | None, offset: int, limit: int,
    ) -> tuple[list[tuple[str, str, str, str]], int]:
        """Paginated (track_id, track_name, artist, status) rows missing a local file for
        one playlist, optionally filtered by artist/track substring.

        Returns (rows, total_count_before_pagination).
        """
        return self._get_incomplete_tracks_page(
            " AND t.track_id IN (SELECT track_id FROM playlist_tracks WHERE playlist_url = ?)",
            [playlist_url], search, offset, limit,
        )

    def get_incomplete_tracks_for_folder(
        self, folder_name: str, search: str | None, offset: int, limit: int,
    ) -> tuple[list[tuple[str, str, str, str]], int]:
        """Paginated (track_id, track_name, artist, status) rows missing a local file
        across every playlist in one folder (unique tracks), optionally filtered by
        artist/track substring. Returns (rows, total_count_before_pagination).
        """
        return self._get_incomplete_tracks_page(
            " AND t.track_id IN (SELECT pt.track_id FROM playlist_tracks pt "
            "JOIN playlist_folder_memberships pfm ON pfm.playlist_url = pt.playlist_url "
            "WHERE pfm.folder_name = ?)",
            [folder_name], search, offset, limit,
        )

    def get_all_incomplete_tracks_with_playlists(self) -> list[tuple[str, str, str, str, str]]:
        """Return (track_id, track_name, artist, download_status, playlists) for every
        track missing a local file, with playlists as a comma-joined display string."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT t.track_id, t.track_name, t.artist, t.download_status,
                   GROUP_CONCAT(p.playlist_name, ', ') AS playlists
            FROM tracks t
            LEFT JOIN playlist_tracks pt ON t.track_id = pt.track_id
            LEFT JOIN playlists p ON pt.playlist_url = p.playlist_url
            WHERE t.local_file_path IS NULL OR TRIM(t.local_file_path) = ''
            GROUP BY t.track_id, t.track_name, t.artist, t.download_status
            ORDER BY t.artist, t.track_name
            """,
        )
        return cursor.fetchall()

    def search_completed_tracks(
        self, search: str | None, offset: int, limit: int,
    ) -> tuple[list[tuple[str, str, str, str, str, int, str, str]], int]:
        """Paginated (track_id, track_name, artist, local_file_path, extension, bitrate,
        username, slskd_file_name) rows for tracks that have a local file, optionally
        filtered by artist/track substring. Returns (rows, total_count_before_pagination).
        """
        cursor = self.conn.cursor()
        where_search = ""
        params: list[str] = []
        if search:
            where_search = " AND (LOWER(track_name) LIKE ? OR LOWER(artist) LIKE ?)"
            like = f"%{search.lower()}%"
            params.extend([like, like])

        cursor.execute(
            "SELECT COUNT(*) FROM tracks "
            "WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != ''" + where_search,
            params,
        )
        total = cursor.fetchone()[0]

        cursor.execute(
            "SELECT track_id, track_name, artist, local_file_path, extension, bitrate, "
            "username, slskd_file_name FROM tracks "
            "WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != ''"
            + where_search + " ORDER BY artist, track_name LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        return cursor.fetchall(), total

    def get_completed_track_by_id(self, track_id: str) -> tuple[str, str, str, str, str, int, str, str] | None:
        """Return the same columns as search_completed_tracks for one track_id, or None
        if it has no local file (already blacklisted/never downloaded/unknown id)."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_id, track_name, artist, local_file_path, extension, bitrate, "
            "username, slskd_file_name FROM tracks "
            "WHERE track_id = ? AND local_file_path IS NOT NULL AND TRIM(local_file_path) != ''",
            (track_id,),
        )
        return cursor.fetchone()

    def clear_track_download_metadata(self, track_id: str) -> None:
        """Null out a track's local_file_path/bitrate/extension/username/slskd_file_name
        -- used when blacklisting a track, to fully reset it for a fresh search."""
        write_log.debug(
            "TRACK_DOWNLOAD_METADATA_CLEAR", "Clearing download metadata for track.",
            {"track_id": track_id},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE tracks
            SET local_file_path = NULL, bitrate = NULL, extension = NULL,
                username = NULL, slskd_file_name = NULL
            WHERE track_id = ?
            """,
            (track_id,),
        )
        self.conn.commit()

    def restore_track_download_metadata(  # noqa: PLR0913, PLR0917
        self, track_id: str, local_file_path: str | None, bitrate: int | None, extension: str | None,
        username: str | None, slskd_file_name: str | None, download_status: str,
    ) -> None:
        """Restore a track's download fields -- used to roll back clear_track_download_metadata()
        if a later step in blacklisting fails."""
        write_log.warn(
            "TRACK_DOWNLOAD_METADATA_RESTORE", "Rolling back download metadata for track.",
            {"track_id": track_id},
        )
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE tracks
            SET local_file_path = ?, bitrate = ?, extension = ?,
                username = ?, slskd_file_name = ?,
                status_changed_at = CASE WHEN download_status IS NOT ? THEN CURRENT_TIMESTAMP
                                         ELSE status_changed_at END,
                download_status = ?
            WHERE track_id = ?
            """,
            (local_file_path, bitrate, extension, username, slskd_file_name,
             download_status, download_status, track_id),
        )
        self.conn.commit()

    # ------------------------------------------------------------------------------------
    # Database explorer -- strictly read-only reads for the dashboard's Database tab
    # (docs/adr/0008-dashboard-database-explorer.md).
    #
    # Guardrails, because self.conn is shared across the dashboard's request threads and
    # its own blacklist/import writes, and every open read blocks the workflow's commits:
    # each method is one bounded fetchall() with no cursor left open, and none of them
    # commits, rolls back, or changes a pragma or row_factory on the connection.
    # ------------------------------------------------------------------------------------

    def _explorer_table_names(self) -> list[str]:
        """Every user table, in the explorer's display order (auto-discovered, so a table
        added later shows up without a code change)."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'",
        )
        names = [row[0] for row in cursor.fetchall()]
        rank = {name: i for i, name in enumerate(_EXPLORER_TABLE_ORDER)}
        return sorted(names, key=lambda name: (rank.get(name, len(rank)), name))

    def list_tables(self) -> list[tuple[str, int]]:
        """Return (table_name, row_count) for every table, in display order."""
        cursor = self.conn.cursor()
        result = []
        for name in self._explorer_table_names():
            cursor.execute(f"SELECT COUNT(*) FROM {_quote_ident(name)}")
            result.append((name, cursor.fetchone()[0]))
        return result

    def get_table_schema(self, table: str) -> dict | None:
        """Return a table's columns, declared foreign keys and indexes, or None if `table`
        isn't a table in this database (which is what keeps request input out of SQL)."""
        if table not in self._explorer_table_names():
            return None
        cursor = self.conn.cursor()
        cursor.execute('SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info(?) ORDER BY cid', (table,))
        columns = [
            {"name": name, "type": col_type, "notnull": bool(notnull), "default": default, "pk": pk}
            for name, col_type, notnull, default, pk in cursor.fetchall()
        ]
        cursor.execute('SELECT "from", "table", "to" FROM pragma_foreign_key_list(?)', (table,))
        foreign_keys = [
            {"column": from_col, "ref_table": ref_table, "ref_column": to_col}
            for from_col, ref_table, to_col in cursor.fetchall()
        ]
        cursor.execute('SELECT name, "unique" FROM pragma_index_list(?) ORDER BY name', (table,))
        index_rows = cursor.fetchall()
        indexes = []
        for index_name, is_unique in index_rows:
            cursor.execute("SELECT name FROM pragma_index_info(?) ORDER BY seqno", (index_name,))
            indexes.append({
                "name": index_name,
                "unique": bool(is_unique),
                "columns": [row[0] for row in cursor.fetchall()],
            })
        return {"name": table, "columns": columns, "foreign_keys": foreign_keys, "indexes": indexes}

    def browse_table(  # noqa: PLR0913
        self,
        table: str,
        *,
        offset: int = 0,
        limit: int = 25,
        sort: str | None = None,
        descending: bool = False,
        filters: dict[str, str] | None = None,
    ) -> dict | None:
        """Return one page of a table, or None if `table` doesn't exist.

        The table and every column name are validated against the schema (unknown sort or
        filter columns are ignored, never interpolated); filter text is matched as a
        case-insensitive substring via a bound LIKE parameter. `limit` is clamped to
        EXPLORER_MAX_PAGE_SIZE. Ordering always ends in the primary key so paging is stable.
        """
        schema = self.get_table_schema(table)
        if schema is None:
            return None
        columns = [col["name"] for col in schema["columns"]]
        limit = max(1, min(int(limit), EXPLORER_MAX_PAGE_SIZE))
        offset = max(0, int(offset))

        clauses: list[str] = []
        params: list = []
        for column, text in (filters or {}).items():
            if column in columns and text:
                clauses.append(f"CAST({_quote_ident(column)} AS TEXT) LIKE ? ESCAPE '\\'")
                params.append(f"%{_escape_like(text)}%")
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""

        pk_columns = [col["name"] for col in sorted(schema["columns"], key=lambda c: c["pk"]) if col["pk"]]
        tiebreak = [_quote_ident(c) for c in pk_columns] or ["rowid"]
        order_terms = []
        if sort in columns:
            order_terms.append(f"{_quote_ident(sort)} {'DESC' if descending else 'ASC'}")
            tiebreak = [t for t in tiebreak if t != _quote_ident(sort)]
        order = " ORDER BY " + ", ".join([*order_terms, *tiebreak])

        table_sql = _quote_ident(table)
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_sql}{where}", params)
        total = cursor.fetchone()[0]
        select_list = ", ".join(_quote_ident(c) for c in columns)
        cursor.execute(
            f"SELECT {select_list} FROM {table_sql}{where}{order} LIMIT ? OFFSET ?",
            [*params, limit, offset],
        )
        rows = cursor.fetchall()
        return {
            "columns": columns, "rows": rows, "total": total,
            "limit": limit, "offset": offset, "schema": schema,
        }

    def get_track_detail(self, track_id: str) -> dict | None:
        """Return everything the explorer's track detail view shows that comes from the
        database: the full row, its playlists (with folders), matching blacklist entries and
        how long it has been in its current status. None if there's no such track."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM tracks WHERE track_id = ?", (track_id,))
        row = cursor.fetchone()
        if row is None:
            return None
        track = dict(zip([col[0] for col in cursor.description], row, strict=True))

        # LEFT JOIN so a playlist_tracks row pointing at a missing playlist still shows up
        # (flagged) -- surfacing that kind of dangling link is the point of the view.
        cursor.execute(
            """
            SELECT pt.playlist_url, p.playlist_name, p.m3u8_path, p.playlist_url IS NOT NULL
            FROM playlist_tracks pt
            LEFT JOIN playlists p ON p.playlist_url = pt.playlist_url
            WHERE pt.track_id = ?
            ORDER BY p.display_order IS NULL, p.display_order, pt.playlist_url
            """,
            (track_id,),
        )
        playlist_rows = cursor.fetchall()
        playlists = []
        for playlist_url, playlist_name, m3u8_path, exists in playlist_rows:
            cursor.execute(
                "SELECT folder_name FROM playlist_folder_memberships WHERE playlist_url = ? ORDER BY csv_sequence",
                (playlist_url,),
            )
            playlists.append({
                "playlist_url": playlist_url,
                "playlist_name": playlist_name,
                "m3u8_path": m3u8_path,
                "in_playlists_table": bool(exists),
                "folders": [r[0] for r in cursor.fetchall()],  # '' means the root
            })

        blacklist_entries: list[dict] = []
        if track.get("username") and track.get("slskd_file_name"):
            cursor.execute(
                "SELECT username, slskd_file_name, reason, added_at FROM slskd_blacklist "
                "WHERE username = ? AND slskd_file_name = ?",
                (track["username"], normalize_slskd_filename(track["slskd_file_name"])),
            )
            blacklist_entries = [
                {"username": u, "slskd_file_name": f, "reason": r, "added_at": a}
                for u, f, r, a in cursor.fetchall()
            ]

        return {
            "track": track,
            "playlists": playlists,
            "blacklist_entries": blacklist_entries,
            "status_age_seconds": status_age_seconds(track.get("status_changed_at")),
        }

    def count_audit_check(self, check_id: str) -> int | None:
        """Number of rows a DB audit check flags, or None for an unknown check id."""
        check = AUDIT_CHECKS_BY_ID.get(check_id)
        if check is None:
            return None
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM ({check.sql})", check.params)
        return cursor.fetchone()[0]

    def get_audit_check_rows(self, check_id: str, offset: int = 0, limit: int = 25) -> dict | None:
        """One page of the rows a DB audit check flags ({"columns", "rows"}), or None for an
        unknown check id. `limit` is clamped to EXPLORER_MAX_PAGE_SIZE."""
        check = AUDIT_CHECKS_BY_ID.get(check_id)
        if check is None:
            return None
        limit = max(1, min(int(limit), EXPLORER_MAX_PAGE_SIZE))
        cursor = self.conn.cursor()
        cursor.execute(f"{check.sql} LIMIT ? OFFSET ?", [*check.params, limit, max(0, int(offset))])
        rows = cursor.fetchall()
        return {"columns": [col[0] for col in cursor.description], "rows": rows}

    def get_completed_track_files(self) -> list[tuple[str, str, str, str]]:
        """(track_id, artist, track_name, local_file_path) for every completed track that
        records a file -- the input to the "completed track's file is missing" disk check.
        Returned whole so the caller can stat the files without the DB being involved."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT track_id, artist, track_name, local_file_path FROM tracks "
            "WHERE download_status = 'completed' AND local_file_path IS NOT NULL "
            "AND TRIM(local_file_path) != '' ORDER BY track_id",
        )
        return cursor.fetchall()

    def get_playlist_m3u8_paths(self) -> list[tuple[str, str | None, str | None]]:
        """(playlist_url, playlist_name, m3u8_path) for every playlist, in CSV order."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT playlist_url, playlist_name, m3u8_path FROM playlists "
            "ORDER BY display_order IS NULL, display_order",
        )
        return cursor.fetchall()

    def get_all_local_file_paths(self) -> list[str]:
        """Every non-blank local_file_path recorded on any track -- the input to the
        "files in imported/ that no track points at" disk check."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT local_file_path FROM tracks WHERE local_file_path IS NOT NULL AND TRIM(local_file_path) != ''",
        )
        return [row[0] for row in cursor.fetchall()]

    def get_database_health(self) -> dict:
        """Cheap facts about the database file itself for the Database health section. Only
        reads pragmas (no value given, so nothing is changed on the shared connection)."""
        cursor = self.conn.cursor()

        def pragma(name: str) -> int | str:
            cursor.execute(f"PRAGMA {name}")
            return cursor.fetchone()[0]

        synchronous = pragma("synchronous")
        return {
            "path": self.db_path,
            "file_size_bytes": os.path.getsize(self.db_path) if os.path.exists(self.db_path) else None,
            "page_size": pragma("page_size"),
            "page_count": pragma("page_count"),
            "freelist_count": pragma("freelist_count"),
            "journal_mode": pragma("journal_mode"),
            "synchronous": _SYNCHRONOUS_NAMES.get(synchronous, str(synchronous)),
            # A rollback journal exists only while a write is in flight; one that lingers
            # means a write crashed mid-transaction (SQLite rolls it back on next open).
            "journal_file_present": os.path.exists(f"{self.db_path}-journal"),
        }

    def run_quick_check(self) -> list[str]:
        """Run PRAGMA quick_check and return its lines: ["ok"] when healthy, otherwise up to
        100 problem descriptions. Reads the whole database file, so it is click-only."""
        cursor = self.conn.cursor()
        cursor.execute("PRAGMA quick_check")
        return [row[0] for row in cursor.fetchall()]

    def close(self) -> None:
        """Close the database connection."""
        write_log.info("DB_CLOSE", "Closing database connection.")
        self.conn.close()

# --- Dashboard Helper Functions ---


def get_playlists(db_path: str) -> tuple[Optional["pd.DataFrame"], str | None]:
    """Retrieve all playlists from the database.

    Args:
        db_path: Path to the SQLite database file
    Returns:
        Tuple of (DataFrame with playlists, error message if any)

    """
    try:
        import pandas as pd  # noqa: PLC0415
        conn = sqlite3.connect(db_path)
        # Order by display_order if available, put NULLs last
        query = (
            "SELECT playlist_name, playlist_url FROM playlists "
            "ORDER BY display_order IS NULL, display_order"
        )
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)

def get_track_status_breakdown(db_path: str) -> tuple[Optional["pd.DataFrame"], str | None]:
    """Retrieve track download status breakdown from the database.

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


def get_failed_reason_breakdown(db_path: str) -> tuple[Optional["pd.DataFrame"], str | None]:
    """Retrieve breakdown of reasons why tracks don't have a local_file_path.

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
