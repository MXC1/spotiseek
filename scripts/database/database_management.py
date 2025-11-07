import logging

import sqlite3
from typing import Optional, List, Tuple
import os
from logs_utils import setup_logging

# Ensure logging is set up for both console and file per invocation
setup_logging(log_name_prefix="database_management")
DB_PATH = os.path.join(os.path.dirname(__file__), 'tracks.db')


class TrackDB:
    def __init__(self, db_path: str = DB_PATH):
        logging.info(f"Connecting to database at {db_path}")
        self.conn = sqlite3.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        logging.info("Creating tables if not exist.")
        cursor = self.conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            spotify_id TEXT PRIMARY KEY,
            track_name TEXT,
            artist TEXT,
            download_status TEXT,
            file_path TEXT,
            added_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS playlists (
            id INTEGER PRIMARY KEY,
            playlist_name TEXT
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS playlist_tracks (
            playlist_id INTEGER,
            spotify_id TEXT,
            FOREIGN KEY (playlist_id) REFERENCES playlists(id),
            FOREIGN KEY (spotify_id) REFERENCES tracks(spotify_id),
            PRIMARY KEY (playlist_id, spotify_id)
        )''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS slskd_mapping (
            slskd_uuid TEXT PRIMARY KEY,
            spotify_id TEXT,
            FOREIGN KEY (spotify_id) REFERENCES tracks(spotify_id)
        )''')
        self.conn.commit()

    def add_track(self, spotify_id: str, track_name: str, artist: str, download_status: str = 'pending', file_path: Optional[str] = None):
        logging.info(f"Adding track: spotify_id={spotify_id}, track_name={track_name}, artist={artist}, status={download_status}")
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT OR IGNORE INTO tracks (spotify_id, track_name, artist, download_status, file_path)
        VALUES (?, ?, ?, ?, ?)
        ''', (spotify_id, track_name, artist, download_status, file_path))
        self.conn.commit()

    def add_playlist(self, playlist_name: str) -> int:
        logging.info(f"Adding playlist: {playlist_name}")
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT INTO playlists (playlist_name) VALUES (?)
        ''', (playlist_name,))
        self.conn.commit()
        return cursor.lastrowid

    def link_track_to_playlist(self, spotify_id: int, playlist_id: int):
        logging.info(f"Linking track {spotify_id} to playlist {playlist_id}")
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT OR IGNORE INTO playlist_tracks (playlist_id, spotify_id) VALUES (?, ?)
        ''', (playlist_id, spotify_id))
        self.conn.commit()

    def update_track_status(self, spotify_id: str, status: str, file_path: Optional[str] = None):
        logging.info(f"Updating track {spotify_id} status to {status}, file_path={file_path}")
        cursor = self.conn.cursor()
        if file_path:
            cursor.execute('''
            UPDATE tracks SET download_status = ?, file_path = ? WHERE spotify_id = ?
            ''', (status, file_path, spotify_id))
        else:
            cursor.execute('''
            UPDATE tracks SET download_status = ? WHERE spotify_id = ?
            ''', (status, spotify_id))
        self.conn.commit()

    def get_tracks_by_status(self, status: str) -> List[Tuple]:
        logging.info(f"Querying tracks by status: {status}")
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT * FROM tracks WHERE download_status = ?
        ''', (status,))
        return cursor.fetchall()

    def add_slskd_mapping(self, slskd_uuid: str, spotify_id: str):
        """Add a mapping between slskd UUID and Spotify ID."""
        logging.info(f"Adding slskd mapping: slskd_uuid={slskd_uuid}, spotify_id={spotify_id}")
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT OR IGNORE INTO slskd_mapping (slskd_uuid, spotify_id) VALUES (?, ?)
        ''', (slskd_uuid, spotify_id))
        self.conn.commit()

    def get_spotify_id_by_slskd_uuid(self, slskd_uuid: str) -> Optional[str]:
        """Retrieve the Spotify ID for a given slskd UUID."""
        logging.info(f"Querying Spotify ID for slskd_uuid={slskd_uuid}")
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT spotify_id FROM slskd_mapping WHERE slskd_uuid = ?
        ''', (slskd_uuid,))
        result = cursor.fetchone()
        return result[0] if result else None

    def close(self):
        logging.info("Closing database connection.")
        self.conn.close()
