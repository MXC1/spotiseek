
import sqlite3
from typing import Optional, List, Tuple
import os
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
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
            id INTEGER PRIMARY KEY,
            spotify_id TEXT UNIQUE,
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
            track_id INTEGER,
            FOREIGN KEY (playlist_id) REFERENCES playlists(id),
            FOREIGN KEY (track_id) REFERENCES tracks(id),
            PRIMARY KEY (playlist_id, track_id)
        )''')
        self.conn.commit()

    def add_track(self, spotify_id: str, track_name: str, artist: str, download_status: str = 'pending', file_path: Optional[str] = None) -> int:
        logging.info(f"Adding track: spotify_id={spotify_id}, track_name={track_name}, artist={artist}, status={download_status}")
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT INTO tracks (spotify_id, track_name, artist, download_status, file_path)
        VALUES (?, ?, ?, ?, ?)
        ''', (spotify_id, track_name, artist, download_status, file_path))
        self.conn.commit()
        return cursor.lastrowid

    def add_playlist(self, playlist_name: str) -> int:
        logging.info(f"Adding playlist: {playlist_name}")
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT INTO playlists (playlist_name) VALUES (?)
        ''', (playlist_name,))
        self.conn.commit()
        return cursor.lastrowid

    def link_track_to_playlist(self, track_id: int, playlist_id: int):
        logging.info(f"Linking track {track_id} to playlist {playlist_id}")
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT OR IGNORE INTO playlist_tracks (playlist_id, track_id) VALUES (?, ?)
        ''', (playlist_id, track_id))
        self.conn.commit()

    def update_track_status(self, track_id: int, status: str, file_path: Optional[str] = None):
        logging.info(f"Updating track {track_id} status to {status}, file_path={file_path}")
        cursor = self.conn.cursor()
        if file_path:
            cursor.execute('''
            UPDATE tracks SET download_status = ?, file_path = ? WHERE id = ?
            ''', (status, file_path, track_id))
        else:
            cursor.execute('''
            UPDATE tracks SET download_status = ? WHERE id = ?
            ''', (status, track_id))
        self.conn.commit()

    def get_tracks_by_status(self, status: str) -> List[Tuple]:
        logging.info(f"Querying tracks by status: {status}")
        cursor = self.conn.cursor()
        cursor.execute('''
        SELECT * FROM tracks WHERE download_status = ?
        ''', (status,))
        return cursor.fetchall()

    def close(self):
        logging.info("Closing database connection.")
        self.conn.close()
