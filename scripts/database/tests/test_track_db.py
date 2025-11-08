import unittest
import os
import sys

# Ensure the database module is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from database_management import TrackDB, DB_PATH as PROD_DB_PATH
from clear_database import clear_database

TEST_DB_PATH = os.path.join(os.path.dirname(__file__), 'test_db.sqlite')

class TestTrackDB(unittest.TestCase):
    def setUp(self):
        # Wipe and recreate the test database before each test
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        self.db = TrackDB(db_path=TEST_DB_PATH)

    def tearDown(self):
        self.db.close()
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def test_add_and_query_track(self):
        track_id = self.db.add_track('spotify123', 'Song1', 'Artist1')
        tracks = self.db.get_tracks_by_status('pending')
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0][0], track_id)
        self.assertEqual(tracks[0][1], 'spotify123')

    def test_unique_spotify_id(self):
        self.db.add_track('spotify123', 'Song1', 'Artist1')
        with self.assertRaises(Exception):
            self.db.add_track('spotify123', 'Song1', 'Artist1')

    def test_add_playlist_and_link(self):
        playlist_id = self.db.add_playlist('Playlist1')
        track_id = self.db.add_track('spotify456', 'Song2', 'Artist2')
        self.db.link_track_to_playlist(track_id, playlist_id)
        # Check linking by direct query
        cursor = self.db.conn.cursor()
        cursor.execute('SELECT * FROM playlist_tracks WHERE playlist_id=? AND track_id=?', (playlist_id, track_id))
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], playlist_id)
        self.assertEqual(result[1], track_id)

    def test_update_track_status_and_slskd_file_name(self):
        spotify_id = self.db.add_track('spotify789', 'Song3', 'Artist3')
        self.db.update_track_status(spotify_id, 'downloaded', slskd_file_name='/tmp/song3.mp3')
        tracks = self.db.get_tracks_by_status('downloaded')
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0][0], spotify_id)
        self.assertEqual(tracks[0][5], '/tmp/song3.mp3')

    def test_multiple_playlists_per_track(self):
        playlist1 = self.db.add_playlist('PlaylistA')
        playlist2 = self.db.add_playlist('PlaylistB')
        track_id = self.db.add_track('spotify999', 'SongX', 'ArtistX')
        self.db.link_track_to_playlist(track_id, playlist1)
        self.db.link_track_to_playlist(track_id, playlist2)
        cursor = self.db.conn.cursor()
        cursor.execute('SELECT playlist_id FROM playlist_tracks WHERE track_id=?', (track_id,))
        playlists = cursor.fetchall()
        self.assertEqual(set([p[0] for p in playlists]), set([playlist1, playlist2]))

if __name__ == '__main__':
    unittest.main()
