"""
Unit tests for TrackDB database management class.

Tests cover track management, playlist operations, status updates,
and relationship mappings between tracks, playlists, and Soulseek downloads.
"""

import os
import sys
import unittest

# Ensure parent modules are importable
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from database_management import TrackDB

TEST_DB_PATH = os.path.join(os.path.dirname(__file__), "test_db.sqlite")


class TestTrackDB(unittest.TestCase):
    """Test suite for TrackDB database operations."""
    
    def setUp(self):
        """Create a fresh test database before each test."""
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
        self.db = TrackDB(db_path=TEST_DB_PATH)

    def tearDown(self):
        """Clean up test database after each test."""
        self.db.close()
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)

    def test_add_and_query_track(self):
        """Test adding a track and querying by status."""
        self.db.add_track("spotify123", "Song1", "Artist1")
        tracks = self.db.get_tracks_by_status("pending")
        
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0][0], "spotify123")  # spotify_id is primary key
        self.assertEqual(tracks[0][1], "Song1")       # track_name
        self.assertEqual(tracks[0][2], "Artist1")     # artist

    def test_duplicate_track_ignored(self):
        """Test that duplicate tracks are ignored (INSERT OR IGNORE)."""
        self.db.add_track("spotify123", "Song1", "Artist1")
        self.db.add_track("spotify123", "Song1", "Artist1")  # Should be ignored
        
        tracks = self.db.get_tracks_by_status("pending")
        self.assertEqual(len(tracks), 1)

    def test_add_playlist_and_link(self):
        """Test playlist creation and linking tracks to playlists."""
        playlist_id = self.db.add_playlist("Playlist1")
        self.db.add_track("spotify456", "Song2", "Artist2")
        self.db.link_track_to_playlist("spotify456", playlist_id)
        
        # Verify link exists
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT * FROM playlist_tracks WHERE playlist_id=? AND spotify_id=?",
            (playlist_id, "spotify456")
        )
        result = cursor.fetchone()
        
        self.assertIsNotNone(result)
        self.assertEqual(result[0], playlist_id)
        self.assertEqual(result[1], "spotify456")

    def test_update_track_status_and_slskd_file_name(self):
        """Test updating track status and Soulseek filename."""
        self.db.add_track("spotify789", "Song3", "Artist3")
        self.db.update_track_status(
            "spotify789",
            "downloaded",
            slskd_file_name="/tmp/song3.mp3"
        )
        
        tracks = self.db.get_tracks_by_status("downloaded")
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0][0], "spotify789")
        self.assertEqual(tracks[0][4], "/tmp/song3.mp3")  # slskd_file_name column

    def test_multiple_playlists_per_track(self):
        """Test that a single track can be linked to multiple playlists."""
        playlist1 = self.db.add_playlist("PlaylistA")
        playlist2 = self.db.add_playlist("PlaylistB")
        self.db.add_track("spotify999", "SongX", "ArtistX")
        
        self.db.link_track_to_playlist("spotify999", playlist1)
        self.db.link_track_to_playlist("spotify999", playlist2)
        
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT playlist_id FROM playlist_tracks WHERE spotify_id=?",
            ("spotify999",)
        )
        playlists = cursor.fetchall()
        
        self.assertEqual(set([p[0] for p in playlists]), {playlist1, playlist2})

    def test_slskd_mapping(self):
        """Test mapping between Soulseek UUIDs and Spotify IDs."""
        self.db.add_track("spotify111", "TestTrack", "TestArtist")
        self.db.add_slskd_mapping("slskd-uuid-123", "spotify111")
        
        result = self.db.get_spotify_id_by_slskd_uuid("slskd-uuid-123")
        self.assertEqual(result, "spotify111")
        
        # Test non-existent mapping
        result = self.db.get_spotify_id_by_slskd_uuid("non-existent")
        self.assertIsNone(result)

    def test_get_track_status(self):
        """Test retrieving track download status."""
        self.db.add_track("spotify222", "StatusTrack", "StatusArtist")
        
        status = self.db.get_track_status("spotify222")
        self.assertEqual(status, "pending")
        
        self.db.update_track_status("spotify222", "completed")
        status = self.db.get_track_status("spotify222")
        self.assertEqual(status, "completed")

    def test_update_local_file_path(self):
        """Test updating local file path for downloaded tracks."""
        self.db.add_track("spotify333", "LocalTrack", "LocalArtist")
        self.db.update_local_file_path("spotify333", "/downloads/track.mp3")
        
        cursor = self.db.conn.cursor()
        cursor.execute(
            "SELECT local_file_path FROM tracks WHERE spotify_id=?",
            ("spotify333",)
        )
        result = cursor.fetchone()
        
        self.assertEqual(result[0], "/downloads/track.mp3")


if __name__ == "__main__":
    unittest.main()
