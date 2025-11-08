"""
Unit tests for Spotify playlist scraping functionality.

Tests cover name cleaning, URL validation, authentication, and track extraction.
"""

import sys
from unittest.mock import patch

import pytest

from scrape_spotify_playlist import clean_name, get_tracks_from_playlist


class TestCleanName:
    """Tests for the clean_name function."""
    
    def test_removes_commas_and_dashes(self):
        """Test that commas and dashes are removed correctly."""
        assert clean_name("DC Breaks, InsideInfo Gambino - InsideInfo Remix") == \
               "DC Breaks InsideInfo Gambino InsideInfo Remix"
        
        assert clean_name("Skream, Jackmaster The Attention Deficit Track - Edit") == \
               "Skream Jackmaster The Attention Deficit Track Edit"
        
        assert clean_name("Trampsta, Heavy Drop Get Down") == \
               "Trampsta Heavy Drop Get Down"
    
    def test_removes_ampersands(self):
        """Test that ampersands are removed."""
        assert clean_name("Jonny L, Superfly 7 Back to Your Roots - Friction & K-Tee Remix") == \
               "Jonny L Superfly 7 Back to Your Roots Friction K-Tee Remix"
    
    def test_preserves_clean_names(self):
        """Test that already clean names remain unchanged."""
        assert clean_name("No punctuation here") == "No punctuation here"
    
    def test_normalizes_whitespace(self):
        """Test that extra whitespace is normalized."""
        assert clean_name("  Extra   spaces  - here, ") == "Extra spaces here"


class TestGetTracksFromPlaylist:
    """Tests for the get_tracks_from_playlist function."""
    
    def test_missing_credentials(self, monkeypatch):
        """Test that missing API credentials raise an error."""
        monkeypatch.delenv("SPOTIFY_CLIENT_ID", raising=False)
        monkeypatch.delenv("SPOTIFY_CLIENT_SECRET", raising=False)
        
        with pytest.raises(ValueError, match="Missing Spotify API credentials"):
            get_tracks_from_playlist("https://open.spotify.com/playlist/123")
    
    def test_invalid_url_format(self, monkeypatch):
        """Test that invalid playlist URLs raise an error."""
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "dummy")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "dummy")
        
        with patch("spotipy.Spotify"):
            with pytest.raises(ValueError, match="Invalid playlist URL"):
                get_tracks_from_playlist("not_a_spotify_url")
    
    def test_authentication_failure(self, monkeypatch):
        """Test that authentication failures are properly raised."""
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "dummy")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "dummy")
        
        with patch("spotipy.Spotify", side_effect=Exception("auth fail")):
            with pytest.raises(Exception, match="auth fail"):
                get_tracks_from_playlist("https://open.spotify.com/playlist/123")
    
    def test_fetch_tracks_successfully(self, monkeypatch):
        """Test successful track fetching with mock data."""
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "dummy")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "dummy")
        
        fake_tracks = [
            {"track": {"id": "id1", "artists": [{"name": "Artist1"}], "name": "Track1"}},
            {"track": {"id": "id2", "artists": [{"name": "Artist2"}], "name": "Track2"}},
            {"track": None},  # Should be skipped
        ]
        fake_results = {"items": fake_tracks, "next": None}
        
        with patch("spotipy.Spotify") as mock_spotify:
            mock_spotify.return_value.playlist_tracks.return_value = fake_results
            
            tracks = get_tracks_from_playlist("https://open.spotify.com/playlist/123")
        
        assert len(tracks) == 2
        assert tracks[0] == ("id1", "Artist1", "Track1")
        assert tracks[1] == ("id2", "Artist2", "Track2")
    
    def test_handles_pagination(self, monkeypatch):
        """Test that paginated results are properly combined."""
        monkeypatch.setenv("SPOTIFY_CLIENT_ID", "dummy")
        monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "dummy")
        
        page1 = {
            "items": [{"track": {"id": "id1", "artists": [{"name": "Artist1"}], "name": "Track1"}}],
            "next": "url_to_page2"
        }
        page2 = {
            "items": [{"track": {"id": "id2", "artists": [{"name": "Artist2"}], "name": "Track2"}}],
            "next": None
        }
        
        with patch("spotipy.Spotify") as mock_spotify:
            mock_instance = mock_spotify.return_value
            mock_instance.playlist_tracks.return_value = page1
            mock_instance.next.return_value = page2
            
            tracks = get_tracks_from_playlist("https://open.spotify.com/playlist/123")
        
        assert len(tracks) == 2
        assert tracks[0][0] == "id1"
        assert tracks[1][0] == "id2"
