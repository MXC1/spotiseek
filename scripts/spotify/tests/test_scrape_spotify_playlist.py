import pytest
import logging
from unittest.mock import patch, MagicMock
from scrape_spotify_playlist import main as scrape_spotify_playlist, clean_name

def test_clean_name_removes_commas_and_dashes():
    assert clean_name('DC Breaks, InsideInfo Gambino - InsideInfo Remix') == 'DC Breaks InsideInfo Gambino InsideInfo Remix'
    assert clean_name('Skream, Jackmaster The Attention Deficit Track - Edit') == 'Skream Jackmaster The Attention Deficit Track Edit'
    assert clean_name('Trampsta, Heavy Drop Get Down') == 'Trampsta Heavy Drop Get Down'
    assert clean_name('Jonny L, Superfly 7 Back to Your Roots - Friction & K-Tee Remix') == 'Jonny L Superfly 7 Back to Your Roots Friction K-Tee Remix'
    assert clean_name('No punctuation here') == 'No punctuation here'
    assert clean_name('  Extra   spaces  - here, ') == 'Extra spaces here'

def test_main_invalid_url(monkeypatch, caplog):
    monkeypatch.setenv('SPOTIFY_CLIENT_ID', 'dummy')
    monkeypatch.setenv('SPOTIFY_CLIENT_SECRET', 'dummy')
    test_args = ['prog', 'not_a_spotify_url']
    monkeypatch.setattr('sys.argv', test_args)
    with patch('spotipy.Spotify'):
        with pytest.raises(SystemExit):
            scrape_spotify_playlist()
    # caplog.text contains all log output
    assert 'Invalid playlist URL' in caplog.text

def test_main_authentication_failure(monkeypatch):
    monkeypatch.setenv('SPOTIFY_CLIENT_ID', 'dummy')
    monkeypatch.setenv('SPOTIFY_CLIENT_SECRET', 'dummy')
    test_args = ['prog', 'https://open.spotify.com/playlist/123']
    with patch('spotipy.Spotify', side_effect=Exception('auth fail')):
        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                scrape_spotify_playlist()

def test_main_fetch_tracks(monkeypatch, capsys):
    monkeypatch.setenv('SPOTIFY_CLIENT_ID', 'dummy')
    monkeypatch.setenv('SPOTIFY_CLIENT_SECRET', 'dummy')
    test_args = ['prog', 'https://open.spotify.com/playlist/123']
    fake_tracks = [
        {'track': {'artists': [{'name': 'Artist1'}], 'name': 'Track1'}},
        {'track': {'artists': [{'name': 'Artist2'}], 'name': 'Track2'}},
        {'track': None},  # Should be skipped
    ]
    fake_results = {'items': fake_tracks, 'next': None}
    with patch('spotipy.Spotify') as mock_spotify:
        mock_spotify.return_value.playlist_tracks.return_value = fake_results
        with patch('sys.argv', test_args):
            scrape_spotify_playlist()
    out = capsys.readouterr().out
    assert 'Artist1 Track1' in out
    assert 'Artist2 Track2' in out
    assert 'Track1' in out and 'Track2' in out
