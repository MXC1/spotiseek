from database_management import TrackDB

db = TrackDB()

# Add a playlist
playlist_id = db.add_playlist("My Playlist")

# Add a track
track_id = db.add_track(
    spotify_id="12345",
    track_name="Test Song",
    artist="Test Artist"
)

# Link track to playlist
db.link_track_to_playlist(track_id, playlist_id)

# Update track status
db.update_track_status(track_id, "downloaded", file_path="C:/Music/Test Song.mp3")

# Query tracks by status
downloaded_tracks = db.get_tracks_by_status("downloaded")
print(downloaded_tracks)

db.close()