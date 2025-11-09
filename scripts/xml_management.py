
import os
import xml.etree.ElementTree as ET
from database_management import TrackDB
from logs_utils import write_log

def export_itunes_xml(xml_path: str, music_folder_url: str = None):
	"""
	Export all playlists and tracks from the database to an XML file matching the iTunes Music Library.xml format.
	Args:
		xml_path: Path to output XML file
		music_folder_url: Optional base URL for the <Music Folder> key
	"""
	db = TrackDB()
	conn = db.conn
	cursor = conn.cursor()

	# Fetch all tracks
	cursor.execute("SELECT spotify_id, track_name, artist, download_status, slskd_file_name, local_file_path, added_at FROM tracks")
	tracks = cursor.fetchall()

	# Fetch all playlists
	cursor.execute("SELECT playlist_url, playlist_name FROM playlists")
	playlists = cursor.fetchall()

	# Map playlist_url to list of spotify_ids
	cursor.execute("SELECT playlist_url, spotify_id FROM playlist_tracks")
	playlist_tracks = {}
	for playlist_url, spotify_id in cursor.fetchall():
		playlist_tracks.setdefault(playlist_url, []).append(spotify_id)

	# Build XML
	plist = ET.Element('plist', version="1.0")
	dict_root = ET.SubElement(plist, 'dict')

	# Top-level keys
	ET.SubElement(dict_root, 'key').text = 'Major Version'
	ET.SubElement(dict_root, 'integer').text = '1'
	ET.SubElement(dict_root, 'key').text = 'Minor Version'
	ET.SubElement(dict_root, 'integer').text = '1'
	ET.SubElement(dict_root, 'key').text = 'Application Version'
	ET.SubElement(dict_root, 'string').text = '3.5.8698.34385'
	ET.SubElement(dict_root, 'key').text = 'Music Folder'
	ET.SubElement(dict_root, 'string').text = music_folder_url or ""
	ET.SubElement(dict_root, 'key').text = 'Library Persistent ID'
	ET.SubElement(dict_root, 'string').text = 'SPOTISEEKLIB0000001'

	# Tracks
	ET.SubElement(dict_root, 'key').text = 'Tracks'
	tracks_dict = ET.SubElement(dict_root, 'dict')
	# Build a mapping from spotify_id to track integer ID, only for downloaded tracks
	spotifyid_to_trackid = {}
	downloaded_tracks = [t for t in tracks if t[5]]  # local_file_path is at index 5
	for idx, (spotify_id, track_name, artist, download_status, slskd_file_name, local_file_path, added_at) in enumerate(downloaded_tracks, 1):
		track_key = ET.SubElement(tracks_dict, 'key')
		track_key.text = str(idx)
		track_dict = ET.SubElement(tracks_dict, 'dict')
		ET.SubElement(track_dict, 'key').text = 'Track ID'
		ET.SubElement(track_dict, 'integer').text = str(idx)
		ET.SubElement(track_dict, 'key').text = 'Name'
		ET.SubElement(track_dict, 'string').text = track_name or ''
		ET.SubElement(track_dict, 'key').text = 'Artist'
		ET.SubElement(track_dict, 'string').text = artist or ''
		ET.SubElement(track_dict, 'key').text = 'Kind'
		ET.SubElement(track_dict, 'string').text = 'MPEG audio file'
		ET.SubElement(track_dict, 'key').text = 'Track Type'
		ET.SubElement(track_dict, 'string').text = 'File'
		ET.SubElement(track_dict, 'key').text = 'Persistent ID'
		ET.SubElement(track_dict, 'string').text = spotify_id or ''
		ET.SubElement(track_dict, 'key').text = 'Location'
		ET.SubElement(track_dict, 'string').text = f'file://localhost/{local_file_path.replace(os.sep, "/")}'
		# Map spotify_id to idx for playlist reference
		spotifyid_to_trackid[spotify_id] = idx

	# Playlists
	ET.SubElement(dict_root, 'key').text = 'Playlists'
	playlists_array = ET.SubElement(dict_root, 'array')
	for playlist_idx, (playlist_url, playlist_name) in enumerate(playlists, 1):
		playlist_dict = ET.SubElement(playlists_array, 'dict')
		# Playlist ID
		ET.SubElement(playlist_dict, 'key').text = 'Playlist ID'
		ET.SubElement(playlist_dict, 'integer').text = str(playlist_idx)
		# Playlist Persistent ID (generate a dummy hex string based on index, or use a real one if available)
		persistent_id = f"PL{playlist_idx:014X}"  # e.g., PL00000000000001
		ET.SubElement(playlist_dict, 'key').text = 'Playlist Persistent ID'
		ET.SubElement(playlist_dict, 'string').text = persistent_id
		# All Items
		ET.SubElement(playlist_dict, 'key').text = 'All Items'
		ET.SubElement(playlist_dict, 'true')
		# Name
		ET.SubElement(playlist_dict, 'key').text = 'Name'
		ET.SubElement(playlist_dict, 'string').text = (playlist_name or playlist_url).replace(' ', '_')
		# Playlist Items
		ET.SubElement(playlist_dict, 'key').text = 'Playlist Items'
		items_array = ET.SubElement(playlist_dict, 'array')
		for spotify_id in playlist_tracks.get(playlist_url, []):
			item_dict = ET.SubElement(items_array, 'dict')
			ET.SubElement(item_dict, 'key').text = 'Track ID'
			ET.SubElement(item_dict, 'integer').text = str(spotifyid_to_trackid.get(spotify_id, ''))

	# Write XML to file with custom header and DOCTYPE
	tree = ET.ElementTree(plist)
	ET.indent(tree, space="\t", level=0)
	import io
	xml_io = io.BytesIO()
	tree.write(xml_io, encoding="utf-8", xml_declaration=False)
	xml_content = xml_io.getvalue().decode("utf-8")

	# Write the required header and DOCTYPE manually
	with open(xml_path, "w", encoding="utf-8") as f:
		f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
		f.write('<!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n')
		xml_content = xml_content.lstrip()
		f.write(xml_content)
	write_log.info("XML_EXPORT_SUCCESS", "Exported iTunes-style XML successfully.", {"xml_path": xml_path})