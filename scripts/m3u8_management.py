
import os
import glob

def write_playlist_m3u8(m3u8_path, tracks):
	"""
	Write a new m3u8 file with commented rows for each track.
	Args:
		m3u8_path: Path to the m3u8 file
		tracks: List of (spotify_id, artist, track_name)
	"""
	with open(m3u8_path, 'w', encoding='utf-8') as m3u8_file:
		m3u8_file.write('#EXTM3U\n')
		for track in tracks:
			spotify_id, artist, track_name = track
			comment = f"# {spotify_id} - {artist} - {track_name}\n"
			m3u8_file.write(comment)

def update_track_in_m3u8(m3u8_path, spotify_id, local_file_path):
	"""
	Replace the comment line for a track with the actual file path.
	Args:
		m3u8_path: Path to the m3u8 file
		spotify_id: Spotify track ID
		local_file_path: Path to the completed file
	"""
	if not os.path.exists(m3u8_path):
		return
	with open(m3u8_path, 'r', encoding='utf-8') as f:
		lines = f.readlines()
	comment_prefix = f"# {spotify_id} - "
	new_lines = []
	replaced = False
	for line in lines:
		if line.startswith(comment_prefix) and not replaced:
			new_lines.append(local_file_path + '\n')
			replaced = True
		else:
			new_lines.append(line)
	if replaced:
		with open(m3u8_path, 'w', encoding='utf-8') as f:
			f.writelines(new_lines)

def delete_all_m3u8_files(m3u8_dir):
    """
    Delete all .m3u8 files in the specified directory and its subdirectories.
    """
    pattern = os.path.join(m3u8_dir, '**', '*.m3u8')
    files = glob.glob(pattern, recursive=True)
    for file in files:
        try:
            os.remove(file)
        except Exception as e:
            print(f"Failed to delete {file}: {e}")
