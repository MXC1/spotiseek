"""
Shared import logic for the Manual Import and Auto Import tabs.

Every caller here already has a real file on disk (a staged upload for Manual Import,
a discovered file for Auto Import), so there's no separate "is_upload" branch to worry
about.
"""

import os
import shutil
from pathlib import Path

from mutagen import File as MutagenFile

from observability.dashboard.config import BASE_DIR, IMPORTED_DIR, IS_DOCKER, track_db
from scripts.audio_validation import is_audio_valid
from scripts.constants import LOSSLESS_FORMATS, MIN_BITRATE_KBPS
from scripts.logs_utils import write_log
from scripts.m3u8_manager import update_track_in_m3u8
from scripts.soulseek_client import remove_download_from_slskd, remove_search_from_slskd


def sanitize_filename(artist: str, track_name: str, extension: str) -> str:
    """Create a safe filename from artist and track name."""
    if not extension.startswith("."):
        extension = f".{extension}"
    raw = f"{artist}_{track_name}{extension}".replace(" ", "_")
    return "".join(c for c in raw if c.isalnum() or c in ("_", ".", "-"))


def normalize_docker_path(path: str) -> str:
    """Rewrite a host-style absolute path to /app/... when running in Docker."""
    if IS_DOCKER and not path.startswith("/app/"):
        return path.replace(BASE_DIR, "/app")
    return path


def extract_metadata_from_file(file_path: str) -> dict:
    """Extract extension and bitrate (kbps) from an audio file using mutagen."""
    metadata = {"extension": None, "bitrate": None}
    try:
        metadata["extension"] = Path(file_path).suffix.lstrip(".").lower()
        audio = MutagenFile(file_path, easy=False)
        if audio is not None and getattr(audio.info, "bitrate", None):
            metadata["bitrate"] = int(audio.info.bitrate / 1000)
    except Exception as e:
        write_log.error("IMPORT_METADATA_FAIL", "Failed to extract metadata.",
                         {"file_path": file_path, "error": str(e)})
    return metadata


def is_quality_worse_than_mp3_320(extension: str, bitrate: int | None) -> tuple[bool, str]:
    """Check if an audio file is worse quality than MP3 320kbps."""
    if extension.lower() in LOSSLESS_FORMATS:
        return False, ""
    if bitrate is None:
        return True, "Could not determine bitrate"
    if bitrate < MIN_BITRATE_KBPS:
        return True, f"{extension.upper()} {bitrate}kbps is lower quality than MP3 {MIN_BITRATE_KBPS}kbps"
    return False, ""


def do_track_import(track_id: str, source_path: str, artist: str, track_name: str) -> tuple[bool, str]:
    """Copy source_path into IMPORTED_DIR and update the DB/m3u8s for track_id."""
    try:
        if not is_audio_valid(source_path):
            write_log.warn("IMPORT_INVALID_AUDIO", "Source file failed audio integrity check; import rejected.",
                            {"track_id": track_id, "source_path": source_path})
            return False, f"Rejected: {os.path.basename(source_path)} is not a valid/decodable audio file"

        file_extension = os.path.splitext(source_path)[1]
        safe_filename = sanitize_filename(artist, track_name, file_extension)
        destination_path = os.path.abspath(os.path.join(IMPORTED_DIR, safe_filename))
        destination_path = normalize_docker_path(destination_path)

        shutil.copy2(source_path, destination_path)
        write_log.info("IMPORT_FILE_SAVED", "Saved imported file.",
                        {"track_id": track_id, "destination": destination_path})

        search_uuid = track_db.get_search_uuid_by_track_id(track_id)
        if search_uuid:
            remove_search_from_slskd(search_uuid, track_id)

        download_uuid = track_db.get_download_uuid_by_track_id(track_id)
        if download_uuid:
            username = track_db.get_username_by_slskd_uuid(download_uuid)
            if username:
                remove_download_from_slskd(username, download_uuid)

        metadata = extract_metadata_from_file(destination_path)

        is_worse, reason = is_quality_worse_than_mp3_320(metadata.get("extension") or "", metadata.get("bitrate"))
        if is_worse:
            write_log.warn("IMPORT_LOW_QUALITY", "Imported file has lower quality than target.",
                            {"track_id": track_id, "reason": reason})

        track_db.update_local_file_path(track_id, destination_path)
        track_db.update_extension_bitrate(track_id, extension=metadata["extension"], bitrate=metadata["bitrate"])
        track_db.update_track_status(track_id, "completed")

        write_log.info("IMPORT_DB_UPDATED", "Updated database for imported track.",
                        {"track_id": track_id, "extension": metadata["extension"], "bitrate": metadata["bitrate"]})

        for playlist_url in track_db.get_playlists_for_track(track_id):
            m3u8_path = track_db.get_m3u8_path_for_playlist(playlist_url)
            if m3u8_path:
                update_track_in_m3u8(m3u8_path, track_id, destination_path)

        return True, f"Successfully imported: {artist} - {track_name}"

    except Exception as e:
        write_log.error("IMPORT_TRACK_FAIL", "Failed to import track.", {"track_id": track_id, "error": str(e)})
        return False, f"Failed to import track: {e}"
