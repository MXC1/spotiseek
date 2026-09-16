"""Shared audio file integrity checking.

Used by both the automated Soulseek remux pipeline (workflow.py) and the
dashboard's manual/auto import paths (observability/dashboard_next/import_helpers.py)
so a corrupt or truncated file is rejected the same way regardless of how it
arrived.
"""

import subprocess

from scripts.logs_utils import write_log


def is_audio_valid(audio_path: str) -> bool:
    """Use ffmpeg to check if an audio file is valid and decodable.

    Returns True if valid, False otherwise.
    """
    try:
        result = subprocess.run([
            "ffmpeg", "-v", "error", "-i", audio_path, "-f", "null", "-",
        ], check=False, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        write_log.error(
            "AUDIO_CHECK_FAIL",
            "Failed to check audio file integrity.",
            {"audio_path": audio_path, "error": str(e)},
        )
        return False
