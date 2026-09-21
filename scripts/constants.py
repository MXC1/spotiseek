"""Shared constants for Spotiseek application.

This module contains constants used across multiple modules to ensure consistency
and avoid duplication of format definitions, thresholds, and other shared values.

Audio Format Categories:
- LOSSLESS_FORMATS: Formats that preserve full audio quality (remuxed to WAV)
- LOSSY_FORMATS: Compressed formats (remuxed to MP3 320kbps)
- SUPPORTED_AUDIO_FORMATS: All formats that can be processed

Quality Thresholds:
- MIN_BITRATE_KBPS: Minimum acceptable bitrate for lossy formats
"""

# Lossless audio formats (all will be remuxed to WAV when PREFER_MP3=False)
LOSSLESS_FORMATS: frozenset[str] = frozenset({"wav", "flac", "alac", "ape", "aiff"})

# Lossy audio formats (all will be remuxed to MP3 320kbps)
LOSSY_FORMATS: frozenset[str] = frozenset({"ogg", "m4a", "aac", "wma", "opus"})

# All supported audio formats for processing
SUPPORTED_AUDIO_FORMATS: frozenset[str] = LOSSLESS_FORMATS | LOSSY_FORMATS | frozenset({"mp3"})

# Minimum acceptable bitrate for lossy formats (kbps)
MIN_BITRATE_KBPS: int = 320

# How long a track may sit in each in-flight status before it counts as stuck (hours) --
# see docs/adr/0009-track-status-changed-at.md. The keys ARE the in-flight statuses: a
# track in any other status (completed, failed, not_found, no_suitable_file, blacklisted)
# is a final outcome and is never stuck. Each threshold is sized against how that status
# normally lasts: searches are initiated hourly and polled every 15 minutes, a Soulseek
# queue can legitimately sit for days, and quality-upgrade searches are batched/rate-limited
# so a backlog of redownload_pending tracks takes a while to drain.
STUCK_THRESHOLD_HOURS: dict[str, int] = {
    "pending": 3,
    "searching": 3,
    "queued": 48,
    "downloading": 6,
    "redownload_pending": 48,
}
IN_FLIGHT_STATUSES: frozenset[str] = frozenset(STUCK_THRESHOLD_HOURS)
