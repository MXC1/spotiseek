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
LOSSLESS_FORMATS: frozenset[str] = frozenset({"wav", "flac", "alac", "ape"})

# Lossy audio formats (all will be remuxed to MP3 320kbps)
LOSSY_FORMATS: frozenset[str] = frozenset({"ogg", "m4a", "aac", "wma", "opus"})

# All supported audio formats for processing
SUPPORTED_AUDIO_FORMATS: frozenset[str] = LOSSLESS_FORMATS | LOSSY_FORMATS | frozenset({"mp3"})

# Minimum acceptable bitrate for lossy formats (kbps)
MIN_BITRATE_KBPS: int = 320
