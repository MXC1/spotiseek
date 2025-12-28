"""
Unified playlist scraper module.

This module provides a unified interface for extracting track information from
various music streaming platforms. It automatically dispatches to the appropriate
scraper based on the playlist URL pattern.

Supported Platforms:
- Spotify (open.spotify.com/playlist/...)
- SoundCloud (soundcloud.com/.../sets/...)

Key Features:
- Automatic platform detection from URL
- Consistent return format across all platforms
- Extensible architecture for adding new platforms

Public API:
- get_tracks_from_playlist(): Main function to fetch playlist tracks
- detect_platform(): Utility to identify platform from URL
- clean_name(): Utility to normalize track/artist names
"""

import re

from scripts.logs_utils import write_log


def detect_platform(playlist_url: str) -> str:
    """
    Detect the music platform from a playlist URL.

    Args:
        playlist_url: Full playlist URL

    Returns:
        Platform identifier: 'spotify', 'soundcloud', or 'unknown'

    Example:
        >>> detect_platform("https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M")
        "spotify"
        >>> detect_platform("https://soundcloud.com/user/sets/playlist-name")
        "soundcloud"
    """
    if "open.spotify.com/playlist/" in playlist_url or "spotify.com/playlist/" in playlist_url:
        return "spotify"
    elif "soundcloud.com/" in playlist_url and "/sets/" in playlist_url:
        return "soundcloud"
    else:
        return "unknown"


def clean_name(name: str) -> str:
    """
    Normalize track and artist names for improved search consistency.

    Removes common punctuation, promotional text, emojis, and normalizes
    whitespace that may interfere with Soulseek search queries.
    This improves match rates, especially for SoundCloud tracks.

    Args:
        name: Original track or artist name

    Returns:
        Cleaned name with normalized spacing and removed junk

    Example:
        >>> clean_name("DC Breaks, InsideInfo - Remix")
        "DC Breaks InsideInfo Remix"
        >>> clean_name("Track Name [FREE D/L]")
        "Track Name"
        >>> clean_name("Song 👉 FREE DOWNLOAD 👈")
        "Song"
    """
    # Remove content in square brackets (often promotional: [FREE D/L], [OUT NOW], etc.)
    name = re.sub(r'\[[^\]]*\]', '', name)

    # Remove common promotional phrases (case-insensitive)
    promo_patterns = [
        r'free\s*d/?l',
        r'free\s*download',
        r'out\s*now',
        r'buy\s*=\s*free',
        r'click\s*buy',
    ]
    for pattern in promo_patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)

    # Remove emojis and other Unicode symbols
    # This pattern covers most emoji ranges
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002700-\U000027BF"  # dingbats
        "\U0001F900-\U0001F9FF"  # supplemental symbols
        "\U00002600-\U000026FF"  # misc symbols
        "\U0001FA00-\U0001FA6F"  # chess symbols
        "\U0001FA70-\U0001FAFF"  # symbols extended-A
        "]+",
        flags=re.UNICODE
    )
    name = emoji_pattern.sub('', name)

    # Remove problematic punctuation that interferes with searches
    name = name.replace(",", "")
    name = name.replace(" - ", " ")
    name = name.replace("&", "")

    # Remove empty or whitespace-only parentheses left after other cleaning
    name = re.sub(r'\(\s*\)', '', name)
    # Remove parentheses containing only short junk (1-3 words of non-meaningful content)
    name = re.sub(r'\(\s*(\d+\s+years?\s+of\s+)?[\w\s]{0,30}\s*\)', _clean_parens, name)

    # Normalize whitespace (collapse multiple spaces)
    name = " ".join(name.split())

    return name


def _clean_parens(match: re.Match) -> str:
    """Helper to remove parentheses if they contain only junk words."""
    content = match.group(0)
    inner = content.strip('()')
    # Keep if it looks like meaningful info (remix, edit, mix, version, original, vip, etc.)
    meaningful_keywords = [
        'remix', 'edit', 'mix', 'version', 'original', 'vip', 'flip',
        'rework', 'dub', 'extended', 'radio', 'club', 'instrumental', 'bootleg'
    ]
    if any(kw in inner.lower() for kw in meaningful_keywords):
        return content
    # Remove if it's just promotional junk
    return ''


def get_tracks_from_playlist(playlist_url: str) -> tuple[str, list[tuple[str, str, str]], str]:
    """
    Extract track information and playlist name from a playlist URL.

    This function automatically detects the platform and dispatches to the
    appropriate scraper. All scrapers return data in a consistent format.

    Args:
        playlist_url: Full playlist URL from any supported platform

    Returns:
        Tuple containing:
            - playlist_name (str): The name of the playlist
            - tracks (List[Tuple]): List of (track_id, artists, track_name) tuples.
              Artist names are space-concatenated and cleaned.
            - source (str): Platform identifier ('spotify', 'soundcloud')

    Raises:
        ValueError: If the platform is not supported or URL is invalid

    Example:
        >>> playlist_name, tracks, source = get_tracks_from_playlist("https://open.spotify.com/playlist/...")
        >>> print(playlist_name)
        "My Playlist Name"
        >>> print(tracks[0])
        ("5ms8IkagrFWObtzSOahVrx", "MASTER BOOT RECORD", "Skynet")
        >>> print(source)
        "spotify"
    """
    platform = detect_platform(playlist_url)

    write_log.info(
        "PLAYLIST_SCRAPE_START",
        "Starting playlist scrape.",
        {"playlist_url": playlist_url, "platform": platform}
    )

    if platform == "spotify":
        from scripts.spotify_scraper import (  # noqa: PLC0415
            get_tracks_from_playlist as spotify_get_tracks,
        )
        playlist_name, tracks = spotify_get_tracks(playlist_url)
        return playlist_name, tracks, "spotify"

    elif platform == "soundcloud":
        from scripts.soundcloud_scraper import (  # noqa: PLC0415
            get_tracks_from_playlist as soundcloud_get_tracks,
        )
        playlist_name, tracks = soundcloud_get_tracks(playlist_url)
        return playlist_name, tracks, "soundcloud"

    else:
        write_log.error(
            "PLAYLIST_UNKNOWN_PLATFORM",
            "Unknown or unsupported playlist platform.",
            {"playlist_url": playlist_url}
        )
        raise ValueError(
            f"Unsupported playlist URL format: {playlist_url}. "
            "Supported formats: Spotify (open.spotify.com/playlist/...), "
            "SoundCloud (soundcloud.com/.../sets/...)"
        )


def generate_track_id(platform: str, identifier: str) -> str:  # noqa: ARG001
    """
    Generate a standardized track ID for a given platform.

    Args:
        platform: Platform identifier ('spotify', 'soundcloud')
            Reserved for future platform-specific ID transformations.
        identifier: Platform-specific identifier
            - Spotify: The Spotify track ID (e.g., "5ms8IkagrFWObtzSOahVrx")
            - SoundCloud: The track URL path (e.g., "artist/track-name")

    Returns:
        Standardized track ID. For Spotify, returns the ID as-is.
        For SoundCloud, returns the URL slug.

    Example:
        >>> generate_track_id("spotify", "5ms8IkagrFWObtzSOahVrx")
        "5ms8IkagrFWObtzSOahVrx"
        >>> generate_track_id("soundcloud", "lobsta-b/7th-element-vip")
        "lobsta-b/7th-element-vip"
    """
    # Both platforms can use their native identifiers directly
    # Spotify IDs are alphanumeric, SoundCloud uses URL slugs
    return identifier
