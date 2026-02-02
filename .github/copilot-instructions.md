# Spotiseek - AI Coding Guidelines

## Project Overview
Spotiseek automates downloading playlists from **Spotify and SoundCloud** via Soulseek. It scrapes playlist metadata, searches/downloads through the slskd API, remuxes files to preferred formats (lossless→WAV, lossy→MP3 320kbps), and exports iTunes-compatible XML libraries.

## Architecture

### Core Components
```
scripts/
├── workflow.py          # Main orchestrator - playlist processing, downloads, exports
├── task_scheduler.py    # Radarr-style task scheduler with intervals and dependencies
├── playlist_scraper.py  # Unified abstraction for multi-platform playlist scraping
├── spotify_scraper.py   # Spotify API integration (spotipy client credentials flow)
├── soundcloud_scraper.py # SoundCloud scraping (no API key required)
├── soulseek_client.py   # slskd API client - search, download, quality selection
├── database_management.py # Thread-safe singleton SQLite (TrackDB class)
├── m3u8_manager.py      # M3U8 playlist files with comment→path replacement
├── xml_exporter.py      # iTunes Music Library.xml generation
└── logs_utils.py        # JSON-structured logging with write_log static class
```

### Docker Services (docker-compose.yml)
- **slskd**: Soulseek daemon for peer-to-peer downloads
- **workflow**: Task scheduler daemon (`--daemon` mode)
- **dashboard**: Streamlit UI for monitoring/manual imports

### Data Flow
1. CSV playlist URLs (`input_playlists/playlists_{APP_ENV}.csv`) → Spotify API or SoundCloud scraper
2. Track metadata → SQLite database (`output/{APP_ENV}/database_{APP_ENV}.db`)
3. slskd search/download → `slskd_docker_data/{APP_ENV}/downloads/`
4. Remux & import → `slskd_docker_data/{APP_ENV}/imported/`
5. M3U8 playlists → `output/{APP_ENV}/m3u8s/`
6. iTunes XML → `output/{APP_ENV}/library_{APP_ENV}.xml`

## Supported Platforms

### Spotify
- Uses official Spotify API via spotipy (client credentials flow)
- Requires `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` in `.env`
- Track ID format: Alphanumeric ID (e.g., `5ms8IkagrFWObtzSOahVrx`)

### SoundCloud
- **No API key required** - scrapes public data
- Extracts `__sc_hydration` JSON from HTML for initial metadata
- Dynamically discovers `client_id` from SoundCloud JS bundles for API calls
- Uses SoundCloud API v2 to fetch full details for stub tracks
- Track ID format: URL slug (e.g., `lobsta-b/7th-element-vip`)

### Adding Playlists to CSV
Mix Spotify and SoundCloud URLs in the same CSV file:
```csv
https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M
https://soundcloud.com/courtjester-uk/sets/donk-and-bits
https://open.spotify.com/playlist/0vvXsWCC9xrXsKd4FyS8kM
```

## Environment Configuration

**Critical**: All operations are environment-scoped via `APP_ENV` in `.env`:
- `test`, `prod`, `stage` - separate databases, logs, and file directories
- Use `invoke setenv <env>` to switch environments and restart containers

Required `.env` variables are documented in `.env.example`.

## Developer Workflow

### Common Commands (Invoke tasks in tasks.py)
```powershell
invoke up              # Start all Docker services with rebuild (necessary for code changes to take effect)
invoke down            # Stop services
invoke logs --service workflow  # Follow logs for specific service
invoke run-all-tasks   # Execute all tasks in dependency order (inside container)
invoke nuke --env=test # DESTRUCTIVE: Remove all data for environment
invoke lint-fix        # Auto-fix with ruff
invoke setenv test2     # Switch environment and restart
```

### Running Locally (without Docker)
The script should not be run locally.
All operations depend on the Dockerized slskd service and environment isolation.

## Code Patterns

### Logging Convention
All logs (except FFMPEG) are consolidated in daily `task_scheduler.log.YYYY-MM-DD` files.
Always use structured logging via `write_log`:
```python
from scripts.logs_utils import setup_logging, write_log

# For all modules, use task_scheduler prefix with daily rotation for unified logs
setup_logging(log_name_prefix="task_scheduler", rotate_daily=True)
write_log.info("EVENT_ID", "Human message.", {"key": "value"})
write_log.error("DOWNLOAD_FAIL", "Download failed.", {"track_id": "abc", "error": str(e)})
```

### Database Access
Use `TrackDB` singleton - never create direct SQLite connections:
```python
from scripts.database_management import TrackDB, TrackData
track_db = TrackDB()  # Returns same instance per db_path
track_db.add_track(TrackData(track_id="...", track_name="...", artist="...", source="spotify"))
```

**Database Schema Notes:**
- `track_id`: Primary key - Spotify alphanumeric ID or SoundCloud URL slug
- `source`: Platform identifier (`'spotify'` or `'soundcloud'`)
- All track lookups use `track_id`, not platform-specific IDs

### Environment Imports Pattern
All scripts follow this pattern for environment loading:
```python
import os
import sys
sys.dont_write_bytecode = True
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)
# Then import local modules AFTER load_dotenv
from scripts.database_management import TrackDB  # noqa: E402
```

### Task Registration (task_scheduler.py)
Tasks have intervals, dependencies, and status tracking:
```python
TaskDefinition(
    name="sync_download_status",
    display_name="Sync Downloads",
    function=update_download_statuses,
    interval_env_var="TASK_SYNC_INTERVAL_MINUTES",
    default_interval_minutes=5,
    dependencies=["initiate_searches"]
)
```

## Ruff Linting
Configured in `pyproject.toml` with Python 3.10+ target. Key rules:
- Import sorting (I), Bugbear (B), Modern syntax (UP)
- Excluded: `slskd_docker_data/`, `output/`, `observability/`
