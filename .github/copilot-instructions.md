# Spotiseek - AI Coding Guidelines

## Project Overview
Spotiseek automates downloading Spotify playlists via Soulseek. It scrapes Spotify for track metadata, searches/downloads through the slskd API, remuxes files to preferred formats (lossless→WAV, lossy→MP3 320kbps), and exports iTunes-compatible XML libraries.

## Architecture

### Core Components
```
scripts/
├── workflow.py          # Main orchestrator - playlist processing, downloads, exports
├── task_scheduler.py    # Radarr-style task scheduler with intervals and dependencies  
├── spotify_scraper.py   # Spotify API integration (spotipy client credentials flow)
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
1. CSV playlist URLs (`input_playlists/playlists_{APP_ENV}.csv`) → Spotify API
2. Track metadata → SQLite database (`database/{APP_ENV}/`)
3. slskd search/download → `slskd_docker_data/{APP_ENV}/downloads/`
4. Remux & import → `slskd_docker_data/{APP_ENV}/imported/`
5. M3U8 playlists → `database/m3u8s/{APP_ENV}/`
6. iTunes XML → `database/xml/{APP_ENV}/`

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
Always use structured logging via `write_log`:
```python
from scripts.logs_utils import setup_logging, write_log

setup_logging(log_name_prefix="my_module")
write_log.info("EVENT_ID", "Human message.", {"key": "value"})
write_log.error("DOWNLOAD_FAIL", "Download failed.", {"track_id": "abc", "error": str(e)})
```

### Database Access
Use `TrackDB` singleton - never create direct SQLite connections:
```python
from scripts.database_management import TrackDB, TrackData
track_db = TrackDB()  # Returns same instance per db_path
track_db.add_track(TrackData(spotify_id="...", track_name="...", artist="..."))
```

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
- Excluded: `slskd_docker_data/`, `database/`, `observability/`
