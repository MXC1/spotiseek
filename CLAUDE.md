# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spotiseek automates downloading playlists from **Spotify and SoundCloud** via Soulseek. It scrapes playlist metadata, searches/downloads tracks through the [slskd](https://github.com/slskd/slskd) API, remuxes files to preferred formats (lossless → WAV, lossy → MP3 320kbps), and exports iTunes-compatible XML libraries for use in Rekordbox/iTunes.

The whole system runs as three Docker Compose services (`slskd`, `workflow`, `dashboard`) and is driven by a Radarr-style task scheduler rather than a single linear script.

## Common Commands

All common operations are `invoke` tasks defined in `tasks.py` (run from the repo root, Windows host):

```powershell
invoke up                       # Start all services (docker-compose up -d --build)
invoke down                     # Stop all services
invoke build                    # Build images only
invoke logs --service workflow  # Follow logs for a specific service (or all if omitted)
invoke exec --service <svc> --command '<cmd>'  # Run a command inside a running container
invoke run-all-tasks            # Run all scheduler tasks once, in dependency order, inside the workflow container
invoke setenv <env>             # Change APP_ENV in .env, then re-runs `invoke up`
invoke nuke [--env=<env>]       # DESTRUCTIVE: docker-compose down + system prune + delete that env's data dirs
invoke lint                     # ruff check scripts/ tasks.py
invoke lint-fix                 # ruff check --fix scripts/ tasks.py
invoke clean                    # Remove __pycache__/*.pyc
invoke test                     # Run pytest via .venv\Scripts\python.exe
```

On Windows hosts, docker-compose/docker invocations are automatically wrapped with `wsl` (see `wrap_docker_cmd` in `tasks.py`); no need to prefix commands yourself.

### Tests

- Test suite: `tests/` (pytest). Run everything with `invoke test`, or directly with `pytest` / `pytest tests/test_playlist_prune.py -k test_name` from an environment that has the project's dependencies installed (see `requirements.txt` / `.venv`).
- Tests do **not** require Docker or a live slskd instance — they set `APP_ENV=test` and monkeypatch `scripts.database_management._BASE_DB_DIR` to a `tmp_path`, then exercise `scripts.workflow` helper functions directly against a real (temporary) SQLite DB.
- The application code itself (`scripts/workflow.py`, `scripts/task_scheduler.py`) is meant to run **inside the Docker containers**, not directly on the host — it depends on the dockerized `slskd` service and environment-scoped file layout. Use `invoke exec --service workflow --command '...'` to run one-off commands inside the container instead of invoking scripts locally.

### Linting

Ruff config lives in `pyproject.toml`, targeting Python 3.10+. Enabled rule groups include pycodestyle (E/W), Pyflakes (F), isort (I), bugbear (B), comprehensions (C4), pyupgrade (UP), simplify (SIM), pie (PIE), Ruff-specific (RUF), pylint (PL), perflint (PERF), and arg-names (ARG). `slskd_docker_data/`, `output/`, and `observability/` are excluded from linting; `tests/*` has relaxed rules for magic values and asserts.

## Architecture

### Core Components (`scripts/`)

| File | Responsibility |
|---|---|
| `workflow.py` | Main orchestrator — defines the task functions (`task_scrape_playlists`, `task_initiate_searches`, `task_poll_search_results`, `task_sync_download_status`, `task_mark_quality_upgrades`, `task_process_upgrades`, `task_export_library`, `task_remux_existing_files`) that the scheduler runs, plus playlist pruning/orphan-cleanup logic |
| `task_scheduler.py` | Radarr-style scheduler: registers `TaskDefinition`s with intervals/dependencies, tracks `TaskRun` history in the DB, runs as a daemon or via `--run`/`--run-all`/`--list` CLI flags |
| `playlist_scraper.py` | Unified dispatch layer — `get_tracks_from_playlist()` detects Spotify vs SoundCloud from the URL and delegates to the matching scraper |
| `spotify_scraper.py` | Spotify integration via spotipy client-credentials flow |
| `soundcloud_scraper.py` | SoundCloud scraping with no API key — pulls `__sc_hydration` JSON from playlist HTML, then dynamically discovers a `client_id` to call the SoundCloud v2 API |
| `soulseek_client.py` | slskd HTTP API client — search, async download, quality/format selection, blacklist handling, redownload queueing |
| `database_management.py` | `TrackDB` — thread-safe SQLite singleton, one instance per resolved `db_path` (see below) |
| `m3u8_manager.py` | Writes/updates `.m3u8` playlist files, replacing placeholder comments with resolved local file paths |
| `xml_exporter.py` | Generates the iTunes-compatible `Library.xml` consumed by Rekordbox/iTunes |
| `logs_utils.py` | JSON-structured logging (`write_log` static class) plus log-parsing helpers reused by the dashboard |
| `constants.py` | Shared audio format sets (`LOSSLESS_FORMATS`, `LOSSY_FORMATS`) and `MIN_BITRATE_KBPS` |

`observability/` holds the Streamlit dashboard: `combined_dashboard.py` is the entry point, with feature tabs under `observability/dashboard/tabs/` (`auto_import`, `blacklist`, `docs`, `execution_inspection`, `manual_import`, `overall_stats`, `tasks`).

### Docker Services (`docker-compose.yml`)

- **slskd** — the Soulseek P2P daemon (ports 5030/5031), configured via `slskd_docker_data/slskd.yml` and `SLSKD_USERNAME`/`SLSKD_PASSWORD`.
- **workflow** — runs `scripts.task_scheduler --daemon` (built from `infra/Dockerfile.workflow`); this is where all downloading/processing happens.
- **dashboard** — Streamlit UI on port 8501 (built from `infra/Dockerfile.dashboard`), for monitoring, manual imports, and triggering tasks.

Each service mounts `./output`, `./observability`, and the relevant `slskd_docker_data/${APP_ENV}` subfolders as volumes, so code changes to `scripts/` require a rebuild (`invoke up` always passes `--build`) but data persists on the host.

### Data Flow

1. Playlist URLs in `input_playlists/playlists_{APP_ENV}.csv` (Spotify and SoundCloud URLs can be mixed; `# Heading` lines group the playlists below them into folders — see `docs/CONFIGURATION.md`) →
2. `playlist_scraper.get_tracks_from_playlist()` → track metadata →
3. `TrackDB` (SQLite at `output/{APP_ENV}/database_{APP_ENV}.db`) →
4. `soulseek_client` searches/downloads via slskd → files land in `slskd_docker_data/{APP_ENV}/downloads/` →
5. Remux to preferred format (lossless → WAV, lossy → MP3 320kbps, governed by `PREFER_MP3`) and import → `slskd_docker_data/{APP_ENV}/imported/` →
6. `m3u8_manager` updates `output/{APP_ENV}/m3u8s/` →
7. `xml_exporter` regenerates `output/{APP_ENV}/library_{APP_ENV}.xml` for Rekordbox/iTunes import.

Playlist pruning is deferred/two-phase (see `_prune_removed_tracks_for_playlist`, `_prune_missing_playlists`, `_cleanup_orphaned_tracks` in `workflow.py` and `tests/test_playlist_prune.py`): tracks removed from one playlist are only collected as *orphan candidates*, and only deleted from the DB/disk after all playlists have been reprocessed. This prevents mass re-downloads when playlists are split, merged, or reordered in the CSV.

Folder membership (`playlist_folder_memberships` table) is fully derived from the CSV and rebuilt from scratch on every `task_scrape_playlists` run (`folder_name = ''` means top level). `xml_exporter` reads it to emit iTunes folder `<dict>`s (`Folder`/`Parent Persistent ID`); a playlist in N folders becomes N playlist entries with deterministic persistent IDs. Folders are XML/Rekordbox-only — m3u8 files and the dashboard stay flat. See `CONTEXT.md` for the glossary and `docs/adr/0001-playlist-folders-in-itunes-xml.md` for the rationale.

### Environment Isolation (`APP_ENV`)

Every operation is scoped by `APP_ENV` (`test`, `stage`, `prod`, or any custom name), read from `.env`. This isolates, per environment:

- `output/{APP_ENV}/database_{APP_ENV}.db` — SQLite database
- `output/{APP_ENV}/library_{APP_ENV}.xml` — iTunes XML export
- `output/{APP_ENV}/m3u8s/` — M3U8 playlists
- `slskd_docker_data/{APP_ENV}/downloads/` and `.../imported/`
- `observability/logs/{APP_ENV}/`
- `input_playlists/playlists_{APP_ENV}.csv`

Switch environments with `invoke setenv <env>` (updates `.env` and restarts containers), never by editing paths by hand. `TrackDB` re-reads `APP_ENV` per construction rather than binding it at import time, so it stays correct in the long-lived scheduler/dashboard processes.

## Code Patterns

### Logging

Structured JSON logging is consolidated into daily `task_scheduler.log.YYYY-MM-DD` files (FFMPEG output is the only exception). Always log via `write_log`, not the stdlib `logging` module directly:

```python
from scripts.logs_utils import setup_logging, write_log

setup_logging(log_name_prefix="task_scheduler", rotate_daily=True)
write_log.info("EVENT_ID", "Human message.", {"key": "value"})
write_log.error("DOWNLOAD_FAIL", "Download failed.", {"track_id": "abc", "error": str(e)})
```

### Database Access

Always go through the `TrackDB` singleton (`scripts/database_management.py`) — never open a raw `sqlite3.connect`. `TrackDB()` returns the same instance per resolved absolute `db_path`.

- `track_id` is the primary key: a Spotify alphanumeric ID or a SoundCloud URL slug (e.g. `lobsta-b/7th-element-vip`) — never a platform-specific numeric ID.
- `source` is `'spotify'` or `'soundcloud'`.
- Track lookups always go through `track_id`, regardless of source platform.

### Module Import Pattern

Every entry-point-style module in `scripts/` loads `.env` and disables bytecode caching before importing local modules, and validates `APP_ENV` is set:

```python
import os
import sys
sys.dont_write_bytecode = True
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)
from scripts.database_management import TrackDB  # noqa: E402 (import after load_dotenv)
```

### Task Registration (`task_scheduler.py`)

New scheduled tasks are added as `TaskDefinition`s with an interval env var, a default interval, and optional dependencies on other tasks:

```python
TaskDefinition(
    name="sync_download_status",
    display_name="Sync Downloads",
    function=update_download_statuses,
    interval_env_var="TASK_SYNC_INTERVAL_MINUTES",
    default_interval_minutes=5,
    dependencies=["initiate_searches"],
)
```

Task execution history is tracked in the `task_runs`/`task_state` tables inside the same SQLite DB.

## Documentation

- `docs/CONFIGURATION.md` — full environment variable reference, track-selection/quality algorithm, task CLI usage
- `docs/DASHBOARD.md` — dashboard usage guide
- `docs/TROUBLESHOOTING.md` — common issues
