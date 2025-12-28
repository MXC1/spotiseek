# Spotiseek Configuration

Complete reference for all environment variables and configuration options.

## Required Variables

These must be set for Spotiseek to function:

| Variable | Description |
|----------|-------------|
| `SLSKD_USERNAME` | Your Soulseek account username |
| `SLSKD_PASSWORD` | Your Soulseek account password |
| `TOKEN` | Random API key for slskd API authentication (generate any secure string) |
| `APP_ENV` | Environment name (`test`, `stage`, `prod`, or custom) |
| `HOST_BASE_PATH` | Absolute path to the spotiseek directory on your host machine |

## Spotify Credentials (Optional)

Required only if importing Spotify playlists:

| Variable | Description |
|----------|-------------|
| `SPOTIFY_CLIENT_ID` | From [Spotify Developer Dashboard](https://developer.spotify.com/dashboard) |
| `SPOTIFY_CLIENT_SECRET` | From [Spotify Developer Dashboard](https://developer.spotify.com/dashboard) |

### Getting Spotify API Credentials

1. Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Log in with your Spotify account
3. Click **Create App**
4. Fill in the app details:
   - **App name**: Spotiseek (or any name)
   - **App description**: Playlist downloader
   - **Redirect URI**: `http://localhost:8888/callback` (not used, but required)
5. Check the terms of service box and click **Create**
6. On your app page, click **Settings**
7. Copy the **Client ID** and **Client Secret** to your `.env` file

> **Note**: SoundCloud playlists do not require any API credentials.

---

## Remuxing Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PREFER_MP3` | `true` | `true` = Convert all downloads to MP3 320kbps. `false` = Keep lossless as WAV, convert lossy to MP3 320kbps |

---

## Logging Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Console logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

File logging always captures WARNING/ERROR and dashboard-critical events regardless of this setting.

---

## Task Scheduler Intervals

Configure how often each automated task runs (in minutes). Set to `0` to disable a task.

| Variable | Default | Description |
|----------|---------|-------------|
| `TASK_SCRAPE_PLAYLISTS_INTERVAL` | `1440` | Scrape Spotify/SoundCloud playlists (daily) |
| `TASK_INITIATE_SEARCHES_INTERVAL` | `60` | Start new Soulseek searches (hourly) |
| `TASK_POLL_SEARCH_RESULTS_INTERVAL` | `15` | Check search results and select best matches |
| `TASK_SYNC_DOWNLOAD_STATUS_INTERVAL` | `5` | Sync download status from slskd API |
| `TASK_MARK_QUALITY_UPGRADES_INTERVAL` | `1440` | Check for quality upgrades (daily) |
| `TASK_PROCESS_UPGRADES_INTERVAL` | `60` | Process upgrade queue (hourly) |
| `TASK_EXPORT_LIBRARY_INTERVAL` | `1440` | Export iTunes library (daily) |
| `TASK_REMUX_EXISTING_FILES_INTERVAL` | `360` | Remux files to match preferences (6 hours) |

---

## Example .env File

```env
# Soulseek credentials
SLSKD_USERNAME=your_soulseek_username
SLSKD_PASSWORD=your_soulseek_password

# API authentication (generate any random string)
TOKEN=your_random_api_key_here

# Spotify API (optional - only for Spotify playlists)
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

# Environment
APP_ENV=prod
HOST_BASE_PATH=/path/to/spotiseek

# Format preferences
PREFER_MP3=true

# Logging
LOG_LEVEL=INFO

# Task intervals (minutes) - customize as needed
TASK_SCRAPE_PLAYLISTS_INTERVAL=1440
TASK_INITIATE_SEARCHES_INTERVAL=60
TASK_POLL_SEARCH_RESULTS_INTERVAL=15
TASK_SYNC_DOWNLOAD_STATUS_INTERVAL=5
TASK_MARK_QUALITY_UPGRADES_INTERVAL=1440
TASK_PROCESS_UPGRADES_INTERVAL=60
TASK_EXPORT_LIBRARY_INTERVAL=1440
TASK_REMUX_EXISTING_FILES_INTERVAL=360
```

---

## Track Selection Algorithm

When multiple search results are found for a track, Spotiseek automatically selects the best file based on:

### Selection Process

1. **Filter blacklisted files** - Previously failed downloads are skipped
2. **Filter non-audio files** - Only supported formats: WAV, FLAC, MP3, OGG, M4A, AAC, ALAC, APE, WMA, OPUS
3. **Filter low-bitrate files** - Lossy files must be ≥320kbps
4. **Filter remixes/edits** - Unless the search query includes terms like "remix", "edit", "bootleg", etc.
5. **Sort by quality** - Best quality file is selected

### Quality Priority

Depends on `PREFER_MP3` setting:

**When `PREFER_MP3=true`:**
1. MP3 files (already in target format, no conversion needed)
2. Other formats by bitrate (will be converted to MP3 320kbps)

**When `PREFER_MP3=false`:**
1. Lossless formats: WAV, FLAC, ALAC, APE (will be remuxed to WAV)
2. MP3 by bitrate (will be converted to MP3 320kbps)
3. Other lossy formats: OGG, M4A, AAC, WMA, OPUS (will be converted to MP3 320kbps)

### Excluded Keywords

These variations are filtered out unless explicitly in the search query:
`remix`, `edit`, `bootleg`, `mashup`, `mix`, `acapella`, `instrumental`, `sped up`, `slowed`, `cover`, `karaoke`, `tribute`, `demo`, `live`, `acoustic`, `version`, `remaster`, `flip`, `extended`, `rework`, `re-edit`, `dub`, `radio`

---

## CLI Task Invocation

Tasks can be run directly from the command line without the dashboard:

```bash
# Inside the workflow container
python -m scripts.task_scheduler --list           # List all available tasks
python -m scripts.task_scheduler --run <task>     # Run a specific task
python -m scripts.task_scheduler --run-all        # Run all tasks in dependency order
python -m scripts.task_scheduler --daemon         # Start scheduler daemon (default in Docker)
```

### Via Invoke (from host)

```bash
invoke run-all-tasks                              # Execute all tasks immediately
```

### Task Names

| Task Name | Description |
|-----------|-------------|
| `scrape_playlists` | Fetch track metadata from playlists |
| `initiate_searches` | Queue new tracks for Soulseek search |
| `poll_search_results` | Process completed searches |
| `sync_download_status` | Update status from slskd API |
| `mark_quality_upgrades` | Identify tracks for upgrade |
| `process_upgrades` | Initiate upgrade searches |
| `export_library` | Generate iTunes XML |
| `remux_existing_files` | Convert to preferred formats |

---

## Environment Isolation

All data is scoped by `APP_ENV`:

| Path | Description |
|------|-------------|
| `output/{APP_ENV}/database_{APP_ENV}.db` | SQLite database |
| `output/{APP_ENV}/library_{APP_ENV}.xml` | iTunes XML export |
| `output/{APP_ENV}/m3u8s/` | M3U8 playlist files |
| `slskd_docker_data/{APP_ENV}/downloads/` | Downloaded files |
| `slskd_docker_data/{APP_ENV}/imported/` | Manually imported files |
| `observability/logs/{APP_ENV}/` | Application logs |

Switch environments with:

```bash
invoke setenv <environment_name>
```

This updates `.env` and restarts Docker containers.
