# Spotiseek Dashboard Guide

The dashboard (FastAPI + HTMX, see [ADR-0003](adr/0003-dashboard-rewrite-fastapi-htmx.md))
provides a web interface for monitoring and managing Spotiseek. Access it at
**http://localhost:8501** after starting the services.

## 📊 Stats Tab

The **Overall Stats** tab provides a comprehensive overview of your library:

### Metrics Displayed

- **Unique Playlists**: List of all imported playlists with track counts
- **Track Download Status**: Breakdown of tracks by status (pending, searching, downloading, completed, failed)
- **File Extension Breakdown**: Distribution of downloaded file formats (MP3, FLAC, WAV, etc.)
- **Bitrate Breakdown**: Quality distribution including lossless detection and effective bitrate calculation
- **Download Status**: Downloaded vs. not-yet-downloaded track counts
- **Tracks Without Local Files**: Detailed breakdown of why tracks haven't been downloaded (no results, search pending, etc.)
- **Warning/Error Log Summary**: Aggregated view of recent issues with sample log inspection

---

## ⏱️ Tasks Tab

The **Tasks** tab manages the automated task scheduler (Radarr-style).

### Quick Actions

- **Run All Tasks**: Execute all tasks in dependency order immediately
- **Refresh Status**: Update the task status display

### Task Overview

Each task displays:
- Current status (idle, running, completed, failed)
- Configured interval (from environment variables)
- Last run timestamp
- Next scheduled run
- Manual run button (▶️)

### Available Tasks

See [CONFIGURATION.md](CONFIGURATION.md#task-scheduler-intervals) for the full list of tasks and their default intervals.

### Task History

Filterable execution history showing:
- Task name, status, start/completion times
- Tracks processed count
- Error messages for failed runs

### Task Scheduler Logs

Browse and filter log files with level filtering (INFO, WARNING, ERROR, DEBUG).

---

## 🔍 Execution Inspection Tab

The **Execution Inspection** tab is a live log stream: it tails `docker logs -f` for every
currently running Compose container over SSE and renders lines as they arrive, each tagged
with a best-effort severity.

### Features

- **Per-Container Filtering**: Show/hide log lines by container (workflow, slskd, backup, dashboard)
- **Per-Level Filtering**: Show/hide by severity (TRACE/DEBUG/INFO/WARNING/ERROR/FATAL); DEBUG is hidden by default
- **Per-Task Filtering**: For `workflow`'s log lines specifically, filter to one or more task names
- **Autoscroll**: Toggle whether the pane follows new lines as they arrive
- **Clear**: Wipe the pane without disconnecting the stream

Manually-triggered task runs (from the **Tasks** tab) are pushed into this same view, since
`docker exec` output never reaches `docker logs -f` on its own.

---

## 📥 Manual Import Tab

The **Manual Import** tab allows you to manually import audio files for tracks that couldn't be found on Soulseek.

### Workflow

1. **Select Scope**: Choose **All Playlists** (every track missing a file, including any not in a playlist), a **folder** (the unique missing tracks across its playlists), or a single playlist. Folders and playlists are only listed while they have incomplete tracks
2. **Browse Tracks**: Paginated, searchable list of tracks needing files
3. **Upload File**: Drag and drop or select an audio file (MP3, FLAC, WAV, M4A, OGG, WMA)
4. **Quality Check**: Automatic warning if uploaded file is lower quality than MP3 320kbps
5. **Import**: File is saved, database updated, M3U8 and iTunes XML regenerated

### Features

- Search by artist or track name
- Configurable page size (10, 25, 50, 100 tracks)
- Quality warnings for low-bitrate uploads
- Automatic library export after import

---

## 🤖 Auto Import Tab

The **Auto Import** tab automatically matches audio files from a directory on your system with tracks missing from your library using fuzzy matching.

### Setup: Mounting Your Music Directory

Since the dashboard runs inside Docker, you must mount your music directory into the container. Add a volume to the `dashboard` service in `docker-compose.yml`:

```yaml
dashboard:
  volumes:
    # ... existing volumes ...
    - /mnt/e/Music:/mnt/music:ro  # Add your music directory (read-only)
```

> **Path Format**: `invoke up` runs `docker-compose` inside WSL on Windows hosts (see `wrap_docker_cmd` in `tasks.py`), so the host path is parsed by WSL, not Windows. A raw Windows path such as `E:/Music:/mnt/music:ro` fails with `invalid volume specification` — the drive-letter colon collides with docker's `host:container:mode` separator.
>
> Examples:
> - Windows: use the WSL-mounted-drive path — `/mnt/e/Music/MyLibrary:/mnt/music:ro` (WSL2 auto-mounts fixed drives under `/mnt/<lowercase-drive-letter>`)
> - Linux/Mac: `/home/user/Music:/mnt/music:ro`

After editing, restart the services:

```bash
invoke up
```

### Workflow

1. **Enter Container Path**: Use the mounted path (e.g., `/mnt/music/folder`), not the Windows path
2. **Scan Directory**: Click "Scan Directory" to recursively find all audio files
3. **Review Matches**: Matches are displayed sorted by fuzzy match score (highest first)
4. **Check Quality**: Each file shows quality status:
   - ✅ = Acceptable quality (MP3 ≥320kbps or lossless)
   - ⚠️ = Below MP3 320kbps quality
5. **Select Matches**: Use checkboxes to select which matches to import
6. **Import**: Click "Import Selected Tracks" to copy files to your library

### Match Scoring

The tool uses multiple matching strategies and displays the best score:

| Score | Indicator | Confidence |
|-------|-----------|------------|
| ≥90%  | 🟢 | Excellent - high confidence match |
| 70-89% | 🟡 | Good - review recommended |
| 50-69% | 🟠 | Fair - careful review needed |
| <50%  | 🔴 | Poor - likely incorrect |

### Features

- **Recursive Scanning**: Finds audio files in all subdirectories
- **Metadata Extraction**: Reads artist/title from ID3 tags, falls back to filename parsing
- **Quality Warnings**: Warns before importing files below MP3 320kbps
- **Bulk Import**: Select multiple matches and import all at once
- **Non-Destructive**: Files are copied (originals remain in place)
- **Filtering**: Filter by minimum score or search by track/artist name
- **Pagination**: Handle large match lists with configurable page sizes

### Supported Audio Formats

MP3, FLAC, WAV, M4A, OGG, WMA, AAC, ALAC, AIFF

---

## 🗄️ Database Tab

Design: `docs/adr/0008-dashboard-database-explorer.md` (the tab) and `docs/adr/0009-track-status-changed-at.md` (the status age it relies on).

The **Database** tab is a read-only explorer for the current environment's SQLite database, for working out why a track is stuck or failed, checking that the data still agrees with itself, and browsing what's there. It never edits data, and it only ever shows the environment the dashboard is currently running against.

It has two sub-views, **Tables** and **Audit**. Data only refreshes when you click **Refresh** or reload — it does not poll.

### Tables

- Lists every table in the database (including the scheduler's own `task_runs`/`task_state`), each with its row count and a viewable schema.
- Each table pages (default 25 rows; 10/25/50/100 available), sorts by any column, and filters per column.
- Foreign-key columns are links: a `playlist_tracks` row links to its track and its playlist.
- Clicking a track opens its **track detail** page (`/database/track?track_id=…`): the full row, its playlists and folders, any matching blacklist entry, how long it has been in its current status, and — computed when you open the page — whether its file exists (with size and last-modified time) and whether each playlist's `.m3u8` holds its file path or still the placeholder comment. The page URL is shareable and works with the back button.

### Audit

Audit checks report a count and the offending rows; every track row links to its detail page.

| Group | Runs | What it checks |
|-------|------|----------------|
| **Referential integrity** | On load | `playlist_tracks` rows pointing at a missing track or playlist; tracks in no playlist (orphans); folder memberships for unknown playlists; playlists with no folder membership |
| **Status / field consistency** | On load | `completed` with no file path; `blacklisted` that still has a file path; `searching` with no search UUID; `queued`/`downloading` with no download UUID or no Soulseek username; `failed` with no reason |
| **Stuck tracks** | On load | Tracks in an in-flight status longer than that status normally lasts (see below) |
| **Disk checks** | Only when you click **Run** | Completed tracks whose file is missing; playlists whose `.m3u8` is missing (or has no path recorded); audio files in `imported/` that no track points at |
| **Database health** | Only when you click **Run** | File size, page/freelist counts, journal mode, any leftover `-journal` file, and SQLite's `PRAGMA quick_check` |

Only `blacklisted` tracks are checked for a stray file path: a quality upgrade legitimately keeps a track's old file while it is `searching`/`downloading` again, so a path on those statuses is normal.

Disk checks and the health check read every file or the whole database file, so they run only on demand and can be slow on a large library.

**What the file checks can see.** A track's recorded file path names the workflow container's filesystem, and the dashboard container only mounts part of it: `imported/`, and `downloads/` read-only at `/mnt/spotiseek/downloads` (see `docker-compose.yml`). A file anywhere else is reported as **not checked** (the track detail page says "not visible from here"), never as missing, and if nothing could be checked the result says so instead of "nothing flagged". If a disk check reports many tracks as not checked, the dashboard container was probably started before that mount existed: run `invoke up` to recreate it.

#### Stuck tracks

A track is **stuck** when it has been in one in-flight status longer than that status normally lasts. The clock is `status_changed_at`, which moves only when the status *value* changes — a retry that sets `searching` again on a track already searching does not reset it. The thresholds are constants in `scripts/constants.py` (`STUCK_THRESHOLD_HOURS`):

| Status | Stuck after |
|--------|-------------|
| `pending` | 3 h |
| `searching` | 3 h |
| `downloading` | 6 h |
| `queued` | 48 h |
| `redownload_pending` | 48 h |

Tracks in a final status (`completed`, `failed`, `not_found`, `no_suitable_file`, `blacklisted`) are never stuck. The track detail page shows a track's status age and flags it when it is over its threshold.

> **After the first deploy:** the migration that adds `status_changed_at` backfills every existing track with the migration time, so the stuck-track check reports nothing until statuses have had time to age past their thresholds.

---

## Accessing slskd Web UI

The slskd daemon has its own web interface for direct Soulseek management:

- **URL**: http://localhost:5030
- **Credentials**: Use the `SLSKD_USERNAME` and `SLSKD_PASSWORD` from your `.env`

This is useful for:
- Monitoring active downloads directly
- Checking Soulseek connection status
- Browsing user shares
- Managing the download queue manually
