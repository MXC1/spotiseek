# Spotiseek Dashboard Guide

The Streamlit dashboard provides a web interface for monitoring and managing Spotiseek. Access it at **http://localhost:8501** after starting the services.

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

The **Execution Inspection** tab provides deep-dive analysis of workflow runs.

### Features

- **Run Selection**: Dropdown to select specific workflow runs by date
- **Status Badge**: Visual indicator (🟢 completed, 🔴 failed, 🟡 incomplete)

### Summary Statistics

- Total logs, errors, warnings
- New and upgrade searches initiated
- Playlists and tracks added/removed
- Downloads completed (new vs. upgrades)
- Failed downloads

### Analysis Tools

- **Workflow Timeline**: Chronological event log
- **Expandable Error/Warning Sections**: Full context for debugging issues

---

## 📥 Manual Import Tab

The **Manual Import** tab allows you to manually import audio files for tracks that couldn't be found on Soulseek.

### Workflow

1. **Select Playlist**: Choose from playlists with incomplete tracks
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
    - E:/Music:/mnt/music:ro  # Add your music directory (read-only)
```

> **Path Format**: Use forward slashes and the format `HOST_PATH:/CONTAINER_PATH:ro`
> 
> Examples:
> - Windows: `E:/Music/MyLibrary:/mnt/music:ro`
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

## Accessing slskd Web UI

The slskd daemon has its own web interface for direct Soulseek management:

- **URL**: http://localhost:5030
- **Credentials**: Use the `SLSKD_USERNAME` and `SLSKD_PASSWORD` from your `.env`

This is useful for:
- Monitoring active downloads directly
- Checking Soulseek connection status
- Browsing user shares
- Managing the download queue manually
