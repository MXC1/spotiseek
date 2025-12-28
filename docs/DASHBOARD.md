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

## Accessing slskd Web UI

The slskd daemon has its own web interface for direct Soulseek management:

- **URL**: http://localhost:5030
- **Credentials**: Use the `SLSKD_USERNAME` and `SLSKD_PASSWORD` from your `.env`

This is useful for:
- Monitoring active downloads directly
- Checking Soulseek connection status
- Browsing user shares
- Managing the download queue manually
