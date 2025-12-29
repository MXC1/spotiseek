# 🎵 Spotiseek

Spotiseek automates downloading playlists from **Spotify** and **SoundCloud** via Soulseek. It scrapes playlist metadata, searches and downloads tracks through the [slskd](https://github.com/slskd/slskd) API, remuxes files to preferred formats, and exports iTunes-compatible XML libraries.

## Features

- **Multi-Platform**: Import playlists from Spotify and SoundCloud
- **Automated Downloads**: Searches Soulseek and manages downloads automatically
- **Quality Control**: Remuxes to preferred formats (lossless → WAV, lossy → MP3 320kbps)
- **Library Export**: Generates iTunes-compatible XML and M3U8 playlists
- **Task Scheduler**: Radarr-style automation with configurable intervals
- **Web Dashboard**: Streamlit UI for monitoring, manual imports, and task management
- **Auto Import**: Fuzzy-match local audio files to missing tracks with bulk import
- **Quality Upgrades**: Automatically identifies and upgrades lower-quality tracks

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- A Soulseek account ([slsknet.org](http://www.slsknet.org/))
- Spotify API credentials (optional, only for Spotify playlists)

### 1. Clone and Configure

```bash
git clone https://github.com/MXC1/spotiseek.git
cd spotiseek
cp .env.example .env
```

Edit `.env` with your Soulseek credentials, API token, and environment settings. See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for all options and examples.

### 2. Add Playlists

Create `input_playlists/playlists_prod.csv`:

```csv
https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M
https://soundcloud.com/courtjester-uk/sets/donk-and-bits
```

### 3. Start Services

```bash
pip install invoke  # If not installed
invoke up
```

This starts:
- **slskd** (Soulseek daemon) - http://localhost:5030
- **workflow** (Task scheduler)
- **dashboard** (Web UI) - http://localhost:8501

---

## Common Commands

| Command | Description |
|---------|-------------|
| `invoke up` | Start all services (with rebuild) |
| `invoke down` | Stop all services |
| `invoke logs --service workflow` | Follow logs for a service |
| `invoke run-all-tasks` | Execute all tasks immediately |
| `invoke setenv test` | Switch environment and restart |
| `invoke nuke --env=test` | **DESTRUCTIVE**: Remove all data for an environment |

---

## Architecture

```
CSV Playlist URLs
    ↓
Spotify API / SoundCloud Scraper
    ↓
SQLite Database (track metadata)
    ↓
slskd Search & Download
    ↓
Remux & Import
    ↓
├── M3U8 Playlists
└── iTunes XML Library
```

### Docker Services

| Service | Port | Purpose |
|---------|------|---------|
| `slskd` | 5030, 5031 | Soulseek P2P daemon |
| `workflow` | - | Task scheduler daemon |
| `dashboard` | 8501 | Streamlit web UI |

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for all task scheduler intervals and settings.

### Directory Structure

```
spotiseek/
├── input_playlists/          # Playlist CSV files
├── scripts/                  # Core application code
├── observability/            # Dashboard and logs
├── output/{APP_ENV}/         # Database, XML, M3U8s
└── slskd_docker_data/{APP_ENV}/  # Downloads
```

---

## Supported Platforms

### Spotify
- Uses official API via [spotipy](https://spotipy.readthedocs.io/)
- Requires `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET`
- See [docs/CONFIGURATION.md](docs/CONFIGURATION.md#getting-spotify-api-credentials) for setup

### SoundCloud
- **No API key required** - scrapes public data
- Just add playlist URLs to your CSV

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | All environment variables and settings |
| [docs/DASHBOARD.md](docs/DASHBOARD.md) | Web dashboard guide |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues and solutions |

---

## Development

```bash
invoke lint        # Run ruff linter
invoke lint-fix    # Auto-fix linting issues
invoke clean       # Remove __pycache__ files
```

---

## Acknowledgments

- [slskd](https://github.com/slskd/slskd) - Soulseek daemon
- [spotipy](https://spotipy.readthedocs.io/) - Spotify API client
- [Streamlit](https://streamlit.io/) - Dashboard framework
- [mutagen](https://mutagen.readthedocs.io/) - Audio metadata handling
