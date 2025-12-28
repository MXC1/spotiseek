# Troubleshooting Spotiseek

Common issues and their solutions.

## Viewing Logs

### Via Docker

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f workflow
docker-compose logs -f slskd
docker-compose logs -f dashboard
```

### Via Invoke

```bash
invoke logs --service workflow
```

### Log Files

Structured JSON logs are stored in `observability/logs/{APP_ENV}/`.

---

## Common Issues

### "Database file not found"

**Cause**: The database is created on first workflow run.

**Solutions**:
- Wait for the scheduler to run automatically, or
- Manually trigger: `invoke run-all-tasks`

---

### "Spotify credentials invalid"

**Solutions**:
1. Verify `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` in `.env`
2. Check that your Spotify app is active in the [Developer Dashboard](https://developer.spotify.com/dashboard)
3. Ensure credentials have no extra whitespace
4. Regenerate the client secret if needed

---

### "No search results found"

**Cause**: The track isn't available on Soulseek.

**Solutions**:
1. Wait for the scheduler to retry automatically (searches are retried periodically)
2. Use the **Manual Import** tab in the dashboard to upload the file yourself
3. Check track details - sometimes artist/title variations help
4. Try searching manually in slskd web UI (http://localhost:5030)

---

### "slskd connection refused"

**Solutions**:
1. Ensure the slskd container is running:
   ```bash
   docker-compose ps
   ```
2. Check slskd logs for errors:
   ```bash
   docker-compose logs slskd
   ```
3. Verify your Soulseek credentials are correct in `.env`
4. Check if Soulseek servers are online (try the official client)

---

### Tasks not running

**Solutions**:
1. Check task intervals in `.env` (set to `0` = disabled)
2. Verify the workflow container is running:
   ```bash
   docker-compose ps
   ```
3. Check for errors:
   ```bash
   docker-compose logs workflow
   ```
4. Use the Tasks tab in the dashboard to manually trigger tasks

---

### Track stuck in "searching" status

**Cause**: Search completed but no suitable results found, or slskd hasn't responded.

**Solutions**:
1. Check the slskd web UI (http://localhost:5030) for search status
2. Run the "Poll Search Results" task manually from the dashboard
3. If persists, the track may not be available on Soulseek - use Manual Import

---

### Downloads stuck at 0%

**Cause**: The peer went offline or has download restrictions.

**Solutions**:
1. Wait - the scheduler will retry with different sources
2. Check slskd web UI for queue status
3. The "Sync Download Status" task will mark failed downloads for retry

---

### Wrong file format after download

**Cause**: `PREFER_MP3` setting doesn't match your preference.

**Solutions**:
1. Update `PREFER_MP3` in `.env`:
   - `true` = All files converted to MP3 320kbps
   - `false` = Lossless kept as WAV, lossy converted to MP3 320kbps
2. Run "Remux Existing Files" task to convert existing downloads

---

## Resetting an Environment

To completely reset an environment (⚠️ **DESTRUCTIVE**):

```bash
invoke nuke --env=test
```

This removes:
- Downloaded files
- Database
- Logs
- M3U8 and XML exports

You'll be prompted for confirmation on `prod` and `stage` environments.

---

## Inspecting the Database

The SQLite database is at `output/{APP_ENV}/database_{APP_ENV}.db`.

### Using sqlite3

```bash
sqlite3 output/prod/database_prod.db
```

### Key Tables

| Table | Purpose |
|-------|---------|
| `playlists` | Imported playlist metadata |
| `tracks` | Track information and download status |
| `playlist_tracks` | Many-to-many playlist/track relationships |
| `task_runs` | Task execution history |
| `task_state` | Current task scheduling state |

### Useful Queries

```sql
-- Count tracks by status
SELECT download_status, COUNT(*) FROM tracks GROUP BY download_status;

-- Find failed tracks
SELECT track_name, artist, failed_reason FROM tracks WHERE download_status = 'failed';

-- List recent task runs
SELECT task_name, status, started_at FROM task_runs ORDER BY started_at DESC LIMIT 20;
```

---

## Download Status Lifecycle

Each track progresses through these statuses:

```
┌─────────────────────── NEW DOWNLOAD FLOW ───────────────────────┐
│                                                                  │
│  pending → searching → queued → downloading → completed          │
│                 │                    │                           │
│                 └──────► failed ◄────┘                           │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

┌─────────────────────── QUALITY UPGRADE FLOW ────────────────────┐
│                                                                  │
│  completed → redownload_pending → requested → inprogress         │
│       ▲              │                              │            │
│       │              └───────► failed ◄─────────────┘            │
│       │                                             │            │
│       └─────────────────────────────────────────────┘            │
│                      (on success)                                │
└──────────────────────────────────────────────────────────────────┘
```

| Status | Description | Next Action |
|--------|-------------|-------------|
| `pending` | Track added, not yet searched | "Initiate Searches" task |
| `searching` | Search in progress on slskd | "Poll Search Results" task |
| `queued` | Best file selected, download requested | slskd processes queue |
| `downloading` | Download in progress | "Sync Download Status" task monitors |
| `completed` | File downloaded and remuxed | None - track is done |
| `failed` | Download failed (reason stored) | Scheduler retries automatically |
| `redownload_pending` | Marked for quality upgrade | "Process Upgrades" task |
| `requested` | Upgrade search initiated | "Poll Search Results" task |
| `inprogress` | Upgrade download in progress | "Sync Download Status" task |

### Failed Reasons

Common `failed_reason` values:
- `no_results` - No files found on Soulseek
- `no_suitable_files` - Files found but none met quality requirements
- `download_failed` - slskd reported download failure
- `peer_offline` - Source user went offline
- `file_not_found` - File no longer available

---

## SoundCloud-Specific Behavior

SoundCloud playlists have some unique characteristics compared to Spotify:

### No API Key Required

Unlike Spotify, SoundCloud playlists work without any API credentials. The scraper:
1. Fetches the playlist page HTML
2. Extracts embedded `__sc_hydration` JSON data for initial metadata
3. Dynamically discovers the `client_id` from SoundCloud's JavaScript bundles
4. Uses SoundCloud API v2 to fetch full details for tracks

### Stub Track Limitation

SoundCloud only embeds full metadata for the **first 5 tracks** in a playlist. Additional tracks are "stubs" with minimal data. Spotiseek automatically detects these and fetches complete details via API.

### Track ID Format

SoundCloud tracks use URL slugs as IDs (e.g., `lobsta-b/7th-element-vip`) rather than alphanumeric IDs like Spotify. This affects database queries and manual lookups.

### Common SoundCloud Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| "No tracks found" | Private playlist or invalid URL | Ensure playlist is public |
| Missing tracks | Deleted or geo-restricted tracks | These cannot be recovered |
| Wrong artist/title | SoundCloud metadata often informal | Track will still search on Soulseek |

---

## Download Blacklist System

When a download fails, Spotiseek blacklists that specific Soulseek file to avoid retrying the same broken source.

### How It Works

1. Download fails (peer offline, corrupt file, etc.)
2. The `slskd_uuid` is added to `slskd_blacklist` table with a reason
3. Future searches skip blacklisted files when selecting best match
4. The track remains in `failed` status for retry with a different source

### Viewing Blacklisted Files

```sql
SELECT slskd_uuid, reason, added_at FROM slskd_blacklist ORDER BY added_at DESC;
```

### Clearing Blacklist (Use Sparingly)

```sql
-- Clear all blacklist entries
DELETE FROM slskd_blacklist;

-- Clear specific entry
DELETE FROM slskd_blacklist WHERE slskd_uuid = 'uuid-here';
```

---

## Manually Resetting Track Status

If a track is stuck or you want to force a re-download:

### Reset a Single Track

```sql
-- Reset to pending (will search again)
UPDATE tracks 
SET download_status = 'pending', 
    failed_reason = NULL, 
    slskd_search_uuid = NULL,
    slskd_download_uuid = NULL
WHERE track_id = 'your-track-id';
```

### Reset All Failed Tracks

```sql
UPDATE tracks 
SET download_status = 'pending', 
    failed_reason = NULL,
    slskd_search_uuid = NULL,
    slskd_download_uuid = NULL
WHERE download_status = 'failed';
```

### Force Quality Upgrade

```sql
-- Mark a completed track for upgrade search
UPDATE tracks 
SET download_status = 'redownload_pending'
WHERE track_id = 'your-track-id' AND download_status = 'completed';
```

---

## LOG_LEVEL Behavior

The `LOG_LEVEL` environment variable controls logging verbosity differently for console vs file output:

### Console Output

- Shows only logs at or above the configured level
- Default: `INFO` (shows INFO, WARNING, ERROR)
- Set to `DEBUG` for verbose troubleshooting

### File Output

File logging (`observability/logs/{APP_ENV}/`) **always** captures:
- All WARNING and ERROR messages
- Dashboard-critical events (track status changes, task completions)
- This ensures important events are logged regardless of console setting

### Recommended Settings

| Scenario | LOG_LEVEL | Notes |
|----------|-----------|-------|
| Normal operation | `INFO` | Default, shows task progress |
| Troubleshooting | `DEBUG` | Verbose output, includes API calls |
| Minimal output | `WARNING` | Only problems shown |
| Production (quiet) | `ERROR` | Only errors shown |

---

## Getting Help

1. Check the [Dashboard](DASHBOARD.md) for real-time status
2. Review logs in `observability/logs/{APP_ENV}/`
3. Open an issue on GitHub with:
   - Your `APP_ENV` setting
   - Relevant log excerpts
   - Steps to reproduce
