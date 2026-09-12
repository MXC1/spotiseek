# Spotiseek

Spotiseek automates downloading playlists from Spotify and SoundCloud via Soulseek, remuxing the results, and exporting an iTunes-compatible library for Rekordbox.

## Language

**Environment**:
An isolated instance of the whole pipeline, identified by `APP_ENV`, with its own database, XML export, downloads/imports, logs, and playlist CSV. Only one environment's containers ever run at a time.
_Avoid_: instance, workspace, profile

**Hot environment**:
The environment matching the current `APP_ENV` — the one with running containers and files that may be open or mid-write. Every other environment is cold: inert on disk, safe to copy directly without pausing anything.
_Avoid_: active environment, current environment

**Backup**:
A restic snapshot of one environment's `downloads/`, `imported/`, exported library (database, XML, m3u8 playlists), per-environment config, and capped logs, stored in that environment's own restic repository. Taken on-demand or on a schedule for configured environments.
_Avoid_: archive, dump, export (export already means the iTunes XML generation step)

**Restore (in-place)**:
Replacing an environment's on-disk data with one of its own backup snapshots, keeping the same environment name. No path rewriting is needed — every path embedded in the restored data already matches.

**Clone**:
Restoring a backup snapshot into a *new* environment name. The database's environment-scoped path columns are rewritten to the new name; the exported XML and `.m3u8` playlists are regenerated from the database rather than copied, since both are fully derived from it.
_Avoid_: fork, copy environment
