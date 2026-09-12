# Spotiseek

Domain glossary for Spotiseek, which scrapes music playlists, downloads their tracks
via Soulseek, and exports an iTunes-format library consumed by Rekordbox/iTunes. Keep
this file a glossary only — no implementation detail.

## Language

### Environments & backups

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

### Playlists & folders

**Playlist**:
The tracklist scraped from a single Spotify or SoundCloud URL. The URL is its identity;
the display name comes from the source platform, not from the CSV.

**Playlists CSV**:
The per-environment input file (`input_playlists/playlists_{APP_ENV}.csv`) listing one
playlist URL per line, in the order they should appear downstream. Text after `#` on a
URL line is a human annotation and is ignored.
_Avoid_: playlist list, input file

**Folder**:
A named grouping of playlists, exported so Rekordbox shows them nested under a common
heading in its playlist tree. Declared by a comment-only line in the Playlists CSV
(`# Some Name`); the folder's name is the verbatim text after the `#`. A folder has no
identity beyond its name — editing a heading's text produces a different folder, not a
renamed one. Folders are flat in v1 — a folder never contains another folder.
_Avoid_: group, category, directory

**Folder heading**:
The comment-only CSV line that declares a folder and opens its scope. Its scope runs to
the next folder heading or end of file; blank lines do not close it. Two headings with
the same name refer to the same folder.

**Folder membership**:
The relation "this playlist appears under this folder heading". Additive and
many-to-many: a playlist belongs to every folder under whose heading its URL is listed,
and its membership in one folder never removes it from another.

**Root playlist**:
A playlist whose URL is listed before the first folder heading. It appears at the top
level of the exported tree, outside every folder. A playlist can be both a root playlist
and a member of one or more folders.
_Avoid_: top-level playlist, ungrouped playlist, loose playlist
