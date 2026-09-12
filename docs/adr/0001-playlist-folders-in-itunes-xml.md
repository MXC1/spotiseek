---
status: accepted
---

# Playlist folders in the iTunes XML export

## Context

Rekordbox can show playlists nested inside folders for easier navigation while
DJing. Spotiseek's only channel into Rekordbox is the iTunes-format
`Library.xml` it exports, consumed via Rekordbox's "iTunes library" mirror. We
want folders declared in the Playlists CSV to survive that round trip.

Research into how Rekordbox reads folders from an iTunes-format XML found:

- A folder is a normal playlist `<dict>` in the `Playlists` array carrying
  `<key>Folder</key><true/>`; children link to it by matching its
  `Playlist Persistent ID` in their `Parent Persistent ID`. Array order is the
  only ordering signal the format carries.
- `Parent Persistent ID` is single-valued — the format cannot express a
  playlist belonging to two folders at once.
- Rekordbox *does* parse this structure, but has a decade-long (v3–v6.6.8,
  2015–2022), never-definitively-fixed pattern of nested (2+ level) folders
  importing empty. No confirmation either way for current 7.x.
- DJ-software XML parsers can render a folder empty if the folder's own
  `Playlist Items` array is missing (confirmed for Traktor).

## Decision

1. **Flat folders only.** A folder never contains another folder. Nesting is
   deferred until it can be tested against a current Rekordbox.
2. **A folder is identified by its name.** Two CSV headings with the same
   trimmed, case-sensitive text are the same folder. There is no separate
   folder ID.
3. **Multi-folder membership is emitted as duplicate playlist entries.** A
   playlist listed in the root and two folders produces three `<dict>` entries
   — same name, same track list, distinct persistent IDs, each with its own
   `Parent Persistent ID` (omitted for the root copy).
4. **Each folder `<dict>` carries the union of its members' tracks** in its own
   `Playlist Items`, defensively.
5. **Persistent IDs are derived deterministically** (`hashlib` over
   `"folder:"|"playlist:"` + folder name + playlist URL), so an unchanged CSV
   produces byte-identical XML and the mirror sees stable identities.
6. **Folder membership lives in a `playlist_folder_memberships` table rebuilt
   from scratch on every scrape.** Folders sit entirely on top of the existing
   playlist/track model; the existing prune logic is untouched and folder edits
   never delete tracks, files, or m3u8s.

## Considered options

- **Nested folders now.** Rejected: strong evidence Rekordbox mishandles them,
  and no current-version test rig to confirm otherwise.
- **Explicit stable folder key in the CSV** (`# [abc] Folder Called ABC`).
  Rejected for v1: complicates the format for everyone to serve an uncommon
  case (folder renames). Forward-compatible, so it can be added later.
- **Mirroring folders as m3u8 subdirectories.** Rejected: m3u8s are consumed as
  a flat list, Rekordbox's path here is the XML, and a file can't live in two
  directories.

## Consequences

- Renaming or moving a folder in the CSV is a remove-then-add in the XML (the
  old name's entries vanish, new ones appear with fresh IDs). The
  iTunes-library mirror re-syncs this cleanly on next import; a one-time
  snapshot import would not.
- The `Playlists` array now contains more entries than there are playlists.
- **Rekordbox must consume the library via Sync Manager, not the default
  iTunes tree view.** Verified 2026-09-10: the exported XML is spec-correct
  (each child's `Parent Persistent ID` matches its folder's
  `Playlist Persistent ID`), but Rekordbox's plain iTunes view has a
  long-standing bug where playlists nested in a folder render empty. Enabling
  Sync Manager for the iTunes library fixes it. The duplicate-entry trick for
  multi-folder membership works once Sync Manager is used. See
  `docs/TROUBLESHOOTING.md`.
