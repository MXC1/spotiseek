---
status: accepted
---

# Master playlists for playlist folders

## Context

A Rekordbox folder (see ADR 0001) is only a container — it shows no tracks of its
own — so a DJ who wants "everything in this folder" as one crate has to open each
member playlist or build the union by hand. The union already exists in the
exporter (each folder `<dict>` carries it defensively), but a folder can't be
loaded or browsed as a tracklist. The union has to live in a playlist inside the
folder.

## Decision

1. **Every named folder gets a master playlist**, named `ALL <folder name>` (fixed
   `ALL ` prefix, folder name verbatim), containing every downloaded track from the
   folder's member playlists. Root playlists sit outside every folder, so there is
   no root-level master. A folder with a single member still gets one.
2. **Master playlists exist only in the XML export, derived at export time** from
   folder memberships, exactly like the folder entries themselves. No database row,
   no m3u8, no dashboard presence. `playlist_folder_memberships` and the prune /
   orphan logic are untouched, and a master can never be stale relative to its
   members. With no folder memberships recorded (the flat fallback export) no
   masters are emitted.
3. **A master is first in its folder by array position** — emitted immediately after
   the folder's `<dict>` and before its member playlists.
4. **Track order:** member playlists in CSV order, each in its own order, first
   occurrence wins — the same union the folder `<dict>` already carries.
5. **Masters are scoped per folder.** A playlist in two folders contributes to both
   masters; a root copy of a playlist contributes to none. A folder with tracks not
   yet downloaded gets a master with only the downloaded ones (empty if none).
6. **Persistent ID is derived deterministically** from `"master:"` + folder name,
   the same scheme as ADR 0001 decision 5, so an unchanged CSV yields identical IDs.
7. **The folder `<dict>` keeps its own union of items.** ADR 0001 decision 4 stands
   unchanged; the master does not replace it.
8. **No configuration.** The prefix is hard-coded and there is no off switch.

## Considered options

- **Also write an m3u8 per master.** Rejected: ADR 0001 keeps m3u8s flat and
  one-per-playlist, and a master file would need its own collision, rewrite-on-every-
  download and rename-cleanup story for a consumer (Rekordbox via XML) that doesn't
  read m3u8s.
- **A real playlist row in the database.** Rejected: a playlist's identity is its
  URL; a master would need a synthetic identity and would interact with pruning,
  display order, and the dashboard's playlist views.
- **Only folders with two or more playlists.** Rejected: a master would appear and
  vanish in Rekordbox as the CSV is edited.
- **Configurable prefix or an env-var off switch.** Rejected as an unrequested knob;
  forward-compatible, so it can be added later.
- **Newest-added-first or alphabetical track order.** Rejected: CSV-order union
  matches the folder entry, is deterministic, and Rekordbox can re-sort by column.
- **Emptying the folder `<dict>`'s own items now that the master holds the list.**
  Rejected: reopens the empty-folder parser risk ADR 0001 guarded against.
- **A sort-forcing name (leading symbol/space) so the master sorts first even if
  Rekordbox sorts alphabetically.** Rejected for now: deviates from the requested
  `ALL <folder>` name. Kept as the fallback (see below).

## Consequences

- **Unverified assumption: Rekordbox honours XML array order within a folder.** Not
  checked as of 2026-09-19. Verify by viewing one folder in Rekordbox after the first
  export. If Rekordbox turns out to sort alphabetically, the fallback is a
  sort-forcing prefix, recorded in a follow-up ADR. See `docs/TROUBLESHOOTING.md`.
- Each folder's tracks are serialized twice in the XML (folder union + master).
- On the first export after this ships, each folder gains a new playlist in Sync
  Manager, and the sequential `Playlist ID` integers of every later entry shift once.
  Identity is the persistent ID, so the mirror should not churn.
- Renaming a folder in the CSV removes and re-adds its master along with the folder
  (ADR 0001's rename behaviour).
- A real member playlist literally named `ALL <folder name>` is not special-cased;
  the two are told apart by persistent ID and position.
- Like folders, masters render correctly in Rekordbox only via Sync Manager
  (ADR 0001).
