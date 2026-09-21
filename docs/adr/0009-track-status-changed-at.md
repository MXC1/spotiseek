---
status: accepted
---

# Tracks record when their status last changed (`status_changed_at`), so "stuck" can be measured

`tracks` records only `added_at`; nothing says when a track's `download_status` last changed, so a track that has been `searching` or `downloading` for a day is indistinguishable from one that started a minute ago. Debugging stuck tracks is the primary job of the Database explorer (ADR 0008), so we add a `tracks.status_changed_at` column to the pipeline's schema — a small change to the workflow's write path, made for the sake of a dashboard feature, which is why it gets its own ADR.

## Decision

1. **The column records the last change of status *value*.** It is not bumped when a status is re-set to the value it already has. The search code calls `update_track_status(id, "searching")` again on retries, so an every-call bump would keep resetting the age of a track that is really stuck. Every writer of `download_status` follows the same rule: `update_track_status`, `restore_track_download_metadata` (the blacklist rollback), and the initial insert in `add_track` (which sets it to the insert time). A `failed` → `failed` write with a different reason therefore does not bump it.
2. **Existing rows are backfilled with the migration time.** Every in-flight track looks freshly changed at first; genuinely stuck ones surface once they age past their threshold. SQLite's `ADD COLUMN` cannot take a `CURRENT_TIMESTAMP` default, so the migration adds the column and then runs an explicit `UPDATE … WHERE status_changed_at IS NULL`.
3. **The migration is idempotent under a concurrent first start.** `TrackDB` runs its migrations in every process that opens the DB, and `invoke up` starts `workflow` and `dashboard` together. Both can see the column missing; the loser must tolerate the duplicate-column error (or re-check inside a write transaction) rather than crash-loop its container.
4. **"Stuck" is age past a per-status threshold, for in-flight statuses only** — `pending`, `searching`, `queued`, `downloading`, `redownload_pending`. Thresholds are constants in code (a dict of status → duration, defaults tuned at implementation); a track in a final status is never stuck. Inconsistent combinations that need no clock (e.g. `downloading` with no download UUID) remain ordinary status/field consistency checks (ADR 0008), not "stuck".
5. **Rollout.** The change is additive: older code ignores the column, and nothing does a positional `SELECT * FROM tracks` (checked 2026-09-21), so rolling back a deploy is safe. Because it touches the workflow's write path and the real `all_playlists` database, it ships through `invoke deploy` for the gated environment, preceded by an on-demand backup — which pauses `slskd`/`workflow` for its duration (see ADR 0001, backup/restore).

## Considered options

- **Static checks only — no timestamp.** Rejected: the explorer could flag inconsistent states but never say "stuck for N hours", which is the question that started this. It also keeps the pipeline untouched, and remains the fallback if this column proves not worth its cost.
- **Bump on every `update_track_status` call.** Rejected: simpler (an unconditional `SET`), but retry loops re-setting the same status would hide real stuck tracks.
- **Backfill NULL and surface "unknown age" as its own finding.** Rejected: honest but noisy — every old in-flight track appears until its status next changes.
- **Backfill from `added_at`.** Rejected: surfaces old stuck tracks immediately but overstates the age of anything since re-searched or redownloaded.
- **One adjustable threshold in the UI, or per-status env vars.** Rejected: one number is wrong for either `queued` (a Soulseek queue can legitimately sit for days) or `searching` (a search should not), and env vars add a configuration surface (`docs/CONFIGURATION.md` entries, explicit passthrough in the dashboard's compose `environment:` list) for a single-user tool. Changing a threshold is a code change.

## Consequences

- Restoring or cloning a backup taken before this change reopens through the migration and is backfilled to the moment of that reopen; the column needs no path rewriting on clone.
- Until the first threshold elapses after deploy, the Audit view's stuck-by-age check reports nothing, by design of the backfill.
- The migration writes to every `tracks` row once, at startup, in whichever process opens the DB first — an ordinary `TrackDB` write on the shared connection, not an ad hoc one.
