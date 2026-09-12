---
status: accepted
---

# Backup & restore run as a dedicated in-WSL container, backed by per-environment restic repositories

We back up each environment's `downloads/`, `imported/`, exported library (database, XML, m3u8s), per-environment config, and capped logs via a new always-on `backup` compose service — not Windows Task Scheduler, not the existing `workflow` scheduler — storing them in one restic repository per environment under a repo-local, gitignored `backups/` directory. The `backup` service holds the Docker socket so it can stop/start `slskd` and `workflow` around a copy of whichever environment is currently the [hot environment](../../CONTEXT.md); every other environment is already cold and copies safely with no pause.

**Considered options:**
- *Execution locus*: Windows Task Scheduler was rejected outright — no Windows-side involvement. Extending the existing `workflow` scheduler was rejected — it only mounts the active environment's data and holds no Docker socket, so it can't see or pause siblings. A one-shot container triggered by cron inside the Ubuntu WSL distro was rejected in favor of an always-on compose service, since the distro's uptime isn't guaranteed the way Docker Desktop's `restart: unless-stopped` is.
- *Storage engine*: rsync with hard-linked snapshots and plain per-run tarballs were both rejected in favor of restic. At ~45G for the largest environment against a ~154G disk budget, restic's content-addressed dedup and built-in retention (`forget --keep-daily/--keep-weekly/--keep-monthly`) are what make *frequent* backups viable at all.
- *Repo layout*: one shared restic repo with snapshots tagged by environment was rejected in favor of one repo per environment, matching this codebase's existing convention of scoping everything (database, XML, logs, downloads) by `APP_ENV`, and keeping one environment's repo corruption from touching another's.

**Consequences:**
- The `backup` service has Docker socket access — broad privilege, accepted deliberately for a single-user local setup.
- Backups default to living inside the repo (`backups/`, gitignored) rather than on a separate physical disk, because the other drives on this machine are dedicated to another service. This protects against software mistakes (bad migrations, `invoke nuke`, corruption) but is **not** disaster recovery — a lost or failing `E:` drive takes the backups down with the data. Repointing `BACKUP_DEST` at another disk is a one-line config change if that tradeoff needs revisiting later.
- The restic repo password lives in `.env` unencrypted. Acceptable only because `backups/` shares the same trust boundary as `.env` itself (local, gitignored, single-user) — this would need revisiting if `BACKUP_DEST` ever moves off-box.
- Dashboard visibility (last-run status, snapshot list) was explicitly deferred rather than built now; backup runs are log-only for this iteration, following the same `write_log` pattern as every other task.
