---
status: accepted
---

# Complete the ADR-0004 dashboard cutover: delete Streamlit, promote the FastAPI dashboard to :8501

[ADR-0005](0005-defer-dashboard-cutover-keep-streamlit-as-rollback.md) deferred the real
cutover because `dashboard-next` hadn't earned enough real-world trust yet to remove its
only fallback. By now it's been the daily-driver dashboard for a while, and doing the
cutover surfaced the actual trigger: `dashboard-next` was never covered by the gated
deploy — its code always mounted the working tree directly, gated environment or not —
which ADR-0005 explicitly accepted as a consequence at the time. That gap is now closed:
the old Streamlit dashboard (`observability/dashboard/`, `observability/combined_dashboard.py`)
is deleted, `dashboard-next`'s code moves into `observability/dashboard/`, the service is
renamed from `dashboard-next` to `dashboard` and takes over port 8501 (8502 is retired),
and its code mounts via `${CODE_ROOT}` exactly like `workflow` — so the gated environment's
dashboard code changes exclusively through `invoke deploy` again. The old code stays
recoverable from git history; there is no more running fallback.

**Considered options:**
- *Leave `dashboard-next` running in parallel indefinitely with the gated-deploy gap
  accepted*: rejected — the reason ADR-0005 gave for that gap (not enough real usage yet
  to trust `dashboard-next`) no longer held once it had already become the dashboard used
  day to day.
- *Special-case `dashboard-next` into the gated snapshot while keeping it a separate
  service*: rejected — extra permanent plumbing for what ADR-0004 always intended as
  temporary scaffolding, when doing the real cutover removes the need for it entirely.

**Consequences:**
- `infra/Dockerfile.dashboard-next` and the `dashboard-next` Compose service are deleted;
  `infra/Dockerfile.dashboard` and `docker-compose.yml`'s `dashboard` service now build the
  FastAPI app (merged from the two Dockerfiles — and gained `ffmpeg`, needed by
  `scripts/audio_validation.py`'s decode check for Manual/Auto Import, which
  `Dockerfile.dashboard-next` had been missing).
- `invoke deploy`'s post-deploy restart now restarts `dashboard` alongside `workflow`;
  `DEPLOY_ARCHIVE_PATHS` drops `observability/combined_dashboard.py` (deleted, with no
  successor file — the FastAPI app's entry point lives inside `observability/dashboard/app.py`).
- The old Streamlit-only Execution Inspection view (workflow-run picker + summary
  metrics/timeline/errors) has no equivalent in the new dashboard — it was already
  replaced by a live `docker logs -f` stream in an earlier change, not ported 1:1. Its
  backing helpers (`get_workflow_runs`, `analyze_workflow_run`, `get_log_files` in
  `scripts/logs_utils.py`) are deleted as dead code now that nothing calls them.
- `requirements.txt` drops `streamlit` and its now-orphaned transitive dependencies
  (altair, blinker, cachetools, gitpython/gitdb/smmap, narwhals, pillow, plotly, protobuf,
  pyarrow, pydeck, tenacity, toml, tornado, watchdog), verified via `pip show`'s
  `Required-by` against the rest of the pinned set before removal.
