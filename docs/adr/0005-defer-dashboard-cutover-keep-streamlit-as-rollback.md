---
status: superseded by ADR-0006
---

# Defer the ADR-0004 cutover: stop Streamlit but keep its code as a rollback path

All 7 tabs of the FastAPI + HTMX dashboard (`observability/dashboard_next/`,
[ADR-0003](0003-dashboard-rewrite-fastapi-htmx.md)) were ported, manually verified
against real live data, and committed, which is the point [ADR-0004](0004-dashboard-migration-parallel-service-cutover.md)
originally described as ready for the final cutover: delete the Streamlit dashboard,
move `dashboard_next`'s code into `observability/combined_dashboard.py`/`observability/dashboard/`,
and promote its service from port 8502 to 8501. Instead, the cutover's *deletion* step is
deferred: the new dashboard is trusted enough to become the one used day to day, but not
yet trusted enough to remove the only thing to fall back to if it has a problem that
weeks of casual daily use hasn't surfaced. The Streamlit dashboard (`dashboard` service,
`observability/dashboard/`, `observability/combined_dashboard.py`) stays in the repo,
unmodified and functionally intact, but stopped by default — moved behind Docker
Compose's `deprecated` profile, so `invoke up`/plain `docker-compose up` no longer start
it. `dashboard-next` continues running on its existing port 8502 rather than moving to
8501; freeing 8501 for it was considered and rejected for now, as an unforced extra
change to make right when the goal is to change as little as possible.

**Considered options:**
- *Do the full ADR-0004 cutover now (delete Streamlit, move code, promote port)*:
  rejected — the new dashboard hasn't had enough real-world usage yet to be certain no
  tab has a bug that only shows up in some workflow not yet exercised, and once
  Streamlit's code is deleted, un-deleting it means digging through git history under
  time pressure instead of running one Compose command.
- *Leave `dashboard` running alongside `dashboard-next` indefinitely*: rejected — the
  user only wants a fallback, not two dashboards to actually keep using; a Compose
  profile gets the "keep the code, don't run it" outcome directly, no manual
  remember-to-stop-it discipline required after every `invoke up`.
- *Promote `dashboard-next` to port 8501 now, while still deferring deletion*: rejected
  for the same reason as the full cutover — it's additional change beyond what's needed
  to satisfy the actual request, at a moment explicitly about being cautious. Revisit
  once there's more confidence.

**Consequences:**
- `invoke deploy`'s post-deploy restart only restarts `workflow` now, not `dashboard` —
  restarting a stopped, profile-gated service would just fail, and `dashboard-next` was
  never part of the deployed snapshot in the first place (its code always mounts the
  working tree directly), so restarting it would accomplish nothing.
- `docs/DASHBOARD.md` still documents Streamlit's UI by default (unchanged tab behavior
  applies equally to `dashboard-next`), now with a banner pointing at the active
  dashboard's URL and this ADR.
- Bringing Streamlit back after a `dashboard-next` problem is one command:
  `docker compose --profile deprecated up -d dashboard`.
- The real ADR-0004 cutover (delete Streamlit's code, move `dashboard_next`'s code into
  its place, promote the port) is not cancelled, just not yet scheduled — do it once
  `dashboard-next` has enough real usage behind it that Streamlit is no longer needed as
  a safety net, then retire this ADR's `deprecated` profile along with the code it gates.
