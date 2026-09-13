---
status: accepted
---

# The dashboard rewrite migrates tab-by-tab behind a temporary parallel service, not a big-bang rewrite

Three of the dashboard's seven tabs are live write paths relied on daily — Manual Import, Auto Import, and Blacklist mutate the database and filesystem, and Tasks triggers task runs — so replacing all seven tabs in one release risks a stretch with no working dashboard if the rewrite has a bug. The rewrite instead happens one tab at a time, starting with the read-only Docs tab, and is an explicit like-for-like port per tab (no feature trimming or UX redesign folded in) so scope stays bounded to the technology swap. While tabs are being ported, the new app runs as a fully separate `dashboard-next` service on port 8502, alongside the untouched Streamlit `dashboard` service on 8501 — new code doesn't touch `observability/combined_dashboard.py` or `observability/dashboard/` until the very end. Once every tab is ported, the Streamlit files are deleted, the new app's code moves into those same paths, and the service takes over port 8501 — landing exactly where [ADR-0002](./0002-gated-environment-deploys-via-git-archive.md)'s `invoke deploy` already expects dashboard code to live, so the gated-environment deploy tooling needs no changes.

**Considered options:**
- *Big-bang rewrite of all 7 tabs at once*: rejected given how much daily-relied-on write-path functionality (imports, blacklist edits, task triggers) would be down at once if something in the rewrite were wrong.
- *A reverse proxy routing both apps by path behind one port*: rejected for the migration period specifically — it would keep a single URL throughout, but that's more upfront infra to stand up and tear down again for what's meant to be a temporary state.
- *Building the new app directly at the existing `observability/combined_dashboard.py` path from the start (e.g. behind a feature flag)*: rejected — it would mean the in-progress rewrite and the still-relied-on Streamlit app share the same files, risking exactly the instability the tab-by-tab approach is meant to avoid.

**Consequences:**
- Two dashboard URLs (8501 and 8502) exist side by side for the duration of the migration; there's no single-URL experience until cutover.
- `infra/Dockerfile.dashboard-next` and a `dashboard-next` compose service are temporary scaffolding, deleted at cutover alongside the Streamlit files — they are not meant to become permanent fixtures.
- Because each tab is ported as a strict 1:1 port, any desired redesign of the dashboard's UX or feature set is deferred to separate follow-up work, not bundled into this migration.
