---
status: accepted
---

# The dashboard is rewritten in FastAPI + HTMX, not a JS SPA or another Python dashboard framework

The Streamlit dashboard (`observability/combined_dashboard.py` + `observability/dashboard/tabs/`) reruns its entire script top-to-bottom on every widget interaction, which shows up as visible flicker/lag especially on tabs that hit the database or parse logs, on top of a slow cold start and limited control over layout/styling. The replacement is a FastAPI + Jinja2 server-rendered app: a persistent page shell swaps only the active tab's content via HTMX partial requests (not a full page reload) for tab switching, HTMX polling (`hx-trigger="every Ns"`) for the tabs that already auto-refresh today (auto_import, blacklist, manual_import, tasks), and a light Alpine.js sprinkle — vendored into the image, not pulled from a CDN — for pieces of purely client-side state (Auto Import's batch checkbox selection, Manual Import's upload flow) that a server round-trip can't drive responsively. Styling stays hand-written CSS; no utility/component framework.

**Considered options:**
- *Full JS/TS SPA with a Python API backend*: rejected as disproportionate — most of the dashboard's 7 tabs are simple read-only tables and forms, and a single local user doesn't need SPA-grade interactivity everywhere. It remains the fallback if HTMX + Alpine ever turns out to sacrifice speed/interactivity on a specific tab.
- *A different Python dashboard framework (NiceGUI, Reflex, Dash)*: rejected as a lateral move — these are closer conceptual swap-ins for Streamlit, but several share a similar declarative-rerun execution model, so they wouldn't necessarily fix the jank that motivated the rewrite.
- *Server-Sent Events or WebSockets for live updates*: rejected in favor of plain HTMX polling — with exactly one local user, the extra polling requests cost nothing, and polling avoids building and wiring an event-stream endpoint into `task_scheduler`.
- *A CDN-loaded CSS framework (Tailwind/Bootstrap)*: rejected — the dashboard's UI (tables, forms, tabs) is simple enough not to need one, and hand-written CSS keeps full control without depending on an external asset host.

**Consequences:**
- All dashboard database access — including the tabs that currently bypass it with raw `sqlite3` queries (Overall Stats, Manual Import, Blacklist) — moves onto the `TrackDB` singleton as part of the rewrite, closing the ad-hoc-connection corruption risk called out for this bind-mounted DB (see CLAUDE.md's Database Access section). `TrackDB` gains whatever read methods those tabs' aggregate queries need.
- Per-tab transient UI state (row selections, in-progress scan caches, pagination) lives in a simple in-memory, cookie-keyed server session, scoped to one process and one user — it resets on a dashboard restart, the same way Streamlit's `session_state` does today.
- HTMX and Alpine.js are vendored files in the repo/image rather than CDN `<script>` tags, matching the project's existing offline-friendly posture (no other component depends on an external service being reachable at runtime).
