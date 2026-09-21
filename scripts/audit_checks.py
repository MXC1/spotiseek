"""Audit check definitions for the dashboard's Database tab.

See docs/adr/0008-dashboard-database-explorer.md. Each check is a read-only SELECT
returning exactly the offending rows. TrackDB executes them (TrackDB.count_audit_check /
TrackDB.get_audit_check_rows) -- this module only holds the definitions, so all SQL still
runs through the one TrackDB connection and nothing here touches the database itself.

Only checks that are pure queries over the database live here. The disk checks (a
completed track's file missing, an m3u8 missing, files in imported/ that no track points
at) compare the database to the filesystem and live with the dashboard routes.

A check whose result columns include ``track_id`` gets a link to the track detail view,
and one with ``playlist_url`` gets a link to that playlist's row.
"""

from dataclasses import dataclass

from scripts.constants import STUCK_THRESHOLD_HOURS

GROUP_INTEGRITY = "Referential integrity"
GROUP_CONSISTENCY = "Status / field consistency"
GROUP_STUCK = "Stuck tracks"

GROUP_ORDER = (GROUP_INTEGRITY, GROUP_CONSISTENCY, GROUP_STUCK)

# A blank string counts as "no value" everywhere: several columns are written as '' rather
# than NULL by some code paths.
_NO_FILE_PATH = "(local_file_path IS NULL OR TRIM(local_file_path) = '')"


@dataclass(frozen=True)
class AuditCheck:
    """One named audit check: ``sql`` selects the offending rows (with a stable ORDER BY
    and no LIMIT -- TrackDB wraps it for counting and paging)."""

    id: str
    group: str
    title: str
    description: str
    sql: str
    params: tuple = ()


def _stuck_check() -> AuditCheck:
    """Tracks that have sat in one in-flight status past that status's threshold.

    Age is measured in SQLite's own clock (CURRENT_TIMESTAMP is UTC, as is 'now'). Rows
    with a NULL status_changed_at are excluded -- their age is unknown, and the migration
    backfills them at the next start.
    """
    conditions = " OR ".join("(download_status = ? AND age_seconds > ?)" for _ in STUCK_THRESHOLD_HOURS)
    params: list = []
    for status, hours in STUCK_THRESHOLD_HOURS.items():
        params.extend([status, hours * 3600])
    thresholds = ", ".join(f"{status} > {hours}h" for status, hours in STUCK_THRESHOLD_HOURS.items())
    sql = f"""
        SELECT track_id, track_name, artist, download_status, status_changed_at,
               ROUND(age_seconds / 3600.0, 1) AS hours_in_status
        FROM (
            SELECT track_id, track_name, artist, download_status, status_changed_at,
                   CAST(strftime('%s', 'now') AS INTEGER)
                       - CAST(strftime('%s', status_changed_at) AS INTEGER) AS age_seconds
            FROM tracks
            WHERE status_changed_at IS NOT NULL
        )
        WHERE {conditions}
        ORDER BY age_seconds DESC, track_id
    """
    return AuditCheck(
        id="stuck_tracks",
        group=GROUP_STUCK,
        title="Tracks stuck in an in-flight status",
        description=f"In one in-flight status for longer than its threshold ({thresholds}).",
        sql=sql,
        params=tuple(params),
    )


AUDIT_CHECKS: tuple[AuditCheck, ...] = (
    # --- Referential integrity ------------------------------------------------------------
    AuditCheck(
        id="playlist_tracks_missing_track",
        group=GROUP_INTEGRITY,
        title="playlist_tracks rows pointing at a missing track",
        description="A playlist lists a track_id that has no row in tracks.",
        sql="""
            SELECT pt.playlist_url, pt.track_id
            FROM playlist_tracks pt
            LEFT JOIN tracks t ON t.track_id = pt.track_id
            WHERE t.track_id IS NULL
            ORDER BY pt.playlist_url, pt.track_id
        """,
    ),
    AuditCheck(
        id="playlist_tracks_missing_playlist",
        group=GROUP_INTEGRITY,
        title="playlist_tracks rows pointing at a missing playlist",
        description="A track is linked to a playlist_url that has no row in playlists.",
        sql="""
            SELECT pt.playlist_url, pt.track_id
            FROM playlist_tracks pt
            LEFT JOIN playlists p ON p.playlist_url = pt.playlist_url
            WHERE p.playlist_url IS NULL
            ORDER BY pt.playlist_url, pt.track_id
        """,
    ),
    AuditCheck(
        id="tracks_in_no_playlist",
        group=GROUP_INTEGRITY,
        title="Orphaned tracks (in no playlist)",
        description=(
            "Pruning removes orphans once every playlist has been reprocessed, so one that "
            "survives a full scrape is inconsistent."
        ),
        sql="""
            SELECT t.track_id, t.track_name, t.artist, t.download_status
            FROM tracks t
            WHERE NOT EXISTS (SELECT 1 FROM playlist_tracks pt WHERE pt.track_id = t.track_id)
            ORDER BY t.track_id
        """,
    ),
    AuditCheck(
        id="folder_memberships_unknown_playlist",
        group=GROUP_INTEGRITY,
        title="Folder memberships for an unknown playlist",
        description="A folder membership names a playlist_url that has no row in playlists.",
        sql="""
            SELECT m.playlist_url, m.folder_name, m.csv_sequence
            FROM playlist_folder_memberships m
            LEFT JOIN playlists p ON p.playlist_url = m.playlist_url
            WHERE p.playlist_url IS NULL
            ORDER BY m.playlist_url, m.folder_name
        """,
    ),
    AuditCheck(
        id="playlists_without_membership",
        group=GROUP_INTEGRITY,
        title="Playlists with no folder membership",
        description=(
            "Every scraped playlist gets at least a root or folder membership row, rebuilt "
            "from the CSV on each scrape."
        ),
        sql="""
            SELECT p.playlist_url, p.playlist_name
            FROM playlists p
            WHERE NOT EXISTS (
                SELECT 1 FROM playlist_folder_memberships m WHERE m.playlist_url = p.playlist_url
            )
            ORDER BY p.playlist_url
        """,
    ),
    # --- Status / field consistency -------------------------------------------------------
    AuditCheck(
        id="completed_without_file_path",
        group=GROUP_CONSISTENCY,
        title="Completed tracks with no local file path",
        description="download_status is 'completed' but no file is recorded for the track.",
        sql=f"""
            SELECT track_id, track_name, artist, download_status
            FROM tracks
            WHERE download_status = 'completed' AND {_NO_FILE_PATH}
            ORDER BY track_id
        """,
    ),
    AuditCheck(
        id="blacklisted_with_file_path",
        group=GROUP_CONSISTENCY,
        title="Blacklisted tracks that still have a local file path",
        description=(
            "Blacklisting clears a track's download metadata. Other non-completed statuses "
            "can legitimately keep the old file path while a quality upgrade is in progress, "
            "so only 'blacklisted' is flagged."
        ),
        sql=f"""
            SELECT track_id, track_name, artist, download_status, local_file_path
            FROM tracks
            WHERE download_status = 'blacklisted' AND NOT {_NO_FILE_PATH}
            ORDER BY track_id
        """,
    ),
    AuditCheck(
        id="searching_without_search_uuid",
        group=GROUP_CONSISTENCY,
        title="Searching tracks with no search UUID",
        description="download_status is 'searching' but no slskd search is recorded for it.",
        sql="""
            SELECT track_id, track_name, artist, download_status
            FROM tracks
            WHERE download_status = 'searching'
              AND (slskd_search_uuid IS NULL OR TRIM(slskd_search_uuid) = '')
            ORDER BY track_id
        """,
    ),
    AuditCheck(
        id="active_download_without_uuid",
        group=GROUP_CONSISTENCY,
        title="Queued/downloading tracks with no download UUID",
        description="download_status is 'queued' or 'downloading' but no slskd download is recorded.",
        sql="""
            SELECT track_id, track_name, artist, download_status
            FROM tracks
            WHERE download_status IN ('queued', 'downloading')
              AND (slskd_download_uuid IS NULL OR TRIM(slskd_download_uuid) = '')
            ORDER BY track_id
        """,
    ),
    AuditCheck(
        id="active_download_without_username",
        group=GROUP_CONSISTENCY,
        title="Queued/downloading tracks with no Soulseek username",
        description="download_status is 'queued' or 'downloading' but no peer username is recorded.",
        sql="""
            SELECT track_id, track_name, artist, download_status
            FROM tracks
            WHERE download_status IN ('queued', 'downloading')
              AND (username IS NULL OR TRIM(username) = '')
            ORDER BY track_id
        """,
    ),
    AuditCheck(
        id="failed_without_reason",
        group=GROUP_CONSISTENCY,
        title="Failed tracks with no failure reason",
        description="download_status is 'failed' but failed_reason is empty.",
        sql="""
            SELECT track_id, track_name, artist, download_status
            FROM tracks
            WHERE download_status = 'failed' AND (failed_reason IS NULL OR TRIM(failed_reason) = '')
            ORDER BY track_id
        """,
    ),
    # --- Stuck tracks ---------------------------------------------------------------------
    _stuck_check(),
)

AUDIT_CHECKS_BY_ID: dict[str, AuditCheck] = {check.id: check for check in AUDIT_CHECKS}
