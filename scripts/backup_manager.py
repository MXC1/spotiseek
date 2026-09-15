"""Backup and restore management for Spotiseek environments.

Backs up, per environment, the raw Soulseek downloads, the curated/imported
library, the exported database/XML/M3U8s, per-environment config, and logs
into a dedicated restic repository for that environment. See
``docs/adr/0001-backup-restore-architecture.md`` for the design rationale and
``CONTEXT.md`` for the Environment / Hot environment / Backup / Restore /
Clone vocabulary used throughout this module.

Runs inside the always-on ``backup`` compose service, which mounts every
environment's data plus the Docker socket (to pause/resume ``slskd`` and
``workflow`` around a copy of whichever environment is currently "hot").

CLI:
    python -m scripts.backup_manager backup --env <env>
    python -m scripts.backup_manager restore --env <env> [--from <snapshot>] [--as <new_env>]
    python -m scripts.backup_manager daemon
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

sys.dont_write_bytecode = True

from dotenv import load_dotenv  # noqa: E402

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

from scripts.database_management import TrackDB  # noqa: E402
from scripts.docker_control import container_ids_for_service, own_compose_project  # noqa: E402
from scripts.logs_utils import setup_logging, write_log  # noqa: E402

# ---------------------------------------------------------------------------
# Paths and configuration
# ---------------------------------------------------------------------------

APP_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SLSKD_DATA_ROOT = os.path.join(APP_ROOT, "slskd_docker_data")
OUTPUT_ROOT = os.path.join(APP_ROOT, "output")
INPUT_PLAYLISTS_ROOT = os.path.join(APP_ROOT, "input_playlists")
LOGS_ROOT = os.path.join(APP_ROOT, "observability", "logs")
ENV_FILE = os.path.join(APP_ROOT, ".env")
SHARED_SLSKD_YML = os.path.join(SLSKD_DATA_ROOT, "slskd.yml")

# Marks containers currently stopped for a hot pause/resume operation, so an
# abrupt exit (the backup container getting killed or recreated mid-pause --
# see docs/adr/0001-backup-restore-architecture.md) can be recovered from on
# the next daemon start/loop tick instead of leaving them stopped forever.
PAUSE_MARKER_FILE = os.path.join(APP_ROOT, "observability", "logs", "_scheduler", ".hot_pause_state.json")

# Per-env date (YYYY-MM-DD) the scheduled daily backup last ran, so a recreated backup
# container (e.g. every `invoke deploy`) doesn't treat today's backup as still due and
# re-pause slskd/workflow. Timestamp of the last `invoke up`/`invoke deploy` on the
# Windows host, written by tasks.py into this same mounted logs volume -- lets the
# scheduler defer a scheduled run while the user is actively working on the hot
# environment. See BACKUP_SCHEDULE_HOUR / BACKUP_QUIET_MINUTES.
SCHEDULE_STATE_FILE = os.path.join(APP_ROOT, "observability", "logs", "_scheduler", "backup_schedule_state.json")
LAST_INVOKE_ACTIVITY_FILE = os.path.join(APP_ROOT, "observability", "logs", "_scheduler", "last_invoke_activity")

# Fixed container-side mount point for BACKUP_DEST (see docker-compose.yml);
# the host path a user configures in .env is only ever used on the compose
# side of that bind mount, never read here.
RESTIC_REPO_ROOT = "/backups"

RESTIC_PASSWORD = os.getenv("BACKUP_RESTIC_PASSWORD", "")
KEEP_DAILY = os.getenv("BACKUP_KEEP_DAILY", "7")
KEEP_WEEKLY = os.getenv("BACKUP_KEEP_WEEKLY", "4")
KEEP_MONTHLY = os.getenv("BACKUP_KEEP_MONTHLY", "6")

# Containers that may hold files open for whichever environment is "hot".
HOT_CONTAINER_SERVICES = ["slskd", "workflow"]


# ---------------------------------------------------------------------------
# Environment discovery / hot-detection
# ---------------------------------------------------------------------------

def list_known_environments() -> list[str]:
    """Every environment with a slskd_docker_data or output directory."""
    envs = set()
    for root in (SLSKD_DATA_ROOT, OUTPUT_ROOT):
        if os.path.isdir(root):
            for name in os.listdir(root):
                if os.path.isdir(os.path.join(root, name)):
                    envs.add(name)
    return sorted(envs)


def get_current_app_env() -> str | None:
    """Read the live APP_ENV value straight out of .env.

    Not the process's own APP_ENV: a single long-running backup container
    processes many environments over its lifetime, so "which environment is
    hot" always means "whatever .env currently says", read fresh each time.
    """
    if not os.path.exists(ENV_FILE):
        return None
    with open(ENV_FILE, encoding="utf-8") as f:
        for line in f:
            if line.strip().startswith("APP_ENV="):
                return line.strip().split("=", 1)[1].strip()
    return None


def is_hot_environment(env: str) -> bool:
    """True if `env` is the one with containers currently running against it.

    Every other environment's directories are inert on disk and can be
    copied directly with no pause.
    """
    return env == get_current_app_env()


# ---------------------------------------------------------------------------
# Backup scope
# ---------------------------------------------------------------------------

def env_backup_sources(env: str) -> list[str]:
    """Per-environment paths included in a backup, in scope order.

    Excludes slskd's own runtime `data/` (regenerable) and `incomplete/`
    (transient) -- see docs/adr/0001-backup-restore-architecture.md.
    """
    candidates = [
        os.path.join(SLSKD_DATA_ROOT, env, "downloads"),
        os.path.join(SLSKD_DATA_ROOT, env, "imported"),
        os.path.join(SLSKD_DATA_ROOT, env, "slskd.yml"),
        os.path.join(OUTPUT_ROOT, env),
        os.path.join(INPUT_PLAYLISTS_ROOT, f"playlists_{env}.csv"),
        os.path.join(LOGS_ROOT, env),
    ]
    return [p for p in candidates if os.path.exists(p)]


def shared_config_sources() -> list[str]:
    """Shared, non-per-environment config included in every environment's backup."""
    return [p for p in (ENV_FILE, SHARED_SLSKD_YML) if os.path.exists(p)]


def repo_path(env: str) -> str:
    return os.path.join(RESTIC_REPO_ROOT, env)


# ---------------------------------------------------------------------------
# Docker control (pause/resume the hot environment's containers)
# ---------------------------------------------------------------------------

def _container_ids_for_service(service: str) -> list[str]:
    project = own_compose_project()
    if not project:
        write_log.warn(
            "BACKUP_PROJECT_LOOKUP_FAILED",
            "Could not determine this container's own compose project; "
            "container discovery is unscoped and may match unrelated projects.",
        )
    return container_ids_for_service(service, project)


def stop_hot_containers(env: str) -> list[str]:
    """Stop slskd + workflow so the active environment's files are quiescent.

    Updates PAUSE_MARKER_FILE after *each* container stop, not just once at
    the end -- a kill between the two stops must still leave an accurate,
    resumable marker rather than no marker at all.
    """
    stopped: list[str] = []
    for service in HOT_CONTAINER_SERVICES:
        for cid in _container_ids_for_service(service):
            write_log.info(
                "BACKUP_CONTAINER_STOP",
                "Stopping container for a consistent copy.",
                {"service": service, "container_id": cid},
            )
            subprocess.run(["docker", "stop", cid], check=True, capture_output=True, text=True)
            stopped.append(cid)
            _write_pause_marker(env, stopped)
    return stopped


def start_containers(container_ids: list[str]) -> None:
    for cid in container_ids:
        write_log.info("BACKUP_CONTAINER_START", "Restarting container.", {"container_id": cid})
        subprocess.run(["docker", "start", cid], check=True, capture_output=True, text=True)


def _write_pause_marker(env: str, container_ids: list[str]) -> None:
    os.makedirs(os.path.dirname(PAUSE_MARKER_FILE), exist_ok=True)
    with open(PAUSE_MARKER_FILE, "w", encoding="utf-8") as f:
        json.dump({"env": env, "container_ids": container_ids}, f)


def _clear_pause_marker() -> None:
    if os.path.exists(PAUSE_MARKER_FILE):
        os.remove(PAUSE_MARKER_FILE)


def resume_orphaned_pause() -> None:
    """Resume any containers a previous abrupt exit left paused.

    _PausedIfHot writes PAUSE_MARKER_FILE right before stopping containers and
    clears it right after resuming them. If the process dies in between --
    SIGKILLed, crashed, or the backup container itself getting recreated
    mid-operation -- the marker survives (it lives under the logs volume, not
    inside the container) and names exactly which containers need resuming.
    Call this on daemon startup and on every scheduler tick so an orphaned
    pause self-heals within seconds instead of leaving slskd/workflow stopped
    indefinitely, which is what turned a routine backup pause into hours of
    downtime and DB corruption on 2026-09-12.
    """
    if not os.path.exists(PAUSE_MARKER_FILE):
        return
    try:
        with open(PAUSE_MARKER_FILE, encoding="utf-8") as f:
            state = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        write_log.error(
            "BACKUP_ORPHANED_PAUSE_UNREADABLE",
            "Found a pause marker but could not read it; leaving it for manual inspection.",
            {"error": str(e)},
        )
        return

    container_ids = state.get("container_ids", [])
    write_log.warn(
        "BACKUP_ORPHANED_PAUSE_FOUND",
        "Found containers left paused by a previous abrupt exit; resuming them.",
        {"env": state.get("env"), "container_ids": container_ids},
    )
    for cid in container_ids:
        result = subprocess.run(["docker", "start", cid], check=False, capture_output=True, text=True)
        if result.returncode != 0:
            write_log.error(
                "BACKUP_ORPHANED_PAUSE_RESUME_FAILED",
                "Could not resume a container from an orphaned pause; it may have been recreated "
                "since -- check its status manually.",
                {"container_id": cid, "error": result.stderr.strip()},
            )
    _clear_pause_marker()


class _PausedIfHot:
    """Context manager: stop slskd/workflow only if `env` is the hot one."""

    def __init__(self, env: str):
        self.env = env
        self.hot = False
        self._stopped_ids: list[str] = []

    def __enter__(self) -> bool:
        self.hot = is_hot_environment(self.env)
        if self.hot:
            write_log.info(
                "BACKUP_HOT_ENV",
                "Environment is hot; pausing containers for a consistent operation.",
                {"env": self.env},
            )
            self._stopped_ids = stop_hot_containers(self.env)
        return self.hot

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._stopped_ids:
            start_containers(self._stopped_ids)
            _clear_pause_marker()


# ---------------------------------------------------------------------------
# restic wrapper
# ---------------------------------------------------------------------------

def _restic_env(env: str) -> dict:
    e = os.environ.copy()
    e["RESTIC_REPOSITORY"] = repo_path(env)
    e["RESTIC_PASSWORD"] = RESTIC_PASSWORD
    return e


def _restic(env: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["restic", *args], env=_restic_env(env), check=check, capture_output=True, text=True,
    )


def ensure_repo(env: str) -> None:
    os.makedirs(repo_path(env), exist_ok=True)
    probe = _restic(env, "cat", "config", check=False)
    if probe.returncode != 0:
        write_log.info(
            "BACKUP_REPO_INIT", "Initializing restic repository.",
            {"env": env, "repo": repo_path(env)},
        )
        init = _restic(env, "init", check=False)
        if init.returncode != 0:
            raise RuntimeError(f"restic init failed: {init.stderr.strip()}")


def apply_retention(env: str) -> None:
    # `forget --prune` needs an exclusive lock, so a lock file orphaned by an
    # earlier interrupted run (e.g. the backup container getting recreated by
    # `invoke up --build` mid-operation) would otherwise block every future
    # prune forever. restic's own `unlock` only removes locks it verifies are
    # stale (owning PID/host no longer alive), so this is safe even if a
    # concurrent restic operation is genuinely in progress.
    _restic(env, "unlock", check=False)
    result = _restic(
        env, "forget", "--prune",
        "--keep-daily", KEEP_DAILY,
        "--keep-weekly", KEEP_WEEKLY,
        "--keep-monthly", KEEP_MONTHLY,
        check=False,
    )
    if result.returncode != 0:
        write_log.warn(
            "BACKUP_RETENTION_FAILED", "Failed to prune old snapshots.",
            {"env": env, "error": result.stderr.strip()},
        )
    else:
        write_log.info("BACKUP_RETENTION_APPLIED", "Old snapshots pruned per retention policy.", {"env": env})


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

def backup_environment(env: str) -> bool:
    """Back up one environment. Returns True on success."""
    write_log.info("BACKUP_START", "Starting backup.", {"env": env})

    if not RESTIC_PASSWORD:
        write_log.error("BACKUP_NO_PASSWORD", "BACKUP_RESTIC_PASSWORD is not set.", {"env": env})
        return False

    sources = env_backup_sources(env) + shared_config_sources()
    if not sources:
        write_log.warn("BACKUP_NO_SOURCES", "Nothing to back up for environment.", {"env": env})
        return False

    try:
        ensure_repo(env)
        with _PausedIfHot(env) as hot:
            result = _restic(env, "backup", *sources, "--tag", env, check=False)
            if result.returncode != 0:
                raise RuntimeError(f"restic backup failed: {result.stderr.strip()}")

        write_log.info("BACKUP_COMPLETE", "Backup completed.", {"env": env, "hot": hot})
        apply_retention(env)
        return True

    except Exception as e:
        write_log.error("BACKUP_FAILED", "Backup failed.", {"env": env, "error": str(e)})
        return False


# ---------------------------------------------------------------------------
# Restore -- in place
# ---------------------------------------------------------------------------

def _restore_in_place(env: str, snapshot: str) -> bool:
    write_log.info("RESTORE_START", "Starting in-place restore.", {"env": env, "snapshot": snapshot})

    if not RESTIC_PASSWORD:
        write_log.error("RESTORE_NO_PASSWORD", "BACKUP_RESTIC_PASSWORD is not set.", {"env": env})
        return False
    if not os.path.isdir(repo_path(env)):
        write_log.error("RESTORE_NO_REPO", "No backup repository found for environment.", {"env": env})
        return False

    # Shared config (.env, the shared slskd.yml) is captured for
    # disaster-recovery reference but must never be silently overwritten by
    # a routine restore -- .env in particular controls which environment is
    # live, and this container only has it mounted read-only.
    exclude_args = []
    for shared_path in shared_config_sources():
        exclude_args += ["--exclude", shared_path]

    try:
        with _PausedIfHot(env):
            result = _restic(env, "restore", snapshot, "--target", "/", *exclude_args, check=False)
            if result.returncode != 0:
                raise RuntimeError(f"restic restore failed: {result.stderr.strip()}")

        write_log.info("RESTORE_COMPLETE", "In-place restore completed.", {"env": env, "snapshot": snapshot})
        return True

    except Exception as e:
        write_log.error("RESTORE_FAILED", "Restore failed.", {"env": env, "error": str(e)})
        return False


# ---------------------------------------------------------------------------
# Restore -- clone into a new environment
# ---------------------------------------------------------------------------

def _scratch_path(scratch_dir: str, original_abs_path: str) -> str:
    """Where restic puts a file whose original path was `original_abs_path`
    when restoring with `--target scratch_dir` (it preserves the absolute
    path structure under the target directory).
    """
    return os.path.join(scratch_dir, original_abs_path.lstrip("/"))


def _move_cloned_path(scratch_dir: str, original_path: str, new_path: str) -> None:
    restored_path = _scratch_path(scratch_dir, original_path)
    if not os.path.exists(restored_path):
        return
    os.makedirs(os.path.dirname(new_path), exist_ok=True)
    shutil.move(restored_path, new_path)


def _finalize_cloned_output_dir(new_output_dir: str, source_env: str, new_env: str) -> None:
    """Rename the DB into the new env, drop stray WAL/SHM, and clear stale
    derived artifacts (library.xml, .m3u8s) that still reference the old env.
    """
    old_db = os.path.join(new_output_dir, f"database_{source_env}.db")
    new_db = os.path.join(new_output_dir, f"database_{new_env}.db")
    if os.path.exists(old_db):
        os.rename(old_db, new_db)
    for suffix in ("-wal", "-shm"):
        stray = os.path.join(new_output_dir, f"database_{source_env}.db{suffix}")
        if os.path.exists(stray):
            os.remove(stray)

    old_xml = os.path.join(new_output_dir, f"library_{source_env}.xml")
    if os.path.exists(old_xml):
        os.remove(old_xml)

    m3u8_dir = os.path.join(new_output_dir, "m3u8s")
    if os.path.isdir(m3u8_dir):
        for name in os.listdir(m3u8_dir):
            os.remove(os.path.join(m3u8_dir, name))

    if os.path.exists(new_db):
        _rewrite_cloned_db_paths(new_env, source_env)


def _rewrite_cloned_db_paths(new_env: str, old_env: str) -> None:
    """Point the two env-scoped absolute-path columns at the new environment.

    library_{env}.xml and the .m3u8 files are derived entirely from these
    same rows, so they are deliberately not carried over on clone -- run the
    workflow tasks for the new environment to regenerate them instead of
    trying to rewrite already-rendered file contents.
    """
    os.environ["APP_ENV"] = new_env
    db = TrackDB()
    db.conn.execute(
        "UPDATE tracks SET local_file_path = REPLACE(local_file_path, ?, ?) "
        "WHERE local_file_path IS NOT NULL",
        (f"/slskd_docker_data/{old_env}/", f"/slskd_docker_data/{new_env}/"),
    )
    db.conn.execute(
        "UPDATE playlists SET m3u8_path = REPLACE(m3u8_path, ?, ?) "
        "WHERE m3u8_path IS NOT NULL",
        (f"/output/{old_env}/", f"/output/{new_env}/"),
    )
    db.conn.commit()


def _clone_environment(source_env: str, snapshot: str, new_env: str) -> bool:
    write_log.info(
        "RESTORE_CLONE_START", "Starting clone restore.",
        {"source_env": source_env, "new_env": new_env, "snapshot": snapshot},
    )

    if not RESTIC_PASSWORD:
        write_log.error("RESTORE_NO_PASSWORD", "BACKUP_RESTIC_PASSWORD is not set.", {"env": source_env})
        return False
    if new_env in list_known_environments():
        write_log.error(
            "RESTORE_CLONE_TARGET_EXISTS",
            "Target environment already has data on disk; refusing to overwrite via clone.",
            {"new_env": new_env},
        )
        return False
    if not os.path.isdir(repo_path(source_env)):
        write_log.error("RESTORE_NO_REPO", "No backup repository found for environment.", {"env": source_env})
        return False

    scratch = os.path.join("/tmp", f"spotiseek_clone_{source_env}_{int(time.time())}")
    try:
        result = _restic(source_env, "restore", snapshot, "--target", scratch, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"restic restore failed: {result.stderr.strip()}")

        _move_cloned_path(
            scratch, os.path.join(SLSKD_DATA_ROOT, source_env), os.path.join(SLSKD_DATA_ROOT, new_env),
        )
        _move_cloned_path(
            scratch, os.path.join(OUTPUT_ROOT, source_env), os.path.join(OUTPUT_ROOT, new_env),
        )
        _move_cloned_path(
            scratch,
            os.path.join(INPUT_PLAYLISTS_ROOT, f"playlists_{source_env}.csv"),
            os.path.join(INPUT_PLAYLISTS_ROOT, f"playlists_{new_env}.csv"),
        )

        _finalize_cloned_output_dir(os.path.join(OUTPUT_ROOT, new_env), source_env, new_env)

        write_log.info(
            "RESTORE_CLONE_COMPLETE",
            "Clone restore completed. library.xml and the .m3u8 files were not carried over -- "
            "switch to the new environment and run the workflow tasks once to regenerate them.",
            {"source_env": source_env, "new_env": new_env},
        )
        return True

    except Exception as e:
        write_log.error(
            "RESTORE_CLONE_FAILED", "Clone restore failed.",
            {"source_env": source_env, "new_env": new_env, "error": str(e)},
        )
        return False

    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def restore_environment(env: str, snapshot: str = "latest", clone_as: str | None = None) -> bool:
    if clone_as:
        return _clone_environment(env, snapshot, clone_as)
    return _restore_in_place(env, snapshot)


# ---------------------------------------------------------------------------
# Self-scheduling daemon (the backup service's main process)
# ---------------------------------------------------------------------------

_shutdown = threading.Event()


def _handle_signal(signum, frame) -> None:  # noqa: ARG001
    _shutdown.set()


def _load_schedule_state() -> dict:
    if not os.path.exists(SCHEDULE_STATE_FILE):
        return {}
    try:
        with open(SCHEDULE_STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_schedule_state(state: dict) -> None:
    os.makedirs(os.path.dirname(SCHEDULE_STATE_FILE), exist_ok=True)
    with open(SCHEDULE_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def _in_schedule_window(hour: int, target_hour: int, window_hours: int = 1) -> bool:
    """True if `hour` is within `window_hours` of `target_hour`, wrapping around midnight.

    A bare `hour == target_hour` check would miss the day entirely if the daemon happened
    to be down for the whole of that hour (e.g. rebuilding during `invoke deploy`), so this
    gives it a few hours either side to still catch that day's backup -- e.g. target_hour=4,
    window_hours=1 is due anywhere from 3am up to (not including) 6am.
    """
    diff = (hour - target_hour) % 24
    return diff <= window_hours or diff >= 24 - window_hours


def _minutes_since_last_invoke_activity() -> float | None:
    """Minutes since `invoke up`/`invoke deploy` last ran on the host, or None if
    never recorded (treated as "quiet enough" -- a fresh checkout shouldn't block
    scheduled backups forever).
    """
    if not os.path.exists(LAST_INVOKE_ACTIVITY_FILE):
        return None
    try:
        with open(LAST_INVOKE_ACTIVITY_FILE, encoding="utf-8") as f:
            last = float(f.read().strip())
    except (OSError, ValueError):
        return None
    return (time.time() - last) / 60


def _run_backup_subprocess(env: str) -> None:
    """Run a single environment's backup as a fresh process.

    A fresh process per environment (rather than calling backup_environment()
    in-process) is required for correct log routing: setup_logging() is a
    process-wide singleton keyed by APP_ENV, and the daemon handles many
    environments over its lifetime.
    """
    result = subprocess.run(
        [sys.executable, "-m", "scripts.backup_manager", "backup", "--env", env],
        check=False,
    )
    if result.returncode != 0:
        write_log.error(
            "BACKUP_SCHEDULER_RUN_FAILED", "Scheduled backup subprocess exited non-zero.",
            {"env": env, "returncode": result.returncode},
        )


def run_scheduler_daemon() -> None:
    setup_logging(
        logs_dir=os.path.join(LOGS_ROOT, "_scheduler"),
        log_name_prefix="backup_scheduler",
        rotate_daily=True,
    )

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    resume_orphaned_pause()

    schedule_envs = [e.strip() for e in os.getenv("BACKUP_SCHEDULE_ENVS", "").split(",") if e.strip()]
    schedule_hour = int(os.getenv("BACKUP_SCHEDULE_HOUR", "4"))
    quiet_minutes = int(os.getenv("BACKUP_QUIET_MINUTES", "60"))
    tz = ZoneInfo(os.getenv("BACKUP_SCHEDULE_TIMEZONE", "Europe/London"))

    write_log.info(
        "BACKUP_SCHEDULER_START", "Backup scheduler started.",
        {
            "envs": schedule_envs, "schedule_hour": schedule_hour,
            "quiet_minutes": quiet_minutes, "timezone": str(tz),
        },
    )
    if not schedule_envs:
        write_log.info(
            "BACKUP_SCHEDULER_IDLE",
            "No environments configured for scheduled backups (BACKUP_SCHEDULE_ENVS is empty). "
            "invoke backup is still available for on-demand runs.",
        )

    # Per-env date a scheduled backup last ran, so a recreated container (every
    # `invoke deploy` rebuilds this service) doesn't treat today's backup as still due.
    last_run_dates = _load_schedule_state()

    while not _shutdown.is_set():
        now_local = datetime.now(tz)
        today = now_local.date().isoformat()
        if _in_schedule_window(now_local.hour, schedule_hour):
            for env in schedule_envs:
                if last_run_dates.get(env) == today:
                    continue  # already ran today
                if is_hot_environment(env):
                    quiet_since = _minutes_since_last_invoke_activity()
                    if quiet_since is not None and quiet_since < quiet_minutes:
                        write_log.info(
                            "BACKUP_SCHEDULER_DEFERRED",
                            "Deferring scheduled backup -- too soon after invoke up/deploy.",
                            {"env": env, "minutes_since_invoke_activity": round(quiet_since, 1)},
                        )
                        continue  # retry on a later tick
                write_log.info("BACKUP_SCHEDULER_TRIGGER", "Triggering scheduled backup.", {"env": env})
                _run_backup_subprocess(env)
                last_run_dates[env] = today
                _save_schedule_state(last_run_dates)
        resume_orphaned_pause()
        _shutdown.wait(timeout=30)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Spotiseek backup/restore manager")
    sub = parser.add_subparsers(dest="command", required=True)

    backup_p = sub.add_parser("backup", help="Back up a single environment")
    backup_p.add_argument("--env", required=True)

    restore_p = sub.add_parser("restore", help="Restore a single environment")
    restore_p.add_argument("--env", required=True, help="Environment to restore from")
    restore_p.add_argument("--from", dest="snapshot", default="latest", help="Snapshot ID (default: latest)")
    restore_p.add_argument("--as", dest="clone_as", default=None, help="Clone into this new environment name")

    sub.add_parser("daemon", help="Run the self-scheduling backup daemon")

    args = parser.parse_args()

    if args.command == "daemon":
        run_scheduler_daemon()
        return

    if args.command == "backup":
        os.environ["APP_ENV"] = args.env
        setup_logging(log_name_prefix="backup", rotate_daily=True)
        resume_orphaned_pause()
        success = backup_environment(args.env)
        sys.exit(0 if success else 1)

    if args.command == "restore":
        os.environ["APP_ENV"] = args.clone_as or args.env
        setup_logging(log_name_prefix="backup", rotate_daily=True)
        resume_orphaned_pause()
        success = restore_environment(args.env, snapshot=args.snapshot, clone_as=args.clone_as)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
