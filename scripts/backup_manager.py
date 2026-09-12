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
import os
import shutil
import signal
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True

from dotenv import load_dotenv  # noqa: E402

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path)

from scripts.database_management import TrackDB  # noqa: E402
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

def _own_compose_project() -> str | None:
    """The compose project this container itself belongs to.

    Filtering only by `com.docker.compose.service` is not enough to scope
    docker ps to this project: an unrelated compose project elsewhere on the
    host can use the same service name (e.g. another "slskd" service) and
    would otherwise get matched too. Docker sets HOSTNAME to the container's
    own short ID by default, which lets us look up our own project label and
    use it to scope every other query.
    """
    own_id = os.getenv("HOSTNAME", "")
    if not own_id:
        return None
    result = subprocess.run(
        ["docker", "inspect", "--format", '{{index .Config.Labels "com.docker.compose.project"}}', own_id],
        capture_output=True, text=True, check=False,
    )
    project = result.stdout.strip()
    return project or None


def _container_ids_for_service(service: str) -> list[str]:
    filters = ["--filter", f"label=com.docker.compose.service={service}"]
    project = _own_compose_project()
    if project:
        filters += ["--filter", f"label=com.docker.compose.project={project}"]
    else:
        write_log.warn(
            "BACKUP_PROJECT_LOOKUP_FAILED",
            "Could not determine this container's own compose project; "
            "container discovery is unscoped and may match unrelated projects.",
        )
    result = subprocess.run(["docker", "ps", "-q", *filters], check=True, capture_output=True, text=True)
    return [cid for cid in result.stdout.split() if cid]


def stop_hot_containers() -> list[str]:
    """Stop slskd + workflow so the active environment's files are quiescent."""
    stopped = []
    for service in HOT_CONTAINER_SERVICES:
        for cid in _container_ids_for_service(service):
            write_log.info(
                "BACKUP_CONTAINER_STOP",
                "Stopping container for a consistent copy.",
                {"service": service, "container_id": cid},
            )
            subprocess.run(["docker", "stop", cid], check=True, capture_output=True, text=True)
            stopped.append(cid)
    return stopped


def start_containers(container_ids: list[str]) -> None:
    for cid in container_ids:
        write_log.info("BACKUP_CONTAINER_START", "Restarting container.", {"container_id": cid})
        subprocess.run(["docker", "start", cid], check=True, capture_output=True, text=True)


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
            self._stopped_ids = stop_hot_containers()
        return self.hot

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._stopped_ids:
            start_containers(self._stopped_ids)


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

    schedule_envs = [e.strip() for e in os.getenv("BACKUP_SCHEDULE_ENVS", "").split(",") if e.strip()]
    interval_minutes = int(os.getenv("BACKUP_INTERVAL_MINUTES", "1440"))

    write_log.info(
        "BACKUP_SCHEDULER_START", "Backup scheduler started.",
        {"envs": schedule_envs, "interval_minutes": interval_minutes},
    )
    if not schedule_envs:
        write_log.info(
            "BACKUP_SCHEDULER_IDLE",
            "No environments configured for scheduled backups (BACKUP_SCHEDULE_ENVS is empty). "
            "invoke backup is still available for on-demand runs.",
        )

    next_run_at = dict.fromkeys(schedule_envs, 0.0)  # due immediately on startup

    while not _shutdown.is_set():
        now = time.time()
        for env in schedule_envs:
            if now >= next_run_at.get(env, 0.0):
                write_log.info("BACKUP_SCHEDULER_TRIGGER", "Triggering scheduled backup.", {"env": env})
                _run_backup_subprocess(env)
                next_run_at[env] = time.time() + interval_minutes * 60
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
        success = backup_environment(args.env)
        sys.exit(0 if success else 1)

    if args.command == "restore":
        os.environ["APP_ENV"] = args.clone_as or args.env
        setup_logging(log_name_prefix="backup", rotate_daily=True)
        success = restore_environment(args.env, snapshot=args.snapshot, clone_as=args.clone_as)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
