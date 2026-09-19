# ruff: noqa: ARG001
import io
import os
import platform
import subprocess
import tarfile
import time
from pathlib import Path

from invoke import task

ENV_PATH = Path(__file__).parent / ".env"

# Read by the backup daemon's quiet-period gate (BACKUP_QUIET_MINUTES) so a scheduled
# backup doesn't pause slskd/workflow while the user is actively running `invoke
# up`/`invoke deploy` against the hot environment -- see scripts/backup_manager.py.
LAST_INVOKE_ACTIVITY_FILE = Path("observability/logs/_scheduler/last_invoke_activity")

# Files extracted by `invoke deploy` -- see docs/adr/0002-gated-environment-deploys-via-git-archive.md
DEPLOY_DIR = Path(".deploy")
DEPLOYED_CODE_RELPATH = ".deploy/deployed-code"  # forward slashes: written into .env for docker-compose
DEPLOYED_CODE_DIR = Path(DEPLOYED_CODE_RELPATH)
DEPLOYED_SHA_FILE = DEPLOY_DIR / "DEPLOYED_SHA"
DEPLOY_ARCHIVE_PATHS = [
    "scripts",
    "observability/dashboard",
    "requirements.txt",
    "infra/Dockerfile.backup",
]


def _read_env_var(key: str) -> str | None:
    """Read a single KEY=value line from .env."""
    if not ENV_PATH.exists():
        return None
    with open(ENV_PATH) as f:
        for line in f:
            if line.strip().startswith(f"{key}="):
                return line.strip().split("=", 1)[1]
    return None


def _write_env_var(key: str, value: str) -> None:
    """Set (or append) a single KEY=value line in .env."""
    lines = []
    if ENV_PATH.exists():
        with open(ENV_PATH) as f:
            lines = f.readlines()

    found = False
    for i, line in enumerate(lines):
        if line.strip().startswith(f"{key}="):
            lines[i] = f"{key}={value}\n"
            found = True
            break
    if not found:
        lines.append(f"{key}={value}\n")

    with open(ENV_PATH, "w") as f:
        f.writelines(lines)


def get_app_env():
    """Read APP_ENV from .env file."""
    if not ENV_PATH.exists():
        print(".env file not found!")
        return None
    value = _read_env_var("APP_ENV")
    if value is None:
        print("APP_ENV not found in .env!")
    return value


def get_deploy_gated_env():
    """Read DEPLOY_GATED_ENV from .env -- the environment whose workflow/dashboard/backup
    code only changes via `invoke deploy`, never just by being the hot environment.
    """
    return _read_env_var("DEPLOY_GATED_ENV")


def _sync_code_root() -> None:
    """Point CODE_ROOT (read by docker-compose.yml for workflow/dashboard's code volume
    mounts) at the deployed snapshot when the gated environment is hot, otherwise at the
    working tree.
    """
    app_env = get_app_env()
    gated_env = get_deploy_gated_env()
    code_root = f"./{DEPLOYED_CODE_RELPATH}" if (gated_env and app_env == gated_env) else "."
    _write_env_var("CODE_ROOT", code_root)


def _require_deployed_code() -> bool:
    """The `backup` service always builds from .deploy/deployed-code/, for every
    environment, so it must exist before docker-compose can bring anything up.
    """
    if not DEPLOYED_CODE_DIR.exists():
        print(
            f"No deployed code found at {DEPLOYED_CODE_RELPATH}/ -- run `invoke deploy` first.\n"
            "The 'backup' service (and the gated environment's workflow/dashboard, if it's "
            "the one currently hot) build/mount from that snapshot.",
        )
        return False
    return True


def _record_invoke_activity() -> None:
    """Timestamp `invoke up`/`invoke deploy` so the backup daemon's quiet-period gate
    can see how recently the user last touched the hot environment's infrastructure.
    """
    LAST_INVOKE_ACTIVITY_FILE.parent.mkdir(parents=True, exist_ok=True)
    LAST_INVOKE_ACTIVITY_FILE.write_text(str(time.time()))


def running_inside_wsl() -> bool:
    """Detect whether the current process is inside a WSL environment."""
    if os.environ.get("WSL_DISTRO_NAME"):
        return True
    try:
        return "microsoft" in Path("/proc/version").read_text().lower()
    except OSError:
        return False


def wrap_docker_cmd(cmd: list[str]) -> list[str]:
    """Prefix Docker commands with `wsl` when launched from Windows host shells."""
    if platform.system() == "Windows" and not running_inside_wsl():
        return ["wsl", *cmd]
    return cmd


def _force_remove_dir(c, target: Path) -> None:
    """Delete a directory tree, tolerating read-only files (e.g. restic's data blobs,
    or downloaded files slskd marks read-only) that shutil.rmtree chokes on via Python
    on Windows.
    """
    abs_target = str(target.resolve())
    try:
        # Windows rmdir handles problematic paths (read-only files, long paths) better
        # than Python's own file-removal APIs.
        c.run(f'rmdir /s /q "{abs_target}"', hide=True)
    except Exception:
        print("Warning: rmdir failed, trying PowerShell...")
        try:
            # Fallback to PowerShell with force and no confirmation
            ps_cmd = (
                f'powershell -Command "Remove-Item -LiteralPath \'{abs_target}\' '
                '-Recurse -Force -Confirm:$false -ErrorAction Stop"'
            )
            c.run(ps_cmd, hide=True)
        except Exception as e2:
            print(f"Error: Could not delete {target}: {e2}")
            print("You may need to manually delete this directory or reboot and try again.")
            return

    # A stale WSL2/Docker file-sharing view of the directory can report "not
    # empty" for a moment right after every file inside was actually removed,
    # so rmdir/PowerShell above can leave a tree of empty directories behind.
    # By now enough time has passed for that to settle, so clean those up too.
    if target.exists():
        for root, dirs, _files in os.walk(abs_target, topdown=False):
            for d in dirs:
                try:
                    os.rmdir(os.path.join(root, d))
                except OSError:
                    pass
        try:
            target.rmdir()
        except OSError:
            pass

@task(help={
    "env": "Optional environment name to override APP_ENV (e.g. test_new)",
})
def nuke(c, env=None):
    """Stop containers, prune system, and delete environment directories.

    Usage:
      invoke nuke [--env=<environment>]

    If --env is provided, that value is used for app_env. Otherwise, APP_ENV is
    read from .env via get_app_env().
    """
    subprocess.run(wrap_docker_cmd(["docker-compose", "down"]), check=True)
    subprocess.run(
        wrap_docker_cmd(["docker", "system", "prune", "-a", "--volumes", "-f"]),
        check=True,
    )
    app_env = env if env else get_app_env()
    if app_env:
        # Deliberately excludes backups/{app_env}: nuke wipes the environment
        # you're recovering from, so its backups must survive. Use
        # `invoke backup-forget` to delete backup history on purpose.
        targets = [
            Path("slskd_docker_data") / app_env,
            Path("observability") / "logs" / app_env,
            Path("output") / app_env,
            Path("output") / app_env / "m3u8s",
        ]
        if app_env.lower() in ["prod", "stage"]:
            print(
                f"WARNING: You are about to delete directories for APP_ENV='{app_env}'. "
                "This is a critical environment!",
            )
            for t in targets:
                print(f"  - {t}")
            confirm = input("Are you sure you want to delete these directories? Type 'YES' to confirm: ")
            if confirm != "YES":
                print("Aborting directory deletion.")
                return
        for target in targets:
            if target.exists():
                print(f"Deleting {target} ...")
                if target.is_dir():
                    _force_remove_dir(c, target)
                else:
                    target.unlink()
            else:
                print(f"{target} does not exist.")
    else:
        print("Could not determine APP_ENV, skipping directory deletion.")

@task
def exec(c, service, command):
    """Execute a command inside a running Docker container.
    Usage: invoke exec --service <service_name> --command '<command>'
    """
    if not service or not command:
        print("You must specify both --service and --command.")
        return
    subprocess.run(
        wrap_docker_cmd(["docker-compose", "exec", service, *command.split()]),
        check=True,
    )

@task
def build(c):
    """Build all Docker images"""
    if not _require_deployed_code():
        return
    _sync_code_root()
    subprocess.run(wrap_docker_cmd(["docker-compose", "build"]), check=True)

@task
def up(c, service=None):
    """Start all services using docker-compose.

    Use --build to force image rebuild. Optionally specify a service (e.g. invoke up dashboard).
    """
    if not _require_deployed_code():
        return
    _sync_code_root()
    _record_invoke_activity()
    cmd = ["docker-compose", "up", "-d", "--build"]
    if service:
        cmd.append(service)
    subprocess.run(wrap_docker_cmd(cmd), check=True)

@task
def down(c):
    """Stop all services using docker-compose"""
    subprocess.run(wrap_docker_cmd(["docker-compose", "down"]), check=True)

@task
def logs(c, service=None):
    """Show logs for all services"""
    cmd = ["docker-compose", "logs", "-f"]
    if service:
        cmd.append(service)
    subprocess.run(wrap_docker_cmd(cmd), check=True)

@task
def prune(c):
    """Remove all stopped containers, networks, images, and volumes"""
    subprocess.run(
        wrap_docker_cmd(["docker", "system", "prune", "-a", "--volumes", "-f"]),
        check=True,
    )

@task
def clean(c):
    """Remove __pycache__ and *.pyc files recursively"""
    c.run('powershell -Command "Get-ChildItem -Recurse -Include __pycache__,*.pyc | Remove-Item -Recurse -Force"')

@task
def test(c):
    """Run Python tests (pytest)"""
    c.run(".\\.venv\\Scripts\\python.exe -m pytest")

def _backup_dest() -> Path:
    """Resolve BACKUP_DEST from .env (host path), defaulting to ./backups."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                if line.strip().startswith("BACKUP_DEST="):
                    return Path(line.strip().split("=", 1)[1])
    return Path("backups")


def _discover_environments() -> list[str]:
    """Every environment with a slskd_docker_data or output directory."""
    envs = set()
    for base in (Path("slskd_docker_data"), Path("output")):
        if base.is_dir():
            for p in base.iterdir():
                if p.is_dir():
                    envs.add(p.name)
    return sorted(envs)


@task(help={
    "env": "Environment to back up (defaults to the current APP_ENV)",
    "all": "Back up every environment found under slskd_docker_data/ and output/",
})
def backup(c, env=None, all=False):
    """Back up one, or every, Spotiseek environment via the backup service.

    Usage:
      invoke backup                  # back up the current APP_ENV
      invoke backup --env=prod       # back up a specific environment
      invoke backup --all            # back up every environment with data on disk
    """
    if all:
        envs = _discover_environments()
        if not envs:
            print("No environments found under slskd_docker_data/ or output/.")
            return
    else:
        target = env or get_app_env()
        if not target:
            print("No environment specified and APP_ENV could not be determined.")
            return
        envs = [target]

    for e in envs:
        print(f"Backing up '{e}'...")
        subprocess.run(
            wrap_docker_cmd([
                "docker-compose", "exec", "backup",
                "python", "-m", "scripts.backup_manager", "backup", "--env", e,
            ]),
            check=True,
        )


@task(help={
    "env": "Environment to restore from",
    "from_snapshot": "Restic snapshot ID to restore (defaults to the latest)",
    "as_env": "Restore into a new environment name instead of overwriting --env",
})
def restore(c, env, from_snapshot="latest", as_env=None):
    """Restore a Spotiseek environment from backup, in place or as a clone.

    Usage:
      invoke restore --env=prod                            # restore prod from its latest backup, in place
      invoke restore --env=prod --from-snapshot=abc123      # restore a specific snapshot
      invoke restore --env=prod --as-env=prod_recovered     # clone prod's latest backup into a new environment
    """
    cmd = [
        "docker-compose", "exec", "backup",
        "python", "-m", "scripts.backup_manager", "restore",
        "--env", env, "--from", from_snapshot,
    ]
    if as_env:
        cmd.extend(["--as", as_env])
    subprocess.run(wrap_docker_cmd(cmd), check=True)


@task(help={
    "env": "Environment whose entire backup history should be permanently deleted",
})
def backup_forget(c, env):
    """Permanently delete every backup for one environment (the whole restic repository).

    Usage:
      invoke backup-forget --env=old_test_env

    Note: `invoke nuke` never touches backups/ -- this is the only way to delete
    backup history, and it's deliberately separate from deleting live environment data.
    """
    target = _backup_dest() / env
    if not target.exists():
        print(f"No backups found for '{env}' at {target}")
        return

    print(f"This will permanently delete ALL backups for '{env}':")
    print(f"  {target.resolve()}")
    confirm = input("Type 'YES' to confirm: ")
    if confirm != "YES":
        print("Aborted.")
        return

    _force_remove_dir(c, target)
    print(f"Deleted all backups for '{env}'.")


@task
def run_all_tasks(c, attach=False):
    """Run all task scheduler tasks in dependency order inside the Docker container"""
    cmd = ["docker-compose", "exec"]
    if not attach:
        cmd.append("-d")
    cmd.extend(["workflow", "python", "-m", "scripts.task_scheduler", "--run-all"])
    subprocess.run(wrap_docker_cmd(cmd), check=True)

@task
def lint(c):
    """Run ruff linter on scripts/ and tasks.py"""
    c.run("ruff check scripts/ tasks.py")

@task
def lint_fix(c):
    """Run ruff linter with auto-fix on scripts/ and tasks.py"""
    c.run("ruff check scripts/ tasks.py --fix")

@task
def setenv(c, env):
    """Change the APP_ENV variable in .env file. Usage: invoke setenv <environment>"""
    if not ENV_PATH.exists():
        print(".env file not found!")
        return

    _write_env_var("APP_ENV", env)

    print(f"APP_ENV set to '{env}'")
    print("Running 'invoke up' to apply environment change...")
    subprocess.run(["invoke", "up"], check=True)


@task(help={
    "ref": "Git ref to deploy (default: origin/main, fetched first)",
})
def deploy(c, ref="origin/main"):
    """Extract a curated code snapshot into .deploy/deployed-code/ for the gated environment.

    Usage:
      invoke deploy                # deploy origin/main (fetches first)
      invoke deploy --ref=<ref>    # deploy an arbitrary branch, tag, or commit

    Only scripts/, the dashboard code under observability/, requirements.txt, and
    infra/Dockerfile.backup are extracted -- see
    docs/adr/0002-gated-environment-deploys-via-git-archive.md. The 'backup' service
    always builds from this snapshot; 'workflow' and 'dashboard' only mount from it
    while the gated environment (DEPLOY_GATED_ENV) is hot.
    """
    _record_invoke_activity()
    print("Fetching origin...")
    subprocess.run(["git", "fetch", "origin"], check=True)

    resolved = subprocess.run(
        ["git", "rev-parse", ref], check=True, capture_output=True, text=True,
    ).stdout.strip()

    previous_sha = DEPLOYED_SHA_FILE.read_text().strip() if DEPLOYED_SHA_FILE.exists() else None
    if previous_sha == resolved:
        print(f"Already deployed at {resolved} -- nothing to do.")
        return

    change = f" (was {previous_sha})" if previous_sha else ""
    print(f"Deploying {ref} -> {resolved}{change}")

    if DEPLOYED_CODE_DIR.exists():
        _force_remove_dir(c, DEPLOYED_CODE_DIR)
    DEPLOYED_CODE_DIR.mkdir(parents=True, exist_ok=True)

    archive = subprocess.run(
        ["git", "archive", resolved, "--", *DEPLOY_ARCHIVE_PATHS],
        check=True, capture_output=True,
    )
    with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as tf:
        tf.extractall(DEPLOYED_CODE_DIR, filter="data")

    DEPLOYED_SHA_FILE.write_text(resolved + "\n")
    print(f"Deployed {resolved} to {DEPLOYED_CODE_RELPATH}/")

    print("Rebuilding 'backup' from the deployed snapshot...")
    subprocess.run(
        wrap_docker_cmd(["docker-compose", "up", "-d", "--build", "backup"]),
        check=True,
    )

    gated_env = get_deploy_gated_env()
    if gated_env and get_app_env() == gated_env:
        print(f"'{gated_env}' is the hot environment -- restarting workflow and dashboard...")
        subprocess.run(
            wrap_docker_cmd(["docker-compose", "restart", "workflow", "dashboard"]),
            check=True,
        )

    print("Deploy complete.")

@task(default=True)
def help(c):
    """Show available tasks"""
    c.run("invoke --list")
