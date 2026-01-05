# ruff: noqa: ARG001
import os
import platform
import subprocess
from pathlib import Path

from invoke import task


def get_app_env():
    """Read APP_ENV from .env file."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        print(".env file not found!")
        return None
    with open(env_path) as f:
        for line in f:
            if line.strip().startswith("APP_ENV="):
                return line.strip().split("=", 1)[1]
    print("APP_ENV not found in .env!")
    return None


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
                    # Use Windows rmdir command which handles problematic paths better
                    try:
                        abs_target = str(target.resolve())
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
    subprocess.run(wrap_docker_cmd(["docker-compose", "build"]), check=True)

@task
def up(c, service=None):
    """Start all services using docker-compose.

    Use --build to force image rebuild. Optionally specify a service (e.g. invoke up streamlit).
    """
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
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        print(".env file not found!")
        return

    # Read current .env content
    with open(env_path) as f:
        lines = f.readlines()

    # Update or add APP_ENV
    found = False
    for i, line in enumerate(lines):
        if line.strip().startswith("APP_ENV="):
            lines[i] = f"APP_ENV={env}\n"
            found = True
            break

    if not found:
        lines.append(f"APP_ENV={env}\n")

    # Write back to .env
    with open(env_path, "w") as f:
        f.writelines(lines)

    print(f"APP_ENV set to '{env}'")
    print("Running 'invoke up' to apply environment change...")
    subprocess.run(["invoke", "up"], check=True)

@task(default=True)
def help(c):
    """Show available tasks"""
    c.run("invoke --list")
