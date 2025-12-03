import os
from pathlib import Path
import subprocess
from invoke import task

def get_app_env():
    """Read APP_ENV from .env file."""
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        print(".env file not found!")
        return None
    with open(env_path, 'r') as f:
        for line in f:
            if line.strip().startswith('APP_ENV='):
                return line.strip().split('=', 1)[1]
    print("APP_ENV not found in .env!")
    return None

@task
def nuke(c):
    """Stop containers, prune system, and delete slskd_docker_data/<APP_ENV> directory."""
    subprocess.run(["docker-compose", "down"], check=True)
    subprocess.run(["docker", "system", "prune", "-a", "--volumes", "-f"], check=True)
    app_env = get_app_env()
    if app_env:
        import shutil
        targets = [
            Path('slskd_docker_data') / app_env,
            Path('observability') / 'logs' / app_env,
            Path('database') / app_env,
            Path('database') / 'm3u8s' / app_env
        ]
        if app_env.lower() in ["prod", "stage"]:
            print(f"WARNING: You are about to delete directories for APP_ENV='{app_env}'. This is a critical environment!")
            for t in targets:
                print(f"  - {t}")
            confirm = input(f"Are you sure you want to delete these directories? Type 'YES' to confirm: ")
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
                    except Exception as e:
                        print(f"Warning: rmdir failed, trying PowerShell...")
                        try:
                            # Fallback to PowerShell with force and no confirmation
                            c.run(f'powershell -Command "Remove-Item -LiteralPath \'{abs_target}\' -Recurse -Force -Confirm:$false -ErrorAction Stop"', hide=True)
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
def workflow(c):
    """Run the workflow script inside the Docker container."""
@task
def workflow(c, d=False):
    """Run the workflow script inside the Docker container. Use --detached to run in detached mode."""
    flag = "-d " if d else ""
    c.run(f"docker-compose exec {flag}workflow python scripts/workflow.py")

@task
def build(c):
    """Build all Docker images"""
    subprocess.run(["docker-compose", "build"], check=True)

@task
def up(c):
    """Start all services using docker-compose. Use --build to force image rebuild."""
    subprocess.run(["docker-compose", "up", "-d", "--build"], check=True)

@task
def down(c):
    """Stop all services using docker-compose"""
    subprocess.run(["docker-compose", "down"], check=True)

@task
def logs(c):
    """Show logs for all services"""
    subprocess.run(["docker-compose", "logs", "-f"], check=True)

@task
def prune(c):
    """Remove all stopped containers, networks, images, and volumes"""
    subprocess.run(["docker", "system", "prune", "-a", "--volumes", "-f"], check=True)

@task
def clean(c):
    """Remove __pycache__ and *.pyc files recursively"""
    c.run("powershell -Command \"Get-ChildItem -Recurse -Include __pycache__,*.pyc | Remove-Item -Recurse -Force\"")

@task
def test(c):
    """Run Python tests (pytest)"""
    c.run(".\\.venv\\Scripts\\python.exe -m pytest")

@task
def lint(c):
    """Run flake8 linter on scripts/"""
    c.run(".\\.venv\\Scripts\\flake8 scripts/")

@task
def setenv(c, env):
    """Change the APP_ENV variable in .env file. Usage: invoke setenv <environment>"""
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        print(".env file not found!")
        return
    
    # Read current .env content
    with open(env_path, 'r') as f:
        lines = f.readlines()
    
    # Update or add APP_ENV
    found = False
    for i, line in enumerate(lines):
        if line.strip().startswith('APP_ENV='):
            lines[i] = f'APP_ENV={env}\n'
            found = True
            break
    
    if not found:
        lines.append(f'APP_ENV={env}\n')
    
    # Write back to .env
    with open(env_path, 'w') as f:
        f.writelines(lines)
    
    print(f"APP_ENV set to '{env}'")
    print("Running 'invoke up' to apply environment change...")
    subprocess.run(["invoke", "up"], check=True)

@task(default=True)
def help(c):
    """Show available tasks"""
    c.run("invoke --list")
