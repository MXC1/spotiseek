"""Shared helpers for talking to the Docker daemon over the mounted socket.

Used by both the `backup` service (pause/resume slskd + workflow around a
consistent snapshot) and the `dashboard` service (live log streaming in
the Execution Inspection tab) -- both run in containers with
/var/run/docker.sock mounted and the `docker` CLI installed (see
infra/Dockerfile.backup, infra/Dockerfile.dashboard).
"""

import os
import subprocess


def own_compose_project() -> str | None:
    """The compose project this container itself belongs to.

    Filtering only by `com.docker.compose.service` is not enough to scope
    `docker ps` to this project: an unrelated compose project elsewhere on
    the host can use the same service name (e.g. another "slskd" service)
    and would otherwise get matched too. Docker sets HOSTNAME to the
    container's own short ID by default, which lets us look up our own
    project label and use it to scope every other query.
    """
    own_id = os.getenv("HOSTNAME", "")
    if not own_id:
        return None
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", '{{index .Config.Labels "com.docker.compose.project"}}', own_id],
            capture_output=True, text=True, check=False,
        )
    except OSError:
        return None
    project = result.stdout.strip()
    return project or None


def container_ids_for_service(service: str, project: str | None = None) -> list[str]:
    """Running container IDs for one compose service, scoped to `project` if given."""
    filters = ["--filter", f"label=com.docker.compose.service={service}"]
    if project:
        filters += ["--filter", f"label=com.docker.compose.project={project}"]
    try:
        result = subprocess.run(["docker", "ps", "-q", *filters], check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError):
        return []
    return [cid for cid in result.stdout.split() if cid]


def list_running_containers(project: str | None = None) -> list[dict[str, str]]:
    """Every running container in `project` (or unscoped if None), as [{"id", "service"}, ...].

    Used to populate the live log stream's container checkboxes with whatever
    is actually up right now, rather than a hardcoded service list.
    """
    filters = ["--filter", "status=running"]
    if project:
        filters += ["--filter", f"label=com.docker.compose.project={project}"]
    fmt = '{{.ID}}\t{{.Label "com.docker.compose.service"}}'
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", fmt, *filters], check=True, capture_output=True, text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return []

    containers = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 1)
        cid = parts[0]
        service = parts[1] if len(parts) > 1 and parts[1] else cid[:12]
        containers.append({"id": cid, "service": service})
    return containers
