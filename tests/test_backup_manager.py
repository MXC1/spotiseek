"""Tests for scripts.backup_manager: backup scope, hot-environment detection,
and the clone-restore database path rewrite.

Pure-logic unit tests only -- no Docker, restic, or live slskd required,
consistent with the rest of this suite. Container-path arithmetic used by the
clone restore's file-moving step (_scratch_path/_move_cloned_path) targets
Linux container paths specifically and is exercised in the container at
runtime rather than here, since Windows path semantics don't let that logic
be exercised meaningfully from this (Windows) test host.
"""

import json
import os
import sqlite3
import subprocess

import pytest

os.environ.setdefault("APP_ENV", "test")

from scripts import backup_manager
from scripts.database_management import TrackDB


@pytest.fixture()
def repo_root(tmp_path, monkeypatch):
    """Point every backup_manager path constant at a temp directory tree."""
    slskd_root = tmp_path / "slskd_docker_data"
    output_root = tmp_path / "output"
    playlists_root = tmp_path / "input_playlists"
    logs_root = tmp_path / "observability" / "logs"
    env_file = tmp_path / ".env"
    shared_slskd_yml = slskd_root / "slskd.yml"

    for d in (slskd_root, output_root, playlists_root, logs_root):
        d.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(backup_manager, "SLSKD_DATA_ROOT", str(slskd_root))
    monkeypatch.setattr(backup_manager, "OUTPUT_ROOT", str(output_root))
    monkeypatch.setattr(backup_manager, "INPUT_PLAYLISTS_ROOT", str(playlists_root))
    monkeypatch.setattr(backup_manager, "LOGS_ROOT", str(logs_root))
    monkeypatch.setattr(backup_manager, "ENV_FILE", str(env_file))
    monkeypatch.setattr(backup_manager, "SHARED_SLSKD_YML", str(shared_slskd_yml))
    monkeypatch.setattr(backup_manager, "PAUSE_MARKER_FILE", str(logs_root / "_scheduler" / ".hot_pause_state.json"))

    return tmp_path


def _make_env_dirs(repo_root, env, *, with_imported=True, with_output=True):
    slskd_env = repo_root / "slskd_docker_data" / env
    (slskd_env / "downloads").mkdir(parents=True, exist_ok=True)
    (slskd_env / "downloads" / "track.mp3").write_text("fake audio")
    if with_imported:
        (slskd_env / "imported").mkdir(parents=True, exist_ok=True)
        (slskd_env / "imported" / "track.mp3").write_text("fake audio")
    if with_output:
        (repo_root / "output" / env).mkdir(parents=True, exist_ok=True)
    return slskd_env


# ---------------------------------------------------------------------------
# Backup scope
# ---------------------------------------------------------------------------

def test_env_backup_sources_includes_only_existing_paths(repo_root):
    env = "test_env"
    _make_env_dirs(repo_root, env, with_imported=False)

    sources = backup_manager.env_backup_sources(env)

    downloads = str(repo_root / "slskd_docker_data" / env / "downloads")
    imported = str(repo_root / "slskd_docker_data" / env / "imported")
    output = str(repo_root / "output" / env)

    assert downloads in sources
    assert output in sources
    assert imported not in sources  # doesn't exist on disk -> excluded


def test_env_backup_sources_excludes_slskd_runtime_data_and_incomplete(repo_root):
    env = "test_env"
    slskd_env = _make_env_dirs(repo_root, env)
    (slskd_env / "data").mkdir()
    (slskd_env / "incomplete").mkdir()

    sources = backup_manager.env_backup_sources(env)

    assert str(slskd_env / "data") not in sources
    assert str(slskd_env / "incomplete") not in sources


def test_shared_config_sources_includes_env_file_and_shared_slskd_yml(repo_root):
    (repo_root / ".env").write_text("APP_ENV=prod\n")
    (repo_root / "slskd_docker_data" / "slskd.yml").write_text("web: {}\n")

    sources = backup_manager.shared_config_sources()

    assert str(repo_root / ".env") in sources
    assert str(repo_root / "slskd_docker_data" / "slskd.yml") in sources


def test_shared_config_sources_empty_when_neither_file_exists(repo_root):  # noqa: ARG001
    assert backup_manager.shared_config_sources() == []


# ---------------------------------------------------------------------------
# Hot-environment detection
# ---------------------------------------------------------------------------

def test_is_hot_environment_matches_live_app_env_in_dotenv(repo_root):
    (repo_root / ".env").write_text("APP_ENV=prod\nOTHER=1\n")

    assert backup_manager.is_hot_environment("prod") is True
    assert backup_manager.is_hot_environment("staging") is False


def test_is_hot_environment_false_when_no_env_file(repo_root):  # noqa: ARG001
    assert backup_manager.is_hot_environment("anything") is False


def test_list_known_environments_finds_envs_across_both_roots(repo_root):
    _make_env_dirs(repo_root, "prod")
    (repo_root / "output" / "orphan_output_only").mkdir(parents=True)

    envs = backup_manager.list_known_environments()

    assert envs == ["orphan_output_only", "prod"]


# ---------------------------------------------------------------------------
# Clone restore: database path rewrite
# ---------------------------------------------------------------------------

@pytest.fixture()
def cloned_db(repo_root, monkeypatch):
    """A DB already moved+renamed into the new env, still holding old-env paths."""
    monkeypatch.setattr("scripts.database_management._BASE_DB_DIR", str(repo_root / "output"))

    new_env_dir = repo_root / "output" / "new_env"
    new_env_dir.mkdir(parents=True)
    db_path = new_env_dir / "database_new_env.db"

    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE tracks (track_id TEXT PRIMARY KEY, local_file_path TEXT)")
    conn.execute("CREATE TABLE playlists (playlist_url TEXT PRIMARY KEY, m3u8_path TEXT)")
    conn.execute(
        "INSERT INTO tracks VALUES ('t1', ?)",
        ("/app/slskd_docker_data/old_env/imported/track.mp3",),
    )
    conn.execute("INSERT INTO tracks VALUES ('t2', NULL)")
    conn.execute(
        "INSERT INTO playlists VALUES ('http://example/p1', ?)",
        ("/app/output/old_env/m3u8s/my_playlist.m3u8",),
    )
    conn.commit()
    conn.close()

    yield db_path

    TrackDB._instances.pop(os.path.abspath(str(db_path)), None)


def test_rewrite_cloned_db_paths_swaps_env_token(cloned_db):
    backup_manager._rewrite_cloned_db_paths("new_env", "old_env")

    conn = sqlite3.connect(str(cloned_db))
    try:
        local_path = conn.execute(
            "SELECT local_file_path FROM tracks WHERE track_id = 't1'",
        ).fetchone()[0]
        null_path = conn.execute(
            "SELECT local_file_path FROM tracks WHERE track_id = 't2'",
        ).fetchone()[0]
        m3u8_path = conn.execute(
            "SELECT m3u8_path FROM playlists WHERE playlist_url = 'http://example/p1'",
        ).fetchone()[0]
    finally:
        conn.close()

    assert local_path == "/app/slskd_docker_data/new_env/imported/track.mp3"
    assert null_path is None
    assert m3u8_path == "/app/output/new_env/m3u8s/my_playlist.m3u8"


def test_rewrite_cloned_db_paths_does_not_touch_unrelated_substrings(cloned_db):
    """Guard against a naive rewrite matching env names that are substrings of each other."""
    conn = sqlite3.connect(str(cloned_db))
    conn.execute(
        "INSERT INTO tracks VALUES ('t3', ?)",
        ("/app/slskd_docker_data/old_env_archive/imported/other.mp3",),
    )
    conn.commit()
    conn.close()

    backup_manager._rewrite_cloned_db_paths("new_env", "old_env")

    conn = sqlite3.connect(str(cloned_db))
    try:
        untouched = conn.execute(
            "SELECT local_file_path FROM tracks WHERE track_id = 't3'",
        ).fetchone()[0]
    finally:
        conn.close()

    # "old_env_archive" is a different environment name and must be left alone;
    # only the exact "/old_env/" path segment is a match.
    assert untouched == "/app/slskd_docker_data/old_env_archive/imported/other.mp3"


# ---------------------------------------------------------------------------
# Orphaned pause self-healing
# ---------------------------------------------------------------------------
#
# Regression coverage for the 2026-09-12 incident: a hot backup stopped
# slskd/workflow, then the backup container itself got recreated mid-pause
# (an `invoke up --build` collided with the pause window) before it could
# resume them -- leaving both containers stopped indefinitely and, this time,
# leading to real DB corruption. See docs/adr/0001-backup-restore-architecture.md.

def test_stop_hot_containers_updates_marker_after_each_container(repo_root, monkeypatch):
    """A kill between the two stops must still leave an accurate marker --
    not no marker, and not one claiming a container was stopped when it
    wasn't yet.
    """
    (repo_root / ".env").write_text("APP_ENV=prod\n")
    seen_markers_at_stop_time = []

    def fake_ids_for_service(service):
        return {"slskd": ["cid1"], "workflow": ["cid2"]}[service]

    def fake_run(cmd, **_kwargs):
        if cmd[:2] == ["docker", "stop"]:
            marker = None
            if os.path.exists(backup_manager.PAUSE_MARKER_FILE):
                with open(backup_manager.PAUSE_MARKER_FILE, encoding="utf-8") as f:
                    marker = json.load(f)
            seen_markers_at_stop_time.append(marker)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(backup_manager, "_container_ids_for_service", fake_ids_for_service)
    monkeypatch.setattr(backup_manager.subprocess, "run", fake_run)

    stopped = backup_manager.stop_hot_containers("prod")

    assert stopped == ["cid1", "cid2"]
    # Before stopping cid1, nothing had been recorded yet; before stopping
    # cid2, cid1's stop was already durably marked.
    assert seen_markers_at_stop_time == [
        None,
        {"env": "prod", "container_ids": ["cid1"]},
    ]
    with open(backup_manager.PAUSE_MARKER_FILE, encoding="utf-8") as f:
        assert json.load(f) == {"env": "prod", "container_ids": ["cid1", "cid2"]}


def test_paused_if_hot_clears_marker_on_clean_exit(repo_root, monkeypatch):
    (repo_root / ".env").write_text("APP_ENV=prod\n")
    calls = []

    def fake_stop_hot_containers(env):
        calls.append(("stop", env))
        backup_manager._write_pause_marker(env, ["cid1", "cid2"])
        return ["cid1", "cid2"]

    def fake_start_containers(container_ids):
        calls.append(("start", container_ids))

    monkeypatch.setattr(backup_manager, "stop_hot_containers", fake_stop_hot_containers)
    monkeypatch.setattr(backup_manager, "start_containers", fake_start_containers)

    with backup_manager._PausedIfHot("prod") as hot:
        assert hot is True
        assert os.path.exists(backup_manager.PAUSE_MARKER_FILE)

    assert calls == [("stop", "prod"), ("start", ["cid1", "cid2"])]
    assert not os.path.exists(backup_manager.PAUSE_MARKER_FILE)


def test_paused_if_hot_leaves_marker_behind_on_abrupt_exit(repo_root, monkeypatch):
    """Simulates the actual incident: __exit__ never runs (process killed),
    so the marker must survive on disk for the next process to find.
    """
    (repo_root / ".env").write_text("APP_ENV=prod\n")

    def fake_stop_hot_containers(env):
        backup_manager._write_pause_marker(env, ["cid1"])
        return ["cid1"]

    monkeypatch.setattr(backup_manager, "stop_hot_containers", fake_stop_hot_containers)
    monkeypatch.setattr(backup_manager, "start_containers", lambda _ids: None)

    ctx = backup_manager._PausedIfHot("prod")
    ctx.__enter__()  # no matching __exit__ -- simulates a hard kill mid-pause

    assert os.path.exists(backup_manager.PAUSE_MARKER_FILE)
    with open(backup_manager.PAUSE_MARKER_FILE, encoding="utf-8") as f:
        assert json.load(f) == {"env": "prod", "container_ids": ["cid1"]}


def test_paused_if_hot_writes_no_marker_when_env_is_not_hot(repo_root):
    (repo_root / ".env").write_text("APP_ENV=staging\n")

    with backup_manager._PausedIfHot("prod") as hot:
        assert hot is False
        assert not os.path.exists(backup_manager.PAUSE_MARKER_FILE)


def test_resume_orphaned_pause_does_nothing_without_a_marker(repo_root, monkeypatch):  # noqa: ARG001
    calls = []
    monkeypatch.setattr(backup_manager.subprocess, "run", lambda *a, **k: calls.append((a, k)))

    backup_manager.resume_orphaned_pause()

    assert calls == []


def test_resume_orphaned_pause_starts_marked_containers_and_clears_marker(repo_root, monkeypatch):  # noqa: ARG001
    backup_manager._write_pause_marker("prod", ["cid1", "cid2"])
    started = []

    def fake_run(cmd, **_kwargs):
        started.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(backup_manager.subprocess, "run", fake_run)

    backup_manager.resume_orphaned_pause()

    assert started == [["docker", "start", "cid1"], ["docker", "start", "cid2"]]
    assert not os.path.exists(backup_manager.PAUSE_MARKER_FILE)


def test_resume_orphaned_pause_clears_marker_even_if_a_container_is_gone(repo_root, monkeypatch):  # noqa: ARG001
    """A stale container ID (e.g. it was recreated too) must not block cleanup
    or crash the caller -- log it and move on rather than retry forever.
    """
    backup_manager._write_pause_marker("prod", ["cid1"])

    def fake_run(cmd, **_kwargs):
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="No such container: cid1")

    monkeypatch.setattr(backup_manager.subprocess, "run", fake_run)

    backup_manager.resume_orphaned_pause()  # must not raise

    assert not os.path.exists(backup_manager.PAUSE_MARKER_FILE)
