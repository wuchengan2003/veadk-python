# Copyright (c) 2025 Beijing Volcano Engine Technology Co., Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Offline checks for source locking and the unified runtime contract."""

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


build = load("runtime_build", HERE / "build.py")
prepare_studio = build.prepare_studio
healthcheck = load("runtime_healthcheck", HERE / "healthcheck.py")
launch = load("runtime_launch", HERE / "launch.py")


@pytest.mark.parametrize("value", ["main", "abc123", "a" * 39, "A" * 40, "a" * 41])
def test_source_lock_requires_full_commit(value):
    with pytest.raises(ValueError):
        build.commit_sha(value)


@pytest.mark.parametrize(
    "value", ["base:latest", "base@sha256:abc", "base@sha256:" + "A" * 64]
)
def test_base_requires_digest(value):
    with pytest.raises(ValueError):
        build.pinned_image(value)


def test_archive_uses_commit_not_worktree(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    build.git(repo, "config", "user.name", "Test")
    build.git(repo, "config", "user.email", "test@example.invalid")
    (repo / "service.py").write_text("committed\n")
    build.git(repo, "add", "service.py")
    build.git(repo, "commit", "--quiet", "-m", "fixture")
    revision = build.git(repo, "rev-parse", "HEAD")
    (repo / "service.py").write_text("dirty\n")
    (repo / ".env").write_text("untracked\n")
    output = tmp_path / "export"
    build.export_commit(repo, revision, output)
    assert (output / "service.py").read_text() == "committed\n"
    assert not (output / ".env").exists()
    assert not (output / ".git").exists()


def test_build_records_both_revisions_and_only_loads_locally():
    image = "example.invalid/base@sha256:" + "b" * 64
    command = build.build_command(
        image,
        "a" * 40,
        "c" * 40,
        "test:local",
        studio_repository="https://example.invalid/studio.git",
        studio_ref="main",
    )
    assert f"PYTHON_IMAGE={image}" in command
    assert f"VCS_REF={'a' * 40}" in command
    assert f"STUDIO_REVISION={'c' * 40}" in command
    assert "STUDIO_SOURCE=https://example.invalid/studio.git" in command
    assert "STUDIO_REF=main" in command
    assert command[-2:] == ["--load", "."]
    assert "--push" not in command


def test_prepare_studio_fetches_configured_ref_without_changing_cache(tmp_path):
    studio = tmp_path / "studio"
    subprocess.run(["git", "init", "--quiet", str(studio)], check=True)
    build.git(studio, "config", "user.name", "Test")
    build.git(studio, "config", "user.email", "test@example.invalid")
    build.git(studio, "config", "commit.gpgsign", "false")
    build.git(studio, "config", "uploadpack.allowReachableSHA1InWant", "true")
    requirements = studio / "servers/harness_app_server/requirements.lock"
    requirements.parent.mkdir(parents=True)
    requirements.write_text("first\n")
    build.git(studio, "add", ".")
    build.git(studio, "commit", "--quiet", "-m", "first")
    first_sha = build.git(studio, "rev-parse", "HEAD")

    cache = tmp_path / "cache"
    subprocess.run(["git", "clone", "--quiet", str(studio), str(cache)], check=True)
    cached_requirements = cache / requirements.relative_to(studio)
    cached_requirements.write_text("developer edit\n")
    cache_status = build.git(cache, "status", "--porcelain")

    requirements.write_text("latest\n")
    build.git(studio, "commit", "--quiet", "-am", "latest")
    latest_sha = build.git(studio, "rev-parse", "HEAD")

    latest_output = tmp_path / "latest"
    assert (
        prepare_studio.prepare(latest_output, cache, str(studio), "HEAD") == latest_sha
    )
    assert (latest_output / "requirements.lock").read_text() == "latest\n"

    pinned_output = tmp_path / "pinned"
    assert (
        prepare_studio.prepare(pinned_output, cache, str(studio), first_sha)
        == first_sha
    )
    assert (pinned_output / "requirements.lock").read_text() == "first\n"
    assert build.git(cache, "status", "--porcelain") == cache_status
    assert cached_requirements.read_text() == "developer edit\n"


def test_build_can_export_pipeline_tarball(tmp_path):
    image = "example.invalid/base@sha256:" + "b" * 64
    tarball = tmp_path / "runtime.tar"
    command = build.build_command(image, "a" * 40, "c" * 40, "test:ci", tarball)
    assert f"type=docker,dest={tarball}" in command
    assert "--load" not in command
    assert command[-1] == "."


@pytest.mark.parametrize(
    "harness,studio,worker,names",
    [
        (True, False, False, ["harness-app-server", "runtime-proxy"]),
        (False, True, False, ["studio-harness", "runtime-proxy"]),
        (True, True, False, ["harness-app-server", "studio-harness", "runtime-proxy"]),
        (
            True,
            False,
            True,
            ["harness-app-server", "veadk-agent-loop", "runtime-proxy"],
        ),
        (False, True, True, ["veadk-agent-loop", "studio-harness", "runtime-proxy"]),
        (
            True,
            True,
            True,
            [
                "harness-app-server",
                "veadk-agent-loop",
                "studio-harness",
                "runtime-proxy",
            ],
        ),
        (False, False, True, ["veadk-agent-loop"]),
    ],
)
def test_component_switch_matrix(harness, studio, worker, names):
    env = {
        "HARNESS_APP_ENABLED": str(harness),
        "STUDIO_ENABLED": str(studio),
        "MANAGED_AGENT_WORKER_ENABLED": str(worker),
    }
    assert [entry[0] for entry in launch.services(env)] == names


def test_default_components_enable_both_http_services_only():
    assert [entry[0] for entry in launch.services({})] == [
        "harness-app-server",
        "studio-harness",
        "runtime-proxy",
    ]


@pytest.mark.parametrize("name", [item[0] for item in launch.FLAGS.values()])
@pytest.mark.parametrize("value", ["", "1", "0", "enabled", "flase"])
def test_component_flags_reject_invalid_values(name, value):
    with pytest.raises(ValueError, match=f"{name} must be true or false"):
        launch.services({name: value})


def test_all_components_disabled_fails_startup():
    with pytest.raises(ValueError, match="At least one"):
        launch.services(
            {
                "HARNESS_APP_ENABLED": "false",
                "STUDIO_ENABLED": "false",
                "MANAGED_AGENT_WORKER_ENABLED": "false",
            }
        )


@pytest.mark.parametrize(
    "harness,studio,expected",
    [(True, True, ("0", "0")), (True, False, ("0", "1")), (False, True, ("1", "0"))],
)
def test_nginx_routes_disabled_services_to_503(tmp_path, harness, studio, expected):
    output = tmp_path / "nginx.conf"
    launch.render_nginx_config(
        HERE / "nginx.conf", output, {"harness": harness, "studio": studio}
    )
    content = output.read_text()
    assert "@@" not in content
    assert f"default {expected[0]};" in content
    assert f"studio {expected[1]};" in content
    assert "return 503" in content


def test_healthcheck_checks_default_http_services(monkeypatch):
    calls = []
    monkeypatch.setattr(
        healthcheck, "request", lambda path, headers=None: calls.append((path, headers))
    )
    monkeypatch.delenv("HARNESS_APP_ENABLED", raising=False)
    monkeypatch.delenv("STUDIO_ENABLED", raising=False)
    monkeypatch.delenv("MANAGED_AGENT_WORKER_ENABLED", raising=False)
    assert healthcheck.main() == 0
    assert calls == [
        ("/health", None),
        ("/ping", {"X-AgentKit-Target": "studio"}),
    ]


def test_healthcheck_supports_worker_only(monkeypatch, tmp_path):
    ready = tmp_path / "ready"
    ready.write_text("worker-id\n")
    monkeypatch.setenv("HARNESS_APP_ENABLED", "false")
    monkeypatch.setenv("STUDIO_ENABLED", "false")
    monkeypatch.setenv("MANAGED_AGENT_WORKER_ENABLED", "true")
    monkeypatch.setenv("MANAGED_AGENT_READY_FILE", str(ready))
    monkeypatch.setattr(
        healthcheck,
        "request",
        lambda *_args, **_kwargs: pytest.fail("worker-only must not probe HTTP"),
    )
    assert healthcheck.main() == 0


def test_healthcheck_rejects_unready_worker(monkeypatch, tmp_path):
    monkeypatch.setenv("HARNESS_APP_ENABLED", "false")
    monkeypatch.setenv("STUDIO_ENABLED", "false")
    monkeypatch.setenv("MANAGED_AGENT_WORKER_ENABLED", "true")
    monkeypatch.setenv("MANAGED_AGENT_READY_FILE", str(tmp_path / "missing"))
    with pytest.raises(RuntimeError, match="MA Worker is not ready"):
        healthcheck.main()


def test_runtime_keeps_paths_packages_and_listeners_separate():
    env = {
        "PATH": "/usr/bin",
        "PYTHONPATH": "/foreign",
        "PYTHONHOME": "/foreign",
        "VIRTUAL_ENV": "/foreign",
        "MANAGED_AGENT_HEALTH_ENABLED": "true",
        "MANAGED_AGENT_ENTRYPOINT_MODE": "claimed",
        "MANAGED_AGENT_WORKER_ENABLED": "true",
        "APPLICATION_SETTING": "retained",
    }
    app, worker, studio, proxy = launch.services(env)
    assert len({entry[0] for entry in (app, worker, studio, proxy)}) == 4
    for entry, port in ((app, "8001"), (studio, "8002")):
        assert entry[1][entry[1].index("--host") + 1] == "127.0.0.1"
        assert entry[1][entry[1].index("--port") + 1] == port
        assert entry[3]["APPLICATION_SETTING"] == "retained"
        assert "PYTHONHOME" not in entry[3]
    assert app[1][0] == "/opt/veadk/.venv/bin/python"
    assert "veadk.cloud.harness_app.server:app" in app[1]
    assert app[2] == "/opt/veadk"
    assert app[3]["VIRTUAL_ENV"] == "/opt/veadk/.venv"
    assert app[3]["UV_SYSTEM_PYTHON"] == "false"
    assert "PYTHONPATH" not in app[3]
    assert "PYTHONPATH" not in worker[3]
    assert worker[3]["MANAGED_AGENT_HEALTH_ENABLED"] == "false"
    assert worker[3]["MANAGED_AGENT_ENTRYPOINT_MODE"] == "worker"
    assert worker[3]["MANAGED_AGENTS_PYTHON"] == "/opt/veadk/.venv/bin/python"
    assert studio[3]["VIRTUAL_ENV"] == "/opt/studio/.venv"
    assert studio[3]["PYTHONPATH"] == "/opt/studio"
    assert studio[1][0:4] == [
        "/usr/bin/setpriv",
        "--reuid=10001",
        "--regid=10001",
        "--clear-groups",
    ]
    assert "harness_app_server.app:create_app" in studio[1]
    assert "--root-path" not in studio[1]
    assert "--factory" in studio[1]
    assert "daemon off;" in proxy[1]
    assert env["VIRTUAL_ENV"] == "/foreign"


@pytest.mark.parametrize(
    "worker_enabled,role",
    [
        (enabled, role)
        for enabled in (False, True)
        for role in ("harness", "worker", "studio", "proxy", "sigterm", "sigint")
        if enabled or role != "worker"
    ],
)
def test_process_groups_stop_on_exit_or_signal(tmp_path, worker_enabled, role):
    roles = tuple(
        name
        for name in ("harness", "worker", "studio", "proxy")
        if worker_enabled or name != "worker"
    )
    child = tmp_path / "child.py"
    child.write_text(
        "import os,signal,sys,time\nfrom pathlib import Path\n"
        "root=Path(sys.argv[1]); role=sys.argv[2]\n"
        "signal.signal(signal.SIGTERM,lambda *args: sys.exit(0))\n"
        "(root/(role+'.pid')).write_text(str(os.getpid()))\n"
        "while not (root/(role+'.exit')).exists(): time.sleep(.01)\n"
    )
    entries = [
        (name, [sys.executable, str(child), str(tmp_path), name], str(tmp_path), {})
        for name in roles
    ]
    runner = (
        "import importlib.util,json,sys; "
        "s=importlib.util.spec_from_file_location('supervisor',sys.argv[1]); "
        "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
        "sys.exit(m.supervise(json.loads(sys.argv[2]),0.3))"
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            runner,
            str(HERE / "supervise.py"),
            json.dumps(entries),
        ]
    )
    try:
        deadline = time.monotonic() + 10
        while not all((tmp_path / f"{name}.pid").exists() for name in roles):
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.02)
        if role.startswith("sig"):
            signum = signal.SIGTERM if role == "sigterm" else signal.SIGINT
            process.send_signal(signum)
            expected = 128 + signum
        else:
            (tmp_path / f"{role}.exit").touch()
            expected = 1
        assert process.wait(timeout=5) == expected
        for name in roles:
            with pytest.raises(ProcessLookupError):
                os.kill(int((tmp_path / f"{name}.pid").read_text()), 0)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        for name in roles:
            pidfile = tmp_path / f"{name}.pid"
            if pidfile.exists():
                try:
                    os.killpg(int(pidfile.read_text()), signal.SIGKILL)
                except ProcessLookupError:
                    pass
