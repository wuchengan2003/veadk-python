"""Launch the enabled services in the unified Harness Runtime image."""

import importlib.util
import math
import os
import sys
from pathlib import Path

FLAGS = {
    "harness": ("HARNESS_APP_ENABLED", "true"),
    "studio": ("STUDIO_ENABLED", "true"),
    "worker": ("MANAGED_AGENT_WORKER_ENABLED", "false"),
}
GENERATED_NGINX_CONFIG = Path("/tmp/harness-runtime-nginx.conf")


def parse_enabled(environ):
    enabled = {}
    for component, (name, default) in FLAGS.items():
        value = environ.get(name, default).strip().lower()
        if value not in {"true", "false"}:
            raise ValueError(f"{name} must be true or false")
        enabled[component] = value == "true"
    if not any(enabled.values()):
        raise ValueError("At least one Harness Runtime component must be enabled")
    return enabled


def render_nginx_config(template, destination, enabled):
    content = Path(template).read_text()
    replacements = {
        "@@HARNESS_DISABLED@@": "0" if enabled["harness"] else "1",
        "@@STUDIO_DISABLED@@": "0" if enabled["studio"] else "1",
    }
    for marker, value in replacements.items():
        if content.count(marker) != 1:
            raise ValueError(f"Nginx template must contain exactly one {marker}")
        content = content.replace(marker, value)
    Path(destination).write_text(content)


def services(environ, nginx_config=GENERATED_NGINX_CONFIG):
    enabled = parse_enabled(environ)
    app_env = dict(environ)
    for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"):
        app_env.pop(key, None)
    app_env.update(
        PATH="/opt/veadk/.venv/bin:" + app_env.get("PATH", os.defpath),
        VIRTUAL_ENV="/opt/veadk/.venv",
        UV_SYSTEM_PYTHON="false",
    )
    entries = []
    if enabled["harness"]:
        entries.append(
            (
                "harness-app-server",
                [
                    "/opt/veadk/.venv/bin/python",
                    "-m",
                    "uvicorn",
                    "veadk.cloud.harness_app.server:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8001",
                    "--proxy-headers",
                    "--forwarded-allow-ips",
                    "127.0.0.1",
                ],
                "/opt/veadk",
                app_env,
            )
        )
    if enabled["worker"]:
        worker_env = dict(
            app_env,
            MANAGED_AGENTS_PYTHON="/opt/veadk/.venv/bin/python",
            MANAGED_AGENT_HEALTH_ENABLED="false",
            MANAGED_AGENT_ENTRYPOINT_MODE="worker",
        )
        worker_env.setdefault("MANAGED_AGENT_WORKDIR", "/workspace")
        entries.append(
            (
                "veadk-agent-loop",
                ["/bin/bash", "/opt/veadk/run-runtime.sh"],
                "/opt/veadk",
                worker_env,
            )
        )
    if enabled["studio"]:
        studio_env = dict(
            app_env,
            PATH="/opt/studio/.venv/bin:/opt/agentkit-headroom/bin:"
            + app_env.get("PATH", os.defpath),
            VIRTUAL_ENV="/opt/studio/.venv",
            PYTHONPATH="/opt/studio",
            HOME="/opt/studio/home",
            PYTHONUNBUFFERED="1",
            PYTHONDONTWRITEBYTECODE="1",
            PIAGENT_BINARY="/opt/piagent/pi/pi",
            TIKTOKEN_CACHE_DIR="/opt/agentkit-headroom/tiktoken-cache",
        )
        entries.append(
            (
                "studio-harness",
                [
                    "/usr/bin/setpriv",
                    "--reuid=10001",
                    "--regid=10001",
                    "--clear-groups",
                    "/opt/studio/.venv/bin/python",
                    "-m",
                    "uvicorn",
                    "harness_app_server.app:create_app",
                    "--factory",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    "8002",
                    "--proxy-headers",
                    "--forwarded-allow-ips",
                    "127.0.0.1",
                    "--no-access-log",
                ],
                "/opt/studio",
                studio_env,
            )
        )
    if enabled["harness"] or enabled["studio"]:
        entries.append(
            (
                "runtime-proxy",
                [
                    "/usr/sbin/nginx",
                    "-c",
                    str(nginx_config),
                    "-g",
                    "daemon off;",
                ],
                "/app",
                app_env,
            )
        )
    return entries


def main():
    if len(sys.argv) != 1:
        raise ValueError("The Harness Runtime entrypoint does not accept arguments")
    grace = float(os.environ.get("DUAL_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS", "30"))
    if not math.isfinite(grace) or grace <= 0:
        raise ValueError("DUAL_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS must be positive")
    enabled = parse_enabled(os.environ)
    if enabled["harness"] or enabled["studio"]:
        directory = Path(__file__).resolve().parent
        render_nginx_config(directory / "nginx.conf", GENERATED_NGINX_CONFIG, enabled)
    path = Path(__file__).resolve().parent / "supervise.py"
    spec = importlib.util.spec_from_file_location("harness_runtime_supervisor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.supervise(services(os.environ), grace)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as exc:
        print(f"harness-runtime: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
