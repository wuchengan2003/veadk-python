"""Check every enabled Harness Runtime component without external dependencies."""

import os
import sys
import urllib.request
from pathlib import Path


def enabled(name, default):
    value = os.environ.get(name, default).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


def request(path, headers=None):
    request = urllib.request.Request(
        f"http://127.0.0.1:8000{path}", headers=headers or {}
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        if response.status != 200:
            raise RuntimeError(f"{path} returned HTTP {response.status}")


def main():
    harness = enabled("HARNESS_APP_ENABLED", "true")
    studio = enabled("STUDIO_ENABLED", "true")
    worker = enabled("MANAGED_AGENT_WORKER_ENABLED", "false")
    if not any((harness, studio, worker)):
        raise ValueError("At least one Harness Runtime component must be enabled")
    if harness:
        request("/health")
    if studio:
        request("/ping", {"X-AgentKit-Target": "studio"})
    if worker:
        ready = Path(
            os.environ.get(
                "MANAGED_AGENT_READY_FILE", "/tmp/managed-agent-worker-ready"
            )
        )
        if not ready.is_file() or not ready.read_text().strip():
            raise RuntimeError("MA Worker is not ready")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"harness-runtime healthcheck: {exc}", file=sys.stderr)
        sys.exit(1)
