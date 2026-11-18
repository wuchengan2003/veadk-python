"""Offline smoke check of the installed Harness HTTP application."""

import os

# This runs only during image construction, without cloud credentials or models.
os.environ.update(
    MODEL_NAME="build-smoke-model",
    MODEL_API_KEY="build-smoke-placeholder",
    MODEL_AGENT_API_KEY="build-smoke-placeholder",
    HARNESS_NAME="build-smoke",
    SHORT_TERM_MEMORY_TYPE="local",
)

from fastapi.testclient import TestClient  # noqa: E402

from veadk.cloud.harness_app.server import app  # noqa: E402

with TestClient(app) as client:
    health = client.get("/health")
    assert health.status_code == 200 and health.json() == {"status": "ok"}
    apps = client.get("/list-apps")
    assert apps.status_code == 200 and apps.json() == ["harness_agent"]
    app_name = apps.json()[0]
    sessions = f"/apps/{app_name}/users/build-smoke/sessions"
    created = client.post(sessions, json={"id": "build-smoke-session"})
    assert created.status_code == 200, created.text
    session_id = created.json()["id"]
    assert client.get(f"{sessions}/{session_id}").status_code == 200
    assert client.delete(f"{sessions}/{session_id}").status_code in (200, 204)
    paths = client.get("/openapi.json").json()["paths"]
    assert all(path in paths for path in ("/run_sse", "/harness/invoke", "/health"))
print("Harness HTTP startup, health, sessions and invocation routes verified")
