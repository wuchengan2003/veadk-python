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

"""Real image HTTP acceptance, reusing the source sandbox write/read scenario.

Only external model, AgentKit control plane and OpenViking protocols are fixtures.
The image's default command, Server, Agent, Runner and tools are never replaced.
Run explicitly with HARNESS_E2E_IMAGE set to a locally built image.
"""

import asyncio
from contextlib import contextmanager
import json
import os
import socket
import subprocess
import threading
import time
from uuid import uuid4

import httpx
import pytest
import uvicorn
from fastapi import Request
from fastapi.responses import StreamingResponse

from tests.cloud.cli_harness.test_harness_sandbox_e2e import AioDataPlane

MEMORY_TEXT = "marker=write-memory-tool"


def docker(*args, check=True):
    return subprocess.run(
        ["docker", *args], check=check, text=True, capture_output=True, timeout=120
    )


def eventually(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for the observed effect")


class Protocols(AioDataPlane):
    """The original AIO file/command fixture, plus public HTTP protocol boundaries."""

    def __init__(self, root):
        super().__init__(root)
        self.memory_requests = []
        self.model_requests = []
        self.sessions = {}
        self.streams = {}
        self.app.api_route("/", methods=["GET", "POST"])(self.control_plane)
        self.app.post("/v1/chat/completions")(self.model)
        self.app.get("/health")(lambda: {"status": "ok"})
        self.app.post("/api/v1/sessions")(self.memory)
        self.app.post("/api/v1/sessions/{tail:path}")(self.memory)

    async def control_plane(self, request: Request):
        action = request.query_params["Action"]
        body = (
            await request.json()
            if request.method == "POST"
            else dict(request.query_params)
        )
        if action == "ListSessions":
            result = {
                "SessionInfos": list(self.sessions.values()),
                "TotalCount": len(self.sessions),
            }
        elif action == "CreateSession":
            result = {
                "SessionId": "sandbox-" + uuid4().hex,
                "UserSessionId": body["UserSessionId"],
                "Endpoint": self.url,
                "InternalEndpoint": self.url,
                "Status": "Ready",
            }
            self.sessions[result["SessionId"]] = result
        elif action == "GetSession":
            result = self.sessions[body["SessionId"]]
        else:
            raise AssertionError(f"Unexpected AgentKit action: {action}")
        return {"ResponseMetadata": {"RequestId": "fixture"}, "Result": result}

    async def memory(self, request: Request):
        body = await request.json()
        self.memory_requests.append((request.url.path, body))
        return {"status": "ok", "result": body}

    async def model(self, request: Request):
        body = await request.json()
        self.model_requests.append(body)
        messages = body["messages"]
        current = max(
            i for i, message in enumerate(messages) if message["role"] == "user"
        )
        prompt = messages[current]["content"]
        if isinstance(prompt, list):
            prompt = "".join(part.get("text", "") for part in prompt)
        responses = [
            message for message in messages[current + 1 :] if message["role"] == "tool"
        ]
        if prompt.startswith("stream-"):
            return StreamingResponse(
                self.model_stream(prompt), media_type="text/event-stream"
            )
        if prompt == "write":
            steps = [
                ("WriteFile", {"path": "note.txt", "content": "hello-e2e"}),
                ("Execute", {"command": "cat note.txt"}),
                ("ReadFile", {"path": "note.txt"}),
            ]
        elif prompt == "remember":
            steps = [("write_memory", {"memory": MEMORY_TEXT})]
        else:
            steps = [("ReadFile", {"path": "note.txt"})]
        message = {"role": "assistant", "content": None}
        if len(responses) < len(steps):
            name, arguments = steps[len(responses)]
            # Also prove this tool was offered by the real Agent assembly.
            assert name in {tool["function"]["name"] for tool in body["tools"]}
            message["tool_calls"] = [
                {
                    "id": "call_" + uuid4().hex,
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(arguments)},
                }
            ]
            finish_reason = "tool_calls"
        else:
            message["content"] = "FINAL[" + responses[-1]["content"] + "]"
            finish_reason = "stop"
        return {
            "id": "chatcmpl-fixture",
            "object": "chat.completion",
            "created": 1,
            "model": body["model"],
            "choices": [
                {"index": 0, "message": message, "finish_reason": finish_reason}
            ],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }

    async def model_stream(self, prompt):
        release, closed = threading.Event(), threading.Event()
        self.streams[prompt] = (release, closed)

        def chunk(content, finish=None):
            return (
                "data: "
                + json.dumps(
                    {
                        "id": "chatcmpl-stream",
                        "object": "chat.completion.chunk",
                        "created": 1,
                        "model": "image-e2e",
                        "choices": [
                            {
                                "index": 0,
                                "delta": {"content": content},
                                "finish_reason": finish,
                            }
                        ],
                    },
                    ensure_ascii=False,
                )
                + "\n\n"
            )

        try:
            yield chunk("甲")
            while not release.is_set():
                await asyncio.sleep(0.02)
            yield chunk("乙")
            yield chunk("", "stop")
            yield "data: [DONE]\n\n"
        finally:
            closed.set()


@pytest.fixture(scope="module")
def protocols(tmp_path_factory):
    root = tmp_path_factory.mktemp("image-sandbox")
    fixture = Protocols(root)
    sock = socket.socket()
    sock.bind(("0.0.0.0", 0))
    port = sock.getsockname()[1]
    fixture.url = f"http://host.docker.internal:{port}"
    server = uvicorn.Server(
        uvicorn.Config(fixture.app, log_level="warning", access_log=False)
    )
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True
    )
    thread.start()
    eventually(lambda: server.started)
    try:
        yield fixture
    finally:
        for release, _ in fixture.streams.values():
            release.set()
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


@contextmanager
def running_image(image, env):
    args = [
        "run",
        "--detach",
        "--name",
        "veadk-harness-e2e-" + uuid4().hex[:12],
        "--add-host",
        "host.docker.internal:host-gateway",
        "--publish",
        "127.0.0.1::8000",
    ]
    for key, value in env.items():
        args.extend(["--env", f"{key}={value}"])
    container = docker(*args, image).stdout.strip()
    try:
        inspect = json.loads(docker("inspect", container).stdout)[0]
        port = inspect["NetworkSettings"]["Ports"]["8000/tcp"][0]["HostPort"]
        with httpx.Client(
            base_url=f"http://127.0.0.1:{port}", timeout=60, trust_env=False
        ) as client:

            def ready():
                try:
                    return client.get("/health").status_code == 200
                except httpx.TransportError:
                    return False

            try:
                eventually(ready, timeout=90)
            except AssertionError:
                pytest.fail(
                    docker("logs", container).stdout + docker("logs", container).stderr
                )
            yield client, container
    finally:
        docker("stop", "--time", "40", container, check=False)
        state = json.loads(docker("inspect", container).stdout)[0]["State"]
        docker("rm", "--force", container, check=False)
        assert not state["Running"] and state["ExitCode"] != 137, state


@pytest.fixture(scope="module")
def image_server(protocols):
    image = os.environ.get("HARNESS_E2E_IMAGE")
    if not image:
        pytest.fail(
            "Set HARNESS_E2E_IMAGE to the image built by docker/harness-runtime/build.py"
        )
    env = {
        "MODEL_NAME": "image-e2e",
        "MODEL_PROVIDER": "openai",
        "MODEL_API_KEY": "fixture-key",
        "MODEL_API_BASE": protocols.url + "/v1",
        "HARNESS_CRONJOB_ENABLED": "false",
        "HARNESS_SANDBOX_WORKSPACE": str(protocols.root),
        "HARNESS_SANDBOX_TOOL_ID": "tool-e2e",
        "VOLCENGINE_ACCESS_KEY": "fixture-access",
        "VOLCENGINE_SECRET_KEY": "fixture-secret",
        "VOLCENGINE_AGENTKIT_HOST": protocols.url.removeprefix("http://"),
        "VOLCENGINE_AGENTKIT_SCHEME": "http",
        "LONG_TERM_MEMORY_TYPE": "openviking",
        "DATABASE_OPENVIKING_URL": protocols.url,
        "DATABASE_OPENVIKING_API_KEY": "fixture-key",
        "DATABASE_OPENVIKING_USER_ID": "default",
        "SHORT_TERM_MEMORY_TYPE": "local",
    }
    with running_image(image, env) as server:
        yield server


def invoke(client, prompt, session="s-1"):
    response = client.post(
        "/harness/invoke",
        json={
            "harness_name": "image-e2e",
            "prompt": prompt,
            "run_agent_request": {"user_id": "alice", "session_id": session},
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert not payload["error"], payload
    return payload


def test_file_task_and_followup_use_real_tools(image_server, protocols):
    client, _ = image_server
    first = invoke(client, "write")
    assert "hello-e2e" in first["output"]
    assert (protocols.root / "note.txt").read_text() == "hello-e2e"
    assert [kind for kind, _ in protocols.requests if kind != "info"][:3] == [
        "write",
        "bash",
        "read",
    ]
    assert "hello-e2e" in invoke(client, "read")["output"]
    sessions = [
        session
        for session in protocols.sessions.values()
        if session["UserSessionId"] == "harness_agent_alice_s-1"
    ]
    assert len(sessions) == 1


def test_explicit_memory_reaches_backend_with_request_identity(image_server, protocols):
    client, _ = image_server
    assert "Memory saved" in invoke(client, "remember", "memory-session")["output"]
    writes = [
        (path, body)
        for path, body in protocols.memory_requests
        if path.endswith("/messages") and body.get("content") == MEMORY_TEXT
    ]
    assert len(writes) == 1, protocols.memory_requests
    path, body = writes[0]
    assert body["peer_id"] == "alice"
    assert "harness_agent" in path and "memory-session" in path
    assert any(
        p == path.removesuffix("/messages") + "/commit"
        for p, _ in protocols.memory_requests
    )


@pytest.mark.parametrize("cancel", [False, True], ids=["complete", "disconnect"])
def test_stream_is_incremental_and_disconnect_releases_model(
    image_server, protocols, cancel
):
    client, container = image_server
    prompt = "stream-cancel" if cancel else "stream-complete"
    body = {
        "app_name": "harness_agent",
        "user_id": "alice",
        "session_id": prompt,
        "streaming": True,
        "new_message": {"role": "user", "parts": [{"text": prompt}]},
    }
    with client.stream("POST", "/run_sse", json=body) as response:
        assert response.status_code == 200
        lines = response.iter_lines()
        first = next(line for line in lines if "甲" in line or "\\u7532" in line)
        assert first.startswith("data:")
        release, closed = protocols.streams[prompt]
        assert not release.is_set() and not closed.is_set()
        if not cancel:
            release.set()
            remaining = "\n".join(lines)
            assert "乙" in remaining or "\\u4e59" in remaining
    eventually(closed.is_set)
    # A subsequent real request must still execute after either termination path.
    assert "hello-e2e" in invoke(client, "write", prompt + "-followup")["output"]
    listing = docker(
        "exec",
        container,
        "/opt/veadk/.venv/bin/python",
        "-c",
        "from pathlib import Path; print(list(Path('/tmp').glob('agentkit_harness_*')))",
    )
    assert listing.stdout.strip() == "[]"


def test_image_uses_installed_wheel_and_framework_contract(image_server):
    client, container = image_server
    info = json.loads(docker("inspect", container).stdout)[0]
    loaded = docker(
        "exec",
        container,
        "/opt/veadk/.venv/bin/python",
        "-c",
        "import veadk,sys; print(sys.executable); print(veadk.__file__)",
    ).stdout
    assert "/opt/veadk/.venv/bin/python" in loaded and "/site-packages/veadk/" in loaded
    assert info["Mounts"] == []
    is_framework = "tini" in " ".join(info["Config"]["Cmd"])
    if is_framework:
        response = client.get("/health", headers={"X-AgentKit-Target": "studio"})
        assert response.status_code == 503 and "Studio Harness 未启用" in response.text
        for flag in ("STUDIO_ENABLED", "MANAGED_AGENT_WORKER_ENABLED"):
            result = docker(
                "run",
                "--rm",
                "--env",
                f"{flag}=true",
                info["Config"]["Image"],
                check=False,
            )
            assert result.returncode != 0
            assert "not implemented" in result.stderr
            assert "started" not in result.stdout
