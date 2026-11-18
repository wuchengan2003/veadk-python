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

"""End-to-end: HTTP API -> request clone -> ADK Runner -> sandbox data plane.

The sandbox control plane (CreateSession) is replaced by a resolver, but the
data plane is a real HTTP server that executes real shell commands and writes
real files. Every layer below the control plane is the production path: the
FastAPI routes, ``spawn_harness_agent``, ``EnvironmentToolset``, the generated
tool declarations, the AIO REST calls and the command/file round trip.
"""

from __future__ import annotations

import asyncio
import base64
import importlib
import json
import socket
import sys
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path
from types import ModuleType
from typing import Any
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.tools.environment import EnvironmentToolset
from google.genai import types

from veadk.cloud.harness_app.server_types import HARNESS_APP_NAME, HarnessConfig


class AioDataPlane:
    """AIO-compatible data plane backed by a real directory and real shell."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.app = self._build_app()
        self.url = ""

    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.post("/v1/bash/exec")
        async def bash_exec(payload: dict[str, Any]) -> dict[str, Any]:
            self.requests.append(("bash", payload))
            return await asyncio.to_thread(self._run_bash, payload)

        @app.post("/v1/file/read")
        async def file_read(payload: dict[str, Any]) -> dict[str, Any]:
            self.requests.append(("read", payload))
            return await asyncio.to_thread(self._read, payload)

        @app.post("/v1/file/write")
        async def file_write(payload: dict[str, Any]) -> dict[str, Any]:
            self.requests.append(("write", payload))
            return await asyncio.to_thread(self._write, payload)

        @app.get("/v1/sandbox")
        async def info() -> dict[str, Any]:
            self.requests.append(("info", {}))
            return {"status": "running"}

        return app

    def _run_bash(self, payload: dict[str, Any]) -> dict[str, Any]:
        import subprocess

        try:
            completed = subprocess.run(
                payload["command"],
                shell=True,
                cwd=payload.get("exec_dir") or str(self.root),
                capture_output=True,
                timeout=float(payload.get("timeout") or 30),
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {"data": {"status": "timed_out", "exit_code": -1}}
        return {
            "data": {
                "status": "completed",
                "exit_code": completed.returncode,
                "stdout": completed.stdout.decode(),
                "stderr": completed.stderr.decode(),
            }
        }

    def _read(self, payload: dict[str, Any]) -> dict[str, Any]:
        path = Path(payload["file"])
        if not path.is_file():
            return {
                "success": False,
                "message": f"Failed to read file: {path}",
                "data": {"error_type": "not_found", "errno_name": "ENOENT"},
            }
        return {"success": True, "data": {"content": path.read_text(encoding="utf-8")}}

    def _write(self, payload: dict[str, Any]) -> dict[str, Any]:
        path = Path(payload["file"])
        path.parent.mkdir(parents=True, exist_ok=True)
        if payload.get("encoding") == "base64":
            content = base64.b64decode(payload["content"])
        else:
            content = str(payload["content"]).encode()
        mode = "ab" if payload.get("append") else "wb"
        with path.open(mode) as handle:
            handle.write(content)
        return {
            "success": True,
            "data": {"file": str(path), "bytes_written": len(content)},
        }


class SandboxScriptModel(BaseLlm):
    """Deterministic model that drives the sandbox tools through ADK."""

    model: str = "sandbox-e2e"

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LlmResponse, None]:
        last = llm_request.contents[-1]
        responses = [
            part.function_response
            for part in last.parts
            if getattr(part, "function_response", None)
        ]
        if responses:
            response = responses[-1]
            if response.name == "WriteFile":
                yield self.call("Execute", {"command": "cat note.txt"})
            elif response.name == "Execute":
                yield self.call("ReadFile", {"path": "note.txt"})
            else:
                content = str((response.response or {}).get("content", ""))
                yield self.text(f"FINAL[{content}]")
            return

        prompt = str(last.parts[0].text or "")
        if prompt.startswith("read"):
            yield self.call("Execute", {"command": "cat note.txt"})
        else:
            yield self.call("WriteFile", {"path": "note.txt", "content": "hello-e2e"})

    @staticmethod
    def call(name: str, args: dict[str, Any]) -> LlmResponse:
        return LlmResponse(
            content=types.Content(
                role="model",
                parts=[
                    types.Part(function_call=types.FunctionCall(name=name, args=args))
                ],
            )
        )

    @staticmethod
    def text(value: str) -> LlmResponse:
        return LlmResponse(
            content=types.Content(role="model", parts=[types.Part(text=value)])
        )


@pytest.fixture
def harness_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    from veadk import Agent
    from veadk.memory.short_term_memory import ShortTermMemory

    monkeypatch.setenv("HARNESS_CRONJOB_ENABLED", "false")
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_required", lambda: True
    )
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_enabled", lambda: False
    )
    previous = sys.modules.pop("veadk.cloud.harness_app.server", None)
    module = importlib.import_module("veadk.cloud.harness_app.server")
    memory = ShortTermMemory(backend="local")
    module.agent = Agent(
        name=HARNESS_APP_NAME,
        model=SandboxScriptModel(),
        model_name="sandbox-e2e",
        model_provider="openai",
        model_api_key=uuid4().hex,
        short_term_memory=memory,
        tools=[],
        skills=[],
    )
    module.short_term_memory = memory
    module.harness_config = HarnessConfig(max_llm_calls=20)
    try:
        yield module
    finally:
        sys.modules.pop("veadk.cloud.harness_app.server", None)
        if previous is not None:
            sys.modules["veadk.cloud.harness_app.server"] = previous


@pytest.fixture
def api(harness_server: ModuleType) -> FastAPI:
    app = FastAPI()
    app.middleware("http")(harness_server.harness_run_sse_override)
    app.post("/harness/invoke")(harness_server.invoke_harness)
    return app


@pytest_asyncio.fixture
async def data_plane(tmp_path: Path) -> AsyncGenerator[AioDataPlane, None]:
    plane = AioDataPlane(tmp_path / "sandbox")
    plane.root.mkdir()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    sock.setblocking(False)
    plane.url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(
        uvicorn.Config(plane.app, log_level="error", lifespan="off")
    )
    serving = asyncio.create_task(server.serve(sockets=[sock]))
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.01)
    assert server.started
    try:
        yield plane
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, timeout=10)


def invoke_body(
    prompt: str, *, tool_id: str = "tool-e2e", session_id: str = "s-1"
) -> dict:
    return {
        "harness_name": "e2e",
        "prompt": prompt,
        "harness": {"sandbox": {"tool_id": tool_id}},
        "run_agent_request": {"user_id": "u-1", "session_id": session_id},
    }


@pytest.mark.asyncio
async def test_invoke_executes_real_commands_and_files(
    harness_server: ModuleType,
    api: FastAPI,
    data_plane: AioDataPlane,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setenv("HARNESS_SANDBOX_WORKSPACE", str(workspace))
    monkeypatch.delenv("HARNESS_SANDBOX_TOOL_ID", raising=False)
    resolutions: list[dict[str, Any]] = []

    def resolver(**kwargs: Any) -> str:
        resolutions.append(kwargs)
        return data_plane.url

    monkeypatch.setattr(
        "veadk.tools.builtin_tools._agentkit.ensure_agentkit_session_endpoint", resolver
    )
    closed: list[Any] = []
    original_close = EnvironmentToolset.close

    async def close(toolset) -> None:
        closed.append(toolset)
        await original_close(toolset)

    monkeypatch.setattr(EnvironmentToolset, "close", close)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://harness"
    ) as client:
        first = await client.post("/harness/invoke", json=invoke_body("write"))
        assert first.status_code == 200, first.text
        payload = first.json()
        assert payload["error"] is None
        assert "FINAL[" in payload["output"] and "hello-e2e" in payload["output"]

        # The command and the file travelled through the real data plane.
        assert (workspace / "note.txt").read_text() == "hello-e2e"
        kinds = [kind for kind, _ in data_plane.requests]
        assert kinds[:3] == ["write", "bash", "read"]
        assert data_plane.requests[1][1]["exec_dir"] == str(workspace)
        # Execute keeps ADK's fixed 30s budget, not the request/deployment value.
        assert data_plane.requests[1][1]["timeout"] == 30
        assert resolutions[0]["tool_id"] == "tool-e2e"
        assert resolutions[0]["tool_user_session_id"] == "harness_agent_u-1_s-1"

        second = await client.post("/harness/invoke", json=invoke_body("read"))
        assert second.status_code == 200, second.text
        assert "hello-e2e" in second.json()["output"]

        third = await client.post(
            "/harness/invoke", json=invoke_body("read", session_id="s-2")
        )
        assert third.status_code == 200, third.text
        assert resolutions[-1]["tool_user_session_id"] == "harness_agent_u-1_s-2"

        streamed = await client.post(
            "/run_sse",
            json={
                "app_name": "harness_agent",
                "user_id": "u-1",
                "session_id": "s-3",
                "new_message": {"role": "user", "parts": [{"text": "read"}]},
                "harness": {"sandbox": {"tool_id": "tool-e2e"}},
            },
        )
        assert streamed.status_code == 200
        events = [
            json.loads(frame.removeprefix("data: "))
            for frame in streamed.text.split("\n\n")
            if frame.startswith("data: ")
        ]
        texts = "".join(
            str(part.get("text", ""))
            for event in events
            for part in (event.get("content") or {}).get("parts", [])
        )
        assert "FINAL[" in texts and "hello-e2e" in texts

    # Every invocation (three /harness/invoke calls + one /run_sse) closed its
    # request-scoped toolset.
    assert len(closed) == 4


@pytest.mark.asyncio
async def test_invoke_rejects_an_invalid_sandbox_tool_id(
    harness_server: ModuleType, api: FastAPI
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://harness"
    ) as client:
        response = await client.post(
            "/harness/invoke", json=invoke_body("write", tool_id="not a tool id")
        )

    assert response.status_code == 422
    assert response.json()["detail"]


@pytest.mark.asyncio
async def test_run_sse_returns_422_instead_of_500(
    harness_server: ModuleType, api: FastAPI
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api), base_url="http://harness"
    ) as client:
        response = await client.post(
            "/run_sse",
            json={
                "app_name": "harness_agent",
                "user_id": "u-1",
                "session_id": "s-1",
                "new_message": {"role": "user", "parts": [{"text": "hi"}]},
                "harness": {"knowledgebase": {"type": "pinecone"}},
            },
        )

    assert response.status_code == 422
    assert response.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("request_fields", [{}, {"harness": None}, {"harness": {}}])
async def test_default_sandbox_run_sse_executes_tools(
    harness_server: ModuleType,
    api: FastAPI,
    data_plane: AioDataPlane,
    monkeypatch: pytest.MonkeyPatch,
    request_fields: dict[str, Any],
) -> None:
    """没有请求级配置时仍使用部署默认沙箱，并释放请求级资源。"""
    monkeypatch.setenv("HARNESS_SANDBOX_TOOL_ID", "tool-default")
    monkeypatch.setenv("HARNESS_SANDBOX_WORKSPACE", str(data_plane.root))
    resolutions: list[dict[str, Any]] = []

    def resolver(**kwargs: Any) -> str:
        resolutions.append(kwargs)
        return data_plane.url

    monkeypatch.setattr(
        "veadk.tools.builtin_tools._agentkit.ensure_agentkit_session_endpoint",
        resolver,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=api),
        base_url="http://harness",
    ) as client:
        response = await client.post(
            "/run_sse",
            json={
                "app_name": HARNESS_APP_NAME,
                "user_id": "default-user",
                "session_id": "default-session",
                "streaming": True,
                "new_message": {"role": "user", "parts": [{"text": "write"}]},
                **request_fields,
            },
        )
    assert response.status_code == 200, response.text
    events = [
        json.loads(line[6:])
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]
    assert not any("error" in event for event in events), events
    parts = [
        part
        for event in events
        for part in (event.get("content") or {}).get("parts", [])
    ]
    names = {
        part["functionResponse"]["name"] for part in parts if "functionResponse" in part
    }
    assert {"WriteFile", "Execute", "ReadFile"} <= names
    assert (data_plane.root / "note.txt").read_text() == "hello-e2e"
    assert resolutions[0]["tool_id"] == "tool-default"
    assert (
        resolutions[0]["tool_user_session_id"]
        == "harness_agent_default-user_default-session"
    )
    assert not any(
        isinstance(tool, EnvironmentToolset) for tool in harness_server.agent.tools
    )
