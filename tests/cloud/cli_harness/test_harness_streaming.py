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

"""真实 Runner 的增量事件必须在整次执行完成前送到响应迭代器。"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import threading
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock
from uuid import uuid4

import anyio
import pytest
from fastapi import FastAPI, Request, Response
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import PrivateAttr

from veadk.cloud.harness_app.server_types import (
    HARNESS_APP_NAME,
    HarnessConfig,
    InvokeHarnessRequest,
)
from veadk.cloud.harness_app.session_metadata import (
    SessionLifecyclePlugin,
    SessionMetadataStore,
    install_session_service_metadata_hooks,
)


class ControlledModel(BaseLlm):
    model: str = "controlled-model"
    fail: bool = False
    _gate: asyncio.Event = PrivateAttr(default_factory=asyncio.Event)
    _closed: asyncio.Event = PrivateAttr(default_factory=asyncio.Event)
    _stream_modes: list[bool] = PrivateAttr(default_factory=list)

    async def generate_content_async(
        self,
        llm_request: LlmRequest,
        stream: bool = False,
    ) -> AsyncGenerator[LlmResponse, None]:
        self._stream_modes.append(stream)
        try:
            if stream:
                yield self.response("甲", partial=True)
                await self._gate.wait()
                if self.fail:
                    raise RuntimeError("controlled model failure")
                yield self.response("乙", partial=True)
            yield self.response("甲乙", partial=False)
        finally:
            self._closed.set()

    @staticmethod
    def response(text: str, *, partial: bool) -> LlmResponse:
        return LlmResponse(
            content=types.Content(role="model", parts=[types.Part(text=text)]),
            partial=partial,
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
        model=ControlledModel(),
        model_name="controlled-model",
        model_provider="openai",
        model_api_key=uuid4().hex,
        short_term_memory=memory,
        tools=[],
        skills=[],
    )
    module.short_term_memory = memory
    module.harness_config = HarnessConfig(max_llm_calls=3)
    module.SESSION_METADATA = SessionMetadataStore(memory.session_service)
    install_session_service_metadata_hooks(
        memory.session_service, module.SESSION_METADATA
    )
    module.SESSION_LIFECYCLE_PLUGIN = SessionLifecyclePlugin(module.SESSION_METADATA)
    try:
        yield module
    finally:
        sys.modules.pop("veadk.cloud.harness_app.server", None)
        if previous is not None:
            sys.modules["veadk.cloud.harness_app.server"] = previous


async def override_response(module: ModuleType, **extra: object) -> Response:
    body = {
        "new_message": {"role": "user", "parts": [{"text": "hello"}]},
        "user_id": "stream-user",
        "session_id": "stream-session",
        "harness": {"system_prompt": "Only answer the user."},
        **extra,
    }
    receive = AsyncMock(
        return_value={"type": "http.request", "body": json.dumps(body).encode()}
    )
    request = Request(
        {"type": "http", "method": "POST", "path": "/run_sse", "headers": []}, receive
    )
    return await module.harness_run_sse_override(
        request, AsyncMock(return_value=Response("passthrough"))
    )


def event_from_frame(frame: str | bytes) -> dict[str, object]:
    if isinstance(frame, bytes):
        frame = frame.decode()
    assert frame.startswith("data: ") and frame.endswith("\n\n")
    return json.loads(frame[6:])


@pytest.mark.asyncio
async def test_first_delta_arrives_before_model_finishes(
    harness_server: ModuleType,
) -> None:
    model = harness_server.agent.model
    response = await override_response(harness_server)
    try:
        first = event_from_frame(
            await asyncio.wait_for(anext(response.body_iterator), 2)
        )
        assert first["partial"] is True
        assert first["content"]["parts"][0]["text"] == "甲"
        assert model._stream_modes == [True]
        assert not model._closed.is_set()
        running = await harness_server.SESSION_METADATA.get(
            HARNESS_APP_NAME, "stream-user", "stream-session"
        )
        assert running.status == "running"
        assert running.title == "hello"
        model._gate.set()
        remaining = [event_from_frame(frame) async for frame in response.body_iterator]
        assert [item["content"]["parts"][0]["text"] for item in remaining] == [
            "乙",
            "甲乙",
        ]
        assert remaining[-1]["partial"] is False
        session = await harness_server.short_term_memory.session_service.get_session(
            app_name=HARNESS_APP_NAME,
            user_id="stream-user",
            session_id="stream-session",
        )
        assert (
            session is not None and session.events[-1].content.parts[0].text == "甲乙"
        )
        completed = await harness_server.SESSION_METADATA.get(
            HARNESS_APP_NAME, "stream-user", "stream-session"
        )
        assert completed.status == "idle"
        assert completed.title == "hello"
    finally:
        model._gate.set()
        await response.body_iterator.aclose()


@pytest.mark.asyncio
async def test_error_after_delta_is_visible(harness_server: ModuleType) -> None:
    model = harness_server.agent.model
    model.fail = True
    model._gate.set()
    response = await override_response(harness_server)
    events = [event_from_frame(frame) async for frame in response.body_iterator]
    assert events[0]["partial"] is True
    assert events[-1]["error"] == "controlled model failure"
    assert model._closed.is_set()
    failed = await harness_server.SESSION_METADATA.get(
        HARNESS_APP_NAME, "stream-user", "stream-session"
    )
    assert failed.status == "error"
    assert failed.title == "hello"


@pytest.mark.asyncio
async def test_cancellation_closes_model_and_removes_request_files(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directories: list[Path] = []
    original = harness_server.spawn_harness_agent

    def spawn(
        base_agent: object, overrides: object, directory: Path, **kwargs
    ) -> object:
        directories.append(directory)
        return original(base_agent, overrides, directory, **kwargs)

    monkeypatch.setattr(harness_server, "spawn_harness_agent", spawn)
    response = await override_response(harness_server)
    first = event_from_frame(await anext(response.body_iterator))
    assert first["partial"] is True
    assert directories[0].is_dir()
    pending = asyncio.create_task(anext(response.body_iterator))
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await response.body_iterator.aclose()
    assert harness_server.agent.model._closed.is_set()
    assert not directories[0].exists()


@pytest.mark.asyncio
async def test_requests_without_overrides_keep_sdk_handler(
    harness_server: ModuleType,
) -> None:
    response = await override_response(harness_server, harness={})
    assert response.body == b"passthrough"


@pytest.mark.asyncio
@pytest.mark.parametrize("disconnect", [False, True])
async def test_http_streaming_and_client_disconnect(
    harness_server: ModuleType, disconnect: bool
) -> None:
    from veadk.cloud.harness_app.mcp import McpRequestMiddleware

    app = FastAPI()
    app.middleware("http")(harness_server.harness_run_sse_override)
    app.add_middleware(McpRequestMiddleware)
    model = harness_server.agent.model
    disconnected = asyncio.Event()
    sent: list[dict] = []
    body = json.dumps(
        {
            "user_id": "http-user",
            "session_id": "http-session",
            "new_message": {"parts": [{"text": "hello"}]},
            "harness": {"system_prompt": "Stream the answer."},
        }
    ).encode()
    delivered = False

    async def receive() -> dict:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": body}
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message: dict) -> None:
        sent.append(message)
        if message["type"] == "http.response.body" and message.get("body"):
            assert not model._gate.is_set() or not disconnect
            if disconnect:
                disconnected.set()
            else:
                model._gate.set()

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.0"},
        "http_version": "1.1",
        "method": "POST",
        "path": "/run_sse",
        "headers": [],
        "query_string": b"",
        "scheme": "http",
    }
    await asyncio.wait_for(app(scope, receive, send), 5)
    frames = [
        event_from_frame(message["body"]) for message in sent if message.get("body")
    ]
    assert frames[0]["partial"] is True
    assert len(frames) == (1 if disconnect else 3)
    assert model._closed.is_set()


class ToolModel(BaseLlm):
    model: str = "tool-model"
    _calls: int = PrivateAttr(default=0)

    async def generate_content_async(
        self,
        llm_request: LlmRequest,
        stream: bool = False,
    ) -> AsyncGenerator[LlmResponse, None]:
        self._calls += 1
        part = (
            types.Part(
                function_call=types.FunctionCall(name="echo", args={"value": "ok"})
            )
            if self._calls == 1
            else types.Part(text="tool completed")
        )
        yield LlmResponse(
            content=types.Content(role="model", parts=[part]), partial=False
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit", "override_limit", "default_limit", "fails"),
    [
        (1, 2, 3, True),
        (2, 1, 3, False),
        (None, 1, 3, True),
        (None, None, 1, True),
        (None, None, None, False),
    ],
)
async def test_tool_events_and_llm_limit_precedence(
    harness_server: ModuleType,
    limit: int | None,
    override_limit: int | None,
    default_limit: int | None,
    fails: bool,
) -> None:
    def echo(value: str) -> str:
        """返回测试工具的输入。"""
        return value

    harness_server.agent.model = ToolModel()
    harness_server.agent.tools = [echo]
    harness_server.harness_config.max_llm_calls = default_limit
    response = await override_response(
        harness_server,
        max_llm_calls=limit,
        harness={"system_prompt": "Use echo.", "max_llm_calls": override_limit},
    )
    events = [event_from_frame(frame) async for frame in response.body_iterator]
    parts = [
        part for event in events for part in event.get("content", {}).get("parts", [])
    ]
    assert any("functionCall" in part for part in parts)
    assert any("functionResponse" in part for part in parts)
    if fails:
        assert "error" in events[-1]
        assert harness_server.agent.model._calls == 1
    else:
        assert parts[-1]["text"] == "tool completed"
        assert harness_server.agent.model._calls == 2


@pytest.mark.asyncio
async def test_synchronous_invoke_still_returns_complete_result(
    harness_server: ModuleType,
) -> None:
    response = await harness_server.invoke_harness(
        InvokeHarnessRequest(
            prompt="hello",
            harness_name="test",
            harness={"system_prompt": "Answer."},
            run_agent_request={"user_id": "sync-user", "session_id": "sync-session"},
        )
    )
    assert response.output == "甲乙" and response.error is None
    assert harness_server.agent.model._stream_modes == [False]


@pytest.mark.asyncio
async def test_disconnect_during_clone_waits_before_removing_files(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    directories: list[Path] = []
    original = harness_server.spawn_harness_agent

    def spawn(
        base_agent: object, overrides: object, directory: Path, **kwargs
    ) -> object:
        directories.append(directory)
        loop.call_soon_threadsafe(started.set)
        if not release.wait(3):
            raise TimeoutError("test did not release clone worker")
        assert directory.is_dir()
        return original(base_agent, overrides, directory, **kwargs)

    monkeypatch.setattr(harness_server, "spawn_harness_agent", spawn)

    async def consume() -> None:
        response = await override_response(harness_server)
        async for _ in response.body_iterator:
            pass

    async with anyio.create_task_group() as group:
        group.start_soon(consume)
        await asyncio.wait_for(started.wait(), 2)
        group.cancel_scope.cancel()
        with anyio.CancelScope(shield=True):
            try:
                await anyio.sleep(0.05)
                assert directories[0].is_dir()
            finally:
                release.set()
    assert not directories[0].exists()


@pytest.mark.asyncio
async def test_skills_session_directory_is_initialized(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from veadk.tools.skills_tools import session_path

    monkeypatch.setenv("VEADK_SKILLS_WORK_DIR", str(tmp_path))
    monkeypatch.setattr(session_path, "_session_path_cache", {})
    harness_server.agent.skills = ["example"]
    harness_server.agent.model._gate.set()
    response = await override_response(harness_server)
    events = [event_from_frame(frame) async for frame in response.body_iterator]
    assert events[-1]["partial"] is False
    assert (tmp_path / "stream-session" / "skills").is_dir()
    assert (tmp_path / "stream-session" / "uploads").is_dir()


@pytest.mark.asyncio
async def test_setup_failure_returns_error_frame_and_cleans_directory(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directories: list[Path] = []

    def fail(
        base_agent: object, overrides: object, directory: Path, **kwargs
    ) -> object:
        directories.append(directory)
        raise RuntimeError("skill download failed")

    monkeypatch.setattr(harness_server, "spawn_harness_agent", fail)
    response = await override_response(harness_server)
    events = [event_from_frame(frame) async for frame in response.body_iterator]
    assert events == [{"error": "skill download failed"}]
    assert not directories[0].exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
async def test_default_sandbox_preserves_adk_message_and_state(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
) -> None:
    """改走默认沙箱路径后，图片、状态和流式开关仍按 ADK 请求传递。"""
    monkeypatch.setenv("HARNESS_SANDBOX_TOOL_ID", "tool-default")
    monkeypatch.setattr(
        "veadk.tools.builtin_tools._agentkit.ensure_agentkit_session_endpoint",
        lambda **kwargs: "http://unused.test",
    )
    model = harness_server.agent.model
    model._gate.set()
    response = await override_response(
        harness_server,
        harness=None,
        streaming=streaming,
        state_delta={"test_state": "preserved"},
        new_message={
            "role": "user",
            "parts": [
                {"text": "hello"},
                {"inlineData": {"mimeType": "image/png", "data": "cGl4ZWxz"}},
            ],
        },
    )
    frames = [event_from_frame(frame) async for frame in response.body_iterator]
    assert not any("error" in frame for frame in frames), frames
    assert model._stream_modes == [streaming]
    session = await harness_server.short_term_memory.session_service.get_session(
        app_name=HARNESS_APP_NAME,
        user_id="stream-user",
        session_id="stream-session",
    )
    assert session.state["test_state"] == "preserved"
    user_event = next(event for event in session.events if event.author == "user")
    assert user_event.content.parts[1].inline_data.data == b"pixels"
    assert user_event.content.parts[1].inline_data.mime_type == "image/png"


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("flush_fails", [False, True])
async def test_artifact_flush_preserves_request_resource_cleanup(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
    flush_fails: bool,
) -> None:
    """产物插件参与真实 Runner；即使写入失败，请求资源也必须释放。"""
    from google.adk.plugins.base_plugin import BasePlugin

    plugin = BasePlugin(name="artifact-cleanup-probe")
    before_run = AsyncMock(return_value=None)
    flush = AsyncMock(side_effect=RuntimeError("flush failed") if flush_fails else None)
    monkeypatch.setattr(plugin, "before_run_callback", before_run)
    monkeypatch.setattr(plugin, "flush", flush, raising=False)

    def plugin_for(*args: object) -> BasePlugin:
        return plugin

    monkeypatch.setattr(harness_server, "request_plugin", plugin_for)
    close = AsyncMock(wraps=harness_server.close_request_resources)
    monkeypatch.setattr(harness_server, "close_request_resources", close)
    harness_server.agent.model._gate.set()
    request = InvokeHarnessRequest(
        prompt="hello",
        harness_name="test",
        harness={"system_prompt": "Only answer the user."},
        run_agent_request={
            "user_id": "artifact-user",
            "session_id": "artifact-session",
        },
    )

    async def invoke() -> None:
        if streaming:
            frames = [frame async for frame in harness_server.stream_harness(request)]
            assert frames and all(
                "error" not in event_from_frame(frame) for frame in frames
            )
        else:
            response = await harness_server.invoke_harness(request)
            assert response.error is None
            assert response.output == "甲乙"

    if flush_fails:
        with pytest.raises(RuntimeError, match="flush failed"):
            await invoke()
    else:
        await invoke()
    before_run.assert_awaited_once()
    assert flush.await_count >= 1
    close.assert_awaited_once()
    assert close.call_args.args[0] is not harness_server.agent
