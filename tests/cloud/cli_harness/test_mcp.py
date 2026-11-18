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

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from google.adk.agents import LlmAgent
from google.adk.events.event import Event
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.runners import InMemoryRunner
from google.adk.tools.base_tool import BaseTool, ToolContext
from google.adk.tools.mcp_tool.mcp_tool import McpTool
from google.genai import types
from mcp.types import Tool
from pydantic import ValidationError

from veadk.cloud.harness_app.harness_agent import config_from_env, spawn_harness_agent
from veadk.cloud.harness_app.mcp import (
    HarnessMcpToolset,
    McpRequestMiddleware,
    mcp_request_scope,
)
from veadk.cloud.harness_app.server_types import HarnessOverrides, McpServerConfig


@pytest.fixture
def connections(monkeypatch):
    opened = []

    class Connection:
        def __init__(self, *, connection_params, tool_name_prefix):
            self.params = connection_params
            self.prefix = tool_name_prefix
            self.closed = False
            opened.append(self)

        async def get_tools_with_prefix(self, context=None):
            assert not self.closed
            if "failed" in self.params.url:
                raise RuntimeError("upstream-error-containing-private-key")
            return [SimpleNamespace(name=f"{self.prefix}_search", owner=self)]

        async def close(self):
            self.closed = True

    monkeypatch.setattr("veadk.cloud.harness_app.mcp.McpToolset", Connection)
    return opened


def test_runtime_environment_parses_mcp_separately_from_tools(monkeypatch):
    monkeypatch.setenv("TOOLS", "web_search,web_fetch")
    monkeypatch.setenv(
        "MCP_SERVERS",
        json.dumps(
            [
                {"endpoint": "https://one.test/mcp", "api_key": "private-key"},
                {"protocol": "sse", "endpoint": "https://two.test/sse"},
            ]
        ),
    )
    config = config_from_env()
    assert config.tools == "web_search,web_fetch"
    assert len(config.mcp_servers) == 2
    assert config.mcp_servers[0].protocol == "streamable-http"
    assert config.mcp_servers[0].api_key.get_secret_value() == "private-key"
    assert "private-key" not in repr(config)


@pytest.mark.parametrize(
    "value",
    [
        {"endpoint": "file:///tmp/mcp"},
        {"endpoint": "https://user:secret@one.test/mcp"},
        {"endpoint": "https://one.test/mcp", "protocol": "stdio"},
        {"endpoint": "https://one.test/mcp", "api_key": "key\r\ninjected-header"},
    ],
)
def test_invalid_servers_are_rejected(value):
    with pytest.raises(ValidationError):
        McpServerConfig(**value)


def test_multiple_servers_inheritance_replacement_and_cleanup(connections):
    async def scenario():
        defaults = [
            McpServerConfig(endpoint="https://default.test/mcp", api_key="default-key")
        ]
        tools = HarnessMcpToolset(defaults)
        async with mcp_request_scope():
            result = await tools.get_tools_with_prefix()
            assert result[0].owner.params.headers == {
                "Authorization": "Bearer default-key"
            }
            assert not connections[0].closed
        assert connections[0].closed

        async with mcp_request_scope(
            [
                McpServerConfig(endpoint="https://one.test/mcp", api_key="request-key"),
                McpServerConfig(protocol="sse", endpoint="https://two.test/sse"),
            ]
        ):
            result = await tools.get_tools_with_prefix()
            assert [tool.name for tool in result] == ["mcp_1_search", "mcp_2_search"]
            assert result[0].owner.params.headers == {
                "Authorization": "Bearer request-key"
            }
            assert type(result[1].owner.params).__name__ == "SseConnectionParams"
            assert result[1].owner.params.headers == {}
        assert all(connection.closed for connection in connections)
        async with mcp_request_scope([]):
            assert await tools.get_tools_with_prefix() == []
        assert len(connections) == 3
        async with mcp_request_scope():
            assert (await tools.get_tools())[0].owner.params.url == defaults[0].endpoint

    asyncio.run(scenario())


def test_concurrent_request_credentials_are_isolated(connections):
    async def scenario():
        tools = HarnessMcpToolset([])
        ready = asyncio.Event()

        async def run(key):
            async with mcp_request_scope(
                [McpServerConfig(endpoint="https://same.test/mcp", api_key=key)]
            ):
                first = (await tools.get_tools_with_prefix())[0]
                if len(connections) == 2:
                    ready.set()
                await ready.wait()
                second = (await tools.get_tools_with_prefix())[0]
                assert first.owner is second.owner
                assert second.owner.params.headers == {"Authorization": f"Bearer {key}"}
                assert not second.owner.closed

        await asyncio.gather(run("alice-key"), run("bob-key"))
        assert len(connections) == 2
        assert all(connection.closed for connection in connections)

    asyncio.run(scenario())


def test_failed_tool_discovery_releases_connections_without_exposing_keys(connections):
    async def scenario():
        with pytest.raises(
            RuntimeError, match="Failed to load tools from MCP server 2"
        ) as error:
            async with mcp_request_scope():
                await HarnessMcpToolset(
                    [
                        McpServerConfig(endpoint="https://one.test/mcp"),
                        McpServerConfig(endpoint="https://failed.test/mcp"),
                    ]
                ).get_tools()
        assert "private-key" not in str(error.value)
        assert all(connection.closed for connection in connections)

    asyncio.run(scenario())


def test_mcp_execution_failure_returns_safe_error_without_masking_other_tools(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from veadk.cloud.harness_app.mcp import mcp_tool_error_fallback

    mcp_tool = Mock(spec=McpTool)
    mcp_tool.name = "mcp_1_search"
    builtin_tool = Mock(spec=BaseTool)
    builtin_tool.name = "builtin_search"
    tool_context = Mock(spec=ToolContext)
    failure = RuntimeError("upstream-error-containing-private-key")

    assert mcp_tool_error_fallback(
        tool=mcp_tool, args={}, tool_context=tool_context, error=failure
    ) == {"error": "MCP tool mcp_1_search failed; its result is unavailable."}
    assert (
        mcp_tool_error_fallback(
            tool=builtin_tool, args={}, tool_context=tool_context, error=failure
        )
        is None
    )
    assert "private-key" not in caplog.text
    assert "RuntimeError" in caplog.text


class _RecoveryLlm(BaseLlm):
    model: str = "offline-recovery"

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LlmResponse, None]:
        del stream
        responses = [
            part.function_response
            for content in llm_request.contents
            for part in content.parts
            if part.function_response
        ]
        if responses:
            assert responses[-1].response == {
                "error": "MCP tool mcp_1_search failed; its result is unavailable."
            }
            yield LlmResponse(
                content=types.Content(
                    role="model", parts=[types.Part(text="recovered")]
                )
            )
        else:
            yield LlmResponse(
                content=types.Content(
                    role="model",
                    parts=[
                        types.Part(
                            function_call=types.FunctionCall(
                                name="mcp_1_search", args={}
                            )
                        )
                    ],
                )
            )


def test_adk_runner_continues_after_mcp_execution_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from veadk.cloud.harness_app.mcp import mcp_tool_error_fallback

    tool = McpTool(
        mcp_tool=Tool(name="mcp_1_search", inputSchema={"type": "object"}),
        mcp_session_manager=Mock(),
    )
    execute = AsyncMock(
        side_effect=RuntimeError("upstream-error-containing-private-key")
    )
    monkeypatch.setattr(tool, "run_async", execute)
    agent = LlmAgent(
        name="harness_agent",
        model=_RecoveryLlm(),
        tools=[tool],
        on_tool_error_callback=mcp_tool_error_fallback,
    )

    async def scenario() -> list[Event]:
        runner = InMemoryRunner(agent=agent, app_name="mcp-recovery")
        await runner.session_service.create_session(
            app_name=runner.app_name, user_id="user", session_id="session"
        )
        return [
            event
            async for event in runner.run_async(
                user_id="user",
                session_id="session",
                new_message=types.Content(
                    role="user", parts=[types.Part(text="search")]
                ),
            )
        ]

    events = asyncio.run(scenario())
    assert execute.await_count == 1
    assert any(
        event.content and any(part.text == "recovered" for part in event.content.parts)
        for event in events
    )
    assert "private-key" not in str(events)
    assert "private-key" not in caplog.text


def test_overrides_do_not_change_builtin_tools_or_base_agent(tmp_path):
    from google.adk.agents import LlmAgent

    def existing_tool() -> str:
        return "ok"

    base = LlmAgent(
        name="harness_agent",
        tools=[
            existing_tool,
            HarnessMcpToolset(
                [
                    McpServerConfig(endpoint="https://default.test/mcp"),
                ]
            ),
        ],
    )
    clone = spawn_harness_agent(base, HarnessOverrides(mcp_servers=[]), tmp_path)
    assert clone.tools[0] is existing_tool
    assert clone.tools[1].servers == ()
    assert base.tools[1].servers[0].endpoint == "https://default.test/mcp"


def test_middleware_preserves_authentication_and_streaming_lifetime(connections):
    app = FastAPI()
    toolset = HarnessMcpToolset([])
    app.add_middleware(McpRequestMiddleware, shared=True)

    @app.post("/run_sse")
    async def run(request: Request):
        if request.headers.get("Authorization") != "Bearer user-token":
            raise HTTPException(401)
        payload = await request.json()
        assert "harness" not in payload
        assert payload["user_id"] == "alice"
        assert payload["session_id"] == "same-session"

        async def events():
            tools = await toolset.get_tools_with_prefix()
            await asyncio.sleep(0)
            assert not tools[0].owner.closed
            yield f"data: {tools[0].name}\n\n"

        return StreamingResponse(events(), media_type="text/event-stream")

    payload = {
        "user_id": "alice",
        "session_id": "same-session",
        "harness": {
            "mcp_servers": [
                {"endpoint": "https://one.test/mcp", "api_key": "tool-key"}
            ],
        },
    }
    with TestClient(app) as client:
        assert client.post("/run_sse", json=payload).status_code == 401
        assert not connections
        response = client.post(
            "/run_sse", headers={"Authorization": "Bearer user-token"}, json=payload
        )
        assert response.status_code == 200
        assert response.text == "data: mcp_1_search\n\n"
        assert all(connection.closed for connection in connections)
        payload["harness"]["system_prompt"] = "not allowed on shared OAuth"
        assert client.post("/run_sse", json=payload).status_code == 400


def test_middleware_validation_does_not_echo_api_keys():
    app = FastAPI()
    app.add_middleware(McpRequestMiddleware)
    with TestClient(app) as client:
        response = client.post(
            "/run_sse",
            json={
                "harness": {
                    "mcp_servers": [
                        {
                            "endpoint": "https://one.test/mcp",
                            "api_key": "private-key\r\n",
                        },
                    ]
                }
            },
        )
        assert response.status_code == 422
        assert "private-key" not in response.text


def test_session_history_survives_switching_override_paths(monkeypatch):
    import importlib
    import sys

    from google.adk.agents import LlmAgent
    from google.adk.models.base_llm import BaseLlm
    from google.adk.models.llm_response import LlmResponse
    from google.genai import types
    from veadk.memory.short_term_memory import ShortTermMemory

    class HistoryLlm(BaseLlm):
        model: str = "history-test"

        async def generate_content_async(self, llm_request, stream=False):
            text = "|".join(
                part.text
                for content in llm_request.contents
                if content.role == "user"
                for part in content.parts
                if part.text
            )
            yield LlmResponse(
                content=types.Content(role="model", parts=[types.Part(text=text)])
            )

    root_agent = LlmAgent(
        name="harness_agent", model=HistoryLlm(), tools=[HarnessMcpToolset([])]
    )
    memory = ShortTermMemory(backend="local")
    monkeypatch.setenv("HARNESS_NAME", "different-deployment-name")
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_ENABLED", "false")
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_required", lambda: False
    )
    monkeypatch.setattr(
        "veadk.cloud.harness_app.harness_agent.init_harness_agent",
        lambda **kwargs: (root_agent, memory),
    )
    sys.modules.pop("veadk.cloud.harness_app.server", None)
    try:
        module = importlib.import_module("veadk.cloud.harness_app.server")
        with TestClient(module.app) as client:
            assert (
                client.post(
                    "/apps/harness_agent/users/u1/sessions/s1", json={}
                ).status_code
                == 200
            )

            def request(text, **extra):
                response = client.post(
                    "/run_sse",
                    json={
                        "app_name": "harness_agent",
                        "user_id": "u1",
                        "session_id": "s1",
                        "new_message": {"role": "user", "parts": [{"text": text}]},
                        **extra,
                    },
                )
                assert response.status_code == 200
                return response.text

            assert "first" in request("first")
            assert "first|second" in request(
                "second", harness={"system_prompt": "Use history", "mcp_servers": []}
            )
            assert "first|second|third" in request("third", harness={"mcp_servers": []})
            assert "first|second|third|fourth" in request("fourth")
    finally:
        sys.modules.pop("veadk.cloud.harness_app.server", None)
