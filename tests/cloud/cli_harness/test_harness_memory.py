# Copyright (c) 2025 Beijing Volcano Engine Technology Co., Ltd. and/or its affiliates.
# SPDX-License-Identifier: Apache-2.0

"""Harness memory adaptation must leave the SDK's Agent and tool catalog intact."""

from types import SimpleNamespace

import pytest
from google.adk.sessions import Session
from pydantic import Field

from veadk import Agent
from veadk.cloud.harness_app.harness_agent import (
    _assemble_agent,
    _load_builtin_tools,
    _tool_name,
    spawn_harness_agent,
)
from veadk.cloud.harness_app.server_types import HarnessConfig, HarnessOverrides
from veadk.memory.long_term_memory import LongTermMemory
from veadk.memory.long_term_memory_backends.base_backend import (
    BaseLongTermMemoryBackend,
)
from veadk.tools import list_builtin_tools


@pytest.mark.parametrize("configured_tools", ["", "write_memory"])
def test_memory_tool_belongs_only_to_harness(monkeypatch, tmp_path, configured_tools):
    monkeypatch.setenv("DATABASE_OPENVIKING_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATABASE_OPENVIKING_API_KEY", "fixture-key")
    harness, _ = _assemble_agent(
        HarnessConfig(
            model_api_key="fixture-key",
            longterm_memory_type="openviking",
            tools=configured_tools,
        )
    )
    original_tools = list(harness.tools)
    request_agent = spawn_harness_agent(
        harness,
        HarnessOverrides(tools="write_memory,calculator"),
        tmp_path,
    )
    sdk_agent = Agent(
        model_api_key="fixture-key", long_term_memory=harness.long_term_memory
    )

    assert [_tool_name(tool) for tool in harness.tools].count("write_memory") == 1
    assert [_tool_name(tool) for tool in request_agent.tools].count("write_memory") == 1
    assert "calculator" in [_tool_name(tool) for tool in request_agent.tools]
    assert harness.tools == original_tools
    assert "write_memory" not in [_tool_name(tool) for tool in sdk_agent.tools]
    assert "write_memory" not in list_builtin_tools()


class RejectingBackend(BaseLongTermMemoryBackend):
    fail_with_exception: bool = False
    attempted_write: dict = Field(default_factory=dict)

    def precheck_index_naming(self):
        pass

    def save_memory(self, user_id: str, event_strings: list[str], **kwargs) -> bool:
        self.attempted_write = {"user_id": user_id, **kwargs}
        if self.fail_with_exception:
            raise RuntimeError("fixture storage unavailable")
        return False

    def search_memory(
        self, user_id: str, query: str, top_k: int, **kwargs
    ) -> list[str]:
        return []


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_with_exception", [False, True])
async def test_harness_write_preserves_backend_failure_and_request_identity(
    fail_with_exception,
):
    backend = RejectingBackend(
        index="deployed-app", fail_with_exception=fail_with_exception
    )
    memory = LongTermMemory(backend=backend, app_name="deployed-app", user_id="unused")
    context = SimpleNamespace(
        _invocation_context=SimpleNamespace(
            agent=SimpleNamespace(long_term_memory=memory),
            session=Session(
                id="request-session", user_id="alice", app_name="request-app"
            ),
        )
    )

    tool = _load_builtin_tools(["write_memory"])[0]
    result = await tool("remember this", context)

    assert result == (
        "Failed to write memory: RuntimeError: fixture storage unavailable"
        if fail_with_exception
        else "Memory was not saved."
    )
    assert backend.attempted_write == {
        "user_id": "alice",
        "app_name": "request-app",
        "session_id": "request-session",
    }
