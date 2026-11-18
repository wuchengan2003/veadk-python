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

"""Assembly of request-scoped knowledge base and sandbox bindings."""

import asyncio

import pytest

from veadk.cloud.harness_app import harness_agent
from veadk.cloud.harness_app.harness_agent import (
    _apply_knowledgebase_override,
    _apply_sandbox_override,
    close_request_resources,
    spawn_harness_agent,
)
from google.adk.tools.environment import EnvironmentToolset

from veadk.cloud.harness_app.knowledgebase import KnowledgeBaseProvider
from veadk.cloud.harness_app.sandbox import (
    SANDBOX_TOOL_ID_ENV,
    TTL_ENV,
    WORKSPACE_ENV,
    AgentkitSandboxEnvironment,
)
from veadk.cloud.harness_app.server_types import HarnessOverrides


class FakeTool:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeKnowledgeBase:
    def __init__(self, backend: str = "viking", index: str = "deployed_index") -> None:
        self.backend = backend
        self.index = index


class FakeAgent:
    def __init__(self, tools=None, knowledgebase=None, runtime="adk") -> None:
        self.tools = list(tools or [])
        self.knowledgebase = knowledgebase
        self.runtime = runtime

    def clone(self, update=None):
        clone = FakeAgent(self.tools, self.knowledgebase, self.runtime)
        for field, value in (update or {}).items():
            setattr(clone, field, value)
        return clone


@pytest.fixture
def fake_provider(monkeypatch):
    created = []

    class FactoryKB(FakeKnowledgeBase):
        def __init__(self, backend, index):
            super().__init__(backend, index)
            created.append(self)

    provider = KnowledgeBaseProvider(
        lambda *, backend, index: FactoryKB(backend, index), cache_size=4
    )
    monkeypatch.setattr(harness_agent, "_KB_PROVIDER", provider)
    return provider, created


def tool_names(agent) -> list[str]:
    return [getattr(tool, "name", type(tool).__name__) for tool in agent.tools]


def test_knowledgebase_override_replaces_the_deployed_tool(monkeypatch):
    monkeypatch.setattr(
        harness_agent,
        "_KB_PROVIDER",
        KnowledgeBaseProvider(
            lambda *, backend, index: FakeKnowledgeBase(backend, index), cache_size=2
        ),
    )
    base = FakeAgent(
        tools=[FakeTool("load_knowledgebase"), FakeTool("web_search")],
        knowledgebase=FakeKnowledgeBase("viking", "deployed_index"),
    )

    cloned = spawn_harness_agent(
        base, HarnessOverrides(knowledgebase={"index": "tenant_42"}), None
    )

    assert tool_names(cloned) == ["web_search", "load_knowledgebase"]
    assert cloned.knowledgebase.backend == "viking"
    assert cloned.knowledgebase.index == "tenant_42"
    assert base.knowledgebase.index == "deployed_index"
    assert getattr(cloned, "_harness_kb_lease") is not None


def test_knowledgebase_override_switches_the_backend_and_index(fake_provider):
    _, created = fake_provider
    base = FakeAgent(
        tools=[FakeTool("load_knowledgebase")],
        knowledgebase=FakeKnowledgeBase("viking", "deployed_index"),
    )

    cloned = spawn_harness_agent(
        base,
        HarnessOverrides(knowledgebase={"index": "other", "type": "redis"}),
        None,
    )

    assert created[0].backend == "redis"
    assert cloned.knowledgebase.index == "other"


def test_knowledgebase_override_requires_a_backend_without_deployment_config(
    fake_provider,
):
    base = FakeAgent(tools=[], knowledgebase=None)

    with pytest.raises(ValueError, match="knowledgebase.type is required"):
        spawn_harness_agent(base, HarnessOverrides(knowledgebase={"index": "kb"}), None)


def test_knowledgebase_override_requires_an_index_without_deployment_config(
    fake_provider,
):
    base = FakeAgent(tools=[], knowledgebase=None)

    with pytest.raises(ValueError, match="knowledgebase.index is required"):
        spawn_harness_agent(
            base, HarnessOverrides(knowledgebase={"type": "viking"}), None
        )


def test_unsupported_custom_backend_instance_requires_an_explicit_type(fake_provider):
    class BackendInstance:
        def __repr__(self):
            return "custom"

    base = FakeAgent(
        tools=[],
        knowledgebase=FakeKnowledgeBase(BackendInstance(), "deployed_index"),
    )

    with pytest.raises(ValueError, match="knowledgebase.type is required"):
        spawn_harness_agent(base, HarnessOverrides(knowledgebase={"index": "kb"}), None)


def test_knowledgebase_lease_is_released_when_sandbox_assembly_fails(
    fake_provider, monkeypatch
):
    provider, created = fake_provider
    base = FakeAgent(
        tools=[],
        knowledgebase=FakeKnowledgeBase("viking", "deployed_index"),
    )

    def fail(*args, **kwargs):
        raise RuntimeError("sandbox unavailable")

    monkeypatch.setattr(harness_agent, "_apply_sandbox_override", fail)

    with pytest.raises(RuntimeError, match="sandbox unavailable"):
        spawn_harness_agent(base, HarnessOverrides(knowledgebase={"index": "kb"}), None)

    entry = provider._entries[("viking", "kb")]
    assert entry.refs == 0


def test_sandbox_override_mounts_the_requested_tool(monkeypatch):
    monkeypatch.delenv(SANDBOX_TOOL_ID_ENV, raising=False)
    monkeypatch.setenv(WORKSPACE_ENV, "/home/gem/work")
    base = FakeAgent(tools=[FakeTool("web_search")])

    cloned = spawn_harness_agent(
        base,
        HarnessOverrides(
            sandbox={
                "tool_id": "request-tool",
                "ttl": 120,
            }
        ),
        None,
        user_id="u-1",
        session_id="s-1",
    )

    toolset = cloned.tools[-1]
    assert isinstance(toolset, EnvironmentToolset)
    environment = toolset._environment
    assert isinstance(environment, AgentkitSandboxEnvironment)
    assert environment._tool_id == "request-tool"
    assert environment._ttl == 120
    assert environment._tool_user_session_id == "harness_agent_u-1_s-1"
    assert str(environment.working_dir) == "/home/gem/work"
    assert tool_names(cloned)[0] == "web_search"


def test_deployment_tool_id_mounts_without_request_overrides(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")
    base = FakeAgent(tools=[])

    cloned = spawn_harness_agent(
        base, HarnessOverrides(), None, user_id="u", session_id="s"
    )

    assert isinstance(cloned.tools[-1], EnvironmentToolset)
    assert cloned.tools[-1]._environment._tool_id == "deployed-tool"
    assert base.tools == []


def test_request_tool_id_wins_over_the_deployment(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")
    base = FakeAgent(tools=[])

    cloned = spawn_harness_agent(
        base,
        HarnessOverrides(sandbox={"tool_id": "request-tool"}),
        None,
        user_id="u",
        session_id="s",
    )

    assert cloned.tools[-1]._environment._tool_id == "request-tool"


def test_without_a_tool_id_no_sandbox_is_mounted(monkeypatch):
    monkeypatch.delenv(SANDBOX_TOOL_ID_ENV, raising=False)
    base = FakeAgent(tools=[])

    cloned = spawn_harness_agent(base, HarnessOverrides(), None)

    assert cloned.tools == []


def test_codex_runtime_keeps_its_native_tools(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")
    base = FakeAgent(tools=[], runtime="codex")

    cloned = spawn_harness_agent(
        base, HarnessOverrides(), None, user_id="u", session_id="s"
    )

    assert cloned.tools == []


def test_sandbox_requires_invocation_identity(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")

    with pytest.raises(ValueError, match="requires user_id and session_id"):
        spawn_harness_agent(FakeAgent(tools=[]), HarnessOverrides(), None)


def test_sandbox_defaults_come_from_the_deployment(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")
    monkeypatch.setenv(TTL_ENV, "7200")
    base = FakeAgent(tools=[])

    cloned = spawn_harness_agent(
        base, HarnessOverrides(), None, user_id="u", session_id="s"
    )

    environment = cloned.tools[-1]._environment
    assert environment._ttl == 7200


def test_sandbox_rejects_a_relative_workspace(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")
    monkeypatch.setenv(WORKSPACE_ENV, "relative/path")

    with pytest.raises(ValueError, match="absolute"):
        spawn_harness_agent(
            FakeAgent(tools=[]), HarnessOverrides(), None, user_id="u", session_id="s"
        )


def test_knowledgebase_provider_is_created_lazily(monkeypatch):
    monkeypatch.setattr(harness_agent, "_KB_PROVIDER", None)
    monkeypatch.setenv("HARNESS_KB_CACHE_SIZE", "3")

    provider = harness_agent._knowledgebase_provider()

    assert provider._cache_size == 3
    assert harness_agent._knowledgebase_provider() is provider


class ClosableToolset:
    def __init__(self, error: Exception | None = None) -> None:
        self.closed = False
        self.error = error

    async def close(self) -> None:
        self.closed = True
        if self.error is not None:
            raise self.error


class ReleasableLease:
    def __init__(self, error: Exception | None = None) -> None:
        self.released = 0
        self.error = error

    def release(self) -> None:
        self.released += 1
        if self.error is not None:
            raise self.error


def test_close_request_resources_closes_toolsets_and_leases():
    toolset = ClosableToolset()
    lease = ReleasableLease()
    agent = FakeAgent()
    agent._harness_request_toolsets = [toolset]
    agent._harness_kb_lease = lease

    asyncio.run(close_request_resources(agent))

    assert toolset.closed is True
    assert lease.released == 1


def test_close_request_resources_is_a_noop_without_bindings():
    asyncio.run(close_request_resources(FakeAgent()))


def test_close_request_resources_swallows_cleanup_errors():
    toolset = ClosableToolset(RuntimeError("close failed"))
    lease = ReleasableLease(RuntimeError("release failed"))
    agent = FakeAgent()
    agent._harness_request_toolsets = [toolset]
    agent._harness_kb_lease = lease

    asyncio.run(close_request_resources(agent))

    assert toolset.closed is True
    assert lease.released == 1


def test_apply_sandbox_override_returns_none_when_disabled(monkeypatch):
    monkeypatch.delenv(SANDBOX_TOOL_ID_ENV, raising=False)

    assert (
        _apply_sandbox_override(
            FakeAgent(), HarnessOverrides(), user_id="u", session_id="s", agent_name="a"
        )
        is None
    )


def test_apply_knowledgebase_override_returns_none_without_fields():
    assert _apply_knowledgebase_override(FakeAgent(), HarnessOverrides()) is None
    assert (
        _apply_knowledgebase_override(FakeAgent(), HarnessOverrides(knowledgebase={}))
        is None
    )
