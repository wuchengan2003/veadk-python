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

"""调度器到真实 Runner、知识库工具及本地 AIO HTTP 数据面的验证。"""

# Pytest discovers imported fixtures by name; parameters request those fixtures.
# ruff: noqa: F811

from collections.abc import AsyncGenerator
from types import ModuleType
from typing import Any

import pytest
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.tools.environment import EnvironmentToolset

from veadk.cloud.harness_app.cronjobs.engine import Engine
from veadk.cloud.harness_app.cronjobs.models import PatchJob
from veadk.cloud.harness_app.knowledgebase import KnowledgeBaseProvider
from .test_cronjob_bindings import BINDINGS, client_for
from .test_cronjobs import Clock, MemoryStore, request, settle

# 复用已有真实本地 AIO 服务和隔离的 Harness 实例。
from .test_harness_sandbox_e2e import (
    AioDataPlane,
    SandboxScriptModel,
    data_plane as data_plane,
    harness_server as harness_server,
)


class SearchBackend:
    enable_profile = False
    query_with_user_profile = False
    description = "Scheduled report source"

    def __init__(self, *, backend: str, index: str) -> None:
        self.backend, self.index, self.name = backend, index, index
        self.queries: list[str] = []
        self.closed = False

    def search(self, query: str) -> list[dict[str, str]]:
        self.queries.append(query)
        return [{"content": f"knowledge:{self.index}"}]

    def close(self) -> None:
        self.closed = True


class ReportModel(SandboxScriptModel):
    async def generate_content_async(
        self,
        llm_request: LlmRequest,
        stream: bool = False,
    ) -> AsyncGenerator[LlmResponse, None]:
        responses = [
            part.function_response
            for part in llm_request.contents[-1].parts
            if part.function_response
        ]
        if not responses:
            yield self.call("load_knowledgebase", {"query": "report"})
        elif responses[-1].name == "load_knowledgebase":
            result = responses[-1].model_dump(mode="json")["response"]["result"]
            knowledge = result["knowledges"][0]["content"]
            yield self.call("WriteFile", {"path": "note.txt", "content": knowledge})
        else:
            async for response in super().generate_content_async(llm_request, stream):
                yield response


@pytest.fixture
def resource_probe(
    harness_server: ModuleType,
    data_plane: AioDataPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    backends: list[SearchBackend] = []
    resolutions: list[dict[str, Any]] = []
    closed: list[EnvironmentToolset] = []

    def factory(**kwargs: str) -> SearchBackend:
        backend = SearchBackend(**kwargs)
        backends.append(backend)
        return backend

    def resolver(**kwargs: Any) -> str:
        resolutions.append(kwargs)
        return data_plane.url

    original_close = EnvironmentToolset.close

    async def close(toolset: EnvironmentToolset) -> None:
        closed.append(toolset)
        await original_close(toolset)

    provider = KnowledgeBaseProvider(factory, cache_size=1)
    monkeypatch.setattr("veadk.cloud.harness_app.harness_agent._KB_PROVIDER", provider)
    monkeypatch.setattr(
        "veadk.tools.builtin_tools._agentkit.ensure_agentkit_session_endpoint", resolver
    )
    monkeypatch.setattr(EnvironmentToolset, "close", close)
    monkeypatch.setenv("HARNESS_SANDBOX_WORKSPACE", str(data_plane.root))
    harness_server.agent.model = ReportModel()
    return {
        "backends": backends,
        "resolutions": resolutions,
        "closed": closed,
        "provider": provider,
    }


@pytest.mark.asyncio
async def test_scheduled_run_searches_selected_kb_and_writes_sandbox(
    harness_server: ModuleType,
    data_plane: AioDataPlane,
    resource_probe: dict[str, Any],
) -> None:
    clock, store = Clock(), MemoryStore()
    engine = Engine(store, harness_server._invoke_scheduled, clock=clock)
    async with client_for(engine) as client:
        response = await client.post(
            "/harness/cronjobs",
            json=request(
                clock,
                harness=BINDINGS,
                user_id="alice",
                schedule={"cron": "* * * * *"},
            ).durable_input(),
        )
        assert response.status_code == 201, response.text
        job_id = response.json()["cronjob_id"]
        # 模拟进程重启后读取持久任务，再调用正式的调度执行适配器。
        restored = Engine(store, harness_server._invoke_scheduled, clock=clock)
        for index in ["reports", "next_reports"]:
            await restored.patch(
                job_id,
                PatchJob(harness={"knowledgebase": {"type": "viking", "index": index}}),
            )
            clock.advance(60)
            await restored.scan_once()
            await restored.work_once()
            await settle([restored])
            run = (await restored.runs(job_id))["items"][0]
            assert run["state"] == "succeeded", run["error"]
            assert f"knowledge:{index}" in run["output"]
            assert (data_plane.root / "note.txt").read_text() == f"knowledge:{index}"
            resolved = resource_probe["resolutions"][-1]
            assert resolved["tool_id"] == "tool-reports"
            assert resolved["ttl"] == 600
            assert (
                resolved["tool_user_session_id"]
                == f"harness_agent_alice_{run['run_id']}"
            )
    assert (
        len({item["tool_user_session_id"] for item in resource_probe["resolutions"]})
        == 2
    )
    assert len(resource_probe["closed"]) == 2
    assert all(backend.queries == ["report"] for backend in resource_probe["backends"])
    assert resource_probe["backends"][0].closed
    resource_probe["provider"].acquire("viking", "cleanup_probe").release()
    assert resource_probe["backends"][1].closed  # 最后一轮也释放了 KB lease。
    assert not any(
        isinstance(tool, EnvironmentToolset) for tool in harness_server.agent.tools
    )


@pytest.mark.asyncio
async def test_scheduled_run_revalidates_tool_allowlist_at_execution(
    harness_server: ModuleType,
    resource_probe: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    monkeypatch.setenv("HARNESS_SANDBOX_TOOL_ID_ALLOWLIST", "tool-reports")
    engine = Engine(MemoryStore(), harness_server._invoke_scheduled, clock=clock)
    job = await engine.create(request(clock, harness=BINDINGS))
    monkeypatch.setenv("HARNESS_SANDBOX_TOOL_ID_ALLOWLIST", "tool-other")
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    run = (await engine.runs(job["cronjob_id"]))["items"][0]
    assert run["state"] == "failed"
    assert "allowlist" in run["error"].lower()
    assert resource_probe["resolutions"] == []
    assert resource_probe["backends"] == []
