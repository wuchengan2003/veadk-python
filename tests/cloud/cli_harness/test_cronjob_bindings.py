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

"""定时任务资源覆盖的持久化、更新和执行边界。"""

from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from veadk.cloud.harness_app.cronjobs.engine import Engine
from veadk.cloud.harness_app.cronjobs.models import PatchJob
from veadk.cloud.harness_app.cronjobs.routes import build_router
from .test_cronjobs import Clock, MemoryStore, request, settle


BINDINGS = {
    "knowledgebase": {"type": "viking", "index": "reports"},
    "sandbox": {"tool_id": "tool-reports", "ttl": 600},
}


def client_for(engine: Engine) -> httpx.AsyncClient:
    app = FastAPI()
    app.include_router(build_router(engine))
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


@pytest.mark.asyncio
async def test_bindings_survive_storage_restart_execution_and_history() -> None:
    clock, store, calls = Clock(), MemoryStore(), []

    async def invoke(run: dict[str, Any]) -> str:
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    body = request(
        clock, harness=BINDINGS, user_id="alice", idempotency_key="resources"
    )
    job = await engine.create(body)
    assert job["harness"] == BINDINGS
    assert (await engine.create(body))["cronjob_id"] == job["cronjob_id"]
    with pytest.raises(ValueError, match="different request"):
        await engine.create(
            request(
                clock, harness={"sandbox": {"ttl": 120}}, idempotency_key="resources"
            )
        )
    restored = Engine(store, invoke, clock=clock)
    clock.advance(11)
    await restored.scan_once()
    await restored.work_once()
    await settle([restored])
    assert len(calls) == 1
    assert calls[0]["harness"] == BINDINGS
    assert calls[0]["user_id"] == "alice"
    history = (await restored.runs(job["cronjob_id"]))["items"]
    assert history[0]["harness"] == BINDINGS
    assert history[0]["state"] == "succeeded"


@pytest.mark.asyncio
async def test_api_patch_replaces_nested_binding_and_updates_queued_run() -> None:
    clock = Clock()
    engine = Engine(MemoryStore(), None, clock=clock)
    async with client_for(engine) as client:
        response = await client.post(
            "/harness/cronjobs", json=request(clock, harness=BINDINGS).durable_input()
        )
        assert response.status_code == 201, response.text
        path = "/harness/cronjobs/" + response.json()["cronjob_id"]
        clock.advance(11)
        await engine.scan_once()
        changed = await client.patch(path, json={"harness": {"sandbox": {"ttl": 120}}})
        assert changed.status_code == 200, changed.text
        expected = {**BINDINGS, "sandbox": {"ttl": 120}}
        assert changed.json()["harness"] == expected
        assert changed.json()["active"]["harness"] == expected
        fetched = await client.get(path)
        assert fetched.json()["harness"] == expected
        cleared = await client.patch(
            path, json={"harness": {"knowledgebase": None, "sandbox": None}}
        )
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["harness"] == {"knowledgebase": None, "sandbox": None}
        cleared_all = await client.patch(path, json={"harness": None})
        assert cleared_all.status_code == 200
        assert cleared_all.json()["active"]["harness"] is None


@pytest.mark.asyncio
async def test_patch_preserves_claimed_snapshot_and_changes_next_occurrence() -> None:
    clock, store, calls = Clock(), MemoryStore(), []

    async def invoke(run: dict[str, Any]) -> str:
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(
        request(clock, harness=BINDINGS, schedule={"cron": "* * * * *"})
    )
    clock.advance(60)
    await engine.scan_once()
    await engine.work_once()  # claim 已冻结本轮输入。
    changed = await engine.patch(
        job["cronjob_id"], PatchJob(harness={"sandbox": {"tool_id": "tool-next"}})
    )
    assert changed["active"]["harness"] == BINDINGS
    await settle([engine])
    clock.advance(60)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    assert [run["harness"]["sandbox"]["tool_id"] for run in calls] == [
        "tool-reports",
        "tool-next",
    ]
    assert len({run["run_id"] for run in calls}) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "binding,field",
    [
        ({"knowledgebase": {"index": "invalid index"}}, "index"),
        ({"knowledgebase": {"type": "unknown"}}, "type"),
        ({"knowledgebase": {"top_k": 2}}, "top_k"),
        ({"sandbox": {"ttl": 59}}, "ttl"),
        ({"sandbox": {"tool_id": "tool-denied"}}, "tool_id"),
        ({"sandbox": {"execute_timeout_seconds": 60}}, "execute_timeout_seconds"),
    ],
)
async def test_api_rejects_invalid_bindings_on_create_and_patch(
    monkeypatch: pytest.MonkeyPatch,
    binding: dict[str, Any],
    field: str,
) -> None:
    monkeypatch.setenv("HARNESS_SANDBOX_TOOL_ID_ALLOWLIST", "tool-allowed")
    clock = Clock()
    engine = Engine(MemoryStore(), None, clock=clock)
    body = request(clock).durable_input()
    async with client_for(engine) as client:
        created = await client.post("/harness/cronjobs", json=body)
        path = "/harness/cronjobs/" + created.json()["cronjob_id"]
        for response in [
            await client.post("/harness/cronjobs", json={**body, "harness": binding}),
            await client.patch(path, json={"harness": binding}),
        ]:
            assert response.status_code == 422, response.text
            assert any(error["loc"][-1] == field for error in response.json()["detail"])
