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

"""Distributed scheduling invariants exercised with competing server replicas"""

import asyncio
import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from veadk.cloud.harness_app.cronjobs.engine import Engine
from veadk.cloud.harness_app.cronjobs.models import (
    DEFAULT_USER_ID,
    CreateJob,
    PatchJob,
    Schedule,
    instant,
)
from veadk.cloud.harness_app.cronjobs.routes import build_router
from veadk.cloud.harness_app.cronjobs.store import Conflict


class MemoryStore:
    def __init__(self):
        self.objects = {}
        self.guard = asyncio.Lock()
        self.fail_history = False
        self.lose_response = False

    async def get(self, key):
        await asyncio.sleep(0)
        return copy.deepcopy(self.objects.get(key))

    async def put(self, key, value, etag=None):
        await asyncio.sleep(0)
        async with self.guard:
            if key.startswith("runs/") and self.fail_history:
                raise ConnectionError("History unavailable")
            previous = self.objects.get(key)
            if (etag is None and previous is not None) or (
                etag is not None and (previous is None or previous[1] != etag)
            ):
                raise Conflict()
            version = hashlib.sha256(
                json.dumps(value, sort_keys=True).encode()
            ).hexdigest()
            self.objects[key] = (copy.deepcopy(value), version)
            if self.lose_response:
                self.lose_response = False
                raise ConnectionError("Write succeeded but response was lost")
            return version

    async def list(self, prefix, *, cursor="", limit=1000):
        keys = sorted(
            key for key in self.objects if key.startswith(prefix) and key > cursor
        )
        page = keys[:limit]
        return page, page[-1] if len(keys) > limit else ""


class Clock:
    def __init__(self):
        self.now = datetime(2026, 9, 16, tzinfo=timezone.utc)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


def request(clock, **changes):
    return CreateJob.model_validate(
        {
            "name": "sample",
            "prompt": "sample",
            "schedule": {"at": clock() + timedelta(seconds=10)},
            **changes,
        }
    )


async def settle(engines):
    tasks = [task for engine in engines for task in engine.executions.values()]
    if tasks:
        await asyncio.gather(*tasks)
    await asyncio.gather(*(engine.scan_once() for engine in engines))


@pytest.mark.asyncio
async def test_user_id_survives_storage_execution_and_history():
    store, clock, calls = MemoryStore(), Clock(), []

    async def invoke(run):
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    body = request(clock, user_id="alice", idempotency_key="alice-report")
    job = await engine.create(body)
    job_id = job["cronjob_id"]
    assert job["user_id"] == "alice"
    assert (await store.get(engine.job_key(job_id)))[0]["user_id"] == "alice"
    assert (await engine.create(body))["cronjob_id"] == job_id
    with pytest.raises(ValueError, match="different request"):
        await engine.create(
            request(clock, user_id="bob", idempotency_key="alice-report")
        )

    # A different replica loads the persisted user before claiming execution
    restored = Engine(store, invoke, clock=clock)
    clock.advance(11)
    await restored.scan_once()
    active = (await restored.get(job_id))[0]["active"]
    assert (await restored.result(job_id, active["run_id"]))["user_id"] == "alice"
    await restored.work_once()
    await settle([restored])
    assert calls[0]["user_id"] == "alice"
    history = (await restored.runs(job_id))["items"]
    assert len(history) == 1 and history[0]["user_id"] == "alice"
    assert (await restored.result(job_id, history[0]["run_id"]))["user_id"] == "alice"


@pytest.mark.asyncio
async def test_user_model_key_reaches_scheduled_run_without_exposure():
    store, clock, calls = MemoryStore(), Clock(), []

    async def invoke(run):
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    body = request(clock, user_id="alice", harness={"model_api_key": "alice-model-key"})
    assert body.durable_input()["harness"]["model_api_key"] == "alice-model-key"
    created = await engine.create(body)
    assert created["harness"]["model_api_key"] == "********"
    job_id = created["cronjob_id"]
    assert (await store.get(engine.job_key(job_id)))[0]["harness"][
        "model_api_key"
    ] == "alice-model-key"

    clock.advance(11)
    await engine.scan_once()
    assert (
        await engine.result(job_id, (await engine.get(job_id))[0]["active"]["run_id"])
    )["harness"]["model_api_key"] == "********"
    await engine.work_once()
    await settle([engine])
    assert calls[0]["user_id"] == "alice"
    assert calls[0]["harness"]["model_api_key"] == "alice-model-key"
    history = (await engine.runs(job_id))["items"][0]
    assert history["harness"]["model_api_key"] == "********"
    assert "alice-model-key" not in json.dumps(history)


@pytest.mark.asyncio
async def test_model_key_is_redacted_from_execution_failure_and_history():
    store, clock = MemoryStore(), Clock()

    async def invoke(run):
        raise RuntimeError(f"model rejected {run['harness']['model_api_key']}")

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock, harness={"model_api_key": "private-key"}))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    history = (await engine.runs(job["cronjob_id"]))["items"][0]
    assert (
        history["state"] == "failed" and history["error"] == "model rejected ********"
    )
    assert "private-key" not in json.dumps(history)


@pytest.mark.asyncio
async def test_patch_reschedules_queued_run_and_rotates_user_key():
    store, clock, calls = MemoryStore(), Clock(), []

    async def invoke(run):
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    body = request(
        clock,
        user_id="alice",
        harness={"model_api_key": "alice-key"},
        idempotency_key="editable",
    )
    created = await engine.create(body)
    job_id = created["cronjob_id"]
    initial_hash = (await engine.get(job_id))[0]["request_hash"]
    clock.advance(11)
    await engine.scan_once()
    queued = (await engine.get(job_id))[0]["active"]
    updated = await engine.patch(
        job_id,
        PatchJob.model_validate(
            {
                "name": "new name",
                "prompt": "new prompt",
                "user_id": "bob",
                "schedule": {"at": clock() + timedelta(seconds=30)},
                "harness": {"model_api_key": "bob-key"},
            }
        ),
    )
    assert updated["revision"] == 2
    assert updated["harness"]["model_api_key"] == "********"
    assert updated["active"]["run_id"] == queued["run_id"]
    assert updated["active"]["state"] == "cancelled"
    assert (await engine.get(job_id))[0]["request_hash"] == initial_hash
    assert (await engine.create(body))["cronjob_id"] == job_id
    await engine.work_once()
    assert calls == []
    await engine.scan_once()
    clock.advance(31)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    assert len(calls) == 1
    assert (
        calls[0]["prompt"],
        calls[0]["user_id"],
        calls[0]["harness"]["model_api_key"],
    ) == ("new prompt", "bob", "bob-key")
    history = (await engine.runs(job_id))["items"]
    assert {item["state"] for item in history} == {"cancelled", "succeeded"}
    assert all("-key" not in json.dumps(item) for item in history)


@pytest.mark.asyncio
async def test_patch_updates_queued_input_and_clear_key_inherits_deployed_default():
    store, clock, calls = MemoryStore(), Clock(), []

    async def invoke(run):
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(
        request(clock, harness={"model_api_key": "old-key", "model_name": "custom"})
    )
    job_id = job["cronjob_id"]
    clock.advance(11)
    await engine.scan_once()
    with pytest.raises(ValueError, match="requires a new or cleared"):
        await engine.patch(job_id, PatchJob(user_id="bob"))
    updated = await engine.patch(
        job_id,
        PatchJob.model_validate(
            {
                "prompt": "edited before claim",
                "user_id": "bob",
                "harness": {"model_api_key": ""},
            }
        ),
    )
    assert updated["active"]["prompt"] == "edited before claim"
    assert updated["active"]["user_id"] == "bob"
    assert updated["active"]["harness"] == {"model_name": "custom"}
    await engine.work_once()
    await settle([engine])
    assert calls[0]["harness"] == {"model_name": "custom"}
    assert calls[0]["prompt"] == "edited before claim"
    assert (await engine.runs(job_id))["items"][0]["revision"] == 2
    await engine.patch(job_id, PatchJob(harness=None))
    assert (await engine.get(job_id))[0]["harness"] is None


@pytest.mark.asyncio
async def test_patch_validation_and_conflict(monkeypatch):
    store, clock = MemoryStore(), Clock()
    engine = Engine(store, None, clock=clock)
    app = FastAPI()
    app.include_router(build_router(engine))
    job = await engine.create(request(clock))
    path = f"/harness/cronjobs/{job['cronjob_id']}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for invalid in [
            {},
            {"name": None},
            {"harness": {}},
            {"timeout_seconds": 0},
            {"schedule": {"at": (clock() - timedelta(seconds=1)).isoformat()}},
        ]:
            assert (await client.patch(path, json=invalid)).status_code == 422
        assert (
            await client.patch(f"/harness/cronjobs/{'0' * 32}", json={"name": "x"})
        ).status_code == 404
        monkeypatch.setattr(store, "put", AsyncMock(side_effect=Conflict()))
        assert (await client.patch(path, json={"name": "x"})).status_code == 409


@pytest.mark.asyncio
async def test_deleted_task_rejects_webhook_changes_and_create_race(monkeypatch):
    store, clock = MemoryStore(), Clock()
    engine = Engine(store, None, clock=clock)
    body = request(clock, idempotency_key="deleted-race")
    job = await engine.create(body)
    await engine.delete(job["cronjob_id"])
    with pytest.raises(KeyError):
        await engine.set_webhook(job["cronjob_id"], None)
    original_get = store.get
    first = True

    async def stale_get(key):
        nonlocal first
        if first:
            first = False
            return None
        return await original_get(key)

    monkeypatch.setattr(store, "get", stale_get)
    with pytest.raises(Conflict):
        await engine.create(body)


@pytest.mark.asyncio
async def test_delete_reports_exhausted_conditional_conflicts(monkeypatch):
    store, clock = MemoryStore(), Clock()
    engine = Engine(store, None, clock=clock)
    job = await engine.create(request(clock))
    monkeypatch.setattr(store, "put", AsyncMock(side_effect=Conflict()))
    with pytest.raises(Conflict):
        await engine.delete(job["cronjob_id"])


@pytest.mark.asyncio
async def test_concurrent_patch_and_delete_cannot_revive_task():
    store, clock = MemoryStore(), Clock()
    first, second = Engine(store, None, clock=clock), Engine(store, None, clock=clock)
    job = await first.create(request(clock))
    outcomes = await asyncio.gather(
        first.patch(job["cronjob_id"], PatchJob(prompt="new")),
        second.delete(job["cronjob_id"]),
        return_exceptions=True,
    )
    assert not any(isinstance(result, Conflict) for result in outcomes)
    assert (await store.get(first.job_key(job["cronjob_id"])))[0]["deleted_at"]
    assert (await first.list_jobs())["items"] == []


@pytest.mark.asyncio
async def test_delete_hides_task_cancels_queue_and_keeps_history():
    store, clock, calls = MemoryStore(), Clock(), []

    async def invoke(run):
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    app = FastAPI()
    app.include_router(build_router(engine))
    body = request(
        clock, harness={"model_api_key": "secret-key"}, idempotency_key="delete-me"
    )
    job = await engine.create(body)
    job_id = job["cronjob_id"]
    clock.advance(11)
    await engine.scan_once()
    path = f"/harness/cronjobs/{job_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        deleted = await client.delete(path)
        assert deleted.status_code == 200
        assert (await client.delete(path)).json() == deleted.json()
        assert (await client.get(path)).status_code == 404
        assert (await client.patch(path, json={"prompt": "new"})).status_code == 404
        assert (await client.post(path + "/resume")).status_code == 404
        assert (await client.get("/harness/cronjobs")).json()["items"] == []
        assert (
            await client.post(
                "/harness/cronjobs",
                json={**body.durable_input(), "idempotency_key": "delete-me"},
            )
        ).status_code == 409
    await engine.work_once()
    await engine.scan_once()
    assert calls == []
    assert (await engine.runs(job_id))["items"][0]["state"] == "cancelled"
    assert "secret-key" not in json.dumps((await store.get(engine.job_key(job_id)))[0])


@pytest.mark.asyncio
async def test_listing_fills_page_past_deleted_tombstones():
    store, clock = MemoryStore(), Clock()
    engine = Engine(store, None, clock=clock)
    jobs = [
        await engine.create(request(clock, idempotency_key=f"page-{index}"))
        for index in range(3)
    ]
    ordered = sorted(jobs, key=lambda job: job["cronjob_id"])
    await engine.delete(ordered[0]["cronjob_id"])
    page = await engine.list_jobs(limit=1)
    assert [item["cronjob_id"] for item in page["items"]] == [ordered[1]["cronjob_id"]]
    assert page["next_cursor"]
    next_page = await engine.list_jobs(cursor=page["next_cursor"], limit=1)
    assert [item["cronjob_id"] for item in next_page["items"]] == [
        ordered[2]["cronjob_id"]
    ]


@pytest.mark.asyncio
async def test_delete_lets_running_execution_finish_without_rescheduling():
    store, clock, gate = MemoryStore(), Clock(), asyncio.Event()
    started = asyncio.Event()

    async def invoke(run):
        started.set()
        await gate.wait()
        return "done"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(
        request(clock, schedule={"cron": "* * * * *", "timezone": "UTC"})
    )
    clock.advance(61)
    await engine.scan_once()
    await engine.work_once()
    await started.wait()
    await engine.delete(job["cronjob_id"])
    gate.set()
    await settle([engine])
    clock.advance(61)
    await engine.scan_once()
    assert [
        (run["state"], run["output"])
        for run in (await engine.runs(job["cronjob_id"]))["items"]
    ] == [("succeeded", "done")]


@pytest.mark.asyncio
async def test_legacy_user_id_defaults_preserve_retries_queued_runs_and_history():
    store, clock, calls = MemoryStore(), Clock(), []

    async def invoke(run):
        calls.append(run)
        return "done"

    engine = Engine(store, invoke, clock=clock)
    body = request(clock, idempotency_key="legacy")
    created = await engine.create(body)
    assert created["user_id"] == DEFAULT_USER_ID
    key = engine.job_key(created["cronjob_id"])
    job, etag = await store.get(key)
    job.pop("user_id")
    old_input = body.durable_input()
    old_input.pop("user_id")
    job["request_hash"] = hashlib.sha256(
        json.dumps(old_input, sort_keys=True).encode()
    ).hexdigest()
    await store.put(key, job, etag)
    assert (await engine.create(body))["user_id"] == DEFAULT_USER_ID
    assert (
        await engine.create(
            request(clock, idempotency_key="legacy", user_id=DEFAULT_USER_ID)
        )
    )["cronjob_id"] == created["cronjob_id"]
    assert (await engine.list_jobs(user_id=DEFAULT_USER_ID))["items"][0][
        "user_id"
    ] == DEFAULT_USER_ID
    with pytest.raises(ValueError, match="different request"):
        await engine.create(request(clock, idempotency_key="legacy", user_id="alice"))

    clock.advance(11)
    await engine.scan_once()
    job, etag = await store.get(key)
    assert job["active"].pop("user_id") == DEFAULT_USER_ID
    await store.put(key, job, etag)
    assert (await engine.result(created["cronjob_id"], job["active"]["run_id"]))[
        "user_id"
    ] == DEFAULT_USER_ID
    await engine.work_once()
    await asyncio.gather(*engine.executions.values())
    assert calls[0]["user_id"] == DEFAULT_USER_ID

    # An older replica may already have written history before acknowledging it
    job, etag = await store.get(key)
    history = copy.deepcopy(job["active"])
    history.pop("user_id")
    history.pop("token")
    history_key = f"runs/{created['cronjob_id']}/{history['run_id']}.json"
    await store.put(history_key, history)
    await engine.scan_once()
    assert (await engine.get(created["cronjob_id"]))[0]["active"] is None
    assert (await engine.runs(created["cronjob_id"]))["items"][0][
        "user_id"
    ] == DEFAULT_USER_ID
    assert (await engine.result(created["cronjob_id"], history["run_id"]))[
        "user_id"
    ] == DEFAULT_USER_ID
    assert "user_id" not in (await store.get(history_key))[0]


@pytest.mark.asyncio
async def test_user_id_api_filter_fills_pages_without_skipping_matches():
    store, clock = MemoryStore(), Clock()
    engine = Engine(store, None, clock=clock)
    app = FastAPI()
    app.include_router(build_router(engine))
    keys = sorted(
        (f"job-{i}" for i in range(6)),
        key=lambda key: hashlib.sha256(key.encode()).hexdigest(),
    )
    expected = []
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for index, key in enumerate(keys):
            body = request(
                clock, user_id="alice+/=" if index % 2 else "bob"
            ).durable_input()
            body["idempotency_key"] = key
            response = await client.post("/harness/cronjobs", json=body)
            assert response.status_code == 201
            job = response.json()
            assert job["user_id"] == body["user_id"]
            detail = await client.get(f"/harness/cronjobs/{job['cronjob_id']}")
            assert detail.json()["user_id"] == body["user_id"]
            if index % 2:
                expected.append(job["cronjob_id"])

        params = {"user_id": "alice+/=", "limit": 2}
        page = (await client.get("/harness/cronjobs", params=params)).json()
        assert [job["cronjob_id"] for job in page["items"]] == expected[:2]
        assert page["next_cursor"]
        page = (
            await client.get(
                "/harness/cronjobs", params={**params, "cursor": page["next_cursor"]}
            )
        ).json()
        assert [job["cronjob_id"] for job in page["items"]] == expected[2:]
        assert page["next_cursor"] == ""
        assert (
            await client.get("/harness/cronjobs", params={"user_id": "missing"})
        ).json() == {"items": [], "next_cursor": ""}
        assert len((await client.get("/harness/cronjobs")).json()["items"]) == 6

        for user_id in ["", " \t", "x" * 257]:
            body["user_id"] = user_id
            assert (
                await client.post("/harness/cronjobs", json=body)
            ).status_code == 422
            assert (
                await client.get("/harness/cronjobs", params={"user_id": user_id})
            ).status_code == 422


@pytest.mark.asyncio
async def test_scheduled_invocation_passes_user_id_and_preserves_run_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib
    import sys
    from veadk.cloud.harness_app.server_types import (
        ArtifactLink,
        InvokeHarnessRequest,
        InvokeHarnessResponse,
    )

    monkeypatch.setenv("HARNESS_CRONJOB_ENABLED", "false")
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_required", lambda: True
    )
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_enabled", lambda: False
    )
    previous = sys.modules.pop("veadk.cloud.harness_app.server", None)
    try:
        server = importlib.import_module("veadk.cloud.harness_app.server")
        requests = []

        artifact = ArtifactLink(
            name="report.txt", url="https://example.com/report.txt", source="publish"
        )

        async def invoke(request: InvokeHarnessRequest) -> InvokeHarnessResponse:
            requests.append(request)
            return InvokeHarnessResponse(
                harness_name="test", output="done", artifacts=[artifact]
            )

        monkeypatch.setattr(server, "invoke_harness", invoke)
        for index, user_id in enumerate(["alice", "bob", None]):
            run = {
                "prompt": "hello",
                "harness": {"model_api_key": "alice-key"} if index == 0 else None,
                "run_id": f"run-{index}",
            }
            if user_id is not None:
                run["user_id"] = user_id
            assert await server._invoke_scheduled(run) == (
                "done",
                [artifact.model_dump()],
            )
            assert requests[-1].run_agent_request.user_id == (
                user_id or DEFAULT_USER_ID
            )
            assert requests[-1].run_agent_request.session_id == run["run_id"]
            if index == 0:
                assert (
                    requests[-1].harness.model_api_key.get_secret_value() == "alice-key"
                )
    finally:
        sys.modules.pop("veadk.cloud.harness_app.server", None)
        if previous is not None:
            sys.modules["veadk.cloud.harness_app.server"] = previous


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model_key,error",
    [
        ("private-key", "invalid model key private-key"),
        ("", "model service unavailable"),
    ],
)
async def test_harness_invoke_sanitizes_user_model_key(
    monkeypatch, caplog, model_key, error
):
    import importlib
    import sys
    from types import SimpleNamespace

    from veadk.cloud.harness_app.server_types import InvokeHarnessRequest
    import veadk.runner

    monkeypatch.setenv("HARNESS_CRONJOB_ENABLED", "false")
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_required", lambda: True
    )
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_enabled", lambda: False
    )
    previous = sys.modules.pop("veadk.cloud.harness_app.server", None)
    try:
        server = importlib.import_module("veadk.cloud.harness_app.server")
        monkeypatch.setattr(
            server, "harness_config", SimpleNamespace(max_llm_calls=None)
        )
        monkeypatch.setattr(server, "spawn_harness_agent", lambda *_, **__: object())
        runner_calls = []

        class FailingRunner:
            def __init__(self, **_kwargs):
                pass

            async def run(self, **kwargs):
                runner_calls.append(kwargs)
                raise RuntimeError(error)

        monkeypatch.setattr(veadk.runner, "Runner", FailingRunner)
        request = InvokeHarnessRequest(
            prompt="hello",
            harness_name="harness",
            harness={"model_api_key": model_key},
            run_agent_request={"user_id": "alice", "session_id": "run-id"},
        )
        with caplog.at_level("ERROR", logger="harness"):
            response = await server.invoke_harness(request)
        assert runner_calls[0]["session_id"] == "run-id"
        assert runner_calls[0]["user_id"] == "alice"
        assert response.error == ("invalid model key ********" if model_key else error)
        if model_key:
            assert "private-key" not in caplog.text
    finally:
        sys.modules.pop("veadk.cloud.harness_app.server", None)
        if previous is not None:
            sys.modules["veadk.cloud.harness_app.server"] = previous


@pytest.mark.asyncio
async def test_120_simultaneous_jobs_across_four_replicas_execute_exactly_once():
    store, clock, gate = MemoryStore(), Clock(), asyncio.Event()
    started = []

    async def invoke(run):
        started.append(run["run_id"])
        await gate.wait()
        return run["prompt"]

    engines = [
        Engine(store, invoke, concurrency=40, clock=clock, replica_id=f"replica-{i}")
        for i in range(4)
    ]
    jobs = await asyncio.gather(
        *(
            engines[0].create(request(clock, name=f"job-{i}", prompt=f"answer-{i}"))
            for i in range(120)
        )
    )
    clock.advance(11)
    await asyncio.gather(*(engine.scan_once() for engine in engines))
    await asyncio.gather(*(engine.work_once() for engine in engines))
    await asyncio.sleep(0)
    assert len(started) == len(set(started)) == 120
    assert all(len(engine.executions) <= 40 for engine in engines)
    gate.set()
    await settle(engines)
    for i, job in enumerate(jobs):
        runs = (await engines[0].runs(job["cronjob_id"]))["items"]
        assert len(runs) == 1
        assert runs[0]["state"] == "succeeded"
        assert runs[0]["output"] == f"answer-{i}"
    await asyncio.gather(*(engine.work_once() for engine in engines))
    assert len(started) == 120


@pytest.mark.asyncio
async def test_crash_after_queue_commit_resumes_without_duplicate_or_lost_schedule():
    store, clock = MemoryStore(), Clock()
    calls = []

    async def invoke(run):
        calls.append(run["run_id"])
        return "done"

    before, after = (
        Engine(store, invoke, clock=clock),
        Engine(store, invoke, clock=clock),
    )
    job = await before.create(request(clock))
    clock.advance(11)
    store.lose_response = True
    await before.scan_once()
    assert (await before.get(job["cronjob_id"]))[0]["active"]["state"] == "queued"
    await after.work_once()
    await settle([after])
    assert len(calls) == 1
    assert (await after.runs(job["cronjob_id"]))["items"][0]["state"] == "succeeded"


@pytest.mark.asyncio
async def test_expired_worker_cannot_write_success_or_restart_unknown_execution():
    store, clock, gate = MemoryStore(), Clock(), asyncio.Event()
    calls = []

    async def invoke(run):
        calls.append(run["run_id"])
        await gate.wait()
        return "stale success"

    old = Engine(store, invoke, clock=clock, lease_seconds=6)
    new = Engine(store, invoke, clock=clock, lease_seconds=6)
    job = await old.create(request(clock))
    clock.advance(11)
    await old.scan_once()
    await old.work_once()
    await asyncio.sleep(0)
    clock.advance(7)
    await new.scan_once()
    await new.scan_once()
    await new.work_once()
    gate.set()
    await settle([old, new])
    state = (await new.get(job["cronjob_id"]))[0]
    assert state["enabled"] is False
    run = (await new.runs(job["cronjob_id"]))["items"][0]
    assert run["state"] == "interrupted" and run["output"] == ""
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_history_outage_keeps_terminal_record_until_archived():
    store, clock = MemoryStore(), Clock()

    async def invoke(run):
        return "durable answer"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    store.fail_history = True
    await settle([engine])
    state = (await engine.get(job["cronjob_id"]))[0]
    assert state["active"]["state"] == "succeeded"
    assert (await engine.result(job["cronjob_id"], state["active"]["run_id"]))[
        "output"
    ] == "durable answer"
    store.fail_history = False
    await engine.scan_once()
    assert (await engine.get(job["cronjob_id"]))[0]["active"] is None
    assert len((await engine.runs(job["cronjob_id"]))["items"]) == 1


@pytest.mark.asyncio
async def test_overlapping_occurrence_skipped_and_history_is_newest_first():
    store, clock, gate = MemoryStore(), Clock(), asyncio.Event()

    async def invoke(run):
        await gate.wait()
        return "first"

    engine = Engine(store, invoke, clock=clock, lease_seconds=600)
    job = await engine.create(
        request(clock, schedule={"cron": "* * * * *", "timezone": "UTC"})
    )
    clock.advance(60)
    await engine.scan_once()
    await engine.work_once()
    clock.advance(60)
    await engine.scan_once()
    await engine.scan_once()
    gate.set()
    await settle([engine])
    runs = (await engine.runs(job["cronjob_id"]))["items"]
    assert [run["state"] for run in runs] == ["skipped", "succeeded"]
    assert instant(
        (await engine.get(job["cronjob_id"]))[0]["next_run_at"]
    ) == clock() + timedelta(seconds=60)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("recovery_delay", "grace", "expected_state"),
    [
        (600, 30, "succeeded"),
        (601, 30, "succeeded"),
        (630, 30, "succeeded"),
        (631, 30, "skipped"),
        (600, 0, "succeeded"),
        (601, 0, "skipped"),
    ],
)
async def test_misfire_grace_uses_latest_occurrence_after_outage(
    recovery_delay, grace, expected_state
):
    store, clock = MemoryStore(), Clock()
    created_at, calls = clock(), []

    async def invoke(run):
        calls.append(run["cronjob_id"])
        return "caught up"

    engines = [Engine(store, invoke, clock=clock) for _ in range(2)]
    engine = engines[0]
    latest = await engine.create(request(clock, schedule={"cron": "* * * * *"}))
    skip = await engine.create(
        request(
            clock,
            schedule={"cron": "* * * * *"},
            misfire_policy="skip",
            misfire_grace_seconds=grace,
        )
    )
    clock.advance(recovery_delay)
    await asyncio.gather(*(replica.scan_once() for replica in engines))
    await asyncio.gather(*(replica.work_once() for replica in engines))
    await settle(engines)
    for job, state in [(latest, "succeeded"), (skip, expected_state)]:
        job_id = job["cronjob_id"]
        runs = (await engine.runs(job_id))["items"]
        assert len(runs) == 1
        assert runs[0]["state"] == state
        assert instant(runs[0]["scheduled_at"]) == created_at + timedelta(seconds=600)
        assert instant(runs[0]["coalesced_from"]) == created_at + timedelta(seconds=60)
        assert calls.count(job_id) == (1 if state == "succeeded" else 0)
        assert instant(
            (await engine.get(job_id))[0]["next_run_at"]
        ) == created_at + timedelta(seconds=660)

    clock.advance(660 - recovery_delay)
    await asyncio.gather(*(replica.scan_once() for replica in engines))
    await asyncio.gather(*(replica.work_once() for replica in engines))
    await settle(engines)
    for job, previous_state in [(latest, "succeeded"), (skip, expected_state)]:
        job_id = job["cronjob_id"]
        runs = (await engine.runs(job_id))["items"]
        assert [run["state"] for run in runs] == ["succeeded", previous_state]
        assert instant(runs[0]["scheduled_at"]) == created_at + timedelta(seconds=660)
        assert calls.count(job_id) == (2 if previous_state == "succeeded" else 1)


@pytest.mark.asyncio
async def test_creation_idempotency_and_override_snapshot_survive_replicas():
    store, clock = MemoryStore(), Clock()
    calls = []

    async def invoke(run):
        calls.append(run)
        return "ok"

    engine = Engine(store, invoke, clock=clock)
    body = request(
        clock,
        idempotency_key="stable",
        harness={
            "system_prompt": "specific",
            "model_name": "model-a",
            "tools": "",
            "mcp_servers": [],
        },
    )
    jobs = await asyncio.gather(*(engine.create(body) for _ in range(10)))
    assert len({job["cronjob_id"] for job in jobs}) == 1
    with pytest.raises(ValueError, match="different request"):
        await engine.create(
            request(clock, idempotency_key="stable", prompt="different")
        )
    default = await engine.create(request(clock))
    clock.advance(11)
    assert (await engine.create(body))["cronjob_id"] == jobs[0]["cronjob_id"]
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    assert len(calls) == 2
    assert (
        next(run for run in calls if run["cronjob_id"] == default["cronjob_id"])[
            "harness"
        ]
        is None
    )
    overridden = next(run for run in calls if run["harness"] is not None)
    assert overridden["harness"] == body.durable_input()["harness"]


@pytest.mark.parametrize(
    "expression,zone,after,expected",
    [
        (
            "0 9 * * 1-5",
            "Asia/Shanghai",
            "2026-09-18T02:00:00Z",
            "2026-09-21T01:00:00Z",
        ),
        (
            "*/15 9-17 * * 1-5",
            "Asia/Shanghai",
            "2026-09-16T01:07:00Z",
            "2026-09-16T01:15:00Z",
        ),
        ("0 0 L * *", "UTC", "2026-02-01T00:00:00Z", "2026-02-28T00:00:00Z"),
        (
            "0 9 * * *",
            "America/New_York",
            "2026-03-07T15:00:00Z",
            "2026-03-08T13:00:00Z",
        ),
    ],
)
def test_complex_schedules(expression, zone, after, expected):
    assert Schedule(cron=expression, timezone=zone).next_after(
        instant(after)
    ) == instant(expected)


@pytest.mark.asyncio
async def test_api_validation_pagination_and_secret_redaction():
    store, clock = MemoryStore(), Clock()
    engine = Engine(store, None, clock=clock)
    app = FastAPI()
    app.include_router(build_router(engine))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        body = request(
            clock,
            harness={
                "mcp_servers": [
                    {"endpoint": "https://example.com/mcp", "api_key": "private-key"}
                ]
            },
        ).durable_input()
        first = await client.post("/harness/cronjobs", json=body)
        assert first.status_code == 201
        assert "private-key" not in first.text
        await client.post("/harness/cronjobs", json=body)
        page = (await client.get("/harness/cronjobs?limit=1")).json()
        assert len(page["items"]) == 1 and page["next_cursor"]
        next_page = (
            await client.get(
                "/harness/cronjobs", params={"limit": 1, "cursor": page["next_cursor"]}
            )
        ).json()
        assert next_page["items"][0]["cronjob_id"] != page["items"][0]["cronjob_id"]
        body["harness"] = {"unknown_option": True}
        assert (await client.post("/harness/cronjobs", json=body)).status_code == 422
        assert (await client.get("/harness/cronjobs/invalid/runs")).status_code == 422


@pytest.mark.asyncio
async def test_live_lease_renews_and_cancellation_stops_execution():
    store, gate = MemoryStore(), asyncio.Event()

    async def invoke(run):
        await gate.wait()
        return "unexpected"

    engine = Engine(store, invoke, lease_seconds=6)
    job = await engine.create(
        CreateJob(
            name="renew",
            prompt="wait",
            schedule={"at": datetime.now(timezone.utc) + timedelta(milliseconds=100)},
        )
    )
    await asyncio.sleep(0.15)
    await engine.scan_once()
    await engine.work_once()
    first = (await engine.get(job["cronjob_id"]))[0]["active"]
    await asyncio.sleep(6.2)
    await engine.scan_once()
    current = (await engine.get(job["cronjob_id"]))[0]
    assert current["enabled"] and current["active"]["state"] == "running"
    assert current["active"]["lease_until"] > first["lease_until"]
    await engine.cancel(job["cronjob_id"], first["run_id"])
    await asyncio.wait_for(settle([engine]), timeout=4)
    assert (await engine.runs(job["cronjob_id"]))["items"][0]["state"] == "cancelled"


@pytest.mark.asyncio
async def test_lost_completion_response_does_not_execute_twice():
    store, clock = MemoryStore(), Clock()
    calls = []

    async def invoke(run):
        calls.append(run["run_id"])
        store.lose_response = True
        return "committed"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    await engine.work_once()
    assert len(calls) == 1
    assert (await engine.runs(job["cronjob_id"]))["items"][0]["output"] == "committed"


@pytest.mark.asyncio
async def test_timeout_and_graceful_shutdown_have_durable_terminal_states():
    store, clock = MemoryStore(), Clock()

    async def invoke(run):
        await asyncio.Event().wait()

    engine = Engine(store, invoke, clock=clock)
    timed = await engine.create(request(clock, timeout_seconds=1))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    assert (await engine.runs(timed["cronjob_id"]))["items"][0][
        "error"
    ] == "Execution timed out"
    stopped = await engine.create(request(clock))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await asyncio.sleep(0)
    await engine.close()
    await engine.scan_once()
    assert (await engine.get(stopped["cronjob_id"]))[0]["enabled"] is False
    assert (await engine.runs(stopped["cronjob_id"]))["items"][0][
        "state"
    ] == "interrupted"
