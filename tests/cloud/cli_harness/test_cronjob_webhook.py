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

"""Webhook 契约、外部 HTTP 边界与多副本完成提交的回归测试。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
import ssl
import subprocess
from pathlib import Path
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import httpx
import pytest
from jsonschema import Draft202012Validator, FormatChecker
from fastapi import FastAPI
from pydantic import ValidationError

from veadk.cloud.harness_app.cronjobs import webhook
from veadk.cloud.harness_app.cronjobs.engine import Engine
from veadk.cloud.harness_app.cronjobs.models import DEFAULT_USER_ID, PatchJob
from veadk.cloud.harness_app.cronjobs.routes import build_router
from veadk.cloud.harness_app.cronjobs.store import Conflict
from veadk.cloud.harness_app.cronjobs.webhook_config import (
    WebhookConfig,
    WebhookPatch,
    authorize_webhook,
)
from .test_cronjobs import Clock, MemoryStore, request, settle

URL = "https://receiver.example/hook"


@pytest.fixture(autouse=True)
def allowed_receiver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HARNESS_CRONJOB_WEBHOOK_ALLOWED_HOSTS", "receiver.example")
    monkeypatch.setenv("HARNESS_CRONJOB_WEBHOOK_TOKEN_ENVS", "HOOK_TOKEN")


def payload() -> dict:
    return {"taskID": "task", "runID": "run", "eventID": "a" * 64}


@pytest.mark.parametrize(
    "changes",
    [
        {"url": "http://receiver.example"},
        {"url": "https://user:pass@receiver.example"},
        {"url": "https://receiver.example/#fragment"},
        {"url": "https://receiver.example/ a"},
        {"url": "https://receiver.example\\@evil.example"},
        {"url": "https://receiver.example:bad"},
        {"url": None},
        {"timeout_seconds": 0},
        {"timeout_seconds": 31},
        {"timeout_seconds": True},
        {"max_retries": -1},
        {"max_retries": 4},
        {"max_retries": "3"},
        {"bearer_token_env": "BAD-NAME"},
        {"bearer_token": "unsupported"},
    ],
)
def test_configuration_rejects_invalid_input(changes: dict) -> None:
    with pytest.raises(ValidationError):
        WebhookConfig.model_validate({"url": URL, **changes})


def test_defaults_partial_patch_and_authorization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert WebhookConfig(url=URL).model_dump() == {
        "url": URL,
        "timeout_seconds": 10,
        "max_retries": 3,
        "bearer_token_env": None,
        "include_artifacts": False,
    }
    assert WebhookPatch(max_retries=0).model_dump(exclude_unset=True) == {
        "max_retries": 0
    }
    with pytest.raises(ValidationError):
        WebhookPatch()
    with pytest.raises(ValueError, match="host"):
        authorize_webhook(WebhookConfig(url="https://evil.example"))
    with pytest.raises(ValueError, match="reference"):
        authorize_webhook(WebhookConfig(url=URL, bearer_token_env="MODEL_API_KEY"))
    monkeypatch.delenv("HARNESS_CRONJOB_WEBHOOK_ALLOWED_HOSTS")
    with pytest.raises(ValueError, match="host"):
        authorize_webhook(WebhookConfig(url=URL))


@pytest.mark.asyncio
async def test_api_add_merge_clear_delete_and_validation() -> None:
    clock, store = Clock(), MemoryStore()
    engine = Engine(store, None, clock=clock)
    app = FastAPI()
    app.include_router(build_router(engine))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        created = await client.post(
            "/harness/cronjobs", json=request(clock).durable_input()
        )
        path = f"/harness/cronjobs/{created.json()['cronjob_id']}/webhook"
        assert (await client.patch(path, json={"max_retries": 0})).status_code == 422
        added = await client.patch(
            path, json={"url": URL, "bearer_token_env": "HOOK_TOKEN"}
        )
        assert added.status_code == 200
        assert added.json()["webhook"]["timeout_seconds"] == 10
        merged = await client.patch(
            path, json={"timeout_seconds": 30, "bearer_token_env": None}
        )
        assert merged.json()["webhook"] == {
            "url": URL,
            "timeout_seconds": 30,
            "max_retries": 3,
            "bearer_token_env": None,
            "include_artifacts": False,
        }
        for body in [
            {},
            {"url": None},
            {"url": "https://evil.example"},
            {"max_retries": 4},
        ]:
            assert (await client.patch(path, json=body)).status_code == 422
        removed = await client.delete(path)
        assert "webhook" not in removed.json()
        assert (await client.delete(path)).json()["revision"] == removed.json()[
            "revision"
        ]
        assert (
            await client.patch(
                f"/harness/cronjobs/{'0' * 32}/webhook", json={"url": URL}
            )
        ).status_code == 404
        original = request(
            clock, webhook={"url": URL}, idempotency_key="webhook-create"
        ).durable_input()
        original["idempotency_key"] = "webhook-create"
        first = await client.post("/harness/cronjobs", json=original)
        assert first.status_code == 201
        assert (await client.post("/harness/cronjobs", json=original)).json()[
            "cronjob_id"
        ] == first.json()["cronjob_id"]


@pytest.mark.asyncio
async def test_webhook_cas_preserves_concurrent_fields_and_reports_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock, store = Clock(), MemoryStore()
    engine = Engine(store, None, clock=clock)
    job = await engine.create(request(clock, webhook={"url": URL}))
    await asyncio.gather(
        engine.set_webhook(job["cronjob_id"], WebhookPatch(timeout_seconds=30)),
        engine.set_webhook(job["cronjob_id"], WebhookPatch(max_retries=0)),
    )
    stored = (await engine.get(job["cronjob_id"]))[0]
    assert (
        stored["webhook"]["timeout_seconds"] == 30
        and stored["webhook"]["max_retries"] == 0
    )
    monkeypatch.setattr(store, "put", AsyncMock(side_effect=Conflict()))
    with pytest.raises(Conflict):
        await engine.set_webhook(job["cronjob_id"], WebhookPatch(max_retries=1))


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["succeeded", "failed", "timeout"])
async def test_completion_snapshot_and_delivery_after_commit(
    outcome: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock, store, deliveries = Clock(), MemoryStore(), []

    async def invoke(run: dict) -> str:
        if outcome == "timeout":
            await asyncio.Event().wait()
        if outcome == "failed":
            raise RuntimeError("agent failure")
        return "回答"

    async def receive(config: dict, event: dict) -> None:
        current = (await engine.get(event["taskID"]))[0]["active"]
        assert (
            current["state"] == event["status"]
            and current["completed_at"] == event["completedAt"]
        )
        deliveries.append((config, event))

    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receive
    )
    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(
        request(clock, webhook={"url": URL}, user_id="alice", timeout_seconds=1)
    )
    clock.advance(11)
    await engine.scan_once()
    # 删除任务上的配置不会改变已排队快照。
    await engine.set_webhook(job["cronjob_id"], None)
    await engine.patch(job["cronjob_id"], PatchJob(name="renamed while queued"))
    replicas = [engine, Engine(store, invoke, clock=clock)]
    await asyncio.gather(*(replica.work_once() for replica in replicas))
    await settle(replicas)
    assert len(deliveries) == 1
    config, event = deliveries[0]
    schema = json.loads(
        (Path(__file__).parent / "schemas" / "cronjob-webhook.schema.json").read_text()
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(event)
    assert config["url"] == URL and event["target"] == {
        "userID": "alice",
        "sessionID": event["runID"],
        "agentID": "harness_agent",
    }
    assert event["taskName"] == "renamed while queued"
    assert event["status"] == ("succeeded" if outcome == "succeeded" else "failed")
    assert event["taskType"] == "once" and event["nextRunAt"] is None
    assert (
        event["eventID"]
        == hashlib.sha256(
            f"cronjob.completed:{event['taskID']}:{event['runID']}".encode()
        ).hexdigest()
    )
    if outcome == "succeeded":
        assert event["result"] == {"output": "回答", "error": None}
    else:
        assert event["result"]["output"] == "" and event["result"]["error"]
    if outcome == "timeout":
        assert event["result"]["error"] == "Execution timed out"
    await Engine(store, invoke, clock=clock).scan_once()
    assert len(deliveries) == 1


@pytest.mark.asyncio
async def test_deleting_running_task_still_delivers_its_completion_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock, store, gate, started, deliveries = (
        Clock(),
        MemoryStore(),
        asyncio.Event(),
        asyncio.Event(),
        [],
    )

    async def invoke(run: dict) -> str:
        started.set()
        await gate.wait()
        return "done"

    async def receive(config: dict, event: dict) -> None:
        deliveries.append(event)

    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receive
    )
    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock, webhook={"url": URL}))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await started.wait()
    await engine.delete(job["cronjob_id"])
    gate.set()
    await settle([engine])
    assert len(deliveries) == 1
    assert deliveries[0]["status"] == "succeeded" and deliveries[0]["nextRunAt"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["lost_response", "lost_lease", "delivery"])
async def test_uncertain_completion_never_replays_and_delivery_does_not_change_result(
    failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock, store = Clock(), MemoryStore()
    receiver = AsyncMock(
        side_effect=RuntimeError("receiver unavailable")
        if failure == "delivery"
        else None
    )
    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receiver
    )

    async def invoke(run: dict) -> str:
        if failure == "lost_response":
            store.lose_response = True
        if failure == "lost_lease":
            clock.advance(61)
        return "committed result"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock, webhook={"url": URL}))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    await engine.scan_once()
    await Engine(store, invoke, clock=clock).scan_once()
    assert receiver.await_count == (1 if failure == "delivery" else 0)
    runs = (await engine.runs(job["cronjob_id"]))["items"]
    if failure != "lost_lease":
        assert (
            runs[0]["state"] == "succeeded" and runs[0]["output"] == "committed result"
        )
    else:
        assert runs[0]["state"] == "interrupted"


@pytest.mark.asyncio
async def test_cancelled_skipped_interrupted_runs_and_legacy_tasks_do_not_notify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receiver = AsyncMock()
    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receiver
    )
    clock, store = Clock(), MemoryStore()

    async def invoke(run: dict) -> str:
        return "done"

    engine = Engine(store, invoke, clock=clock)
    legacy = await engine.create(request(clock))
    assert "webhook" not in request(clock).durable_input()
    cancelled = await engine.create(request(clock, webhook={"url": URL}))
    skipped = await engine.create(
        request(
            clock, webhook={"url": URL}, misfire_policy="skip", misfire_grace_seconds=0
        )
    )
    clock.advance(11)
    await engine.scan_once()
    active = (await engine.get(cancelled["cronjob_id"]))[0]["active"]
    await engine.cancel(cancelled["cronjob_id"], active["run_id"])
    await engine.work_once()
    await settle([engine])
    assert receiver.await_count == 0
    history = (await engine.runs(legacy["cronjob_id"]))["items"][0]
    assert "webhook" not in history and "schedule" not in history
    assert (await engine.runs(skipped["cronjob_id"]))["items"][0]["state"] == "skipped"


def test_cron_payload_disabled_pointer_default_user_and_failure_fallback() -> None:
    run = {
        "schedule": {"cron": "* * * * *", "timezone": "UTC"},
        "cronjob_id": "task",
        "run_id": "run",
        "task_name": "name",
        "scheduled_at": "2026-10-14T00:00:00Z",
        "started_at": "2026-10-14T00:00:01Z",
        "completed_at": "2026-10-14T00:01:01Z",
        "state": "failed",
        "output": "discard",
        "error": "",
    }
    job = {"enabled": True, "next_run_at": "2026-10-14T00:01:00Z"}
    event = webhook.completion_payload(run, job)
    assert (
        event["taskType"] == "cron" and event["schedule"]["expression"] == "* * * * *"
    )
    assert event["schemaVersion"] == "2" and event["target"]["sessionID"] == "run"
    assert event["target"]["agentID"] == "harness_agent"
    assert (
        event["nextRunAt"] == job["next_run_at"]
        and event["target"]["userID"] == DEFAULT_USER_ID
    )
    assert event["result"] == {"output": "", "error": "Execution failed"}
    assert (
        webhook.completion_payload(run, {**job, "enabled": False})["nextRunAt"] is None
    )


def test_v2_schema_requires_execution_target_and_v1_contract_remains_available() -> (
    None
):
    current = json.loads(
        (Path(__file__).parent / "schemas" / "cronjob-webhook.schema.json").read_text()
    )
    previous = json.loads(
        (
            Path(__file__).parent / "schemas" / "cronjob-webhook.v1.schema.json"
        ).read_text()
    )
    assert current["properties"]["schemaVersion"]["const"] == "2"
    assert previous["properties"]["schemaVersion"]["const"] == "1"
    assert {"sessionID", "agentID"} <= set(current["properties"]["target"]["required"])
    assert (
        webhook.response_issue(
            json.dumps(
                {
                    "schemaVersion": "1",
                    "eventID": "a" * 64,
                    "accepted": True,
                    "error": None,
                }
            ).encode(),
            200,
            "a" * 64,
        )
        == "invalid_response_schema"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,attempts",
    [
        (200, 4),
        (202, 4),
        (204, 4),
        (301, 1),
        (400, 1),
        (401, 1),
        (403, 1),
        (429, 4),
        (500, 4),
        (503, 4),
    ],
)
async def test_http_status_and_ack_control_retry(
    status: int, attempts: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[httpx.Request] = []

    async def handle(req: httpx.Request) -> httpx.Response:
        captured.append(req)
        return httpx.Response(
            status, content=b"invalid", headers={"Location": "https://evil.example"}
        )

    factory = httpx.AsyncClient
    monkeypatch.setattr(
        webhook.httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(handle), **kwargs),
    )
    sleep = AsyncMock()
    monkeypatch.setattr(webhook.asyncio, "sleep", sleep)
    await webhook.deliver_webhook({"url": URL}, payload())
    assert len(captured) == attempts
    assert all("Authorization" not in req.headers for req in captured)
    assert [call.args[0] for call in sleep.await_args_list] == (
        [1, 2, 4] if attempts == 4 else []
    )
    assert len({req.content for req in captured}) == 1 and all(
        req.method == "POST" for req in captured
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_body,issue",
    [
        (b"invalid", "invalid_response_schema"),
        (b"", "invalid_response_schema"),
        (
            json.dumps(
                {
                    "schemaVersion": "2",
                    "eventID": "b" * 64,
                    "accepted": True,
                    "error": None,
                }
            ).encode(),
            "mismatched_event_id",
        ),
        (
            json.dumps(
                {
                    "schemaVersion": "2",
                    "eventID": None,
                    "accepted": False,
                    "error": {"code": "REJECTED", "message": "rejected"},
                }
            ).encode(),
            "inconsistent_response_status",
        ),
        (b"x" * 20000, "response_too_large"),
    ],
    ids=["invalid-json", "empty-200", "wrong-event", "rejected", "oversized"],
)
@pytest.mark.parametrize("max_retries,recover", [(0, False), (3, False), (3, True)])
async def test_invalid_success_ack_retries_or_fails(
    bad_body: bytes,
    issue: str,
    max_retries: int,
    recover: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """协议错误不能记为成功；重试保留同一事件，合法 ACK 才结束。"""
    captured: list[httpx.Request] = []

    async def handle(req: httpx.Request) -> httpx.Response:
        captured.append(req)
        if recover and len(captured) == 2:
            return httpx.Response(
                200,
                json={
                    "schemaVersion": "2",
                    "eventID": payload()["eventID"],
                    "accepted": True,
                    "error": None,
                },
            )
        return httpx.Response(200, content=bad_body)

    factory = httpx.AsyncClient
    monkeypatch.setattr(
        webhook.httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(handle), **kwargs),
    )
    sleep = AsyncMock()
    monkeypatch.setattr(webhook.asyncio, "sleep", sleep)
    caplog.set_level("INFO", logger=webhook.logger.name)
    await webhook.deliver_webhook({"url": URL, "max_retries": max_retries}, payload())
    attempts = 2 if recover else max_retries + 1
    assert len(captured) == attempts and len({req.content for req in captured}) == 1
    assert [call.args[0] for call in sleep.await_args_list] == (
        [1] if recover else [1, 2, 4][:max_retries]
    )
    records = [
        record for record in caplog.records if record.name == webhook.logger.name
    ]
    assert [record.delivery for record in records] == ["retrying"] * (attempts - 1) + [
        "delivered" if recover else "delivery_failed"
    ]
    assert [record.protocol_issue for record in records] == [issue] * (attempts - 1) + [
        None if recover else issue
    ]
    assert all(
        record.levelname == "WARNING" for record in records[: attempts - int(recover)]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 202, 204])
async def test_valid_success_ack_stops_after_one_request(
    status: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """合法 ACK 与空 204 保持一次请求成功，不产生退避。"""
    handle = AsyncMock(
        return_value=httpx.Response(
            status,
            content=b""
            if status == 204
            else json.dumps(
                {
                    "schemaVersion": "2",
                    "eventID": payload()["eventID"],
                    "accepted": True,
                    "error": None,
                }
            ).encode(),
        )
    )
    factory = httpx.AsyncClient
    monkeypatch.setattr(
        webhook.httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(handle), **kwargs),
    )
    sleep = AsyncMock()
    monkeypatch.setattr(webhook.asyncio, "sleep", sleep)
    await webhook.deliver_webhook({"url": URL}, payload())
    assert handle.await_count == 1
    sleep.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [httpx.ConnectError, httpx.ReadTimeout, TimeoutError]
)
async def test_transport_failure_then_success_and_fixed_bearer(
    failure: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    token, captured = secrets.token_urlsafe(), []
    monkeypatch.setenv("HOOK_TOKEN", token)

    async def handle(req: httpx.Request) -> httpx.Response:
        captured.append(req)
        if len(captured) == 1:
            raise failure(token)
        return httpx.Response(
            200,
            json={
                "schemaVersion": "2",
                "eventID": payload()["eventID"],
                "accepted": True,
                "error": None,
            },
        )

    factory = httpx.AsyncClient
    monkeypatch.setattr(
        webhook.httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(handle), **kwargs),
    )
    monkeypatch.setattr(webhook.asyncio, "sleep", AsyncMock())
    await webhook.deliver_webhook(
        {"url": URL, "bearer_token_env": "HOOK_TOKEN"}, payload()
    )
    assert len(captured) == 2 and captured[0].content == captured[1].content
    assert all(req.headers["Authorization"] == f"Bearer {token}" for req in captured)
    assert token not in caplog.text and token not in repr(
        [record.__dict__ for record in caplog.records]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "", "\r\n", "非ASCII"])
async def test_invalid_bearer_fails_closed_without_requests(
    value: str | None, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    if value is not None:
        monkeypatch.setenv("HOOK_TOKEN", value)
    else:
        monkeypatch.delenv("HOOK_TOKEN", raising=False)
    client = AsyncMock()
    monkeypatch.setattr(webhook.httpx, "AsyncClient", client)
    await webhook.deliver_webhook(
        {"url": URL, "bearer_token_env": "HOOK_TOKEN"}, payload()
    )
    client.assert_not_called()
    assert "configuration rejected" in caplog.text


@pytest.mark.parametrize(
    "body,status,issue",
    [
        (b"", 204, None),
        (b"invalid", 200, "invalid_response_schema"),
        (
            {
                "schemaVersion": "2",
                "eventID": "b" * 64,
                "accepted": True,
                "error": None,
            },
            200,
            "mismatched_event_id",
        ),
        (
            {"schemaVersion": "2", "eventID": None, "accepted": True, "error": None},
            200,
            "invalid_response_schema",
        ),
        (
            {
                "schemaVersion": "2",
                "eventID": "a" * 64,
                "accepted": True,
                "error": None,
            },
            500,
            "inconsistent_response_status",
        ),
        (
            {
                "schemaVersion": "2",
                "eventID": None,
                "accepted": False,
                "error": {"code": "REJECTED", "message": "rejected"},
            },
            400,
            None,
        ),
        (
            {"schemaVersion": "2", "eventID": None, "accepted": False, "error": None},
            400,
            "invalid_response_schema",
        ),
    ],
)
def test_response_contract(body: dict | bytes, status: int, issue: str | None) -> None:
    raw = json.dumps(body).encode() if isinstance(body, dict) else body
    assert webhook.response_issue(raw, status, "a" * 64) == issue


class SlowResponse(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        for _ in range(100):
            await asyncio.sleep(0.02)
            yield b"x"


@pytest.mark.asyncio
async def test_response_limit_total_deadline_and_cancellation() -> None:
    config = WebhookConfig(url=URL, timeout_seconds=1)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda req: httpx.Response(200, content=b"x" * 20000)
        )
    ) as client:
        assert await webhook.request_once(client, config, b"{}", {}, "a" * 64) == (
            200,
            "response_too_large",
        )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda req: httpx.Response(200, stream=SlowResponse())
        )
    ) as client:
        with pytest.raises(TimeoutError):
            await webhook.request_once(client, config, b"{}", {}, "a" * 64)
        task = asyncio.create_task(
            webhook.request_once(client, config, b"{}", {}, "a" * 64)
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def tls_contexts(directory: Path) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    cert, key = directory / "cert.pem", directory / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(cert, key)
    return server, ssl.create_default_context(cafile=str(cert))


@pytest.mark.asyncio
async def test_real_https_post_bearer_retry_and_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server_tls, client_tls = tls_contexts(tmp_path)
    captured: list[tuple[bytes, bytes]] = []

    async def receive(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 3)
            length = int(
                next(
                    line.split(b":", 1)[1]
                    for line in head.split(b"\r\n")
                    if line.lower().startswith(b"content-length:")
                )
            )
            captured.append((head, await reader.readexactly(length)))
            status = (
                b"503 Service Unavailable" if len(captured) == 1 else b"204 No Content"
            )
            writer.write(
                b"HTTP/1.1 "
                + status
                + b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receive, "127.0.0.1", 0, ssl=server_tls)
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setenv("HARNESS_CRONJOB_WEBHOOK_ALLOWED_HOSTS", "127.0.0.1")
    token = secrets.token_urlsafe()
    monkeypatch.setenv("HOOK_TOKEN", token)
    factory = httpx.AsyncClient
    monkeypatch.setattr(
        webhook.httpx,
        "AsyncClient",
        lambda **kwargs: factory(verify=client_tls, **kwargs),
    )
    try:
        await webhook.deliver_webhook(
            {"url": f"https://127.0.0.1:{port}/hook", "bearer_token_env": "HOOK_TOKEN"},
            payload(),
        )
    finally:
        server.close()
        await server.wait_closed()
    assert len(captured) == 2 and captured[0][1] == captured[1][1]
    assert all(
        head.startswith(b"POST /hook HTTP/1.1")
        and f"Authorization: Bearer {token}".encode() in head
        for head, _ in captured
    )
    assert json.loads(captured[0][1]) == payload()


def test_published_response_schema_matches_runtime_contract() -> None:
    schema = json.loads(
        (
            Path(__file__).parent / "schemas" / "cronjob-webhook-response.schema.json"
        ).read_text()
    )
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    valid = {"schemaVersion": "2", "eventID": "a" * 64, "accepted": True, "error": None}
    validator.validate(valid)
    for changes in [
        {"eventID": None},
        {"accepted": False},
        {"error": {"code": "ERROR", "message": "error"}},
    ]:
        assert list(validator.iter_errors({**valid, **changes}))
        with pytest.raises(ValidationError):
            webhook.WebhookResponse.model_validate({**valid, **changes})


@pytest.mark.asyncio
@pytest.mark.parametrize("retries", [0, 1, 2])
async def test_configured_retry_budget(
    retries: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[httpx.Request] = []

    def receive(req: httpx.Request) -> httpx.Response:
        calls.append(req)
        return httpx.Response(503)

    factory = httpx.AsyncClient
    monkeypatch.setattr(
        webhook.httpx,
        "AsyncClient",
        lambda **kwargs: factory(transport=httpx.MockTransport(receive), **kwargs),
    )
    monkeypatch.setattr(webhook.asyncio, "sleep", AsyncMock())
    await webhook.deliver_webhook({"url": URL, "max_retries": retries}, payload())
    assert len(calls) == retries + 1


@pytest.mark.asyncio
async def test_cron_configuration_updates_only_future_occurrences(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock, store = Clock(), MemoryStore()
    receive = AsyncMock()
    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receive
    )

    async def invoke(run: dict) -> str:
        return "done"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(
        request(
            clock,
            schedule={"cron": "* * * * *", "timezone": "UTC"},
            webhook={"url": URL},
        )
    )
    clock.advance(60)
    await engine.scan_once()
    await engine.set_webhook(job["cronjob_id"], WebhookPatch(timeout_seconds=1))
    await engine.work_once()
    await settle([engine])
    first = receive.await_args_list[0].args
    assert first[0]["timeout_seconds"] == 10 and first[1]["taskType"] == "cron"
    assert first[1]["nextRunAt"] == "2026-09-16T00:02:00Z"
    clock.advance(60)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    second = receive.await_args_list[1].args
    assert (
        second[0]["timeout_seconds"] == 1
        and first[1]["eventID"] != second[1]["eventID"]
    )
    await engine.set_webhook(job["cronjob_id"], None)
    clock.advance(60)
    await engine.scan_once()
    await engine.work_once()
    await settle([engine])
    assert (
        receive.await_count == 2
        and len((await engine.runs(job["cronjob_id"]))["items"]) == 3
    )


@pytest.mark.asyncio
async def test_shutdown_during_agent_execution_does_not_notify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock, store = Clock(), MemoryStore()
    receive = AsyncMock()
    started = asyncio.Event()
    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receive
    )

    async def invoke(run: dict) -> str:
        started.set()
        await asyncio.Event().wait()
        return "unreachable"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock, webhook={"url": URL}))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await started.wait()
    await engine.close()
    await engine.scan_once()
    assert (await engine.runs(job["cronjob_id"]))["items"][0]["state"] == "interrupted"
    receive.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_during_delivery_retains_terminal_result_without_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock, store, receiving = Clock(), MemoryStore(), asyncio.Event()
    calls: list[dict] = []

    async def receive(config: dict, event: dict) -> None:
        calls.append(event)
        receiving.set()
        await asyncio.Event().wait()

    async def invoke(run: dict) -> str:
        return "durable answer"

    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receive
    )
    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock, webhook={"url": URL}))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await receiving.wait()
    await engine.close()
    restored = Engine(store, invoke, clock=clock)
    await restored.scan_once()
    run = (await restored.runs(job["cronjob_id"]))["items"][0]
    assert run["state"] == "succeeded" and run["output"] == "durable answer"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_completion_write_failure_does_not_deliver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock, store = Clock(), MemoryStore()
    receive = AsyncMock()
    monkeypatch.setattr(
        "veadk.cloud.harness_app.cronjobs.engine.deliver_webhook", receive
    )

    async def invoke(run: dict) -> str:
        monkeypatch.setattr(
            store, "put", AsyncMock(side_effect=ConnectionError("store unavailable"))
        )
        return "uncommitted"

    engine = Engine(store, invoke, clock=clock)
    job = await engine.create(request(clock, webhook={"url": URL}))
    clock.advance(11)
    await engine.scan_once()
    await engine.work_once()
    await asyncio.gather(*engine.executions.values())
    assert (await engine.get(job["cronjob_id"]))[0]["active"]["state"] == "running"
    receive.assert_not_awaited()
