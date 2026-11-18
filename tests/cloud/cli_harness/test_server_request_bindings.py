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

"""/run_sse validation and request-scoped agent assembly."""

import importlib
import json
import sys
from types import ModuleType
from unittest.mock import AsyncMock

import pytest
from fastapi import Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from veadk.cloud.harness_app.sandbox import SANDBOX_TOOL_ID_ENV
from veadk.cloud.harness_app.server_types import HarnessOverrides, InvokeHarnessRequest


@pytest.fixture
def harness_server(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("HARNESS_CRONJOB_ENABLED", "false")
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_required", lambda: True
    )
    monkeypatch.setattr(
        "veadk.cloud.harness_app.identity.identity_enabled", lambda: False
    )
    previous = sys.modules.pop("veadk.cloud.harness_app.server", None)
    module = importlib.import_module("veadk.cloud.harness_app.server")
    try:
        yield module
    finally:
        sys.modules.pop("veadk.cloud.harness_app.server", None)
        if previous is not None:
            sys.modules["veadk.cloud.harness_app.server"] = previous


def make_request(body, headers: dict[str, str] | None = None) -> Request:
    payload = body if isinstance(body, bytes) else json.dumps(body).encode()
    receive = AsyncMock(return_value={"type": "http.request", "body": payload})
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/run_sse",
            "headers": [
                (name.lower().encode(), value.encode())
                for name, value in (headers or {}).items()
            ],
        },
        receive,
    )


async def call_override(
    module: ModuleType, body, headers: dict[str, str] | None = None
):
    return await module.harness_run_sse_override(
        make_request(body, headers), AsyncMock(return_value=Response("passthrough"))
    )


def stream_body(**overrides) -> dict:
    body = {
        "app_name": "harness_agent",
        "user_id": "u-1",
        "session_id": "s-1",
        "new_message": {"role": "user", "parts": [{"text": "hello"}]},
    }
    body.update(overrides)
    return body


@pytest.mark.asyncio
async def test_run_sse_rejects_malformed_json(harness_server):
    response = await call_override(harness_server, b"{not json")

    assert response.status_code == 422
    assert json.loads(response.body)["detail"][0]["msg"] == "Request body must be JSON"


@pytest.mark.asyncio
@pytest.mark.parametrize("fields", [{}, {"harness": None}, {"harness": {}}])
async def test_run_sse_without_harness_keeps_the_sdk_handler(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    fields: dict[str, object],
) -> None:
    monkeypatch.delenv(SANDBOX_TOOL_ID_ENV, raising=False)
    response = await call_override(harness_server, stream_body(**fields))

    assert response.body == b"passthrough"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["user_id", "session_id"])
async def test_default_sandbox_validates_adk_request(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "tool-default")
    body = stream_body()
    del body[field]
    response = await call_override(harness_server, body)
    assert response.status_code == 422
    alias = {"user_id": "userId", "session_id": "sessionId"}[field]
    assert any(alias in error["loc"] for error in json.loads(response.body)["detail"])


@pytest.mark.asyncio
async def test_default_sandbox_does_not_intercept_other_apps(
    harness_server: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "tool-default")
    response = await call_override(harness_server, stream_body(app_name="other-app"))
    assert response.body == b"passthrough"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "harness",
    [
        {"knowledgebase": {"type": "pinecone"}},
        {"knowledgebase": {"index": "bad index"}},
        {"knowledgebase": {"index": "kb", "top_k": 3}},
        {"sandbox": {"ttl": 5}},
        {"sandbox": {"execute_timeout_seconds": 3601}},
        {"sandbox": {"tool_id": "not a tool id"}},
        {"sandbox": {"tool_id": "tool-1", "workspace": "/tmp"}},
        "not-an-object",
    ],
)
async def test_run_sse_returns_422_for_invalid_overrides(harness_server, harness):
    response = await call_override(harness_server, stream_body(harness=harness))

    assert isinstance(response, JSONResponse)
    assert response.status_code == 422
    detail = json.loads(response.body)["detail"]
    assert isinstance(detail, list) and detail


@pytest.mark.asyncio
async def test_run_sse_streams_a_valid_overridden_request(harness_server):
    response = await call_override(
        harness_server,
        stream_body(
            harness={
                "knowledgebase": {"index": "tenant_42", "type": "viking"},
                "sandbox": {"tool_id": "tool-1", "ttl": 60},
            }
        ),
    )

    assert isinstance(response, StreamingResponse)
    assert response.media_type == "text/event-stream"
    await response.body_iterator.aclose()


@pytest.mark.asyncio
async def test_model_api_key_header_creates_request_scoped_override(
    harness_server: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests = []

    def fake_stream(request, **_kwargs):
        requests.append(request)

        async def events():
            yield "data: [DONE]\n\n"

        return events()

    monkeypatch.setattr(harness_server, "stream_harness", fake_stream)

    response = await call_override(
        harness_server,
        stream_body(),
        headers={"X-Model-API-Key": "header-model-key"},
    )

    assert isinstance(response, StreamingResponse)
    assert requests[0].harness.model_api_key.get_secret_value() == "header-model-key"
    assert "model_api_key" in requests[0].harness.model_fields_set
    await response.body_iterator.aclose()


@pytest.mark.asyncio
async def test_model_api_key_header_takes_precedence_over_deprecated_body_field(
    harness_server: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests = []

    def fake_stream(request, **_kwargs):
        requests.append(request)

        async def events():
            yield "data: [DONE]\n\n"

        return events()

    monkeypatch.setattr(harness_server, "stream_harness", fake_stream)

    response = await call_override(
        harness_server,
        stream_body(harness={"model_api_key": "body-model-key"}),
        headers={"x-model-api-key": "header-model-key"},
    )

    assert requests[0].harness.model_api_key.get_secret_value() == "header-model-key"
    await response.body_iterator.aclose()


@pytest.mark.asyncio
async def test_deprecated_body_model_api_key_remains_supported(
    harness_server: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests = []

    def fake_stream(request, **_kwargs):
        requests.append(request)

        async def events():
            yield "data: [DONE]\n\n"

        return events()

    monkeypatch.setattr(harness_server, "stream_harness", fake_stream)

    response = await call_override(
        harness_server,
        stream_body(harness={"model_api_key": "body-model-key"}),
    )

    assert requests[0].harness.model_api_key.get_secret_value() == "body-model-key"
    await response.body_iterator.aclose()


def test_request_overrides_prefers_the_request(harness_server, monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")
    request = InvokeHarnessRequest(
        prompt="p",
        harness_name="h",
        harness=HarnessOverrides(sandbox={"tool_id": "request-tool"}),
        run_agent_request={"user_id": "u", "session_id": "s"},
    )

    overrides = harness_server._request_overrides(request)

    assert overrides.sandbox.tool_id == "request-tool"


def test_request_overrides_clone_for_a_deployment_sandbox(harness_server, monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ENV, "deployed-tool")
    request = InvokeHarnessRequest(
        prompt="p",
        harness_name="h",
        run_agent_request={"user_id": "u", "session_id": "s"},
    )

    overrides = harness_server._request_overrides(request)

    assert isinstance(overrides, HarnessOverrides)
    assert not overrides.model_fields_set


def test_request_overrides_are_absent_without_a_sandbox(harness_server, monkeypatch):
    monkeypatch.delenv(SANDBOX_TOOL_ID_ENV, raising=False)
    request = InvokeHarnessRequest(
        prompt="p",
        harness_name="h",
        run_agent_request={"user_id": "u", "session_id": "s"},
    )

    assert harness_server._request_overrides(request) is None
