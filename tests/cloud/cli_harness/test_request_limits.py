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

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from veadk.cloud.harness_app.request_limits import (
    MAX_SHARED_REQUEST_BYTES,
    shared_request_size_limit,
)


def _client() -> TestClient:
    app = FastAPI()
    app.middleware("http")(shared_request_size_limit)

    @app.post("/run_sse")
    async def run_sse(request: Request) -> dict[str, int]:
        return {"size": len(await request.body())}

    return TestClient(app)


def test_bounded_json_is_accepted() -> None:
    response = _client().post("/run_sse", json={"prompt": "hello"})
    assert response.status_code == 200


def test_large_body_and_missing_length_fail_closed() -> None:
    client = _client()
    response = client.post(
        "/run_sse",
        content=b"x" * (MAX_SHARED_REQUEST_BYTES + 1),
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 413

    # TestClient adds Content-Length for ordinary request bodies; exercise the
    # middleware's missing-length branch with a streaming body instead.
    request = client.build_request("POST", "/run_sse", content=iter([b"{}"]))
    response = client.send(request)
    assert response.status_code == 411
