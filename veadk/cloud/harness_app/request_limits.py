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

"""Fail-closed request-size guard for the shared OAuth Harness."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response

MAX_SHARED_REQUEST_BYTES = 128 * 1024
_BODY_METHODS = {"POST", "PUT", "PATCH"}


async def shared_request_size_limit(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Reject unbounded bodies before they can occupy an HTTP/model slot.

    The supported AgentKit chat client sends JSON with Content-Length. Requiring
    it in shared OAuth mode prevents a chunked request from making this simple
    middleware buffer an unbounded body. No request body or credential is logged.
    """

    if request.method.upper() not in _BODY_METHODS:
        return await call_next(request)
    raw_length = request.headers.get("content-length", "").strip()
    try:
        length = int(raw_length)
    except ValueError:
        length = -1
    if length < 0:
        return JSONResponse(
            {"error": "CONTENT_LENGTH_REQUIRED"},
            status_code=411,
            headers={"Cache-Control": "no-store"},
        )
    if length > MAX_SHARED_REQUEST_BYTES:
        return JSONResponse(
            {"error": "REQUEST_TOO_LARGE"},
            status_code=413,
            headers={"Cache-Control": "no-store"},
        )
    return await call_next(request)


__all__ = ["MAX_SHARED_REQUEST_BYTES", "shared_request_size_limit"]
