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

"""AgentKit Sandbox as an ADK environment.

``AgentkitSandboxEnvironment`` implements ADK's ``BaseEnvironment`` on top of
the AIO sandbox data plane (``/v1/bash`` and ``/v1/file``). ``EnvironmentToolset``
turns it into the ``Execute``, ``ReadFile``, ``EditFile`` and ``WriteFile``
tools the model can call.

The control plane (CreateSession / GetSession) stays in VeADK
(``ensure_agentkit_session_endpoint``); this module only speaks to the returned
session endpoint. The environment is request-scoped because ADK environment
methods receive no ``ToolContext``: the AgentKit ``UserSessionId`` is bound at
construction time from the invocation's user and session ids.

Deployment policy lives here too: the workspace and the default TTL are read
from the environment, never from the request. The stock ADK toolset drives
``Execute`` with ADK's fixed 30 second budget; longer work must be started as a
background process inside the sandbox.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import posixpath
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from google.adk.environment import BaseEnvironment, ExecutionResult

logger = logging.getLogger("harness")

DEFAULT_WORKSPACE = "/home/gem"
DEFAULT_TTL_SECONDS = 86400
DEFAULT_READY_TIMEOUT_SECONDS = 120
DEFAULT_MAX_OUTPUT_CHARS = 30000
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0

# ADK's Execute tool always passes its own 30 second budget. Mirrored here for
# direct environment.execute() calls; long work belongs in a background process.
DEFAULT_EXECUTE_TIMEOUT_SECONDS = 30
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60

BASH_EXEC_ROUTE = "/v1/bash/exec"
FILE_READ_ROUTE = "/v1/file/read"
FILE_WRITE_ROUTE = "/v1/file/write"

SANDBOX_TOOL_ID_ENV = "HARNESS_SANDBOX_TOOL_ID"
WORKSPACE_ENV = "HARNESS_SANDBOX_WORKSPACE"
TTL_ENV = "HARNESS_SANDBOX_TTL"
READY_TIMEOUT_ENV = "HARNESS_SANDBOX_READY_TIMEOUT_SECONDS"
MAX_OUTPUT_CHARS_ENV = "HARNESS_SANDBOX_MAX_OUTPUT_CHARS"


def service_url(endpoint: str, route: str) -> str:
    """Append an AIO route to a session endpoint, keeping its auth query."""
    parsed = urlsplit(endpoint)
    path = parsed.path.rstrip("/") + route
    return urlunsplit(
        (parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment)
    )


def configured_tool_id() -> str:
    """The deployment's sandbox tool id, or ``""`` when unset."""
    return os.getenv(SANDBOX_TOOL_ID_ENV, "").strip()


class AgentkitSandboxEnvironment(BaseEnvironment):
    """Runs commands and file I/O in an AgentKit sandbox session."""

    def __init__(
        self,
        *,
        tool_id: str,
        tool_user_session_id: str,
        workspace: str = DEFAULT_WORKSPACE,
        ttl: int = DEFAULT_TTL_SECONDS,
        ready_timeout_seconds: int = DEFAULT_READY_TIMEOUT_SECONDS,
        endpoint: str | None = None,
        endpoint_resolver: Callable[[], str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not tool_id:
            raise ValueError("tool_id is required")
        if not tool_user_session_id:
            raise ValueError("tool_user_session_id is required")
        if not str(workspace).startswith("/"):
            raise ValueError("workspace must be an absolute path")
        self._tool_id = tool_id
        self._tool_user_session_id = tool_user_session_id
        self._workspace = posixpath.normpath(str(workspace))
        self._ttl = ttl
        self._ready_timeout_seconds = ready_timeout_seconds
        self._endpoint = endpoint or None
        self._endpoint_resolver = endpoint_resolver
        self._transport = transport
        self._client: httpx.AsyncClient | None = None
        self._init_lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Resolve the sandbox session endpoint once.

        ``BaseEnvironment`` has no context object, so the session identity was
        bound in ``__init__``; here we only create or reuse the remote session.
        """
        if self._endpoint:
            return
        async with self._init_lock:
            if self._endpoint:
                return
            resolver = self._endpoint_resolver or self._resolve_session_endpoint
            endpoint = await asyncio.to_thread(resolver)
            endpoint = str(endpoint or "").strip()
            if not endpoint:
                raise RuntimeError(
                    f"AgentKit sandbox tool {self._tool_id} returned no session endpoint"
                )
            self._endpoint = endpoint
            logger.info(
                "AgentKit sandbox session ready: tool_id=%s user_session_id=%s",
                self._tool_id,
                self._tool_user_session_id,
            )

    def _resolve_session_endpoint(self) -> str:
        from veadk.tools.builtin_tools._agentkit import ensure_agentkit_session_endpoint

        return ensure_agentkit_session_endpoint(
            tool_id=self._tool_id,
            tool_user_session_id=self._tool_user_session_id,
            ttl=self._ttl,
            prefer_internal_endpoint=True,
            wait_until_ready=True,
            ready_timeout=float(self._ready_timeout_seconds),
        )

    @property
    def working_dir(self) -> Path:
        return Path(self._workspace)

    async def execute(
        self, command: str, *, timeout: float | None = None
    ) -> ExecutionResult:
        """Run one shell command inside the workspace."""
        effective = (
            int(timeout) if timeout is not None else DEFAULT_EXECUTE_TIMEOUT_SECONDS
        )
        effective = max(1, effective)
        data = await self._post(
            BASH_EXEC_ROUTE,
            {
                "command": command,
                "exec_dir": self._workspace,
                "timeout": effective,
                "hard_timeout": effective,
            },
            timeout=effective + 30,
        )
        status = str(data.get("status") or "").lower()
        if status == "running":
            raise RuntimeError(
                f"sandbox command is still running after {effective}s; "
                "shorten it or start it as a background process"
            )
        return ExecutionResult(
            exit_code=int(data.get("exit_code") or 0),
            stdout=str(data.get("stdout") or ""),
            stderr=str(data.get("stderr") or ""),
            timed_out=status in {"timed_out", "killed"},
        )

    async def read_file(self, path: Path | str) -> bytes:
        """Read a text file from the sandbox workspace."""
        data = await self._post(FILE_READ_ROUTE, {"file": str(self._resolve(path))})
        return str(data.get("content") or "").encode("utf-8")

    async def write_file(self, path: Path | str, content: str | bytes) -> None:
        """Create or overwrite a file in the sandbox workspace."""
        payload: dict[str, Any] = {"file": str(self._resolve(path))}
        if isinstance(content, bytes):
            payload["content"] = base64.b64encode(content).decode("ascii")
            payload["encoding"] = "base64"
        else:
            payload["content"] = content
            payload["encoding"] = "utf-8"
        payload["append"] = False
        payload["trailing_newline"] = False
        await self._post(FILE_WRITE_ROUTE, payload)

    async def close(self) -> None:
        """Release the local HTTP client; the remote session lives until its TTL."""
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    def _resolve(self, path: Path | str) -> str:
        raw = str(path)
        candidate = raw if raw.startswith("/") else posixpath.join(self._workspace, raw)
        normalized = posixpath.normpath(candidate)
        if normalized != self._workspace and not normalized.startswith(
            self._workspace.rstrip("/") + "/"
        ):
            raise ValueError(f"path must stay inside {self._workspace}")
        return normalized

    def _client_or_create(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                transport=self._transport,
                timeout=httpx.Timeout(
                    DEFAULT_REQUEST_TIMEOUT_SECONDS,
                    connect=DEFAULT_CONNECT_TIMEOUT_SECONDS,
                ),
                follow_redirects=False,
            )
        return self._client

    async def _post(
        self, route: str, payload: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        await self.initialize()
        if not self._endpoint:
            raise RuntimeError("sandbox session endpoint is not initialized")
        try:
            response = await self._client_or_create().post(
                service_url(self._endpoint, route), json=payload, timeout=timeout
            )
        except httpx.HTTPError as error:
            raise RuntimeError(
                f"sandbox {route} request failed: {type(error).__name__}: {error}"
            ) from error

        if response.status_code >= 400:
            raise RuntimeError(
                f"sandbox {route} failed with HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )
        try:
            body = response.json()
        except ValueError as error:
            raise RuntimeError(
                f"sandbox {route} returned a non-JSON response"
            ) from error
        if not isinstance(body, dict):
            raise RuntimeError(f"sandbox {route} returned an unexpected payload")

        data = body.get("data")
        if body.get("success") is False:
            detail = data if isinstance(data, dict) else {}
            message = str(
                body.get("message")
                or detail.get("message")
                or f"sandbox {route} failed"
            )
            if (
                detail.get("error_type") == "not_found"
                or detail.get("errno_name") == "ENOENT"
            ):
                raise FileNotFoundError(message)
            raise RuntimeError(message)
        if isinstance(data, dict):
            return data
        if data is None:
            return body
        return {"value": data}
