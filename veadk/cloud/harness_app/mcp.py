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

"""Remote MCP tools with connections and credentials scoped to one HTTP request"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

import anyio
from google.adk.tools.base_tool import BaseTool, ToolContext
from google.adk.tools.base_toolset import BaseToolset
from google.adk.tools.mcp_tool.mcp_tool import McpTool
from google.adk.tools.mcp_tool.mcp_session_manager import (
    SseConnectionParams,
    StreamableHTTPConnectionParams,
)
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from pydantic import TypeAdapter, ValidationError
from starlette.responses import JSONResponse

from .server_types import McpServerConfig

logger = logging.getLogger("harness")
_servers_adapter = TypeAdapter(list[McpServerConfig])


def mcp_tool_error_fallback(
    *, tool: BaseTool, args: dict[str, Any], tool_context: ToolContext, error: Exception
) -> dict[str, str] | None:
    """仅把 MCP 执行异常变成可见的失败结果，不额外重试调用。"""
    if not isinstance(tool, McpTool):
        return None
    logger.warning(
        "MCP tool call failed: tool=%s error_type=%s",
        tool.name,
        type(error).__name__,
    )
    return {"error": f"MCP tool {tool.name} failed; its result is unavailable."}


@dataclass
class _RequestState:
    servers: list[McpServerConfig] | None = None
    toolsets: dict[HarnessMcpToolset, list[McpToolset]] = field(default_factory=dict)


_request_state: ContextVar[_RequestState | None] = ContextVar(
    "harness_mcp", default=None
)


@asynccontextmanager
async def mcp_request_scope(servers: list[McpServerConfig] | None = None):
    state = _RequestState(servers=servers)
    token = _request_state.set(state)
    try:
        yield state
    finally:
        try:
            # Also release connections after a disconnect or cancelled run
            with anyio.CancelScope(shield=True):
                for owner, toolsets in state.toolsets.items():
                    for toolset in reversed(toolsets):
                        try:
                            await toolset.close()
                        except Exception as exc:
                            logger.warning(
                                "MCP cleanup failed (%s)", type(exc).__name__
                            )
                    owner._cached_prefixed_tools = None
        finally:
            _request_state.reset(token)


class HarnessMcpToolset(BaseToolset):
    """Keep configuration separate from built-ins and never share request keys"""

    def __init__(self, servers: list[McpServerConfig]):
        super().__init__()
        self.servers = tuple(servers)
        # ADK's shared tool cache must not retain another request's connections
        self._use_invocation_cache = False

    async def get_tools(self, readonly_context=None):
        state = _request_state.get()
        if state is None:
            if not self.servers:
                return []
            raise RuntimeError("MCP tools require an active Harness request")
        servers = self.servers if state.servers is None else state.servers
        if self not in state.toolsets:
            state.toolsets[self] = []
            for index, server in enumerate(servers, start=1):
                key = server.api_key.get_secret_value() if server.api_key else ""
                params = (
                    SseConnectionParams
                    if server.protocol == "sse"
                    else StreamableHTTPConnectionParams
                )
                state.toolsets[self].append(
                    McpToolset(
                        connection_params=params(
                            url=server.endpoint,
                            headers={"Authorization": f"Bearer {key}"} if key else {},
                        ),
                        tool_name_prefix=f"mcp_{index}",
                    )
                )
        tools = []
        for index, toolset in enumerate(state.toolsets[self], start=1):
            try:
                tools.extend(await toolset.get_tools_with_prefix(readonly_context))
            except Exception:
                raise RuntimeError(
                    f"Failed to load tools from MCP server {index}"
                ) from None
        return tools

    async def close(self):
        # Each request owns its connections, including while streaming a response
        pass


class McpRequestMiddleware:
    """Bind MCP overrides around the existing authenticated SDK request handler"""

    def __init__(self, app, *, shared: bool = False):
        self.app = app
        self.shared = shared

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        servers = None
        if scope["method"] == "POST" and scope["path"] in {
            "/run_sse",
            "/run",
            "/harness/invoke",
            "/invoke",
        }:
            chunks = []
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    return
                chunks.append(message.get("body", b""))
                if not message.get("more_body", False):
                    break
            body = b"".join(chunks)
            try:
                payload = json.loads(body)
            except (ValueError, UnicodeDecodeError):
                payload = None
            harness = payload.get("harness") if isinstance(payload, dict) else None
            if isinstance(harness, dict) and "mcp_servers" in harness:
                try:
                    servers = _servers_adapter.validate_python(harness["mcp_servers"])
                except ValidationError as exc:
                    response = JSONResponse(
                        {
                            "detail": exc.errors(
                                include_input=False,
                                include_url=False,
                                include_context=False,
                            )
                        },
                        status_code=422,
                    )
                    return await response(scope, receive, send)
                # MCP changes stay request-scoped; keep authentication, sessions
                # and streaming in the normal SDK handler, including OAuth mode
                remaining = {
                    key: value for key, value in harness.items() if key != "mcp_servers"
                }
                if self.shared and remaining:
                    response = JSONResponse(
                        {
                            "error": "Shared OAuth Harness only accepts mcp_servers overrides"
                        },
                        status_code=400,
                    )
                    return await response(scope, receive, send)
                if remaining:
                    payload["harness"] = remaining
                else:
                    payload.pop("harness")
                body = json.dumps(payload).encode()
                scope = {
                    **scope,
                    "headers": [
                        (key, value)
                        for key, value in scope["headers"]
                        if key.lower() != b"content-length"
                    ]
                    + [(b"content-length", str(len(body)).encode())],
                }

            original_receive = receive
            delivered = False

            async def replay():
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": body, "more_body": False}
                return await original_receive()

            receive = replay

        async with mcp_request_scope(servers):
            await self.app(scope, receive, send)
