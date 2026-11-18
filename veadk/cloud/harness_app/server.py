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

"""Harness server: the standard AgentKit/ADK API server for an env-assembled agent.

Serves the full ADK API — ``/list-apps``, ``/run``, ``/run_sse``, ``/run_live``,
session/artifact management — plus an ``/invoke`` SSE compatibility route, via
``agentkit.apps.AgentkitAgentServerApp`` (which builds on google.adk's
``AdkWebServer``). This is the same server AgentKit deploys for an agent, so the
console and standard ADK clients work against it.

Decoupled from ``veadk.cloud``: it depends only on ``agentkit.apps`` and VeADK's
public API (``Agent`` / ``Runner`` / ``ShortTermMemory`` via ``harness_agent``).
The agent is assembled from the environment (see ``harness_agent``); on the
runtime, the model's Ark credential is resolved from the mounted VeFaaS IAM file.

Run with either:
    python -m veadk.cloud.harness_app.server
    uvicorn veadk.cloud.harness_app.server:app --host 0.0.0.0 --port 8000
"""

import asyncio
import functools
import json
import logging
import os
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing
from pathlib import Path
from typing import Any

from anyio import to_thread
from fastapi import Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.cli.adk_web_server import RunAgentRequest as AdkRunAgentRequest
from pydantic import ValidationError

from .apmplus import configure_apmplus_endpoint
from .artifact_wiring import (
    artifact_links,
    build_artifacts_handler,
    install_adk_service,
    install_artifacts,
    place_harness_routes_before_a2a,
    request_plugin,
    runner_artifact_kwargs,
)
from .artifacts import (
    ArtifactCollector,
    ArtifactPlugin,
    ArtifactSettings,
    TosArtifactService,
)
from .harness_agent import (
    close_request_resources,
    config_from_env,
    init_harness_agent,
    spawn_harness_agent,
)
from .identity import (
    BootstrapServer,
    create_agent_server,
    identity_config,
    identity_enabled,
    identity_required,
)
from .mcp import McpRequestMiddleware
from .request_limits import shared_request_size_limit
from .sandbox import configured_tool_id
from .server_types import (
    HARNESS_APP_NAME,
    HarnessOverrides,
    InvokeHarnessRequest,
    InvokeHarnessResponse,
)
from .session_metadata import (
    SessionLifecyclePlugin,
    SessionMetadataStore,
    install_session_routes,
    install_session_service_metadata_hooks,
    with_session_lifecycle_plugin,
)
from .session_store import initialise_shared_session_store

logger = logging.getLogger("harness")

HARNESS_NAME = os.getenv("HARNESS_NAME", "default")
MODEL_API_KEY_HEADER = "X-Model-API-Key"
IDENTITY_ENABLED = identity_enabled()
IDENTITY_REQUIRED = identity_required()
ARTIFACT_SETTINGS = ArtifactSettings.from_env()
ARTIFACT_SERVICE: TosArtifactService | None = None
ARTIFACT_PLUGIN: ArtifactPlugin | None = None


def _build_server():
    """Assemble the agent from the environment and wrap it in the ADK API server."""
    global ARTIFACT_SERVICE, ARTIFACT_PLUGIN

    identity = identity_config()
    if IDENTITY_REQUIRED and not IDENTITY_ENABLED:
        logger.error(
            "This image requires Harness identity, but the deployment did not enable it; "
            "serving only bootstrap health routes."
        )
        return BootstrapServer(), None, None, None, None, None
    if IDENTITY_ENABLED and identity is None:
        logger.warning(
            "Harness identity is enabled but not bound to a Runtime id; serving only bootstrap health routes."
        )
        return BootstrapServer(), None, None, None, None, None

    from agentkit.apps import AgentkitAgentServerApp

    configure_apmplus_endpoint()
    agent, short_term_memory = init_harness_agent(runtime_identity=identity)
    logger.info(
        f"Harness '{HARNESS_NAME}': agent '{agent.name}' assembled; serving ADK API server."
    )
    entry, ARTIFACT_SERVICE, ARTIFACT_PLUGIN = install_artifacts(
        ARTIFACT_SETTINGS, agent
    )
    metadata_store = SessionMetadataStore(short_term_memory.session_service)
    install_session_service_metadata_hooks(
        short_term_memory.session_service, metadata_store
    )
    lifecycle_plugin = SessionLifecyclePlugin(metadata_store)
    entry = with_session_lifecycle_plugin(entry, lifecycle_plugin)
    server = create_agent_server(
        AgentkitAgentServerApp,
        agent=entry,
        short_term_memory=short_term_memory,
        identity=identity,
    )
    install_adk_service(server, ARTIFACT_SETTINGS, ARTIFACT_SERVICE)
    return (
        server,
        agent,
        short_term_memory,
        config_from_env(),
        metadata_store,
        lifecycle_plugin,
    )


(
    server,
    agent,
    short_term_memory,
    harness_config,
    SESSION_METADATA,
    SESSION_LIFECYCLE_PLUGIN,
) = _build_server()
app = server.app
app.get("/harness/artifacts")(build_artifacts_handler(ARTIFACT_SERVICE, server))

if SESSION_METADATA is not None and short_term_memory is not None:
    install_session_routes(
        app,
        short_term_memory.session_service,
        SESSION_METADATA,
        current_app_name_ref=getattr(
            getattr(server, "server", None), "current_app_name_ref", None
        ),
    )


if harness_config is not None and short_term_memory is not None:

    @app.on_event("startup")
    async def _warm_shared_session_store() -> None:
        await initialise_shared_session_store(
            short_term_memory,
            app_name=HARNESS_APP_NAME,
            backend=harness_config.shortterm_memory_type,
        )
        await SESSION_METADATA.initialize()


def _runner_kwargs(plugin) -> dict[str, Any]:
    kwargs = runner_artifact_kwargs(ARTIFACT_SETTINGS, ARTIFACT_SERVICE, plugin)
    if SESSION_LIFECYCLE_PLUGIN is not None:
        kwargs["plugins"] = [
            *kwargs.get("plugins", []),
            SESSION_LIFECYCLE_PLUGIN,
        ]
    return kwargs


def _validation_error_response(error: ValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422, content={"detail": jsonable_encoder(error.errors())}
    )


async def harness_run_sse_override(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """为请求覆盖配置或部署默认 Sandbox 绑定请求级 Agent。"""
    if request.url.path != "/run_sse" or request.method != "POST":
        return await call_next(request)

    try:
        body = await request.json()
    except ValueError:
        return JSONResponse(
            status_code=422, content={"detail": [{"msg": "Request body must be JSON"}]}
        )
    model_api_key_from_header = request.headers.get(MODEL_API_KEY_HEADER)
    has_model_api_key_header = MODEL_API_KEY_HEADER in request.headers
    if not isinstance(body, dict) or (
        not body.get("harness") and not has_model_api_key_header
    ):
        if (
            isinstance(body, dict)
            and configured_tool_id()
            and body.get("app_name", HARNESS_APP_NAME) in (None, HARNESS_APP_NAME)
            and getattr(agent, "runtime", "adk") == "adk"
        ):
            return _default_sandbox_stream(body)
        return await call_next(request)

    if has_model_api_key_header:
        harness_body = body.get("harness")
        if harness_body is None:
            harness_body = {}
        if isinstance(harness_body, dict):
            # Header credentials are request-scoped and take precedence over the
            # deprecated body field without mutating the parsed request object.
            harness_body = {
                **harness_body,
                "model_api_key": model_api_key_from_header,
            }
            body = {**body, "harness": harness_body}

    parts = (body.get("new_message") or {}).get("parts") or []
    prompt = "\n".join(str(part.get("text", "")) for part in parts if part.get("text"))
    harness_body = body.get("harness") if isinstance(body.get("harness"), dict) else {}
    max_llm_calls = body.get("max_llm_calls") or harness_body.get("max_llm_calls")
    try:
        invoke_request = InvokeHarnessRequest(
            prompt=prompt,
            harness_name=str(body.get("app_name") or HARNESS_NAME),
            harness=body.get("harness"),
            run_agent_request={
                "user_id": str(body.get("user_id") or "default"),
                "session_id": str(body.get("session_id") or "default"),
                "max_llm_calls": max_llm_calls,
            },
        )
    except ValidationError as error:
        # A streaming route cannot raise through the response; report the same
        # 422 body a non-streaming endpoint would.
        return _validation_error_response(error)
    return StreamingResponse(
        stream_harness(invoke_request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _default_sandbox_stream(body: dict[str, object]) -> Response:
    """复用 ADK 请求校验，保留多模态消息、续跑参数和会话状态。"""
    try:
        adk_request = AdkRunAgentRequest.model_validate(body)
        request = InvokeHarnessRequest(
            prompt="",
            harness_name=HARNESS_NAME,
            run_agent_request={
                "user_id": adk_request.user_id,
                "session_id": adk_request.session_id,
                "max_llm_calls": body.get("max_llm_calls"),
            },
        )
    except ValidationError as error:
        return _validation_error_response(error)
    return StreamingResponse(
        stream_harness(request, adk_request=adk_request),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _request_overrides(request: InvokeHarnessRequest) -> HarnessOverrides | None:
    """Overrides to apply, cloning only when a request or the deployment needs it."""
    if request.harness is not None:
        return request.harness
    if configured_tool_id():
        return HarnessOverrides()
    return None


def _stream_run_config(
    request: InvokeHarnessRequest,
    adk_request: AdkRunAgentRequest | None,
) -> RunConfig:
    """保留 ADK 流式选项，同时沿用 Harness 的调用次数优先级。"""
    limit = (
        request.run_agent_request.max_llm_calls
        or (request.harness.max_llm_calls if request.harness else None)
        or harness_config.max_llm_calls
    )
    return RunConfig(
        streaming_mode=(
            StreamingMode.SSE
            if adk_request is None or adk_request.streaming
            else StreamingMode.NONE
        ),
        custom_metadata=adk_request.custom_metadata if adk_request else None,
        **({"max_llm_calls": limit} if limit else {}),
    )


async def stream_harness(
    request: InvokeHarnessRequest,
    *,
    adk_request: AdkRunAgentRequest | None = None,
) -> AsyncIterator[str]:
    """逐帧转发模型事件；请求资源保留到流结束或客户端取消。"""
    from google.genai import types

    from veadk.runner import Runner

    run_config = _stream_run_config(request, adk_request)
    plugin = request_plugin(ARTIFACT_SETTINGS, ARTIFACT_SERVICE)
    run_agent = agent
    try:
        with tempfile.TemporaryDirectory(prefix="agentkit_harness_") as work_dir:
            # 下载技能的线程结束后才能清理目录，避免断连时与后台写文件竞争。
            overrides = _request_overrides(request)
            run_agent = (
                await to_thread.run_sync(
                    functools.partial(
                        spawn_harness_agent,
                        agent,
                        overrides,
                        Path(work_dir),
                        user_id=request.run_agent_request.user_id,
                        session_id=request.run_agent_request.session_id,
                    )
                )
                if overrides is not None
                else agent
            )
            runner = Runner(
                agent=run_agent,
                short_term_memory=short_term_memory,
                app_name=HARNESS_APP_NAME,
                **_runner_kwargs(plugin),
            )
            if runner.short_term_memory is not None:
                await runner.short_term_memory.create_session(
                    app_name=HARNESS_APP_NAME,
                    user_id=request.run_agent_request.user_id,
                    session_id=request.run_agent_request.session_id,
                )
            if getattr(run_agent, "skills", None):
                from veadk.tools.skills_tools.session_path import (
                    initialize_session_path,
                )

                initialize_session_path(request.run_agent_request.session_id)
            # 保留原始 partial、工具调用和最终事件，不自行切分完整答案伪造流式。
            async with aclosing(
                runner.run_async(
                    user_id=request.run_agent_request.user_id,
                    session_id=request.run_agent_request.session_id,
                    new_message=(
                        adk_request.new_message
                        if adk_request
                        else types.Content(
                            role="user", parts=[types.Part(text=request.prompt)]
                        )
                    ),
                    state_delta=adk_request.state_delta if adk_request else None,
                    invocation_id=adk_request.invocation_id if adk_request else None,
                    run_config=run_config,
                )
            ) as events:
                async for event in events:
                    yield f"data: {event.model_dump_json(by_alias=True, exclude_none=True)}\n\n"
    except Exception as exc:
        if SESSION_METADATA is not None:
            await SESSION_METADATA.set_status(
                HARNESS_APP_NAME,
                request.run_agent_request.user_id,
                request.run_agent_request.session_id,
                "error",
            )
        logger.exception("Harness streaming invocation failed")
        yield f"data: {json.dumps({'error': str(exc)}, ensure_ascii=False)}\n\n"
    finally:
        try:
            if plugin is not None:
                await plugin.flush()
        finally:
            await close_request_resources(run_agent)


async def invoke_harness(request: InvokeHarnessRequest) -> InvokeHarnessResponse:
    """Run one request with optional non-persistent Harness overrides."""
    from google.adk.agents import RunConfig

    from veadk.runner import Runner

    max_llm_calls = (
        request.run_agent_request.max_llm_calls
        or (request.harness.max_llm_calls if request.harness else None)
        or harness_config.max_llm_calls
    )
    run_config = (
        RunConfig(max_llm_calls=max_llm_calls) if max_llm_calls else RunConfig()
    )
    collector = ArtifactCollector() if ARTIFACT_SERVICE is not None else None
    plugin = request_plugin(ARTIFACT_SETTINGS, ARTIFACT_SERVICE, collector)

    run_agent = agent
    try:
        with tempfile.TemporaryDirectory(prefix="agentkit_harness_") as work_dir:
            overrides = _request_overrides(request)
            run_agent = (
                await asyncio.to_thread(
                    functools.partial(
                        spawn_harness_agent,
                        agent,
                        overrides,
                        Path(work_dir),
                        user_id=request.run_agent_request.user_id,
                        session_id=request.run_agent_request.session_id,
                    )
                )
                if overrides is not None
                else agent
            )
            runner = Runner(
                agent=run_agent,
                short_term_memory=short_term_memory,
                app_name=HARNESS_APP_NAME,
                **_runner_kwargs(plugin),
            )
            output = await runner.run(
                messages=[request.prompt],
                user_id=request.run_agent_request.user_id,
                session_id=request.run_agent_request.session_id,
                run_config=run_config,
            )
            if plugin is not None:
                # Flush the published registry when an ADK version or an
                # early exit skipped after_run.
                await plugin.flush()
        return InvokeHarnessResponse(
            harness_name=request.harness_name,
            overwrite=request.harness is not None,
            output=str(output),
            artifacts=await artifact_links(ARTIFACT_SERVICE, collector),
        )
    except Exception as exc:
        if SESSION_METADATA is not None:
            await SESSION_METADATA.set_status(
                HARNESS_APP_NAME,
                request.run_agent_request.user_id,
                request.run_agent_request.session_id,
                "error",
            )
        model_key = (
            request.harness.model_api_key.get_secret_value() if request.harness else ""
        )
        if model_key:
            logger.error("Harness invocation failed (%s)", type(exc).__name__)
        else:
            logger.exception("Harness invocation failed")
        return InvokeHarnessResponse(
            harness_name=request.harness_name,
            overwrite=request.harness is not None,
            output="",
            error=str(exc).replace(model_key, "********") if model_key else str(exc),
        )
    finally:
        try:
            if plugin is not None:
                await plugin.flush()
        finally:
            await close_request_resources(run_agent)


if not IDENTITY_REQUIRED and not IDENTITY_ENABLED:
    # General model/tool/prompt overrides remain unavailable on shared OAuth
    # Harnesses; MCP-only overrides pass through the authenticated SDK handler
    # The legacy key-auth Harness keeps this endpoint for compatibility.
    #
    # This route authenticates at the Runtime layer (platform-managed KeyAuth
    # unless a custom JWT authorizer is configured) — nothing in this process
    # checks the caller, and per-request overrides are accepted. Deploy
    # Dockerfile.identity to require OIDC instead: that image sets
    # IDENTITY_REQUIRED and does not register this route at all.
    logger.warning(
        "Harness running in legacy mode: /harness/invoke authenticates at the "
        "Runtime layer only (platform-managed KeyAuth) and accepts per-request "
        "overrides. Confirm the Runtime authorizer is enabled, or deploy the "
        "identity image to require OIDC identity instead."
    )
    app.middleware("http")(harness_run_sse_override)
    app.post("/harness/invoke")(invoke_harness)


# AgentkitAgentServerApp mounts A2A at "/". FastAPI resolves routes in order,
# so keep the Harness endpoints before that catch-all mount.
place_harness_routes_before_a2a(app)


# MCP state wraps both SDK routes and the legacy override handler through the
# final streaming response; the shared size guard remains outermost
app.add_middleware(McpRequestMiddleware, shared=IDENTITY_REQUIRED or IDENTITY_ENABLED)
if IDENTITY_REQUIRED or IDENTITY_ENABLED:
    app.middleware("http")(shared_request_size_limit)


async def _invoke_scheduled(run: dict) -> tuple[str, list[dict]]:
    from .cronjobs.models import DEFAULT_USER_ID
    from .mcp import mcp_request_scope

    async with mcp_request_scope():
        response = await invoke_harness(
            InvokeHarnessRequest(
                prompt=run["prompt"],
                harness_name=HARNESS_NAME,
                harness=run["harness"],
                run_agent_request={
                    "user_id": run.get("user_id", DEFAULT_USER_ID),
                    "session_id": run["run_id"],
                },
            )
        )
    if response.error:
        raise RuntimeError(response.error)
    return response.output, [item.model_dump() for item in response.artifacts]


if os.getenv("HARNESS_CRONJOB_ENABLED", "").lower() in {"1", "true"}:
    from .cronjobs.routes import mount_cronjobs

    mount_cronjobs(
        app, _invoke_scheduled, shared_identity=IDENTITY_REQUIRED or IDENTITY_ENABLED
    )


if __name__ == "__main__":
    server.run(
        host=os.getenv("SERVER_HOST", "0.0.0.0"),
        port=int(os.getenv("SERVER_PORT", "8000")),
    )
