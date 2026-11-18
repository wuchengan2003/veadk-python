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

"""Server glue for the three independent artifact sources.

Kept separate from ``server.py`` so the wiring decisions (when to clone, which
runner gets the TOS service, which routes go before the A2A mount) can be unit
tested without importing VeADK or the AgentKit SDK.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from fastapi import HTTPException, Request

from .artifacts import (
    ArtifactCollector,
    ArtifactPlugin,
    ArtifactSettings,
    TosArtifactService,
)
from .server_types import (
    ArtifactLink,
    HARNESS_APP_NAME,
    HarnessOverrides,
    InvokeHarnessRequest,
)

logger = logging.getLogger("harness.artifacts")


def artifact_overrides(
    settings: ArtifactSettings, request: InvokeHarnessRequest
) -> HarnessOverrides | None:
    """Return the request overrides; artifacts themselves never force a clone."""
    return request.harness


def request_plugin(
    settings: ArtifactSettings,
    service: TosArtifactService | None,
    collector: ArtifactCollector | None = None,
):
    if service is None:
        return None
    return ArtifactPlugin(settings, service, collector=collector)


def runner_artifact_kwargs(
    settings: ArtifactSettings,
    service: TosArtifactService | None,
    plugin,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if settings.adk_enabled:
        kwargs["artifact_service"] = service
    if plugin is not None:
        kwargs["plugins"] = [plugin]
    return kwargs


async def artifact_links(
    service: TosArtifactService | None, collector: ArtifactCollector | None
) -> list[ArtifactLink]:
    if service is None or collector is None or not collector.has_entries():
        return []
    items = await service.items_for_entries(collector.entries())
    return [ArtifactLink(**item.to_dict()) for item in items]


def install_artifacts(
    settings: ArtifactSettings,
    agent,
    *,
    service_factory: Callable[
        [ArtifactSettings], TosArtifactService
    ] = TosArtifactService,
):
    """Build the service/plugin and return the ADK app entry for the SDK."""
    if not settings.any_enabled:
        return agent, None, None
    service = service_factory(settings)
    service.check_bucket()
    if settings.publish_enabled:
        from .artifact_tools import build_publish_tool

        tools = getattr(agent, "tools", None)
        if tools is None:
            agent.tools = []
            tools = agent.tools
        tools.append(build_publish_tool(settings, service))
        logger.info("Harness artifacts: publish_artifact enabled")
    plugin = ArtifactPlugin(settings, service)
    logger.info(
        "Harness artifacts enabled: adk=%s publish=%s",
        settings.adk_enabled,
        settings.publish_enabled,
    )
    from google.adk.apps import App

    return App(name=agent.name, root_agent=agent, plugins=[plugin]), service, plugin


def install_adk_service(
    server, settings: ArtifactSettings, service: TosArtifactService | None
) -> None:
    """Replace the SDK's in-memory service on the ADK API server."""
    if not settings.adk_enabled or service is None:
        return
    api_server = getattr(server, "server", None)
    if api_server is None or not hasattr(api_server, "artifact_service"):
        raise RuntimeError(
            "agentkit-sdk/google-adk no longer exposes server.artifact_service; "
            "refusing to fall back to an in-memory artifact service"
        )
    api_server.artifact_service = service
    if api_server.artifact_service is not service:  # pragma: no cover - defensive
        raise RuntimeError("unable to install the TOS artifact service")
    logger.info(
        "Harness artifacts: ADK artifact service -> TOS bucket %s prefix %s",
        settings.tos_bucket,
        settings.tos_prefix,
    )


def build_artifacts_handler(
    service: TosArtifactService | None, server, app_name: str = HARNESS_APP_NAME
):
    """Build ``GET /harness/artifacts`` for the CLI and structured clients."""

    async def handler(request: Request) -> dict:
        if service is None:
            return {"artifacts": []}
        params = request.query_params
        session_id = params.get("session_id")
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        user_id = params.get("user_id") or "default"
        identity_method = getattr(server, "_request_identity_context", None)
        if callable(identity_method):
            try:
                identity_context = identity_method()
            except Exception:  # noqa: BLE001 - legacy mode has no identity
                identity_context = None
            if identity_context is not None and getattr(
                identity_context, "user_sub", None
            ):
                user_id = identity_context.user_sub
        since_raw = params.get("since")
        try:
            since = float(since_raw) if since_raw else None
        except ValueError:
            raise HTTPException(
                status_code=400, detail="since must be epoch seconds"
            ) from None
        items = await service.list_session_artifacts(
            app_name=params.get("app_name") or app_name,
            user_id=user_id,
            session_id=session_id,
            invocation_id=params.get("invocation_id") or None,
            since=since,
            limit=50,
        )
        return {"artifacts": [item.to_dict() for item in items]}

    return handler


def place_harness_routes_before_a2a(app) -> None:
    """AgentkitAgentServerApp mounts A2A at '/', so harness routes go first."""
    harness_routes = [
        route
        for route in app.routes
        if getattr(route, "path", "").startswith("/harness/")
    ]
    for route in harness_routes:
        app.routes.remove(route)
    mount_index = next(
        (
            index
            for index, route in enumerate(app.routes)
            if route.__class__.__name__ == "Mount"
        ),
        len(app.routes),
    )
    for offset, route in enumerate(harness_routes):
        app.routes.insert(mount_index + offset, route)
