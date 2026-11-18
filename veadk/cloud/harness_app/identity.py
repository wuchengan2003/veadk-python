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

"""Deployment-bound inbound identity for a shared Harness Runtime."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

IDENTITY_ENABLED_ENV = "AGENTKIT_HARNESS_IDENTITY_ENABLED"
IDENTITY_BUILD_MARKER = Path("/app/.agentkit-harness-identity-required")
RUNTIME_ID_ENV = "AGENTKIT_HARNESS_IDENTITY_RUNTIME_ID"
DISCOVERY_URL_ENV = "AGENTKIT_HARNESS_IDENTITY_DISCOVERY_URL"
ISSUER_ENV = "AGENTKIT_HARNESS_IDENTITY_ISSUER"
ALLOWED_CLIENTS_ENV = "AGENTKIT_HARNESS_IDENTITY_ALLOWED_CLIENTS"
MODEL_CREDENTIAL_MODE_ENV = "MODEL_CREDENTIAL_MODE"
MODEL_TARGET_ALIAS_ENV = "MODEL_TARGET_ALIAS"
MODEL_TARGET_AUDIENCE_ENV = "MODEL_TARGET_AUDIENCE"
MODEL_API_BASE_ENV = "MODEL_API_BASE"
MODEL_WORKLOAD_DISCOVERY_URL_ENV = "MODEL_WORKLOAD_DISCOVERY_URL"
MODEL_WORKLOAD_ISSUER_ENV = "MODEL_WORKLOAD_ISSUER"
MODEL_WORKLOAD_POOL_ENV = "MODEL_WORKLOAD_POOL"
MODEL_IDENTITY_REGION_ENV = "MODEL_IDENTITY_REGION"
MANAGED_MODEL_MODE = "obo_broker"


def identity_enabled() -> bool:
    return os.getenv(IDENTITY_ENABLED_ENV, "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def identity_required(marker: Path = IDENTITY_BUILD_MARKER) -> bool:
    """Return the image-owned build mode; Runtime env cannot disable it."""

    return marker.is_file()


def identity_config() -> Any | None:
    """Build immutable verifier config from deployment-owned environment."""

    if not identity_enabled():
        return None
    from agentkit.identity import (
        IdentityClient,
        IdentityRuntimeConfig,
        ProtectedTarget,
        RuntimeIdentity,
    )

    runtime_id = os.getenv(RUNTIME_ID_ENV, "").strip()
    if not runtime_id:
        return None
    discovery_url = os.getenv(DISCOVERY_URL_ENV, "").strip()
    expected_issuer = os.getenv(ISSUER_ENV, "").strip()
    allowed_clients = tuple(
        value.strip()
        for value in os.getenv(ALLOWED_CLIENTS_ENV, "").split(",")
        if value.strip()
    )
    if not discovery_url or not expected_issuer or not allowed_clients:
        raise RuntimeError("Harness identity configuration is incomplete")
    credential_mode = os.getenv(MODEL_CREDENTIAL_MODE_ENV, "").strip()
    if credential_mode not in {"", MANAGED_MODEL_MODE}:
        raise RuntimeError("Harness model credential mode is invalid")
    targets = {}
    workload_discovery_url = None
    workload_issuer = None
    workload_pool = "default"
    region = "cn-beijing"
    if credential_mode == MANAGED_MODEL_MODE:
        target_alias = os.getenv(MODEL_TARGET_ALIAS_ENV, "").strip()
        target_audience = os.getenv(MODEL_TARGET_AUDIENCE_ENV, "").strip()
        model_api_base = os.getenv(MODEL_API_BASE_ENV, "").strip()
        workload_discovery_url = os.getenv(MODEL_WORKLOAD_DISCOVERY_URL_ENV, "").strip()
        workload_issuer = os.getenv(MODEL_WORKLOAD_ISSUER_ENV, "").strip()
        workload_pool = os.getenv(MODEL_WORKLOAD_POOL_ENV, "default").strip()
        region = os.getenv(MODEL_IDENTITY_REGION_ENV, "cn-beijing").strip()
        if not all(
            (
                target_alias,
                target_audience,
                model_api_base,
                workload_discovery_url,
                workload_issuer,
                workload_pool,
                region,
            )
        ):
            raise RuntimeError("Harness managed model identity is incomplete")
        targets[target_alias] = ProtectedTarget(
            alias=target_alias,
            audience=target_audience,
            base_url=model_api_base,
        )

    config = IdentityRuntimeConfig(
        runtime_id=runtime_id,
        discovery_url=discovery_url,
        expected_user_issuer=expected_issuer,
        allowed_clients=allowed_clients,
        targets=targets,
        workload_discovery_url=workload_discovery_url,
        expected_workload_issuer=workload_issuer,
        workload_pool=workload_pool,
        region=region,
    )
    if credential_mode == MANAGED_MODEL_MODE:
        return RuntimeIdentity(config, exchange=IdentityClient(region=region))
    return config


def create_agent_server(
    server_type: Any,
    *,
    agent: Any,
    short_term_memory: Any,
    identity: Any | None,
) -> Any:
    """Preserve the pre-Identity constructor contract for legacy SDKs."""

    if identity is None:
        return server_type(agent=agent, short_term_memory=short_term_memory)
    return server_type(
        agent=agent,
        short_term_memory=short_term_memory,
        identity=identity,
        identity_health_routes=("/readiness", "/healthz"),
    )


class BootstrapServer:
    """Fail-closed app used before a fresh Runtime is bound to its own id."""

    def __init__(self) -> None:
        self.app = FastAPI()

        @self.app.get("/readiness")
        @self.app.get("/healthz")
        async def readiness() -> dict[str, str]:
            return {"status": "bootstrap"}

        @self.app.api_route(
            "/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
        )
        async def unavailable(path: str) -> JSONResponse:
            del path
            return JSONResponse(
                {"error": "RUNTIME_IDENTITY_NOT_BOUND"},
                status_code=503,
                headers={"Cache-Control": "no-store"},
            )

    def run(self, *, host: str, port: int) -> None:
        import uvicorn

        uvicorn.run(self.app, host=host, port=port)


__all__ = [
    "BootstrapServer",
    "IDENTITY_BUILD_MARKER",
    "create_agent_server",
    "identity_config",
    "identity_enabled",
    "identity_required",
]
