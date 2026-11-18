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

"""Distributed startup gate for the horizontally scaled session store."""

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import text

# Stable, deployment-specific signed bigint (ASCII ``PDSASHAR``).  Every
# replica uses the same advisory lock before ADK's lazy create_all, preventing a
# classroom-wide cold start from racing PostgreSQL DDL.
_POSTGRES_INIT_LOCK = 0x5044534153484152


async def initialise_shared_session_store(
    short_term_memory: Any,
    *,
    app_name: str,
    backend: str,
    lock_timeout_seconds: float = 60,
) -> None:
    """Warm ADK session tables once per database before serving traffic.

    Only PostgreSQL needs the distributed lock.  Other backends retain their
    existing behavior.  A timeout or database error is deliberately allowed to
    fail application startup, so a replica with no shared session store never
    becomes Ready and silently falls back to process-local state.
    """

    if backend != "postgresql":
        return
    if not app_name or lock_timeout_seconds <= 0:
        raise RuntimeError("shared session-store bootstrap is misconfigured")
    service = getattr(short_term_memory, "session_service", None)
    engine = getattr(service, "db_engine", None)
    if service is None or engine is None or engine.dialect.name != "postgresql":
        raise RuntimeError("PostgreSQL session backend is not active")

    deadline = asyncio.get_running_loop().time() + lock_timeout_seconds
    acquired = False
    async with engine.connect() as connection:
        while asyncio.get_running_loop().time() < deadline:
            acquired = bool(
                await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:key)"),
                    {"key": _POSTGRES_INIT_LOCK},
                )
            )
            if acquired:
                break
            await asyncio.sleep(0.25)
        if not acquired:
            raise RuntimeError("shared session-store bootstrap lock timed out")
        try:
            await service.list_sessions(
                app_name=app_name,
                user_id="__agentkit_shared_harness_bootstrap__",
            )
        finally:
            released = await connection.scalar(
                text("SELECT pg_advisory_unlock(:key)"),
                {"key": _POSTGRES_INIT_LOCK},
            )
            if released is not True:
                raise RuntimeError("shared session-store bootstrap lock was lost")


__all__ = ["initialise_shared_session_store"]
