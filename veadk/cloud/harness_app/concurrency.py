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

"""Bounded, subject-aware admission for expensive shared Harness model calls."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator


class ModelCapacityError(RuntimeError):
    """The bounded model queue could not admit a request in time."""


@dataclass
class _UserGate:
    semaphore: asyncio.Semaphore
    references: int = 0


class ModelAdmissionController:
    """Limit model pressure globally and per verified user within one replica.

    Runtime ``max_concurrency`` protects the HTTP process.  This narrower gate
    protects the scarce model/Broker path and prevents one user from consuming
    every model slot.  Tenant-wide token limits still belong at Model Gateway.
    """

    def __init__(
        self,
        *,
        max_inflight: int,
        max_inflight_per_user: int,
        queue_timeout_seconds: float,
    ) -> None:
        if max_inflight < 1:
            raise ValueError("max_inflight must be positive")
        if not 1 <= max_inflight_per_user <= max_inflight:
            raise ValueError(
                "max_inflight_per_user must be positive and no greater than max_inflight"
            )
        if queue_timeout_seconds <= 0:
            raise ValueError("queue_timeout_seconds must be positive")
        self._global = asyncio.Semaphore(max_inflight)
        self._max_inflight_per_user = max_inflight_per_user
        self._queue_timeout_seconds = float(queue_timeout_seconds)
        self._users: dict[str, _UserGate] = {}
        self._users_lock = asyncio.Lock()

    async def _retain_user(self, user_key: str) -> _UserGate:
        if not user_key:
            raise ValueError("a verified user key is required")
        async with self._users_lock:
            gate = self._users.get(user_key)
            if gate is None:
                gate = _UserGate(asyncio.Semaphore(self._max_inflight_per_user))
                self._users[user_key] = gate
            gate.references += 1
            return gate

    async def _release_user(self, user_key: str, gate: _UserGate) -> None:
        async with self._users_lock:
            gate.references -= 1
            if gate.references == 0 and self._users.get(user_key) is gate:
                self._users.pop(user_key, None)

    @asynccontextmanager
    async def lease(self, user_key: str) -> AsyncIterator[float]:
        """Admit one call before a shared deadline and release on cancellation."""

        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + self._queue_timeout_seconds
        user_gate = await self._retain_user(user_key)
        user_acquired = False
        global_acquired = False
        try:
            remaining = max(0.0, deadline - loop.time())
            await asyncio.wait_for(user_gate.semaphore.acquire(), timeout=remaining)
            user_acquired = True
            remaining = max(0.0, deadline - loop.time())
            await asyncio.wait_for(self._global.acquire(), timeout=remaining)
            global_acquired = True
            yield loop.time() - started
        except asyncio.TimeoutError:
            raise ModelCapacityError("MODEL_CAPACITY_EXCEEDED") from None
        finally:
            if global_acquired:
                self._global.release()
            if user_acquired:
                user_gate.semaphore.release()
            await self._release_user(user_key, user_gate)


__all__ = ["ModelAdmissionController", "ModelCapacityError"]
