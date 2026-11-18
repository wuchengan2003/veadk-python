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

import asyncio

from veadk.cloud.harness_app.concurrency import (
    ModelAdmissionController,
    ModelCapacityError,
)


def test_global_and_per_user_limits_hold_under_a_burst() -> None:
    async def exercise() -> None:
        controller = ModelAdmissionController(
            max_inflight=3,
            max_inflight_per_user=1,
            queue_timeout_seconds=2,
        )
        active = 0
        peak = 0
        per_user: dict[str, int] = {}

        async def call(user: str) -> None:
            nonlocal active, peak
            async with controller.lease(user):
                active += 1
                peak = max(peak, active)
                per_user[user] = per_user.get(user, 0) + 1
                assert per_user[user] == 1
                await asyncio.sleep(0.01)
                per_user[user] -= 1
                active -= 1

        await asyncio.gather(*(call(f"user-{index % 8}") for index in range(80)))
        assert peak == 3
        assert active == 0

    asyncio.run(exercise())


def test_120_distinct_learners_are_drained_without_capacity_leak() -> None:
    async def exercise() -> None:
        controller = ModelAdmissionController(
            max_inflight=12,
            max_inflight_per_user=1,
            queue_timeout_seconds=3,
        )
        active = 0
        peak = 0
        completed: set[str] = set()

        async def call(user: str) -> None:
            nonlocal active, peak
            async with controller.lease(user):
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.005)
                completed.add(user)
                active -= 1

        await asyncio.gather(*(call(f"learner-{index:03d}") for index in range(120)))
        assert len(completed) == 120
        assert peak == 12
        assert active == 0
        assert controller._users == {}

    asyncio.run(exercise())


def test_queue_timeout_does_not_leak_capacity() -> None:
    async def exercise() -> None:
        controller = ModelAdmissionController(
            max_inflight=1,
            max_inflight_per_user=1,
            queue_timeout_seconds=0.02,
        )
        entered = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> None:
            async with controller.lease("first-user"):
                entered.set()
                await release.wait()

        task = asyncio.create_task(holder())
        await entered.wait()
        try:
            async with controller.lease("second-user"):
                raise AssertionError("overloaded request should not be admitted")
        except ModelCapacityError as exc:
            assert str(exc) == "MODEL_CAPACITY_EXCEEDED"
        release.set()
        await task
        async with controller.lease("second-user"):
            pass

    asyncio.run(exercise())


def test_cancelled_waiter_does_not_block_later_calls() -> None:
    async def exercise() -> None:
        controller = ModelAdmissionController(
            max_inflight=1,
            max_inflight_per_user=1,
            queue_timeout_seconds=2,
        )
        entered = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> None:
            async with controller.lease("same-user"):
                entered.set()
                await release.wait()

        task = asyncio.create_task(holder())
        await entered.wait()

        async def waiter() -> None:
            async with controller.lease("same-user"):
                raise AssertionError("cancelled waiter entered")

        waiting = asyncio.create_task(waiter())
        await asyncio.sleep(0)
        waiting.cancel()
        try:
            await waiting
        except asyncio.CancelledError:
            pass
        release.set()
        await task
        async with controller.lease("same-user"):
            pass

    asyncio.run(exercise())
