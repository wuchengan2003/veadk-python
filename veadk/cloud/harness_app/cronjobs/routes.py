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

"""Management API and server-owned lifecycle for scheduled invocations"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import APIRouter, HTTPException, Path, Query

from .engine import Engine
from .models import CreateJob, PatchJob, public_job
from .webhook_config import WebhookPatch
from .store import Conflict, store_from_env


def mount_cronjobs(app, invoke, *, shared_identity=False):
    if os.getenv("HARNESS_CRONJOB_ENABLED", "").lower() not in {"1", "true"}:
        return
    if shared_identity:
        raise ValueError(
            "Cronjobs require a service-authenticated Harness with direct model credentials"
        )
    engine = Engine(
        store_from_env(),
        invoke,
        concurrency=int(os.getenv("HARNESS_CRONJOB_CONCURRENCY", "8")),
        lease_seconds=int(os.getenv("HARNESS_CRONJOB_LEASE_SECONDS", "60")),
        poll_seconds=float(os.getenv("HARNESS_CRONJOB_POLL_SECONDS", "5")),
    )
    app.state.cronjobs = engine
    router = build_router(engine)
    first = len(app.routes)
    app.include_router(router)
    added = app.routes[first:]
    del app.routes[first:]
    catch_all = next(
        (
            i
            for i, route in enumerate(app.routes)
            if route.__class__.__name__ == "Mount" and route.path in {"", "/"}
        ),
        len(app.routes),
    )
    app.routes[catch_all:catch_all] = added
    previous = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(current):
        async with previous(current):
            await engine.store.probe()
            await engine.start()
            try:
                yield
            finally:
                await engine.close()

    app.router.lifespan_context = lifespan


def build_router(engine):
    router = APIRouter(prefix="/harness/cronjobs")
    job_path = Path(pattern=r"^[a-f0-9]{32}$")
    run_path = Path(pattern=r"^[0-9]{13}-[a-f0-9]{20}$")

    async def call(operation):
        try:
            return await operation
        except KeyError:
            raise HTTPException(404, "Cronjob or execution not found") from None
        except ValueError as error:
            raise HTTPException(422, str(error)) from None
        except Conflict:
            raise HTTPException(
                409, "Cronjob was modified concurrently; retry the request"
            ) from None

    @router.get("/status")
    async def status():
        return engine.status()

    @router.post("", status_code=201)
    async def create(body: CreateJob):
        return await call(engine.create(body))

    @router.get("")
    async def listing(
        cursor: str = Query(default="", max_length=4096),
        limit: int = Query(default=100, ge=1, le=1000),
        user_id: str | None = Query(
            default=None, min_length=1, max_length=256, pattern=r"\S"
        ),
    ):
        return await call(engine.list_jobs(cursor, limit, user_id=user_id))

    @router.get("/{job_id}")
    async def get(job_id: str = job_path):
        job = (await call(engine.get(job_id)))[0]
        if job.get("deleted_at"):
            raise HTTPException(404, "Cronjob or execution not found")
        return public_job(job)

    @router.patch("/{job_id}")
    async def patch(body: PatchJob, job_id: str = job_path):
        return await call(engine.patch(job_id, body))

    @router.delete("/{job_id}")
    async def delete(job_id: str = job_path):
        return await call(engine.delete(job_id))

    @router.post("/{job_id}/pause")
    async def pause(job_id: str = job_path):
        return await call(engine.set_enabled(job_id, False))

    @router.post("/{job_id}/resume")
    async def resume(job_id: str = job_path):
        return await call(engine.set_enabled(job_id, True))

    @router.patch("/{job_id}/webhook")
    async def set_webhook(body: WebhookPatch, job_id: str = job_path) -> dict:
        return await call(engine.set_webhook(job_id, body))

    @router.delete("/{job_id}/webhook")
    async def delete_webhook(job_id: str = job_path) -> dict:
        return await call(engine.set_webhook(job_id, None))

    @router.get("/{job_id}/runs")
    async def runs(
        job_id: str = job_path,
        cursor: str = Query(default="", max_length=4096),
        limit: int = Query(default=20, ge=1, le=1000),
    ):
        return await call(engine.runs(job_id, cursor, limit))

    @router.get("/{job_id}/runs/{run_id}")
    async def result(job_id: str = job_path, run_id: str = run_path):
        return await call(engine.result(job_id, run_id))

    @router.post("/{job_id}/runs/{run_id}/cancel")
    async def cancel(job_id: str = job_path, run_id: str = run_path):
        return await call(engine.cancel(job_id, run_id))

    return router
