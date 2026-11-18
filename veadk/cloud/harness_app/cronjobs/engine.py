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

"""Replica-safe scheduling using a per-job conditional-write journal

The definition, next occurrence, durable queue slot and execution lease share
one TOS object. A successful CAS is the only authority to advance or execute
work. Completed records stay in an outbox until their immutable history object
is confirmed, so there is no cross-object transaction or volatile queue gap.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import random
import socket
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from .models import (
    DEFAULT_USER_ID,
    CreateJob,
    PatchJob,
    Schedule,
    TERMINAL,
    instant,
    public_job,
    stamp,
)
from .store import Conflict
from .webhook_config import WebhookConfig, WebhookPatch, authorize_webhook
from .webhook import completion_payload, deliver_webhook

logger = logging.getLogger("harness.cronjobs")


def utcnow():
    return datetime.now(timezone.utc)


def run_identity(job_id: str, at: datetime) -> str:
    digest = hashlib.sha256(f"{job_id}:{stamp(at)}".encode()).hexdigest()[:20]
    # TOS lists lexicographically; a reverse timestamp gives newest-first pages
    return f"{9_999_999_999_999 - int(at.timestamp() * 1000):013d}-{digest}"


class Engine:
    def __init__(
        self,
        store,
        invoke,
        *,
        concurrency=8,
        lease_seconds=60,
        poll_seconds=5,
        clock=utcnow,
        replica_id=None,
    ):
        if not 1 <= concurrency <= 128 or lease_seconds < 6 or poll_seconds <= 0:
            raise ValueError(
                "Invalid cronjob concurrency, lease or polling configuration"
            )
        self.store, self.invoke, self.clock = store, invoke, clock
        self.concurrency, self.lease_seconds, self.poll_seconds = (
            concurrency,
            lease_seconds,
            poll_seconds,
        )
        self.replica_id = replica_id or f"{socket.gethostname()}:{uuid4().hex[:12]}"
        self.executions: dict[str, asyncio.Task] = {}
        self.loops: list[asyncio.Task] = []
        self.scan_time = None
        self.last_error = None
        self.closed = False

    @staticmethod
    def job_key(job_id):
        return f"jobs/{job_id}.json"

    async def get(self, job_id):
        stored = await self.store.get(self.job_key(job_id))
        if stored is None:
            raise KeyError(job_id)
        return stored

    async def create(self, request: CreateJob):
        if request.webhook is not None:
            authorize_webhook(request.webhook)
        value = request.durable_input()
        now = self.clock()
        # Keep retries of existing tasks compatible with their original hash
        hash_input = {
            key: item
            for key, item in value.items()
            if key != "user_id" or item != DEFAULT_USER_ID
        }
        request_hash = hashlib.sha256(
            json.dumps(hash_input, sort_keys=True).encode()
        ).hexdigest()
        job_id = (
            hashlib.sha256(request.idempotency_key.encode()).hexdigest()[:32]
            if request.idempotency_key
            else uuid4().hex
        )
        if request.idempotency_key:
            stored = await self.store.get(self.job_key(job_id))
            if stored is not None:
                if stored[0].get("deleted_at"):
                    raise Conflict()
                if stored[0].get("request_hash") != request_hash:
                    raise ValueError(
                        "Idempotency key already belongs to a different request"
                    )
                return public_job(stored[0])
        next_time = request.schedule.next_after(now)
        if request.enabled and next_time is None:
            raise ValueError("The one-time schedule must be in the future")
        job = {
            **value,
            "cronjob_id": job_id,
            "revision": 1,
            "request_hash": request_hash,
            "created_at": stamp(now),
            "updated_at": stamp(now),
            "next_run_at": stamp(next_time) if next_time else None,
            "active": None,
            "outbox": [],
            "last_run": None,
        }
        try:
            await self.store.put(self.job_key(job_id), job)
        except Conflict:
            existing, _ = await self.get(job_id)
            if existing.get("deleted_at"):
                raise Conflict() from None
            if (
                not request.idempotency_key
                or existing.get("request_hash") != request_hash
            ):
                raise ValueError(
                    "Idempotency key already belongs to a different request"
                ) from None
            return public_job(existing)
        return public_job(job)

    async def list_jobs(self, cursor="", limit=100, user_id=None):
        items = []
        while True:
            keys, cursor = await self.store.list(
                "jobs/", cursor=cursor, limit=limit - len(items)
            )
            stored = await asyncio.gather(*(self.store.get(key) for key in keys))
            items.extend(
                public_job(item[0])
                for item in stored
                if item
                and not item[0].get("deleted_at")
                and (
                    user_id is None
                    or item[0].get("user_id", DEFAULT_USER_ID) == user_id
                )
            )
            if len(items) >= limit or not cursor:
                return {"items": items, "next_cursor": cursor}

    async def runs(self, job_id, cursor="", limit=20):
        await self.get(job_id)
        keys, cursor = await self.store.list(
            f"runs/{job_id}/", cursor=cursor, limit=limit
        )
        stored = await asyncio.gather(*(self.store.get(key) for key in keys))
        return {
            "items": [public_job(item[0]) for item in stored if item],
            "next_cursor": cursor,
        }

    async def result(self, job_id, run_id):
        stored = await self.store.get(f"runs/{job_id}/{run_id}.json")
        if stored:
            return public_job(stored[0])
        job, _ = await self.get(job_id)
        for run in [job.get("active"), *job.get("outbox", [])]:
            if run and run["run_id"] == run_id:
                return public_job(run)
        raise KeyError(run_id)

    async def set_enabled(self, job_id, enabled):
        for _ in range(8):
            job, etag = await self.get(job_id)
            if job.get("deleted_at"):
                raise KeyError(job_id)
            now = self.clock()
            if enabled and not job["enabled"]:
                next_time = Schedule.model_validate(job["schedule"]).next_after(now)
                if next_time is None:
                    raise ValueError("Completed one-time jobs cannot be resumed")
                job["next_run_at"] = stamp(next_time)
            job["enabled"] = enabled
            job["updated_at"] = stamp(now)
            job["revision"] += 1
            active = job.get("active")
            if not enabled and active and active["state"] == "queued":
                self._terminal(
                    active, "cancelled", now, error="Task paused before execution"
                )
            try:
                await self.store.put(self.job_key(job_id), job, etag)
                return public_job(job)
            except Conflict:
                continue
        raise Conflict()

    async def patch(self, job_id: str, patch: PatchJob) -> dict:
        changes = patch.durable_changes()
        for _ in range(8):
            job, etag = await self.get(job_id)
            if job.get("deleted_at"):
                raise KeyError(job_id)
            if isinstance(changes.get("harness"), dict):
                merged_harness = {**(job.get("harness") or {}), **changes["harness"]}
                if changes["harness"].get("model_api_key") == "":
                    merged_harness.pop("model_api_key", None)
                changes_for_job = {**changes, "harness": merged_harness or None}
            else:
                changes_for_job = changes
            if (
                "user_id" in changes
                and changes["user_id"] != job.get("user_id", DEFAULT_USER_ID)
                and (job.get("harness") or {}).get("model_api_key")
                and not (
                    "harness" in changes
                    and (
                        changes["harness"] is None
                        or "model_api_key" in changes["harness"]
                    )
                )
            ):
                raise ValueError(
                    "Changing the user requires a new or cleared model_api_key"
                )
            definition = {
                field: job[field] for field in CreateJob.model_fields if field in job
            }
            definition.update(changes_for_job)
            updated = CreateJob.model_validate(definition).durable_input()
            now = self.clock()
            changed_schedule = "schedule" in changes
            resuming = updated["enabled"] and not job["enabled"]
            if changed_schedule or resuming:
                next_time = Schedule.model_validate(updated["schedule"]).next_after(now)
                if updated["enabled"] and next_time is None:
                    raise ValueError("The one-time schedule must be in the future")
                job["next_run_at"] = stamp(next_time) if next_time else None
            active = job.get("active")
            if active and active["state"] == "queued":
                if changed_schedule or not updated["enabled"]:
                    self._terminal(
                        active, "cancelled", now, error="Task changed before execution"
                    )
                else:
                    for field in ("prompt", "harness", "user_id", "timeout_seconds"):
                        if field in changes:
                            active[field] = copy.deepcopy(updated[field])
                    if "name" in changes and "task_name" in active:
                        active["task_name"] = updated["name"]
                    active["revision"] = job["revision"] + 1
            job.update(updated)
            job["revision"] += 1
            job["updated_at"] = stamp(now)
            try:
                await self.store.put(self.job_key(job_id), job, etag)
                return public_job(job)
            except Conflict:
                continue
        raise Conflict()

    async def delete(self, job_id: str) -> dict:
        for _ in range(8):
            job, etag = await self.get(job_id)
            if job.get("deleted_at"):
                return {"cronjob_id": job_id, "deleted_at": job["deleted_at"]}
            now = stamp(self.clock())
            active = job.get("active")
            if active and active["state"] == "queued":
                self._terminal(
                    active,
                    "cancelled",
                    self.clock(),
                    error="Task deleted before execution",
                )
                active["harness"] = None
                active["prompt"] = ""
            job.update(
                deleted_at=now,
                updated_at=now,
                enabled=False,
                next_run_at=None,
                harness=None,
                prompt="",
                revision=job["revision"] + 1,
            )
            job.pop("webhook", None)
            try:
                await self.store.put(self.job_key(job_id), job, etag)
                return {"cronjob_id": job_id, "deleted_at": now}
            except Conflict:
                continue
        raise Conflict()

    async def set_webhook(self, job_id: str, patch: WebhookPatch | None) -> dict:
        for _ in range(8):
            job, etag = await self.get(job_id)
            if job.get("deleted_at"):
                raise KeyError(job_id)
            if patch is None:
                if not job.get("webhook"):
                    return public_job(job)
                job.pop("webhook", None)
            else:
                changes = patch.model_dump(exclude_unset=True)
                config = WebhookConfig.model_validate(
                    {**(job.get("webhook") or {}), **changes}
                )
                authorize_webhook(config)
                job["webhook"] = config.model_dump()
            job["revision"] += 1
            job["updated_at"] = stamp(self.clock())
            try:
                await self.store.put(self.job_key(job_id), job, etag)
                return public_job(job)
            except Conflict:
                continue
        raise Conflict()

    async def cancel(self, job_id, run_id):
        for _ in range(8):
            job, etag = await self.get(job_id)
            active = job.get("active")
            if not active or active["run_id"] != run_id:
                return await self.result(job_id, run_id)
            if active["state"] in TERMINAL:
                return public_job(active)
            active["cancel_requested"] = True
            if active["state"] == "queued":
                self._terminal(active, "cancelled", self.clock())
            try:
                await self.store.put(self.job_key(job_id), job, etag)
                return public_job(active)
            except Conflict:
                continue
        raise Conflict()

    async def _keys(self):
        keys, cursor = [], ""
        while True:
            page, cursor = await self.store.list("jobs/", cursor=cursor, limit=1000)
            keys.extend(page)
            if not cursor:
                random.shuffle(keys)
                return keys

    def _new_run(self, job, due, now):
        run = {
            "cronjob_id": job["cronjob_id"],
            "run_id": run_identity(job["cronjob_id"], due),
            "revision": job["revision"],
            "scheduled_at": stamp(due),
            "created_at": stamp(now),
            "prompt": job["prompt"],
            "harness": copy.deepcopy(job["harness"]),
            "user_id": job.get("user_id", DEFAULT_USER_ID),
            "timeout_seconds": job["timeout_seconds"],
            "state": "queued",
            "owner": None,
            "token": None,
            "started_at": None,
            "completed_at": None,
            "lease_until": None,
            "cancel_requested": False,
            "output": "",
            "error": "",
        }
        if job.get("webhook"):
            # 排队时冻结配置，后续修改和删除不影响本次执行。
            run.update(
                webhook=copy.deepcopy(job["webhook"]),
                task_name=job["name"],
                schedule=copy.deepcopy(job["schedule"]),
            )
        return run

    @staticmethod
    def _terminal(run, state, now, *, output="", error="", artifacts=None):
        run.update(state=state, completed_at=stamp(now), output=output, error=error)
        if artifacts:
            run["artifacts"] = artifacts

    async def _archive(self, job, etag):
        records = list(job.get("outbox", []))
        if job.get("active") and job["active"]["state"] in TERMINAL:
            records.append(job["active"])
        if not records:
            return job, etag
        for record in records:
            payload = public_job(record)
            payload.pop("token", None)
            key = f"runs/{job['cronjob_id']}/{record['run_id']}.json"
            try:
                await self.store.put(key, payload)
            except Conflict:
                existing = await self.store.get(key)
                if existing is None or public_job(existing[0]) != payload:
                    raise RuntimeError("Conflicting immutable execution history")
        job["outbox"] = []
        if job.get("active") and job["active"]["state"] in TERMINAL:
            job["active"] = None
        newest = max(records, key=lambda run: run["scheduled_at"])
        if (
            not job.get("last_run")
            or newest["scheduled_at"] >= job["last_run"]["scheduled_at"]
        ):
            job["last_run"] = {
                key: newest[key]
                for key in ("run_id", "state", "scheduled_at", "completed_at", "error")
            }
        etag = await self.store.put(self.job_key(job["cronjob_id"]), job, etag)
        return job, etag

    async def scan_job(self, key):
        try:
            stored = await self.store.get(key)
            if stored is None:
                return
            job, etag = await self._archive(*stored)
            now = self.clock()
            active = job.get("active")
            if (
                active
                and active["state"] == "running"
                and instant(active["lease_until"]) <= now
            ):
                self._terminal(
                    active,
                    "interrupted",
                    now,
                    error="Worker lease expired; final outcome is unknown",
                )
                # Do not overlap an external operation whose outcome is unknown
                job["enabled"] = False
                await self.store.put(key, job, etag)
                return
            if not job["enabled"] or not job.get("next_run_at"):
                return
            first = instant(job["next_run_at"])
            if first > now:
                return
            schedule = Schedule.model_validate(job["schedule"])
            due = schedule.latest_due(now, first)
            run = self._new_run(job, due, now)
            if due != first:
                run["coalesced_from"] = stamp(first)
            future = schedule.next_after(now)
            job["next_run_at"] = stamp(future) if future else None
            job["updated_at"] = stamp(now)
            if active:
                self._terminal(
                    run, "skipped", now, error="Previous execution is still active"
                )
                job["outbox"].append(run)
            elif (
                job["misfire_policy"] == "skip"
                and (now - due).total_seconds() > job["misfire_grace_seconds"]
            ):
                self._terminal(
                    run,
                    "skipped",
                    now,
                    error="Missed schedule outside the allowed grace period",
                )
                job["outbox"].append(run)
            else:
                job["active"] = run
            await self.store.put(key, job, etag)
        except Conflict:
            pass  # Another replica progressed this job; observe it next sweep

    async def scan_once(self):
        semaphore = asyncio.Semaphore(16)

        async def visit(key):
            async with semaphore:
                try:
                    await self.scan_job(key)
                except Exception as error:
                    self.last_error = type(error).__name__
                    logger.exception("Cronjob scan failed for %s", key)

        await asyncio.gather(*(visit(key) for key in await self._keys()))
        self.scan_time = stamp(self.clock())

    async def claim(self, key):
        if len(self.executions) >= self.concurrency or self.closed:
            return False
        stored = await self.store.get(key)
        if stored is None:
            return False
        job, etag = stored
        run = job.get("active")
        if not job["enabled"] or not run or run["state"] != "queued":
            return False
        run.setdefault("user_id", job.get("user_id", DEFAULT_USER_ID))
        now = self.clock()
        run.update(
            state="running",
            owner=self.replica_id,
            token=uuid4().hex,
            started_at=stamp(now),
            lease_until=stamp(now + timedelta(seconds=self.lease_seconds)),
        )
        try:
            await self.store.put(key, job, etag)
        except Conflict:
            return False
        task = asyncio.create_task(self._execute(key, copy.deepcopy(run)))
        self.executions[run["run_id"]] = task
        task.add_done_callback(lambda _: self.executions.pop(run["run_id"], None))
        return True

    async def work_once(self):
        for key in await self._keys():
            if len(self.executions) >= self.concurrency or self.closed:
                break
            await self.claim(key)

    async def _owned_update(self, key, run, change):
        for _ in range(8):
            stored = await self.store.get(key)
            if stored is None:
                return False
            job, etag = stored
            current = job.get("active")
            now = self.clock()
            if (
                not current
                or current["state"] != "running"
                or current["token"] != run["token"]
                or current["owner"] != self.replica_id
                or instant(current["lease_until"]) <= now
            ):
                return False
            change(current, job, now)
            try:
                await self.store.put(key, job, etag)
                return True
            except Conflict:
                continue
        return False

    async def _renew_execution(
        self,
        key: str,
        run: dict,
        invocation: asyncio.Task,
        lost: asyncio.Event,
        cancelled: asyncio.Event,
    ) -> None:
        try:
            while True:
                await asyncio.sleep(self.lease_seconds / 3)

                def update(current: dict, _job: dict, now: datetime) -> None:
                    if current["cancel_requested"]:
                        cancelled.set()
                    current["lease_until"] = stamp(
                        now + timedelta(seconds=self.lease_seconds)
                    )

                if not await self._owned_update(key, run, update):
                    lost.set()
                if lost.is_set() or cancelled.is_set():
                    invocation.cancel()
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            lost.set()
            invocation.cancel()
            logger.exception("Cronjob lease renewal failed")

    async def _invocation_result(
        self,
        invocation: asyncio.Task,
        run: dict,
        cancelled: asyncio.Event,
    ) -> tuple[str, str, str, list[dict]]:
        state, output, error = "succeeded", "", ""
        artifacts: list[dict] = []
        try:
            result = await asyncio.wait_for(invocation, timeout=run["timeout_seconds"])
            if isinstance(result, tuple) and len(result) == 2:
                result, raw_artifacts = result
                if isinstance(raw_artifacts, list):
                    artifacts = [
                        item for item in raw_artifacts if isinstance(item, dict)
                    ][:50]
            output = str(result)
            if len(output.encode()) > 1024 * 1024:
                raise ValueError("Execution output exceeds 1 MiB")
        except asyncio.TimeoutError:
            state, error = "failed", "Execution timed out"
        except asyncio.CancelledError:
            state = "cancelled" if cancelled.is_set() else "interrupted"
            error = (
                "Cancellation requested"
                if cancelled.is_set()
                else "Worker stopped; final outcome may be unknown"
            )
        except Exception as failure:
            error = str(failure)
            harness = run.get("harness") or {}
            secrets = [
                harness.get("model_api_key"),
                *(server.get("api_key") for server in harness.get("mcp_servers", [])),
            ]
            for secret in secrets:
                if isinstance(secret, str) and secret:
                    error = error.replace(secret, "********")
            state, error = "failed", error[:4096]
        finally:
            invocation.cancel()
            await asyncio.gather(invocation, return_exceptions=True)
        return state, output if state == "succeeded" else "", error, artifacts

    async def _persist_completion(
        self,
        key: str,
        run: dict,
        state: str,
        output: str,
        error: str,
        artifacts: list[dict] | None = None,
    ) -> tuple[dict, dict] | None:
        notification: tuple[dict, dict] | None = None

        def finish(current: dict, job: dict, now: datetime) -> None:
            nonlocal notification
            final_state = "cancelled" if current["cancel_requested"] else state
            self._terminal(
                current,
                final_state,
                now,
                output=output,
                error=error,
                artifacts=artifacts,
            )
            if final_state == "interrupted":
                job["enabled"] = False
            if current.get("webhook") and final_state in {"succeeded", "failed"}:
                notification = (
                    copy.deepcopy(current["webhook"]),
                    completion_payload(current, job),
                )

        for attempt in range(3):
            try:
                committed = await self._owned_update(key, run, finish)
                # 仅确认本副本写入成功才通知；写入结果未知时不重放。
                return notification if committed else None
            except Exception:
                if attempt == 2:
                    # 持久化失败时保留 running 日志，交由租约恢复处理。
                    logger.exception("Unable to persist cronjob completion")
                else:
                    await asyncio.sleep(1)
        return None

    async def _execute(self, key: str, run: dict) -> None:
        invocation = asyncio.create_task(self.invoke(run))
        lost, cancelled = asyncio.Event(), asyncio.Event()
        heartbeat = asyncio.create_task(
            self._renew_execution(key, run, invocation, lost, cancelled)
        )
        state, output, error, artifacts = await self._invocation_result(
            invocation, run, cancelled
        )
        notification: tuple[dict, dict] | None = None
        try:
            if not lost.is_set():
                notification = await self._persist_completion(
                    key, run, state, output, error, artifacts=artifacts
                )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        if notification is not None:
            try:
                await deliver_webhook(*notification)
            except asyncio.CancelledError:
                raise
            except Exception as failure:
                # 不将投递异常写回执行结果，也不输出可能含凭据的异常原文。
                logger.error(
                    "Cronjob webhook delivery failed (%s)", type(failure).__name__
                )

    async def start(self):
        self.closed = False

        async def repeat(operation, delay):
            while True:
                try:
                    await operation()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.last_error = type(error).__name__
                    logger.exception("Cronjob background loop failed")
                await asyncio.sleep(delay)

        self.loops = [
            asyncio.create_task(repeat(self.scan_once, self.poll_seconds)),
            asyncio.create_task(repeat(self.work_once, 1)),
        ]

    async def close(self):
        self.closed = True
        tasks = [*self.loops, *self.executions.values()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def status(self):
        return {
            "replica_id": self.replica_id,
            "running": len(self.executions),
            "concurrency": self.concurrency,
            "last_scan_at": self.scan_time,
            "last_error": self.last_error,
            "active": bool(self.loops) and all(not task.done() for task in self.loops),
        }
