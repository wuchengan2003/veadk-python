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

"""任务级 webhook 配置、完成事件映射与进程内有限重试。"""

from __future__ import annotations

import asyncio

try:
    from asyncio import timeout
except ImportError:  # Python 3.10
    from async_timeout import timeout
import hashlib
import json
import logging
import os
import time
from typing import Literal
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..server_types import HARNESS_APP_NAME
from .models import DEFAULT_USER_ID, instant, stamp
from .webhook_config import WebhookConfig, authorize_webhook

logger = logging.getLogger("harness.cronjobs.webhook")


def completion_payload(run: dict, job: dict) -> dict:
    schedule = run["schedule"]
    kind = "once" if schedule.get("at") else "cron"
    definition = {"type": kind, "timezone": schedule["timezone"]}
    definition.update(
        {"at": stamp(instant(schedule["at"]))}
        if kind == "once"
        else {"expression": schedule["cron"]}
    )
    event_id = hashlib.sha256(
        f"cronjob.completed:{run['cronjob_id']}:{run['run_id']}".encode()
    ).hexdigest()
    result = {
        "output": run["output"] if run["state"] == "succeeded" else "",
        "error": None
        if run["state"] == "succeeded"
        else run["error"] or "Execution failed",
    }
    artifacts = run.get("artifacts") or []
    if artifacts and (run.get("webhook") or {}).get("include_artifacts"):
        result["artifacts"] = [
            {
                "name": item.get("name", ""),
                "url": item.get("url", ""),
                "mimeType": item.get("mime_type"),
                "size": item.get("size"),
                "source": item.get("source", ""),
                "version": item.get("version"),
            }
            for item in artifacts[:50]
        ]
    return {
        "schemaVersion": "2",
        "eventType": "cronjob.completed",
        "eventID": event_id,
        "taskID": run["cronjob_id"],
        "runID": run["run_id"],
        "taskName": run["task_name"],
        "taskType": kind,
        "schedule": definition,
        "scheduledAt": run["scheduled_at"],
        "startedAt": run["started_at"],
        "completedAt": run["completed_at"],
        "nextRunAt": job.get("next_run_at") if job["enabled"] else None,
        "status": run["state"],
        "target": {
            "userID": run.get("user_id", DEFAULT_USER_ID),
            "sessionID": run["run_id"],
            "agentID": HARNESS_APP_NAME,
        },
        "result": result,
    }


class ResponseError(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")
    message: str = Field(min_length=1, max_length=1024)


class WebhookResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True, strict=True)
    schemaVersion: Literal["2"]
    eventID: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    accepted: bool
    error: ResponseError | None

    @model_validator(mode="after")
    def validate_ack(self) -> WebhookResponse:
        if self.accepted and (self.error is not None or self.eventID is None):
            raise ValueError("Accepted responses require eventID and null error")
        if not self.accepted and self.error is None:
            raise ValueError("Rejected responses require error")
        return self


def response_issue(body: bytes, status: int, event_id: str) -> str | None:
    if not body and status == 204:
        return None
    try:
        ack = WebhookResponse.model_validate_json(body)
    except ValueError:
        return "invalid_response_schema"
    if ack.accepted != (200 <= status < 300):
        return "inconsistent_response_status"
    if ack.accepted and ack.eventID != event_id:
        return "mismatched_event_id"
    return None


async def request_once(
    client: httpx.AsyncClient,
    config: WebhookConfig,
    body: bytes,
    headers: dict[str, str],
    event_id: str,
) -> tuple[int, str | None]:
    """总超时覆盖响应读取；限制响应内存且不记录接收方原文。"""
    try:
        async with timeout(config.timeout_seconds):
            async with client.stream(
                "POST",
                config.url,
                content=body,
                headers=headers,
                timeout=config.timeout_seconds,
            ) as response:
                chunks = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    chunks.extend(chunk)
                    if len(chunks) > 16384:
                        return response.status_code, "response_too_large"
                return response.status_code, response_issue(
                    bytes(chunks), response.status_code, event_id
                )
    except asyncio.TimeoutError as error:
        raise TimeoutError("Webhook response deadline exceeded") from error


def request_headers(config: WebhookConfig) -> dict[str, str]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if config.bearer_token_env:
        token = os.getenv(config.bearer_token_env, "")
        if not token.strip() or any(
            ord(char) < 32 or ord(char) >= 127 for char in token
        ):
            raise ValueError("Bearer environment value is missing or invalid")
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def deliver_webhook(configuration: dict, payload: dict) -> None:
    context = {
        "taskID": payload["taskID"],
        "runID": payload["runID"],
        "eventID": payload["eventID"],
    }
    try:
        config = WebhookConfig.model_validate(configuration)
        authorize_webhook(config)
        headers = request_headers(config)
    except ValueError:
        logger.warning(
            "Cronjob webhook configuration rejected",
            extra={**context, "delivery": "configuration_error"},
        )
        return
    context["webhook_host"] = urlsplit(config.url).hostname
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
        for attempt in range(config.max_retries + 1):
            started = time.monotonic()
            status, issue = None, None
            try:
                status, issue = await request_once(
                    client, config, body, headers, payload["eventID"]
                )
            except (httpx.TransportError, TimeoutError) as error:
                issue = type(error).__name__  # 异常原文可能含 URL 或凭据，禁止输出。
            http_success = status is not None and 200 <= status < 300
            # 2xx 还须通过 ACK 校验；协议错误按同一事件有限重试，不能记为已投递。
            delivered = http_success and issue is None
            retryable = (
                status is None
                or status == 429
                or (status is not None and 500 <= status <= 599)
                or (http_success and issue is not None)
            )
            retry = retryable and attempt < config.max_retries
            logger.log(
                logging.INFO if delivered and not issue else logging.WARNING,
                "Cronjob webhook attempt",
                extra={
                    **context,
                    "attempt": attempt + 1,
                    "http_status": status,
                    "protocol_issue": issue,
                    "elapsed_seconds": time.monotonic() - started,
                    "delivery": "delivered"
                    if delivered
                    else "retrying"
                    if retry
                    else "delivery_failed",
                },
            )
            if not retry or delivered:
                return
            await asyncio.sleep(2**attempt)
