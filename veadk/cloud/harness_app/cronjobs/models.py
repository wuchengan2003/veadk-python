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

"""Validated schedules and job input shared by the API and scheduler"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from zoneinfo import ZoneInfo

from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..server_types import HarnessOverrides
from .webhook_config import WebhookConfig


DEFAULT_USER_ID = "harness-cronjobs"


def stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Schedule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cron: str | None = None
    at: datetime | None = None
    timezone: str = "Asia/Shanghai"

    @model_validator(mode="after")
    def validate_schedule(self):
        try:
            ZoneInfo(self.timezone)
        except (KeyError, ValueError):
            raise ValueError("timezone must be an IANA timezone") from None
        if (self.cron is None) == (self.at is None):
            raise ValueError("Specify exactly one of cron or at")
        if self.at is not None and self.at.utcoffset() is None:
            raise ValueError("at must include a timezone offset")
        if self.cron is not None:
            if len(self.cron.split()) != 5 or not croniter.is_valid(self.cron):
                raise ValueError("cron must be a valid five-field expression")
            self.next_after(datetime.now(timezone.utc))
        return self

    def next_after(self, value: datetime) -> datetime | None:
        if self.at is not None:
            return self.at.astimezone(timezone.utc) if self.at > value else None
        try:
            local = value.astimezone(ZoneInfo(self.timezone))
            return (
                croniter(self.cron, local, max_years_between_matches=8)
                .get_next(datetime)
                .astimezone(timezone.utc)
            )
        except (ValueError, OverflowError):
            raise ValueError("Schedule has no occurrence within eight years") from None

    def latest_due(self, now: datetime, first: datetime) -> datetime:
        if self.at is not None:
            return self.at.astimezone(timezone.utc)
        # get_prev is exclusive, so include a schedule exactly at now
        from datetime import timedelta

        local = (now + timedelta(microseconds=1)).astimezone(ZoneInfo(self.timezone))
        previous = croniter(self.cron, local, max_years_between_matches=8).get_prev(
            datetime
        )
        return max(first, previous.astimezone(timezone.utc))


class JobHarnessOverrides(HarnessOverrides):
    """任务保存资源选择参数，每次执行仍由 Harness 校验并按运行创建绑定。"""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


def durable_harness(overrides: JobHarnessOverrides) -> dict:
    value = overrides.model_dump(mode="json", exclude_unset=True)
    if "model_api_key" in overrides.model_fields_set:
        value["model_api_key"] = overrides.model_api_key.get_secret_value()
    if "mcp_servers" in value:
        for target, original in zip(value["mcp_servers"], overrides.mcp_servers):
            if original.api_key is not None:
                target["api_key"] = original.api_key.get_secret_value()
    return value


class CreateJob(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    name: str = Field(min_length=1, max_length=128)
    prompt: str = Field(min_length=1, max_length=65536)
    user_id: str = Field(
        default=DEFAULT_USER_ID, min_length=1, max_length=256, pattern=r"\S"
    )
    schedule: Schedule
    harness: JobHarnessOverrides | None = None
    webhook: WebhookConfig | None = None
    enabled: bool = True
    timeout_seconds: int = Field(default=1800, ge=1, le=10800)
    misfire_policy: Literal["latest", "skip"] = "latest"
    misfire_grace_seconds: int = Field(default=120, ge=0, le=86400)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)

    def durable_input(self) -> dict:
        payload = self.model_dump(mode="json", exclude={"idempotency_key", "harness"})
        # 未配置 webhook 时保持旧任务创建幂等哈希不变。
        if self.webhook is None:
            payload.pop("webhook", None)
        payload["harness"] = None
        if self.harness is not None:
            payload["harness"] = durable_harness(self.harness)
        return payload


class PatchJob(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    name: str | None = Field(default=None, min_length=1, max_length=128)
    prompt: str | None = Field(default=None, min_length=1, max_length=65536)
    user_id: str | None = Field(
        default=None, min_length=1, max_length=256, pattern=r"\S"
    )
    schedule: Schedule | None = None
    harness: JobHarnessOverrides | None = None
    enabled: bool | None = None
    timeout_seconds: int | None = Field(default=None, ge=1, le=10800)
    misfire_policy: Literal["latest", "skip"] | None = None
    misfire_grace_seconds: int | None = Field(default=None, ge=0, le=86400)

    @model_validator(mode="after")
    def validate_patch(self):
        if not self.model_fields_set:
            raise ValueError("Provide at least one task field")
        for field in self.model_fields_set - {"harness"}:
            if getattr(self, field) is None:
                raise ValueError(f"{field} cannot be null")
        if (
            self.harness is not None
            and "harness" in self.model_fields_set
            and not self.harness.model_fields_set
        ):
            raise ValueError("Provide at least one harness field or null to clear it")
        return self

    def durable_changes(self) -> dict:
        changes = self.model_dump(mode="json", exclude_unset=True, exclude={"harness"})
        if "harness" in self.model_fields_set:
            changes["harness"] = (
                durable_harness(self.harness) if self.harness is not None else None
            )
        return changes


TERMINAL = {"succeeded", "failed", "cancelled", "skipped", "interrupted"}


def public_job(value: dict) -> dict:
    import copy

    result = copy.deepcopy(value)
    result.setdefault("user_id", DEFAULT_USER_ID)
    result.pop("outbox", None)
    result.pop("request_hash", None)
    result.pop("token", None)
    for config in [result.get("harness"), (result.get("active") or {}).get("harness")]:
        if config and "model_api_key" in config:
            config["model_api_key"] = "********"
        for server in (config or {}).get("mcp_servers", []):
            if server.get("api_key"):
                server["api_key"] = "********"
    active = result.get("active")
    if active:
        active.setdefault("user_id", result["user_id"])
        active.pop("token", None)
    return result
