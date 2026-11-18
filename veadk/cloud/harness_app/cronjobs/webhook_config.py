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

"""部署端授权的 webhook 地址、凭据引用与配置校验。"""

from __future__ import annotations

import os
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class WebhookFields(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True, strict=True)
    url: str | None = Field(default=None, min_length=1, max_length=2048)
    timeout_seconds: int = Field(default=10, ge=1, le=30)
    max_retries: int = Field(default=3, ge=0, le=3)
    bearer_token_env: str | None = Field(
        default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$"
    )
    include_artifacts: bool = False

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str | None) -> str:
        try:
            parts = urlsplit(value or "")
            valid = (
                parts.scheme == "https"
                and parts.hostname
                and not parts.username
                and not parts.password
                and not parts.fragment
                and "\\" not in (value or "")
                and not any(
                    char.isspace() or ord(char) < 32 or ord(char) == 127
                    for char in value or ""
                )
            )
            parts.port
        except ValueError:
            valid = False
        if not valid:
            raise ValueError(
                "webhook.url must be an HTTPS URL without userinfo, whitespace or fragment"
            )
        return value


class WebhookConfig(WebhookFields):
    url: str = Field(min_length=1, max_length=2048)


class WebhookPatch(WebhookFields):
    @model_validator(mode="after")
    def validate_patch(self) -> WebhookPatch:
        if not self.model_fields_set:
            raise ValueError("Provide at least one webhook field")
        return self


def authorize_webhook(config: WebhookConfig) -> None:
    """部署端明确授权地址与凭据引用，拒绝调用者引用任意服务环境变量。"""
    hosts = {
        item.strip().lower()
        for item in os.getenv("HARNESS_CRONJOB_WEBHOOK_ALLOWED_HOSTS", "").split(",")
        if item.strip()
    }
    if urlsplit(config.url).hostname.lower() not in hosts:
        raise ValueError("Webhook host is not in HARNESS_CRONJOB_WEBHOOK_ALLOWED_HOSTS")
    envs = {
        item.strip()
        for item in os.getenv("HARNESS_CRONJOB_WEBHOOK_TOKEN_ENVS", "").split(",")
        if item.strip()
    }
    if config.bearer_token_env and config.bearer_token_env not in envs:
        raise ValueError(
            "Bearer environment reference is not in HARNESS_CRONJOB_WEBHOOK_TOKEN_ENVS"
        )
