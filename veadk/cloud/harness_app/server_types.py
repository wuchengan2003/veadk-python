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

"""Harness parameter schemas for the deployable harness server.

This server was ported from agentkit-cli and depends on VeADK's
*public* API (``Agent``, ``Runner``, ``KnowledgeBase``, ``LongTermMemory``,
``ShortTermMemory``, ``get_builtin_tool``) for core agent assembly. The legacy SDK schemas in ``types.py`` remain separate. Skill-space loading uses VeADK's
skills loader and materializer.

``HarnessConfig`` is the full set of parameters fixed at agent-creation time,
assembled from environment variables (see ``harness_agent.config_from_env``).
``tools`` and ``skills`` are comma-separated strings (e.g. ``"web_search,web_fetch"``).
Skills may be Skill Hub slugs, ``ss-...`` space ids, or ``ss-...:s-...`` refs.
"""

import ipaddress
import json
import os
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, SecretStr, field_validator


HARNESS_APP_NAME = "harness_agent"

# Knowledge base backends VeADK can construct directly (see
# ``veadk.knowledgebase.knowledgebase._get_backend_cls``). A request may only
# select one of these; connection parameters stay with the deployment.
KnowledgebaseBackend = Literal[
    "local",
    "opensearch",
    "redis",
    "milvus",
    "tos_vector",
    "viking",
    "context_search",
    "openviking",
]

# Index names become vector-store collection names, and tool ids are sent to the
# AgentKit control plane — keep both to a conservative, log-safe character set.
KNOWLEDGEBASE_INDEX_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
SANDBOX_TOOL_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$"

SANDBOX_TOOL_ID_ALLOWLIST_ENV = "HARNESS_SANDBOX_TOOL_ID_ALLOWLIST"

# SSRF guardrails. Link-local addresses carry the cloud metadata service
# (169.254.169.254), which hands out instance credentials — no legitimate
# harness endpoint targets them. RFC1918 private ranges and loopback stay
# allowed on purpose: reaching an in-VPC MCP server or a same-container sidecar
# is a supported deployment, so blocking them would break real setups.
_BLOCKED_NETWORKS = (
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("fe80::/10"),
)


def reject_metadata_host(value: str, field: str) -> None:
    """Raise when a user-supplied URL targets a link-local / metadata address.

    Hostnames are deliberately left alone — resolving them belongs to the
    network layer, not to request validation.
    """
    host = urlsplit(value).hostname
    if not host:
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if any(address in network for network in _BLOCKED_NETWORKS):
        raise ValueError(
            f"{field} must not target link-local or cloud metadata addresses"
        )


class McpServerConfig(BaseModel):
    protocol: Literal["streamable-http", "sse"] = "streamable-http"
    endpoint: str
    api_key: SecretStr | None = None

    model_config = {"extra": "forbid", "hide_input_in_errors": True}

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        value = value.strip()
        try:
            url = urlsplit(value)
            valid = (
                url.scheme in {"http", "https"}
                and url.hostname
                and not url.username
                and not url.password
                and not url.fragment
                and not any(ord(char) < 32 for char in value)
            )
            url.port
        except ValueError:
            valid = False
        if not valid:
            raise ValueError(
                "endpoint must be an HTTP(S) URL without userinfo or a fragment"
            )
        reject_metadata_host(value, "endpoint")
        return value

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and any(
            char in value.get_secret_value() for char in "\r\n"
        ):
            raise ValueError("api_key must be a single-line string")
        return value


class KnowledgebaseOverrides(BaseModel):
    """Request-scoped knowledge base selector.

    Only the index and the backend type may change per request; names,
    descriptions, top_k, credentials and connection settings stay with the
    deployment. Unknown fields are rejected so older clients that still send
    ``top_k`` get a clear 422 instead of a silently ignored field.
    """

    model_config = {"extra": "forbid", "hide_input_in_errors": True}

    index: str | None = Field(default=None, pattern=KNOWLEDGEBASE_INDEX_PATTERN)
    type: KnowledgebaseBackend | None = None


class SandboxOverrides(BaseModel):
    """Request-scoped AgentKit Sandbox target.

    ``tool_id`` selects the sandbox for this invocation and ``ttl`` tunes how
    long its session survives. The workspace, region, credentials and the
    per-command budget are fixed by the deployment (``Execute`` always uses
    ADK's 30 second limit).
    """

    model_config = {"extra": "forbid", "hide_input_in_errors": True}

    tool_id: str | None = Field(default=None, pattern=SANDBOX_TOOL_ID_PATTERN)
    ttl: int | None = Field(default=None, ge=60, le=86400)

    @field_validator("tool_id")
    @classmethod
    def validate_allowlist(cls, value: str | None) -> str | None:
        if not value:
            return value
        allowlist = {
            item.strip()
            for item in os.getenv(SANDBOX_TOOL_ID_ALLOWLIST_ENV, "").split(",")
            if item.strip()
        }
        if allowlist and value not in allowlist:
            raise ValueError(
                f"tool_id is not listed in {SANDBOX_TOOL_ID_ALLOWLIST_ENV}"
            )
        return value


class HarnessOverrides(BaseModel):
    """Fields that may be changed for one invocation without redeploying."""

    model_config = {"hide_input_in_errors": True}

    model_name: str = Field(default="", description="Reasoning model name.")
    model_provider: str = Field(default="")
    model_api_base: str = Field(default="")
    model_api_key: SecretStr = Field(default=SecretStr(""), repr=False, exclude=True)
    tools: str = Field(default="", description="Comma-separated built-in tool names.")
    mcp_servers: list[McpServerConfig] = Field(default_factory=list)
    skills: str = Field(
        default="",
        description="Comma-separated Skill Hub slugs, space ids, or space:skill refs.",
    )
    system_prompt: str = Field(default="", description="Agent instruction.")
    runtime: Literal["adk", "codex"] = Field(default="adk")
    registry_space_id: str = Field(default="")
    registry_endpoint: str = Field(default="")
    registry_region: str = Field(default="")
    registry_top_k: int = Field(default=3, ge=1)
    max_llm_calls: int | None = Field(default=None, ge=1)
    knowledgebase: KnowledgebaseOverrides | None = None
    sandbox: SandboxOverrides | None = None

    @field_validator("mcp_servers", mode="before")
    @classmethod
    def parse_mcp_servers(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                raise ValueError("MCP_SERVERS must contain a JSON list") from None
        return value

    @field_validator("model_api_base", "registry_endpoint")
    @classmethod
    def reject_metadata_target(cls, value: str) -> str:
        """Empty means "keep the default"; a set value must not target metadata IPs."""
        if value:
            reject_metadata_host(value, "endpoint target")
        return value


class HarnessConfig(HarnessOverrides):
    """Full harness parameters, parsed from the runtime environment.

    An empty backend ``*_type`` string disables that component (it is not
    created). Everything else falls back to the model/component defaults.
    """

    app_name: str = Field(default="harness_app", alias="name")
    model_provider: str = Field(default="openai")
    model_credential_mode: Literal["", "obo_broker"] = Field(default="")
    model_target_alias: str = Field(default="model")
    description: str = Field(default="")
    knowledgebase_type: str = Field(default="")
    longterm_memory_type: str = Field(default="")
    shortterm_memory_type: str = Field(default="local")
    max_llm_calls: int | None = Field(default=None, ge=1)
    structured_tool_calls: bool = Field(default=False)
    include_tools_every_turn: bool = Field(default=True)
    registry_type: Literal["", "agentkit_a2a"] = Field(default="")

    model_config = {"populate_by_name": True}


class RunAgentRequest(BaseModel):
    user_id: str
    session_id: str
    max_llm_calls: int | None = Field(default=None, ge=1)


class InvokeHarnessRequest(BaseModel):
    prompt: str
    harness_name: str
    harness: HarnessOverrides | None = None
    run_agent_request: RunAgentRequest


class ArtifactLink(BaseModel):
    """One user-visible artifact returned by an invocation."""

    name: str
    url: str
    mime_type: str | None = None
    size: int | None = None
    source: str
    version: int | str | None = None


class InvokeHarnessResponse(BaseModel):
    harness_name: str
    overwrite: bool = False
    output: str
    error: str | None = None
    artifacts: list[ArtifactLink] = Field(default_factory=list)
