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

"""SSRF guardrails on user-supplied endpoints.

Link-local addresses carry the cloud metadata service, which hands out
instance credentials — they are rejected. RFC1918 private ranges and loopback
stay allowed on purpose: reaching an in-VPC MCP server or a same-container
sidecar is a supported deployment, so blocking them would break real setups.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from veadk.cloud.harness_app.server_types import (
    HarnessOverrides,
    McpServerConfig,
    reject_metadata_host,
)

METADATA_URLS = [
    "http://169.254.169.254/latest/meta-data/",
    "http://169.254.1.1/mcp",
    "http://[fe80::1]/mcp",
]

PRIVATE_URLS = [
    "https://example.com/mcp",
    "http://10.0.0.5:8000/mcp",
    "http://127.0.0.1:9000/mcp",
    "http://192.168.1.10/mcp",
    "http://172.16.0.1/mcp",
]


@pytest.mark.parametrize("url", METADATA_URLS)
def test_mcp_endpoint_rejects_metadata_addresses(url: str) -> None:
    with pytest.raises(ValidationError):
        McpServerConfig(endpoint=url)


@pytest.mark.parametrize("url", PRIVATE_URLS)
def test_mcp_endpoint_allows_private_and_loopback(url: str) -> None:
    assert McpServerConfig(endpoint=url).endpoint == url


@pytest.mark.parametrize("field", ["model_api_base", "registry_endpoint"])
@pytest.mark.parametrize("url", METADATA_URLS)
def test_override_targets_reject_metadata_addresses(field: str, url: str) -> None:
    with pytest.raises(ValidationError):
        HarnessOverrides(**{field: url})


@pytest.mark.parametrize("field", ["model_api_base", "registry_endpoint"])
def test_override_targets_allow_empty(field: str) -> None:
    """An unset override keeps its default, so an empty value must stay valid."""
    assert getattr(HarnessOverrides(**{field: ""}), field) == ""


@pytest.mark.parametrize("field", ["model_api_base", "registry_endpoint"])
def test_override_targets_allow_private(field: str) -> None:
    url = "http://10.1.2.3:8080/v1"
    assert getattr(HarnessOverrides(**{field: url}), field) == url


def test_hostnames_are_not_resolved_here() -> None:
    """Resolution belongs to the network layer, so a bare name must pass."""
    reject_metadata_host("http://metadata.internal/mcp", "endpoint")
    reject_metadata_host("https://example.com/mcp", "endpoint")
