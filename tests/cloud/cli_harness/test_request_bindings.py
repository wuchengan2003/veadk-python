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

"""Request schema for knowledge base and sandbox overrides."""

import pytest
from pydantic import ValidationError

from veadk.cloud.harness_app.cronjobs.models import JobHarnessOverrides
from veadk.cloud.harness_app.server_types import (
    HarnessConfig,
    HarnessOverrides,
    KnowledgebaseOverrides,
    SandboxOverrides,
    SANDBOX_TOOL_ID_ALLOWLIST_ENV,
)


def test_overrides_are_absent_by_default():
    overrides = HarnessOverrides()

    assert overrides.knowledgebase is None
    assert overrides.sandbox is None
    assert HarnessConfig().knowledgebase is None
    assert HarnessConfig().sandbox is None


@pytest.mark.parametrize(
    "payload",
    [
        {"index": "kb_1"},
        {"index": "Support.FAQ-2026"},
        {"type": "viking"},
        {"index": "kb_1", "type": "redis"},
    ],
)
def test_knowledgebase_accepts_supported_fields(payload):
    override = KnowledgebaseOverrides(**payload)

    assert override.model_dump(exclude_unset=True) == payload
    assert set(override.model_fields_set) == set(payload)


@pytest.mark.parametrize(
    "backend",
    [
        "local",
        "opensearch",
        "redis",
        "milvus",
        "tos_vector",
        "viking",
        "context_search",
        "openviking",
    ],
)
def test_every_veadk_backend_is_accepted(backend):
    assert KnowledgebaseOverrides(type=backend).type == backend


@pytest.mark.parametrize("index", ["", "-kb", "kb name", "kb/index", "k" * 129])
def test_invalid_index_is_rejected(index):
    with pytest.raises(ValidationError):
        KnowledgebaseOverrides(index=index)


def test_index_at_max_length_is_accepted():
    assert len(KnowledgebaseOverrides(index="k" * 128).index) == 128


def test_unknown_backend_is_rejected():
    with pytest.raises(ValidationError):
        KnowledgebaseOverrides(type="pinecone")


def test_legacy_top_k_field_is_rejected():
    with pytest.raises(ValidationError):
        KnowledgebaseOverrides(index="kb_1", top_k=3)


def test_knowledgebase_override_is_nested_in_harness_overrides():
    overrides = HarnessOverrides(knowledgebase={"index": "tenant_42", "type": "viking"})

    assert isinstance(overrides.knowledgebase, KnowledgebaseOverrides)
    assert overrides.knowledgebase.index == "tenant_42"
    assert overrides.knowledgebase.type == "viking"


def test_sandbox_override_is_nested_in_harness_overrides():
    overrides = HarnessOverrides(sandbox={"tool_id": "tool-abc", "ttl": 60})

    assert isinstance(overrides.sandbox, SandboxOverrides)
    assert set(overrides.sandbox.model_fields_set) == {"tool_id", "ttl"}


@pytest.mark.parametrize("tool_id", ["tool-1", "T", "A" * 128, "tool_1"])
def test_valid_tool_ids(tool_id):
    assert SandboxOverrides(tool_id=tool_id).tool_id == tool_id


@pytest.mark.parametrize("tool_id", ["", "-tool", "tool id", "tool/1", "a" * 129])
def test_invalid_tool_ids_are_rejected(tool_id):
    with pytest.raises(ValidationError):
        SandboxOverrides(tool_id=tool_id)


@pytest.mark.parametrize("ttl", [60, 86400])
def test_ttl_accepts_its_bounds(ttl):
    assert SandboxOverrides(ttl=ttl).ttl == ttl


@pytest.mark.parametrize("ttl", [59, 86401, 0, -1, "soon"])
def test_ttl_outside_bounds_is_rejected(ttl):
    with pytest.raises(ValidationError):
        SandboxOverrides(ttl=ttl)


def test_removed_execute_timeout_field_is_rejected():
    with pytest.raises(ValidationError):
        SandboxOverrides(execute_timeout_seconds=10)


def test_sandbox_rejects_unknown_fields():
    with pytest.raises(ValidationError):
        SandboxOverrides(workspace="/tmp")


def test_tool_id_must_be_in_a_configured_allowlist(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ALLOWLIST_ENV, "tool-a, tool-b")

    assert SandboxOverrides(tool_id="tool-b").tool_id == "tool-b"
    with pytest.raises(ValidationError):
        SandboxOverrides(tool_id="tool-c")


def test_explicit_null_tool_id_is_allowed(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ALLOWLIST_ENV, "tool-a")

    assert SandboxOverrides(tool_id=None).tool_id is None


def test_empty_allowlist_allows_any_valid_tool_id(monkeypatch):
    monkeypatch.setenv(SANDBOX_TOOL_ID_ALLOWLIST_ENV, "  ,  ")

    assert SandboxOverrides(tool_id="tool-c").tool_id == "tool-c"


@pytest.mark.parametrize(
    "bindings",
    [
        {"sandbox": {"tool_id": "tool-1", "ttl": 600}},
        {"knowledgebase": {"index": "kb_1", "type": "viking"}},
        {"knowledgebase": None, "sandbox": None},
        {"knowledgebase": {}, "sandbox": {}},
    ],
)
def test_scheduled_jobs_accept_resource_bindings(bindings: dict[str, object]) -> None:
    overrides = JobHarnessOverrides.model_validate(bindings)
    assert overrides.model_dump(mode="json", exclude_unset=True) == bindings


def test_scheduled_jobs_still_accept_ordinary_overrides():
    assert JobHarnessOverrides(model_name="m").model_name == "m"
