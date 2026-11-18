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

import pytest

from veadk.cloud.harness_app.harness_agent import _assemble_agent
from veadk.cloud.harness_app.server_types import HarnessConfig


class _RuntimeIdentity:
    def _token_for(self, alias: str):  # pragma: no cover - assembly must not call it
        raise AssertionError(f"unexpected token exchange during assembly: {alias}")


def test_managed_model_assembly_does_not_resolve_ark_key(monkeypatch) -> None:
    def fail_raw_key(*args, **kwargs):
        del args, kwargs
        raise AssertionError("VeADK attempted to fetch a raw Ark API key")

    monkeypatch.setattr(
        "veadk.auth.veauth.ark_veauth.get_ark_token",
        fail_raw_key,
    )
    config = HarnessConfig(
        model_name="demo-model",
        model_provider="openai",
        model_api_base="https://model-egress.example.com/v1",
        model_credential_mode="obo_broker",
        model_target_alias="model",
        shortterm_memory_type="local",
    )

    agent, memory = _assemble_agent(config, runtime_identity=_RuntimeIdentity())

    assert type(agent.model).__name__ == "ManagedIdentityLiteLlm"
    assert agent.model_api_key == "request-obo"
    assert agent.generate_content_config.max_output_tokens == 1024
    assert type(memory.session_service).__name__ == "InMemorySessionService"


@pytest.mark.parametrize(
    ("provider", "runtime_identity", "message"),
    [
        ("", _RuntimeIdentity(), "provider, name, and api_base"),
        ("   ", _RuntimeIdentity(), "provider, name, and api_base"),
        ("openai", object(), "active Runtime identity"),
    ],
)
def test_managed_model_assembly_rejects_incomplete_runtime_contract(
    provider: str,
    runtime_identity: object,
    message: str,
) -> None:
    config = HarnessConfig(
        model_name="demo-model",
        model_provider=provider,
        model_api_base="https://model-egress.example.com/v1",
        model_credential_mode="obo_broker",
        model_target_alias="model",
        shortterm_memory_type="local",
    )

    with pytest.raises(RuntimeError, match=message):
        _assemble_agent(config, runtime_identity=runtime_identity)
