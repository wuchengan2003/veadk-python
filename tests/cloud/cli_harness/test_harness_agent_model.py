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

import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from veadk.cloud.harness_app.harness_agent import _assemble_agent, config_from_env
from veadk.cloud.harness_app.server_types import HarnessConfig, HarnessOverrides


@pytest.fixture
def agent_factory(monkeypatch):
    veadk = ModuleType("veadk")
    veadk.Agent = Mock()
    memory = ModuleType("veadk.memory.short_term_memory")
    memory.ShortTermMemory = Mock()
    monkeypatch.setitem(sys.modules, "veadk", veadk)
    monkeypatch.setitem(sys.modules, "veadk.memory.short_term_memory", memory)
    return veadk.Agent


def test_model_environment_reaches_agent_without_exposing_key(
    monkeypatch, agent_factory
):
    with monkeypatch.context() as env:
        env.setattr(
            "os.environ",
            {
                "MODEL_NAME": "custom-model",
                "MODEL_PROVIDER": "openai",
                "MODEL_API_BASE": "https://model.example.com/v1",
                "MODEL_API_KEY": "test-model-key",
            },
        )
        config = config_from_env()

    assert "test-model-key" not in repr(config)
    assert "model_api_key" not in config.model_dump()
    assert "test-model-key" not in config.model_dump_json()
    _assemble_agent(config)

    kwargs = agent_factory.call_args.kwargs
    assert kwargs["model_name"] == "custom-model"
    assert kwargs["model_provider"] == "openai"
    assert kwargs["model_api_base"] == "https://model.example.com/v1"
    assert kwargs["model_api_key"] == "test-model-key"


def test_mcp_error_fallback_is_registered(agent_factory: Mock) -> None:
    from veadk.cloud.harness_app.mcp import mcp_tool_error_fallback

    _assemble_agent(HarnessConfig())

    assert (
        agent_factory.call_args.kwargs["on_tool_error_callback"]
        is mcp_tool_error_fallback
    )


@pytest.mark.parametrize(
    "fields",
    [
        {},
        {"model_name": "custom-model"},
        {"model_provider": "anthropic"},
        {"model_api_base": "https://model.example.com/v1"},
        {"model_api_key": "test-model-key"},
        {"model_api_key": "", "model_provider": "", "model_api_base": ""},
    ],
)
def test_direct_model_preserves_defaults_for_unset_or_empty_fields(
    fields, agent_factory
):
    _assemble_agent(HarnessConfig(**fields))

    actual = {
        name: value
        for name, value in agent_factory.call_args.kwargs.items()
        if name.startswith("model_")
    }
    assert actual == {name: value for name, value in fields.items() if value}


def test_managed_model_rejects_direct_key(agent_factory):
    config = HarnessConfig(
        model_api_key="test-model-key", model_credential_mode="obo_broker"
    )

    with pytest.raises(
        RuntimeError, match="MODEL_API_KEY is forbidden in obo_broker mode"
    ):
        _assemble_agent(config)

    agent_factory.assert_not_called()


def test_model_key_override_is_parsed_without_exposing_key():
    overrides = HarnessOverrides(model_api_key="test-model-key")

    assert "model_api_key" in overrides.model_fields_set
    assert overrides.model_api_key.get_secret_value() == "test-model-key"
    assert "test-model-key" not in repr(overrides)
    assert "test-model-key" not in overrides.model_dump_json()
