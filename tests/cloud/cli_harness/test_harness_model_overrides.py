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

import asyncio
import copy
from typing import Any

import pytest

from veadk.cloud.harness_app.harness_agent import spawn_harness_agent
from veadk.cloud.harness_app.server_types import HarnessOverrides


@pytest.fixture
def deployed_agent():
    from veadk import Agent

    return Agent(
        name="deployed_agent",
        model_name="deployed-model",
        model_provider="openai",
        model_api_base="https://deployed.example.com/v1",
        model_api_key="deployed-model-key",
        model_extra_config={"extra_headers": {"X-Example": "shared"}},
        tools=[],
        skills=[],
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"model_api_key": "request-model-key"},
        {"model_api_key": ""},
        {"model_provider": "anthropic"},
        {"model_api_base": "https://request.example.com/v1"},
        {"model_name": "request-model"},
        {
            "model_name": "request-model",
            "model_provider": "anthropic",
            "model_api_base": "https://request.example.com/v1",
            "model_api_key": "request-model-key",
        },
    ],
)
def test_overrides_reach_model_requests_without_affecting_next_call(
    deployed_agent, overrides, monkeypatch, tmp_path
):
    from google.adk.models.lite_llm import LiteLLMClient
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types
    from litellm import ModelResponse

    requests = []

    async def completion(
        self: LiteLLMClient,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> ModelResponse:
        # 新版 VeADK 的计时客户端以位置参数转发模型请求。
        requests.append(
            copy.deepcopy(
                {
                    "model": model,
                    "messages": messages,
                    "tools": tools,
                    **kwargs,
                }
            )
        )
        return ModelResponse(
            model=model,
            choices=[
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
        )

    monkeypatch.setattr(LiteLLMClient, "acompletion", completion)
    cloned = spawn_harness_agent(
        deployed_agent, HarnessOverrides(**overrides), tmp_path
    )
    following = spawn_harness_agent(deployed_agent, HarnessOverrides(), tmp_path)

    async def invoke(agent):
        request = LlmRequest(
            contents=[types.Content(role="user", parts=[types.Part(text="hello")])]
        )
        return [item async for item in agent.model.generate_content_async(request)]

    asyncio.run(invoke(cloned))
    asyncio.run(invoke(following))

    expected = {
        "model_name": "deployed-model",
        "model_provider": "openai",
        "model_api_base": "https://deployed.example.com/v1",
        "model_api_key": "deployed-model-key",
        **overrides,
    }
    for field, value in expected.items():
        assert getattr(cloned, field) == value
    assert (
        requests[0]["model"] == f"{expected['model_provider']}/{expected['model_name']}"
    )
    assert requests[0]["api_key"] == expected["model_api_key"]
    assert requests[0]["api_base"] == expected["model_api_base"]
    assert requests[0]["extra_headers"]["X-Example"] == "shared"
    assert requests[1]["model"] == "openai/deployed-model"
    assert (
        requests[1]["api_key"] == deployed_agent.model_api_key == "deployed-model-key"
    )
    assert (
        requests[1]["api_base"]
        == deployed_agent.model_api_base
        == "https://deployed.example.com/v1"
    )
    assert cloned.model is not deployed_agent.model
    cloned.model._additional_args["extra_headers"]["X-Example"] = "changed"
    assert (
        deployed_agent.model._additional_args["extra_headers"]["X-Example"] == "shared"
    )
