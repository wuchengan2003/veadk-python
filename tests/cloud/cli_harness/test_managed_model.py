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

import asyncio
from types import SimpleNamespace

from veadk.cloud.harness_app.concurrency import ModelAdmissionController
from veadk.cloud.harness_app.managed_model import ManagedIdentityLiteLlm


class _FakeRuntimeIdentity:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def _token_for(self, alias: str):
        self.calls.append(alias)
        return SimpleNamespace(compact="short-lived-obo-tip")


class _FakeDelegate:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self._additional_args = dict(kwargs)

    async def generate_content_async(self, request, stream=False):
        assert self.kwargs["api_key"] == "short-lived-obo-tip"
        assert self.kwargs["api_base"] == "https://model-egress.example.com/v1"
        assert self.kwargs["num_retries"] == 0
        assert self.kwargs["extra_body"] == {"thinking": {"type": "disabled"}}
        assert request.config.http_options.extra_body["thinking"] == {
            "type": "disabled"
        }
        assert request.model == "openai/demo-model"
        yield {"text": "ok", "stream": stream}


def test_managed_model_uses_request_tip_without_shared_key(monkeypatch) -> None:
    async def exercise() -> None:
        import agentkit.identity
        from google.adk.models.llm_request import LlmRequest

        monkeypatch.setattr(
            agentkit.identity,
            "current_identity",
            lambda required=True: SimpleNamespace(user_sub="verified-user"),
        )
        runtime = _FakeRuntimeIdentity()
        delegates: list[_FakeDelegate] = []

        def factory(**kwargs):
            delegate = _FakeDelegate(**kwargs)
            delegates.append(delegate)
            return delegate

        model = ManagedIdentityLiteLlm(
            model="openai/demo-model",
            runtime_identity=runtime,
            target_alias="model",
            api_base="https://model-egress.example.com/v1",
            admission=ModelAdmissionController(
                max_inflight=2,
                max_inflight_per_user=1,
                queue_timeout_seconds=1,
            ),
            request_timeout_seconds=30,
            delegate_factory=factory,
        )
        responses = [
            item
            async for item in model.generate_content_async(
                LlmRequest(model="openai/demo-model"), stream=True
            )
        ]

        assert responses == [{"text": "ok", "stream": True}]
        assert runtime.calls == ["model"]
        assert delegates[0]._additional_args.get("api_key") is None
        assert model._additional_args["api_key"] == "request-obo"

    asyncio.run(exercise())


def test_managed_model_overrides_request_thinking_without_mutation(monkeypatch) -> None:
    async def exercise() -> None:
        import agentkit.identity
        from google.adk.models.llm_request import LlmRequest
        from google.genai.types import GenerateContentConfig, HttpOptions

        monkeypatch.setattr(
            agentkit.identity,
            "current_identity",
            lambda required=True: SimpleNamespace(user_sub="verified-user"),
        )
        seen: list[object] = []

        class Delegate(_FakeDelegate):
            async def generate_content_async(self, request, stream=False):
                seen.append(request)
                async for response in super().generate_content_async(request, stream):
                    yield response

        model = ManagedIdentityLiteLlm(
            model="openai/demo-model",
            runtime_identity=_FakeRuntimeIdentity(),
            target_alias="model",
            api_base="https://model-egress.example.com/v1",
            admission=ModelAdmissionController(
                max_inflight=2,
                max_inflight_per_user=1,
                queue_timeout_seconds=1,
            ),
            request_timeout_seconds=30,
            delegate_factory=Delegate,
        )
        original = LlmRequest(
            model="openai/demo-model",
            config=GenerateContentConfig(
                http_options=HttpOptions(
                    extra_body={"thinking": {"type": "enabled"}, "safe": True}
                )
            ),
        )

        class NonCopyableTool:
            def __deepcopy__(self, memo):
                del memo
                raise AssertionError("live tool must not be deep-copied")

        tool = NonCopyableTool()
        original.tools_dict["live"] = tool
        async for _ in model.generate_content_async(original, stream=True):
            pass

        assert original.config.http_options.extra_body["thinking"] == {
            "type": "enabled"
        }
        sent = seen[0]
        assert sent is not original
        assert sent.tools_dict["live"] is tool
        assert sent.config.http_options.extra_body == {
            "thinking": {"type": "disabled"},
            "safe": True,
        }

    asyncio.run(exercise())


def test_managed_model_rejects_model_override(monkeypatch) -> None:
    async def exercise() -> None:
        import agentkit.identity

        monkeypatch.setattr(
            agentkit.identity,
            "current_identity",
            lambda required=True: SimpleNamespace(user_sub="verified-user"),
        )
        model = ManagedIdentityLiteLlm(
            model="openai/demo-model",
            runtime_identity=_FakeRuntimeIdentity(),
            target_alias="model",
            api_base="https://model-egress.example.com/v1",
            admission=ModelAdmissionController(
                max_inflight=1,
                max_inflight_per_user=1,
                queue_timeout_seconds=1,
            ),
            request_timeout_seconds=30,
            delegate_factory=_FakeDelegate,
        )
        try:
            async for _ in model.generate_content_async(
                SimpleNamespace(model="openai/attacker-model")
            ):
                pass
        except RuntimeError as exc:
            assert str(exc) == "MODEL_TARGET_MISMATCH"
        else:
            raise AssertionError("model override was accepted")

    asyncio.run(exercise())
