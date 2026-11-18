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

"""Request-scoped OBO model client for the OAuth Shared Harness."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Callable
from urllib.parse import urlsplit

from google.adk.models.lite_llm import LiteLlm
from pydantic import PrivateAttr

from .concurrency import ModelAdmissionController, ModelCapacityError


def _without_provider_thinking(llm_request: Any) -> Any:
    """Return a request-local copy with provider thinking disabled last.

    ADK releases differ in whether ``extra_body`` from the request config is
    merged after the LiteLLM constructor arguments.  Set both boundaries and
    never mutate the shared request object or a module-level nested dictionary.
    """

    from google.genai import types

    # tools_dict can contain live MCP clients/session managers that must remain
    # unique. Shallow-copy the request envelope, then deep-copy only the pure
    # GenerateContentConfig branch that this function modifies.
    request = llm_request.model_copy()
    request.config = llm_request.config.model_copy(deep=True)
    http_options = (
        request.config.http_options.model_copy(deep=True)
        if request.config.http_options is not None
        else types.HttpOptions()
    )
    extra_body = dict(http_options.extra_body or {})
    extra_body["thinking"] = {"type": "disabled"}
    http_options.extra_body = extra_body
    request.config.http_options = http_options
    return request


def _managed_base_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("managed model api_base must be a credential-free HTTPS URL")
    return value.rstrip("/")


class ManagedIdentityLiteLlm(LiteLlm):
    """Create an ephemeral LiteLLM delegate with one target-bound OBO TIP.

    The actual Ark/Model-Gateway credential is held and injected by APIG's
    Broker.  The Harness sees only a short-lived TIP for the verified user and
    its own Runtime/Workload identity.  No credential is stored on this shared
    model object, avoiding cross-request mutation under concurrency.
    """

    _runtime_identity: Any = PrivateAttr()
    _target_alias: str = PrivateAttr()
    _base_url: str = PrivateAttr()
    _admission: ModelAdmissionController = PrivateAttr()
    _request_timeout_seconds: float = PrivateAttr()
    _delegate_factory: Callable[..., LiteLlm] = PrivateAttr()

    def __init__(
        self,
        *,
        model: str,
        runtime_identity: Any,
        target_alias: str,
        api_base: str,
        admission: ModelAdmissionController,
        request_timeout_seconds: float,
        delegate_factory: Callable[..., LiteLlm] = LiteLlm,
    ) -> None:
        if not target_alias:
            raise ValueError("managed model target alias is required")
        if request_timeout_seconds <= 0:
            raise ValueError("managed model request timeout must be positive")
        base_url = _managed_base_url(api_base)
        # This fixed non-secret marker prevents VeADK from resolving an Ark API
        # key while keeping the inherited LiteLlm model metadata well formed.
        super().__init__(
            model=model,
            api_base=base_url,
            api_key="request-obo",
        )
        self._runtime_identity = runtime_identity
        self._target_alias = target_alias
        self._base_url = base_url
        self._admission = admission
        self._request_timeout_seconds = float(request_timeout_seconds)
        self._delegate_factory = delegate_factory

    async def _tip(self) -> str:
        """Mint/reuse a TIP off-loop without exposing it through public APIs."""

        try:
            issued = await asyncio.to_thread(
                self._runtime_identity._token_for, self._target_alias
            )
            compact = issued.compact
            if not isinstance(compact, str) or not compact:
                raise RuntimeError("empty workload token")
            return compact
        except asyncio.CancelledError:
            raise
        except Exception:
            raise RuntimeError("MODEL_IDENTITY_UNAVAILABLE") from None

    async def generate_content_async(
        self, llm_request: Any, stream: bool = False
    ) -> AsyncGenerator[Any, None]:
        from agentkit.identity import current_identity

        identity = current_identity(required=True)
        if identity is None:  # fail closed even if a future SDK changes typing
            raise RuntimeError("MODEL_IDENTITY_UNAVAILABLE")
        requested_model = getattr(llm_request, "model", None)
        if requested_model and requested_model != self.model:
            raise RuntimeError("MODEL_TARGET_MISMATCH")

        token = ""
        delegate: LiteLlm | None = None
        try:
            async with self._admission.lease(identity.user_sub):
                token = await self._tip()
                request = _without_provider_thinking(llm_request)
                delegate = self._delegate_factory(
                    model=self.model,
                    api_base=self._base_url,
                    api_key=token,
                    timeout=self._request_timeout_seconds,
                    num_retries=0,
                    # The classroom chat renders only final answers. Disable
                    # provider reasoning at the request boundary so chain of
                    # thought is neither generated nor sent through the
                    # Runtime/APIG stream as ordinary assistant content.
                    extra_body={"thinking": {"type": "disabled"}},
                )
                async for response in delegate.generate_content_async(
                    request, stream=stream
                ):
                    yield response
        except (ModelCapacityError, asyncio.CancelledError):
            raise
        except Exception:
            # Do not preserve downstream exception frames: they may retain the
            # short-lived TIP or an upstream response containing sensitive data.
            raise RuntimeError("MODEL_UPSTREAM_FAILED") from None
        finally:
            if delegate is not None:
                additional = getattr(delegate, "_additional_args", None)
                if isinstance(additional, dict):
                    additional.pop("api_key", None)
            delegate = None
            token = ""


__all__ = ["ManagedIdentityLiteLlm"]
