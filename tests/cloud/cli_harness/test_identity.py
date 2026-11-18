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

from fastapi.testclient import TestClient

from veadk.cloud.harness_app.identity import (
    BootstrapServer,
    create_agent_server,
    identity_config,
    identity_required,
)


def test_bootstrap_server_exposes_health_but_denies_chat() -> None:
    client = TestClient(BootstrapServer().app)

    assert client.get("/readiness").json() == {"status": "bootstrap"}
    denied = client.post("/run_sse", json={})
    assert denied.status_code == 503
    assert denied.json() == {"error": "RUNTIME_IDENTITY_NOT_BOUND"}
    assert denied.headers["cache-control"] == "no-store"


def test_oauth_image_build_marker_is_fail_closed(monkeypatch, tmp_path) -> None:
    marker = tmp_path / ".agentkit-harness-identity-required"
    marker.touch()
    monkeypatch.delenv("AGENTKIT_HARNESS_IDENTITY_ENABLED", raising=False)

    assert identity_required(marker) is True
    assert identity_config() is None


def test_identity_config_is_bound_to_runtime_and_client(monkeypatch) -> None:
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_ENABLED", "true")
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_RUNTIME_ID", "r-1234567890abcdef")
    monkeypatch.setenv(
        "AGENTKIT_HARNESS_IDENTITY_DISCOVERY_URL",
        "https://issuer.example/.well-known/openid-configuration",
    )
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_ISSUER", "https://issuer.example")
    monkeypatch.setenv(
        "AGENTKIT_HARNESS_IDENTITY_ALLOWED_CLIENTS", "employee-client,other-client"
    )

    config = identity_config()
    assert config.runtime_id == "r-1234567890abcdef"
    assert config.workload_id == "r-1234567890abcdef"
    assert config.allowed_clients == ("employee-client", "other-client")
    assert dict(config.targets) == {}


def test_managed_model_registers_one_workload_target(monkeypatch) -> None:
    import agentkit.identity

    class FakeIdentityClient:
        def __init__(self, *, region: str):
            self.region = region

    class FakeRuntimeIdentity:
        def __init__(self, config, *, exchange):
            self.config = config
            self.exchange = exchange

    monkeypatch.setattr(agentkit.identity, "IdentityClient", FakeIdentityClient)
    monkeypatch.setattr(agentkit.identity, "RuntimeIdentity", FakeRuntimeIdentity)
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_ENABLED", "true")
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_RUNTIME_ID", "r-1234567890abcdef")
    monkeypatch.setenv(
        "AGENTKIT_HARNESS_IDENTITY_DISCOVERY_URL",
        "https://issuer.example/.well-known/openid-configuration",
    )
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_ISSUER", "https://issuer.example")
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_ALLOWED_CLIENTS", "employee-client")
    monkeypatch.setenv("MODEL_CREDENTIAL_MODE", "obo_broker")
    monkeypatch.setenv("MODEL_TARGET_ALIAS", "model")
    monkeypatch.setenv("MODEL_TARGET_AUDIENCE", "trn:agentkit:model-gateway")
    monkeypatch.setenv("MODEL_API_BASE", "https://model-egress.example/v1")
    monkeypatch.setenv(
        "MODEL_WORKLOAD_DISCOVERY_URL",
        "https://workload.example/.well-known/openid-configuration",
    )
    monkeypatch.setenv("MODEL_WORKLOAD_ISSUER", "https://workload.example")
    monkeypatch.setenv("MODEL_WORKLOAD_POOL", "default")
    monkeypatch.setenv("MODEL_IDENTITY_REGION", "cn-beijing")

    identity = identity_config()
    target = identity.config.targets["model"]
    assert identity.config.runtime_id == "r-1234567890abcdef"
    assert target.audience == "trn:agentkit:model-gateway"
    assert target.base_url == "https://model-egress.example/v1"
    assert identity.exchange.region == "cn-beijing"


def test_identity_config_fails_closed_when_incomplete(monkeypatch) -> None:
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_ENABLED", "true")
    monkeypatch.setenv("AGENTKIT_HARNESS_IDENTITY_RUNTIME_ID", "r-1234567890abcdef")
    monkeypatch.delenv("AGENTKIT_HARNESS_IDENTITY_ALLOWED_CLIENTS", raising=False)

    try:
        identity_config()
    except RuntimeError as exc:
        assert "incomplete" in str(exc)
    else:
        raise AssertionError("incomplete identity configuration was accepted")


def test_keyauth_uses_the_legacy_agent_server_constructor() -> None:
    calls = []

    def server_type(**kwargs):
        calls.append(kwargs)
        return object()

    create_agent_server(
        server_type,
        agent="agent",
        short_term_memory="memory",
        identity=None,
    )

    assert calls == [{"agent": "agent", "short_term_memory": "memory"}]


def test_oauth_alone_receives_identity_constructor_arguments() -> None:
    calls = []

    def server_type(**kwargs):
        calls.append(kwargs)
        return object()

    create_agent_server(
        server_type,
        agent="agent",
        short_term_memory="memory",
        identity="identity",
    )

    assert calls == [
        {
            "agent": "agent",
            "short_term_memory": "memory",
            "identity": "identity",
            "identity_health_routes": ("/readiness", "/healthz"),
        }
    ]
