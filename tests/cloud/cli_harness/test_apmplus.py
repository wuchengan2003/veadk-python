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

"""在独立进程中验证真实 VeADK 初始化，避免全局 tracer 状态串扰。"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from veadk.cloud.harness_app.apmplus import configure_apmplus_endpoint


_STARTUP_PROBE = """
import os
import sys
import traceback
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

os.environ["MODEL_API_KEY"] = uuid4().hex
import veadk.auth.veauth.apmplus_veauth as apm

apm.get_credential_from_vefaas_iam = lambda: SimpleNamespace(
    access_key_id=uuid4().hex, secret_access_key=uuid4().hex, session_token=""
)
apm.ve_request = Mock(return_value={"data": {"app_key": uuid4().hex}})
expected_host, mode = sys.argv[1:]
if mode == "configured_key":
    os.environ["OBSERVABILITY_OPENTELEMETRY_APMPLUS_API_KEY"] = uuid4().hex
if mode == "network_error":
    apm.ve_request.side_effect = OSError(101, "Network is unreachable")
try:
    try:
        import veadk.cloud.harness_app.server
    except OSError as error:
        assert mode == "network_error" and error.errno == 101
    else:
        assert mode != "network_error", "Token errors must still propagate"
    if mode in ("disabled", "configured_key"):
        apm.ve_request.assert_not_called()
    else:
        kwargs = apm.ve_request.call_args.kwargs
        assert kwargs["host"] == expected_host, kwargs["host"]
        if mode == "other_region":
            assert kwargs["region"] == "ap-southeast-2"
        assert kwargs["action"] == "GetAppKey"
        assert kwargs["service"] == "apmplus_server"
        assert kwargs["version"] == "2024-07-30"
        assert kwargs["header"]["X-Apmplus-Region"] == kwargs["region"].replace("-", "_")
except BaseException:
    traceback.print_exc()
    sys.stderr.flush()
    os._exit(1)
os._exit(0)
"""


@pytest.mark.parametrize(
    ("provider_env", "provider", "expected_host", "mode"),
    [
        (
            "CLOUD_PROVIDER",
            "byteplus",
            "apmplus-server.ap-southeast-1.byteplusapi.com",
            "enabled",
        ),
        (
            "AGENTKIT_CLOUD_PROVIDER",
            "byteplus",
            "apmplus-server.ap-southeast-1.byteplusapi.com",
            "enabled",
        ),
        (
            "CLOUD_PROVIDER",
            "byteplus",
            "apmplus-server.ap-southeast-1.byteplusapi.com",
            "other_region",
        ),
        ("CLOUD_PROVIDER", "volcengine", "open.volcengineapi.com", "enabled"),
        ("CLOUD_PROVIDER", "", "open.volcengineapi.com", "enabled"),
        ("CLOUD_PROVIDER", "byteplus", "", "disabled"),
        ("CLOUD_PROVIDER", "byteplus", "", "configured_key"),
        (
            "CLOUD_PROVIDER",
            "byteplus",
            "apmplus-server.ap-southeast-1.byteplusapi.com",
            "network_error",
        ),
    ],
)
def test_server_startup_token_endpoint(
    tmp_path: Path,
    provider_env: str,
    provider: str,
    expected_host: str,
    mode: str,
) -> None:
    env = {
        "PATH": os.defpath,
        "PYTHONPATH": str(Path(__file__).resolve().parents[3]),
        provider_env: provider,
        "BYTEPLUS_REGION": "ap-southeast-2"
        if mode == "other_region"
        else "ap-southeast-1",
        "ENABLE_APMPLUS": "false" if mode == "disabled" else "true",
        "MODEL_PROVIDER": "openai",
        "MODEL_NAME": "startup-test",
        "MODEL_API_BASE": "http://127.0.0.1:9/v1",
        "HARNESS_CRONJOB_ENABLED": "false",
    }
    result = subprocess.run(
        [sys.executable, "-c", _STARTUP_PROBE, expected_host, mode],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    ("provider", "region", "expected_host"),
    [
        ("byteplus", "ap-southeast-1", "apmplus-server.ap-southeast-1.byteplusapi.com"),
        ("byteplus", "ap-southeast-2", "apmplus-server.ap-southeast-1.byteplusapi.com"),
        ("volcengine", "ap-southeast-1", "open.volcengineapi.com"),
    ],
)
def test_endpoint_configuration_is_scoped_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    provider: str,
    region: str,
    expected_host: str,
) -> None:
    from veadk.auth.veauth import apmplus_veauth

    monkeypatch.setenv("BYTEPLUS_REGION", region)
    # 恢复全局 resolver，避免适配泄漏到其他单测。
    monkeypatch.setattr(
        apmplus_veauth,
        "apmplus_openapi_host",
        apmplus_veauth.apmplus_openapi_host,
    )
    configure_apmplus_endpoint()
    resolver = apmplus_veauth.apmplus_openapi_host
    configure_apmplus_endpoint()

    assert apmplus_veauth.apmplus_openapi_host is resolver
    assert resolver(provider) == expected_host
