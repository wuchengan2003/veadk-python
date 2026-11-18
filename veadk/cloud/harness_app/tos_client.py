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

"""Shared TOS client construction for harness-side storage.

Two harness features talk to TOS: cronjob run records (``cronjobs/store.py``)
and user artifacts (``artifacts.py``).  Both must use Runtime IAM credentials
when they are available, fall back to environment credentials for local
development, and rebuild the client when the mounted credential rotates.
Keeping that policy in one module avoids two subtly different implementations.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Callable

IAM_CREDENTIAL_PATH = Path("/var/run/secrets/iam/credential")


def tos_endpoint(region: str, provider: str = "volcengine", override: str = "") -> str:
    """Resolve the TOS endpoint, honouring an explicit deployment override."""
    override = (override or "").strip()
    if override:
        return override
    suffix = (
        "bytepluses.com" if (provider or "").lower() == "byteplus" else "volces.com"
    )
    return f"tos-{region}.{suffix}"


def _read_credentials(
    provider: str, environ: dict[str, str], credential_path: Path
) -> tuple[str, str, str]:
    if credential_path.exists():
        raw = json.loads(credential_path.read_text())
        return (
            raw.get("access_key_id") or raw.get("AccessKeyId") or "",
            raw.get("secret_access_key") or raw.get("SecretAccessKey") or "",
            raw.get("session_token") or raw.get("SessionToken") or "",
        )
    name = "BYTEPLUS" if (provider or "").lower() == "byteplus" else "VOLCENGINE"
    return (
        environ.get(f"{name}_ACCESS_KEY", ""),
        environ.get(f"{name}_SECRET_KEY", ""),
        environ.get(f"{name}_SESSION_TOKEN", ""),
    )


def build_client_factory(
    *,
    region: str,
    endpoint: str,
    provider: str = "volcengine",
    credential_path: Path = IAM_CREDENTIAL_PATH,
    environ: dict[str, str] | None = None,
    client_factory: Callable[..., Any] | None = None,
) -> Callable[[], Any]:
    """Return a zero-argument factory that reuses one client per credential set.

    ``client_factory`` is injectable so tests can substitute a fake without a
    network or the ``tos`` package.
    """
    environ = dict(os.environ if environ is None else environ)
    guard = threading.Lock()
    cached: dict[str, Any] = {"signature": None, "client": None}

    def factory() -> Any:
        ak, sk, token = _read_credentials(provider, environ, credential_path)
        if not ak or not sk:
            raise RuntimeError(
                "Harness artifacts require Runtime IAM credentials or local "
                "VOLCENGINE_ACCESS_KEY/SECRET_KEY"
            )
        signature = (ak, sk, token)
        with guard:
            if cached["signature"] != signature:
                if client_factory is not None:
                    cached["client"] = client_factory(
                        ak=ak,
                        sk=sk,
                        security_token=token or None,
                        endpoint=endpoint,
                        region=region,
                    )
                else:
                    import tos

                    cached["client"] = tos.TosClientV2(
                        ak,
                        sk,
                        security_token=token or None,
                        endpoint=endpoint,
                        region=region,
                        connection_time=5,
                        socket_timeout=10,
                        max_retry_count=0,
                    )
                cached["signature"] = signature
            return cached["client"]

    return factory
