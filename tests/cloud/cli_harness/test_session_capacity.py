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

from veadk.cloud.harness_app.harness_agent import _session_database_kwargs


def test_postgresql_pool_is_bounded_per_replica(monkeypatch) -> None:
    monkeypatch.setenv("HARNESS_SESSION_DB_POOL_SIZE", "5")
    monkeypatch.setenv("HARNESS_SESSION_DB_MAX_OVERFLOW", "3")
    monkeypatch.setenv("HARNESS_SESSION_DB_POOL_TIMEOUT_SECONDS", "10")
    monkeypatch.setenv("HARNESS_SESSION_DB_POOL_RECYCLE_SECONDS", "300")

    assert _session_database_kwargs("postgresql") == {
        "pool_size": 5,
        "max_overflow": 3,
        "pool_timeout": 10,
        "pool_recycle": 300,
    }
    assert _session_database_kwargs("local") == {}


def test_session_pool_rejects_unbounded_values(monkeypatch) -> None:
    monkeypatch.setenv("HARNESS_SESSION_DB_POOL_SIZE", "1000")
    try:
        _session_database_kwargs("postgresql")
    except RuntimeError as exc:
        assert "HARNESS_SESSION_DB_POOL_SIZE" in str(exc)
    else:
        raise AssertionError("unbounded database pool was accepted")
