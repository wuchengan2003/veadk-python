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

import pytest
from veadk.cloud.harness_app.session_store import initialise_shared_session_store


class _Connection:
    def __init__(self, answers: list[bool]):
        self.answers = answers
        self.calls: list[str] = []

    async def scalar(self, statement, parameters):
        del parameters
        sql = str(statement)
        self.calls.append(sql)
        return self.answers.pop(0)


class _ConnectionContext:
    def __init__(self, connection: _Connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, traceback):
        del exc_type, exc, traceback


class _Engine:
    def __init__(self, connection: _Connection, dialect: str = "postgresql"):
        self.connection = connection
        self.dialect = SimpleNamespace(name=dialect)

    def connect(self):
        return _ConnectionContext(self.connection)


class _SessionService:
    def __init__(self, engine: _Engine):
        self.db_engine = engine
        self.list_calls: list[tuple[str, str]] = []

    async def list_sessions(self, *, app_name: str, user_id: str):
        self.list_calls.append((app_name, user_id))


def test_postgres_bootstrap_takes_distributed_lock_and_warms_tables():
    connection = _Connection([True, True])
    service = _SessionService(_Engine(connection))
    memory = SimpleNamespace(session_service=service)

    asyncio.run(
        initialise_shared_session_store(
            memory,
            app_name="pdsa-shared-harness-oauth",
            backend="postgresql",
        )
    )

    assert service.list_calls == [
        (
            "pdsa-shared-harness-oauth",
            "__agentkit_shared_harness_bootstrap__",
        )
    ]
    assert "pg_try_advisory_lock" in connection.calls[0]
    assert "pg_advisory_unlock" in connection.calls[1]


def test_postgres_bootstrap_fails_closed_without_database_engine():
    memory = SimpleNamespace(session_service=SimpleNamespace())
    with pytest.raises(RuntimeError, match="not active"):
        asyncio.run(
            initialise_shared_session_store(
                memory,
                app_name="pdsa-shared-harness-oauth",
                backend="postgresql",
            )
        )


def test_non_postgres_backend_keeps_legacy_behavior():
    asyncio.run(
        initialise_shared_session_store(
            None,
            app_name="legacy-keyauth",
            backend="local",
        )
    )
