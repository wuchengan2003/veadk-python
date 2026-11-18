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

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from google.adk.events import Event
from google.adk.sessions import DatabaseSessionService, InMemorySessionService
from google.genai import types

from veadk.cloud.harness_app.session_metadata import (
    DEFAULT_SESSION_TITLE,
    SessionLifecyclePlugin,
    SessionMetadataStore,
    install_session_routes,
    install_session_service_metadata_hooks,
    title_from_user_message,
)


def user_message(text: str) -> types.Content:
    return types.Content(role="user", parts=[types.Part(text=text)])


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("查询北京天气？再推荐衣服", "查询北京天气"),
        ("  第一行\n第二行  ", "第一行"),
        ("没有标点的短问题", "没有标点的短问题"),
        ("", None),
        ("甲" * 50, "甲" * 39 + "…"),
    ],
)
def test_title_from_first_user_sentence(text: str, expected: str | None) -> None:
    assert title_from_user_message(user_message(text)) == expected


@pytest.mark.asyncio
async def test_title_is_generated_once_without_overwriting() -> None:
    store = SessionMetadataStore(InMemorySessionService())
    plugin = SessionLifecyclePlugin(store)
    context = SimpleNamespace(
        invocation_id="inv-1",
        session=SimpleNamespace(app_name="app", user_id="user", id="session"),
    )

    await plugin.on_user_message_callback(
        invocation_context=context, user_message=user_message("第一个问题？后续")
    )
    await plugin.on_user_message_callback(
        invocation_context=context, user_message=user_message("第二个问题")
    )

    metadata = await store.get("app", "user", "session")
    assert metadata.title == "第一个问题"


@pytest.mark.asyncio
async def test_lifecycle_transitions_to_idle_waiting_and_error() -> None:
    store = SessionMetadataStore(InMemorySessionService())
    plugin = SessionLifecyclePlugin(store)

    async def run(invocation_id: str, event: Event | None = None, fail=False):
        context = SimpleNamespace(
            invocation_id=invocation_id,
            session=SimpleNamespace(app_name="app", user_id="user", id=invocation_id),
        )
        await plugin.before_run_callback(invocation_context=context)
        assert (await store.get("app", "user", invocation_id)).status == "running"
        if event is not None:
            await plugin.on_event_callback(invocation_context=context, event=event)
        if fail:
            await plugin.on_model_error_callback(
                callback_context=context,
                llm_request=object(),
                error=RuntimeError("failed"),
            )
        await plugin.after_run_callback(invocation_context=context)
        return (await store.get("app", "user", invocation_id)).status

    assert await run("idle") == "idle"
    waiting_event = Event(
        invocation_id="waiting",
        author="agent",
        long_running_tool_ids={"call-1"},
    )
    assert await run("waiting", waiting_event) == "waiting"
    assert await run("error", fail=True) == "error"


def test_session_read_routes_return_required_metadata_fields() -> None:
    service = InMemorySessionService()
    store = SessionMetadataStore(service)
    app = FastAPI()

    @app.get("/apps/{app_name}/users/{user_id}/sessions")
    async def old_list(app_name: str, user_id: str):
        del app_name, user_id
        return []

    @app.get("/apps/{app_name}/users/{user_id}/sessions/{session_id}")
    async def old_detail(app_name: str, user_id: str, session_id: str):
        del app_name, user_id, session_id
        return {}

    install_session_routes(app, service, store)

    async def seed() -> None:
        await service.create_session(
            app_name="app", user_id="user", session_id="session"
        )
        await store.set_status("app", "user", "session", "running")
        await store.set_title_once("app", "user", "session", "查询北京天气")

    import asyncio

    asyncio.run(seed())
    with TestClient(app) as client:
        listed = client.get("/apps/app/users/user/sessions").json()
        detail = client.get("/apps/app/users/user/sessions/session").json()

    assert listed[0]["status"] == detail["status"] == "running"
    assert listed[0]["title"] == detail["title"] == "查询北京天气"
    assert listed[0]["events"] == []


@pytest.mark.asyncio
async def test_unknown_metadata_uses_required_defaults() -> None:
    store = SessionMetadataStore(InMemorySessionService())
    metadata = await store.get("app", "user", "historical")
    assert metadata.status == "idle"
    assert metadata.title == DEFAULT_SESSION_TITLE


@pytest.mark.asyncio
async def test_database_metadata_persists_independently_from_session_state(
    tmp_path,
) -> None:
    service = DatabaseSessionService(
        db_url=f"sqlite+aiosqlite:///{tmp_path / 'sessions.db'}"
    )
    store = SessionMetadataStore(service)
    install_session_service_metadata_hooks(service, store)

    session = await service.create_session(
        app_name="app", user_id="user", session_id="session"
    )
    await store.set_title_once("app", "user", "session", "数据库标题")
    await store.set_status("app", "user", "session", "waiting")

    reloaded = SessionMetadataStore(service)
    metadata = await reloaded.get("app", "user", "session")
    stored_session = await service.get_session(
        app_name="app", user_id="user", session_id="session"
    )
    assert metadata.title == "数据库标题"
    assert metadata.status == "waiting"
    assert session.state == stored_session.state == {}
    assert session.events == stored_session.events == []

    await service.delete_session("app", "user", "session")
    assert (await store.get("app", "user", "session")).title == DEFAULT_SESSION_TITLE
    await service.close()
