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

"""Persisted Harness Session metadata and ADK lifecycle integration."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from google.adk.cli.cli_eval import EVAL_SESSION_ID_PREFIX
from google.adk.plugins import BasePlugin
from google.adk.sessions import Session
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    Column,
    MetaData,
    String,
    Table,
    and_,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError

SessionStatus = Literal["idle", "running", "waiting", "error"]

DEFAULT_SESSION_TITLE = "新会话"
MAX_SESSION_TITLE_LENGTH = 40

_TITLE_BOUNDARY = re.compile(r"[。！？!?\n\r]")
_WHITESPACE = re.compile(r"\s+")

_METADATA = MetaData()
_SESSION_METADATA = Table(
    "veadk_harness_session_metadata",
    _METADATA,
    Column("app_name", String(255), primary_key=True),
    Column("user_id", String(255), primary_key=True),
    Column("session_id", String(255), primary_key=True),
    Column("status", String(16), nullable=False),
    Column("title", String(255), nullable=False),
)


@dataclass(frozen=True)
class SessionMetadata:
    status: SessionStatus = "idle"
    title: str = DEFAULT_SESSION_TITLE


_ALIASES = {
    "app_name": "appName",
    "user_id": "userId",
    "last_update_time": "lastUpdateTime",
}


def title_from_user_message(message: Any) -> str | None:
    """Return a deterministic title from the first non-empty user text part."""
    for part in getattr(message, "parts", None) or ():
        text = getattr(part, "text", None)
        if not isinstance(text, str):
            continue
        candidate = text.strip()
        if not candidate:
            continue
        boundary = _TITLE_BOUNDARY.search(candidate)
        if boundary:
            candidate = candidate[: boundary.start()]
        normalized = _WHITESPACE.sub(" ", candidate).strip()
        if not normalized:
            continue
        if len(normalized) > MAX_SESSION_TITLE_LENGTH:
            normalized = normalized[: MAX_SESSION_TITLE_LENGTH - 1].rstrip() + "…"
        return normalized
    return None


class HarnessSessionResponse(BaseModel):
    """ADK Session response with Runtime-owned presentation metadata."""

    model_config = ConfigDict(
        alias_generator=lambda name: _ALIASES.get(name, name), populate_by_name=True
    )

    id: str
    app_name: str
    user_id: str
    state: dict[str, Any] = Field(default_factory=dict)
    events: list[Any] = Field(default_factory=list)
    last_update_time: float = 0.0
    status: SessionStatus
    title: str

    @classmethod
    def from_session(
        cls, session: Session, metadata: SessionMetadata
    ) -> HarnessSessionResponse:
        return cls(
            id=session.id,
            app_name=session.app_name,
            user_id=session.user_id,
            state=session.state,
            events=session.events,
            last_update_time=session.last_update_time,
            status=metadata.status,
            title=metadata.title,
        )


class SessionMetadataStore:
    """Store metadata beside, but never inside, ADK Session state or events."""

    def __init__(self, session_service: Any):
        self._engine = getattr(session_service, "db_engine", None)
        self._memory: dict[tuple[str, str, str], SessionMetadata] = {}
        self._lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self, connection: Any | None = None) -> None:
        if self._engine is None or self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            if connection is not None:
                await connection.run_sync(_METADATA.create_all)
            else:
                async with self._engine.begin() as own_connection:
                    await own_connection.run_sync(_METADATA.create_all)
            self._initialized = True

    async def get(
        self, app_name: str, user_id: str, session_id: str
    ) -> SessionMetadata:
        key = (app_name, user_id, session_id)
        if self._engine is None:
            return self._memory.get(key, SessionMetadata())
        await self.initialize()
        async with self._engine.connect() as connection:
            row = (
                await connection.execute(
                    select(_SESSION_METADATA.c.status, _SESSION_METADATA.c.title).where(
                        and_(
                            _SESSION_METADATA.c.app_name == app_name,
                            _SESSION_METADATA.c.user_id == user_id,
                            _SESSION_METADATA.c.session_id == session_id,
                        )
                    )
                )
            ).one_or_none()
        return SessionMetadata(*row) if row else SessionMetadata()

    async def get_many(
        self, app_name: str, user_id: str, session_ids: list[str]
    ) -> dict[str, SessionMetadata]:
        if not session_ids:
            return {}
        if self._engine is None:
            return {
                session_id: self._memory.get(
                    (app_name, user_id, session_id), SessionMetadata()
                )
                for session_id in session_ids
            }
        await self.initialize()
        async with self._engine.connect() as connection:
            rows = (
                await connection.execute(
                    select(
                        _SESSION_METADATA.c.session_id,
                        _SESSION_METADATA.c.status,
                        _SESSION_METADATA.c.title,
                    ).where(
                        and_(
                            _SESSION_METADATA.c.app_name == app_name,
                            _SESSION_METADATA.c.user_id == user_id,
                            _SESSION_METADATA.c.session_id.in_(session_ids),
                        )
                    )
                )
            ).all()
        found = {
            session_id: SessionMetadata(status=status, title=title)
            for session_id, status, title in rows
        }
        return {
            session_id: found.get(session_id, SessionMetadata())
            for session_id in session_ids
        }

    async def reset(self, app_name: str, user_id: str, session_id: str) -> None:
        key = (app_name, user_id, session_id)
        if self._engine is None:
            async with self._lock:
                self._memory[key] = SessionMetadata()
            return
        await self.initialize()
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(_SESSION_METADATA)
                .where(
                    and_(
                        _SESSION_METADATA.c.app_name == app_name,
                        _SESSION_METADATA.c.user_id == user_id,
                        _SESSION_METADATA.c.session_id == session_id,
                    )
                )
                .values(status="idle", title=DEFAULT_SESSION_TITLE)
            )
        if not result.rowcount:
            await self._insert_if_missing(
                app_name, user_id, session_id, SessionMetadata()
            )

    async def delete(self, app_name: str, user_id: str, session_id: str) -> None:
        key = (app_name, user_id, session_id)
        if self._engine is None:
            async with self._lock:
                self._memory.pop(key, None)
            return
        await self.initialize()
        async with self._engine.begin() as connection:
            await connection.execute(
                delete(_SESSION_METADATA).where(
                    and_(
                        _SESSION_METADATA.c.app_name == app_name,
                        _SESSION_METADATA.c.user_id == user_id,
                        _SESSION_METADATA.c.session_id == session_id,
                    )
                )
            )

    async def set_status(
        self, app_name: str, user_id: str, session_id: str, status: SessionStatus
    ) -> None:
        key = (app_name, user_id, session_id)
        if self._engine is None:
            async with self._lock:
                current = self._memory.get(key, SessionMetadata())
                self._memory[key] = SessionMetadata(status=status, title=current.title)
            return
        await self.initialize()
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(_SESSION_METADATA)
                .where(
                    and_(
                        _SESSION_METADATA.c.app_name == app_name,
                        _SESSION_METADATA.c.user_id == user_id,
                        _SESSION_METADATA.c.session_id == session_id,
                    )
                )
                .values(status=status)
            )
        if not result.rowcount:
            await self._insert_if_missing(
                app_name, user_id, session_id, SessionMetadata(status=status)
            )

    async def set_title_once(
        self, app_name: str, user_id: str, session_id: str, title: str
    ) -> None:
        key = (app_name, user_id, session_id)
        if self._engine is None:
            async with self._lock:
                current = self._memory.get(key, SessionMetadata())
                if current.title == DEFAULT_SESSION_TITLE:
                    self._memory[key] = SessionMetadata(
                        status=current.status, title=title
                    )
            return
        await self.initialize()
        async with self._engine.begin() as connection:
            result = await connection.execute(
                update(_SESSION_METADATA)
                .where(
                    and_(
                        _SESSION_METADATA.c.app_name == app_name,
                        _SESSION_METADATA.c.user_id == user_id,
                        _SESSION_METADATA.c.session_id == session_id,
                        _SESSION_METADATA.c.title == DEFAULT_SESSION_TITLE,
                    )
                )
                .values(title=title)
            )
        if not result.rowcount:
            await self._insert_if_missing(
                app_name, user_id, session_id, SessionMetadata(title=title)
            )

    async def _insert_if_missing(
        self,
        app_name: str,
        user_id: str,
        session_id: str,
        metadata: SessionMetadata,
    ) -> None:
        key = (app_name, user_id, session_id)
        if self._engine is None:
            async with self._lock:
                self._memory.setdefault(key, metadata)
            return
        await self.initialize()
        try:
            async with self._engine.begin() as connection:
                await connection.execute(
                    insert(_SESSION_METADATA).values(
                        app_name=app_name,
                        user_id=user_id,
                        session_id=session_id,
                        status=metadata.status,
                        title=metadata.title,
                    )
                )
        except IntegrityError:
            pass


class SessionLifecyclePlugin(BasePlugin):
    """Maintain Session metadata from explicit ADK invocation lifecycle hooks."""

    def __init__(self, store: SessionMetadataStore):
        super().__init__(name="harness_session_lifecycle")
        self.store = store
        self._pending_calls: dict[str, set[str]] = {}
        self._waiting: set[str] = set()
        self._failed: set[str] = set()

    @staticmethod
    def _key(invocation_context: Any) -> tuple[str, str, str]:
        session = invocation_context.session
        return session.app_name, session.user_id, session.id

    async def on_user_message_callback(
        self, *, invocation_context: Any, user_message: Any
    ) -> None:
        title = title_from_user_message(user_message)
        if title:
            await self.store.set_title_once(*self._key(invocation_context), title)

    async def before_run_callback(self, *, invocation_context: Any) -> None:
        invocation_id = invocation_context.invocation_id
        self._pending_calls[invocation_id] = set()
        self._waiting.discard(invocation_id)
        self._failed.discard(invocation_id)
        await self.store.set_status(*self._key(invocation_context), "running")

    async def on_event_callback(self, *, invocation_context: Any, event: Any) -> None:
        invocation_id = invocation_context.invocation_id
        pending = self._pending_calls.setdefault(invocation_id, set())
        pending.update(
            call.id for call in event.get_function_calls() if getattr(call, "id", None)
        )
        pending.difference_update(
            response.id
            for response in event.get_function_responses()
            if getattr(response, "id", None)
        )
        actions = event.actions
        if (
            actions.requested_auth_configs
            or actions.requested_tool_confirmations
            or event.long_running_tool_ids
        ):
            self._waiting.add(invocation_id)
            await self.store.set_status(*self._key(invocation_context), "waiting")

    async def on_model_error_callback(
        self, *, callback_context: Any, llm_request: Any, error: Exception
    ) -> None:
        del llm_request, error
        self._failed.add(callback_context.invocation_id)

    async def after_run_callback(self, *, invocation_context: Any) -> None:
        invocation_id = invocation_context.invocation_id
        key = self._key(invocation_context)
        pending = self._pending_calls.pop(invocation_id, set())
        if invocation_id in self._failed:
            status: SessionStatus = "error"
        elif invocation_id in self._waiting or pending:
            status = "waiting"
        else:
            status = "idle"
        self._waiting.discard(invocation_id)
        self._failed.discard(invocation_id)
        await self.store.set_status(*key, status)


def install_session_routes(
    app: FastAPI,
    session_service: Any,
    store: SessionMetadataStore,
    *,
    current_app_name_ref: Any | None = None,
) -> None:
    """Replace only the two ADK read routes with metadata-aware equivalents."""
    list_path = "/apps/{app_name}/users/{user_id}/sessions"
    detail_path = list_path + "/{session_id}"
    app.router.routes[:] = [
        route
        for route in app.router.routes
        if not (
            getattr(route, "path", None) in {list_path, detail_path}
            and "GET" in (getattr(route, "methods", None) or set())
        )
    ]

    @app.get(detail_path, response_model_exclude_none=True)
    async def get_harness_session(
        app_name: str, user_id: str, session_id: str
    ) -> HarnessSessionResponse:
        session = await session_service.get_session(
            app_name=app_name, user_id=user_id, session_id=session_id
        )
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found")
        if current_app_name_ref is not None:
            current_app_name_ref.value = app_name
        metadata = await store.get(app_name, user_id, session_id)
        return HarnessSessionResponse.from_session(session, metadata)

    @app.get(list_path, response_model_exclude_none=True)
    async def list_harness_sessions(
        app_name: str, user_id: str
    ) -> list[HarnessSessionResponse]:
        response = await session_service.list_sessions(
            app_name=app_name, user_id=user_id
        )
        sessions = [
            session
            for session in response.sessions
            if not session.id.startswith(EVAL_SESSION_ID_PREFIX)
        ]
        metadata = await store.get_many(
            app_name, user_id, [session.id for session in sessions]
        )
        return [
            HarnessSessionResponse.from_session(session, metadata[session.id])
            for session in sessions
        ]

    # FastAPI matches the first path, so move the replacements ahead of the A2A
    # catch-all and any stale route ordering retained by third-party servers.
    replacements = [
        route
        for route in app.router.routes
        if getattr(route, "endpoint", None)
        in {get_harness_session, list_harness_sessions}
    ]
    for route in replacements:
        app.router.routes.remove(route)
    app.router.routes[0:0] = replacements


def install_session_service_metadata_hooks(
    session_service: Any, store: SessionMetadataStore
) -> None:
    """Keep metadata creation/deletion atomic with the Session lifecycle."""
    if getattr(session_service, "_harness_metadata_hooks_installed", False):
        return
    create_session = session_service.create_session
    delete_session = session_service.delete_session

    async def create_with_metadata(*args: Any, **kwargs: Any) -> Session:
        session = await create_session(*args, **kwargs)
        await store.reset(session.app_name, session.user_id, session.id)
        return session

    async def delete_with_metadata(*args: Any, **kwargs: Any) -> None:
        app_name = kwargs.get("app_name", args[0] if len(args) > 0 else None)
        user_id = kwargs.get("user_id", args[1] if len(args) > 1 else None)
        session_id = kwargs.get("session_id", args[2] if len(args) > 2 else None)
        await delete_session(*args, **kwargs)
        if app_name is not None and user_id is not None and session_id is not None:
            await store.delete(str(app_name), str(user_id), str(session_id))

    session_service.create_session = create_with_metadata
    session_service.delete_session = delete_with_metadata
    session_service._harness_metadata_hooks_installed = True


def with_session_lifecycle_plugin(entry: Any, plugin: SessionLifecyclePlugin) -> Any:
    """Attach the plugin while preserving an existing ADK App and its plugins."""
    from google.adk.apps import App

    if isinstance(entry, App):
        entry.plugins.append(plugin)
        return entry
    return App(name=entry.name, root_agent=entry, plugins=[plugin])


__all__ = [
    "DEFAULT_SESSION_TITLE",
    "HarnessSessionResponse",
    "SessionLifecyclePlugin",
    "SessionMetadata",
    "SessionMetadataStore",
    "install_session_routes",
    "install_session_service_metadata_hooks",
    "title_from_user_message",
    "with_session_lifecycle_plugin",
]
