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

"""Request-scoped knowledge base instances for harness overrides.

``veadk.KnowledgeBase`` opens its backend client in ``__init__`` (a Redis
connection pool, a vector-store client, ...). Building one per request would
leak connections, so overrides share a bounded LRU cache keyed by
``(backend, index)``. Entries are reference-counted: a request releases its lease
when the invocation ends, and only unreferenced entries may be evicted and
closed.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("harness")

DEFAULT_CACHE_SIZE = 64
CACHE_SIZE_ENV = "HARNESS_KB_CACHE_SIZE"


def cache_size_from_env() -> int:
    raw = os.getenv(CACHE_SIZE_ENV, "").strip()
    if not raw:
        return DEFAULT_CACHE_SIZE
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{CACHE_SIZE_ENV} must be an integer") from None
    if value < 1:
        raise RuntimeError(f"{CACHE_SIZE_ENV} must be at least 1")
    return value


def _build_knowledgebase(*, backend: str, index: str) -> Any:
    from veadk.knowledgebase import KnowledgeBase

    return KnowledgeBase(backend=backend, index=index)


def _close(knowledgebase: Any) -> None:
    close = getattr(knowledgebase, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception:  # eviction must not fail the request that triggered it
        logger.warning("Failed to close knowledge base backend", exc_info=True)


class _Entry:
    __slots__ = ("knowledgebase", "refs")

    def __init__(self, knowledgebase: Any) -> None:
        self.knowledgebase = knowledgebase
        self.refs = 0


class KnowledgeBaseLease:
    """One request's hold on a cached knowledge base instance."""

    __slots__ = ("_provider", "_key", "_entry", "_released")

    def __init__(self, provider: "KnowledgeBaseProvider", key, entry: _Entry) -> None:
        self._provider = provider
        self._key = key
        self._entry = entry
        self._released = False

    @property
    def knowledgebase(self) -> Any:
        return self._entry.knowledgebase

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._provider.release(self._key, self._entry)


class KnowledgeBaseProvider:
    """Bounded, thread-safe cache of knowledge base instances."""

    def __init__(
        self,
        factory: Callable[..., Any] | None = None,
        *,
        cache_size: int = DEFAULT_CACHE_SIZE,
    ) -> None:
        if cache_size < 1:
            raise ValueError("cache_size must be at least 1")
        self._factory = factory or _build_knowledgebase
        self._cache_size = cache_size
        self._entries: OrderedDict[tuple[str, str], _Entry] = OrderedDict()
        self._lock = threading.Lock()

    def acquire(self, backend: str, index: str) -> KnowledgeBaseLease:
        key = (backend, index)
        evicted: list[Any] = []
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = _Entry(self._factory(backend=backend, index=index))
                self._entries[key] = entry
            entry.refs += 1
            self._entries.move_to_end(key)
            evicted = self._trim_locked()
        for knowledgebase in evicted:
            _close(knowledgebase)
        return KnowledgeBaseLease(self, key, entry)

    def release(self, key: tuple[str, str], entry: _Entry) -> None:
        with self._lock:
            entry.refs = max(0, entry.refs - 1)
            evicted = self._trim_locked()
        # 突发请求结束后及时回收超额实例；关闭客户端不能占用缓存锁。
        for knowledgebase in evicted:
            _close(knowledgebase)

    def _trim_locked(self) -> list[Any]:
        evicted: list[Any] = []
        while len(self._entries) > self._cache_size:
            victim = next(
                (key for key, entry in self._entries.items() if entry.refs == 0),
                None,
            )
            if victim is None:
                break
            entry = self._entries.pop(victim)
            evicted.append(entry.knowledgebase)
        return evicted
