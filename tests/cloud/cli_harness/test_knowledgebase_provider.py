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

"""Bounded, reference-counted cache of request-scoped knowledge bases."""

import threading
import time

import pytest

from veadk.cloud.harness_app import knowledgebase as kb_module
from veadk.cloud.harness_app.knowledgebase import (
    CACHE_SIZE_ENV,
    DEFAULT_CACHE_SIZE,
    KnowledgeBaseProvider,
    cache_size_from_env,
)


class FakeKnowledgeBase:
    def __init__(self, backend: str, index: str) -> None:
        self.backend = backend
        self.index = index
        self.closed = False
        self.close_error: Exception | None = None

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class Factory:
    def __init__(self, delay: float = 0.0) -> None:
        self.created: list[FakeKnowledgeBase] = []
        self.calls: list[tuple[str, str]] = []
        self.delay = delay
        self.lock = threading.Lock()

    def __call__(self, *, backend: str, index: str) -> FakeKnowledgeBase:
        with self.lock:
            self.calls.append((backend, index))
        if self.delay:
            time.sleep(self.delay)
        instance = FakeKnowledgeBase(backend, index)
        with self.lock:
            self.created.append(instance)
        return instance


def test_acquire_builds_once_and_reuses_the_cached_instance():
    factory = Factory()

    def build(**kwargs):
        return factory(**kwargs)

    provider = KnowledgeBaseProvider(build, cache_size=4)
    first = provider.acquire("viking", "kb_1")
    second = provider.acquire("viking", "kb_1")

    assert len(factory.calls) == 1
    assert first.knowledgebase is second.knowledgebase
    assert first.knowledgebase.index == "kb_1"


def test_different_backends_and_indexes_get_distinct_instances():
    factory = Factory()

    provider = KnowledgeBaseProvider(factory, cache_size=4)
    viking = provider.acquire("viking", "kb_1").knowledgebase
    redis = provider.acquire("redis", "kb_1").knowledgebase
    other_index = provider.acquire("viking", "kb_2").knowledgebase

    assert len({id(viking), id(redis), id(other_index)}) == 3
    assert [call[0] for call in factory.calls] == ["viking", "redis", "viking"]


def test_unreferenced_entries_are_evicted_and_closed_in_lru_order():
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=2)
    first = provider.acquire("local", "kb_1")
    first.release()
    second = provider.acquire("local", "kb_2")
    second.release()

    third = provider.acquire("local", "kb_3")

    assert factory.created[0].closed is True
    assert factory.created[1].closed is False
    assert third.knowledgebase.index == "kb_3"


def test_acquiring_an_existing_entry_refreshes_its_lru_position():
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=2)
    first = provider.acquire("local", "kb_1")
    first.release()
    provider.acquire("local", "kb_2").release()

    provider.acquire("local", "kb_1").release()
    provider.acquire("local", "kb_3").release()

    assert factory.created[0].closed is False  # kb_1 was refreshed
    assert factory.created[1].closed is True  # kb_2 became the oldest


def test_referenced_entries_are_never_evicted():
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=1)
    held = provider.acquire("local", "kb_1")
    other = provider.acquire("local", "kb_2")

    assert factory.created[0].closed is False
    provider.acquire("local", "kb_3")
    assert factory.created[0].closed is False  # still leased

    held.release()
    provider.acquire("local", "kb_4")
    assert factory.created[0].closed is True
    assert factory.created[1].closed is False  # kb_2 is still leased
    other.release()


def test_release_is_idempotent():
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=1)
    lease = provider.acquire("local", "kb_1")
    lease.release()
    lease.release()

    provider.acquire("local", "kb_2")

    assert factory.created[0].closed is True


def test_release_trims_overflow_without_another_acquire() -> None:
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=1)
    first = provider.acquire("local", "kb_1")
    shared = provider.acquire("local", "kb_1")
    second = provider.acquire("local", "kb_2")
    third = provider.acquire("local", "kb_3")
    assert not any(instance.closed for instance in factory.created)

    # 模拟并发请求依次结束；还有租约的实例不能提前关闭。
    first.release()
    first.release()
    assert not any(instance.closed for instance in factory.created)
    shared.release()
    assert [instance.closed for instance in factory.created] == [True, False, False]
    second.release()
    assert [instance.closed for instance in factory.created] == [True, True, False]
    third.release()
    assert [instance.closed for instance in factory.created] == [True, True, False]

    # 无需新 acquire 触发回收，容量内的条目仍可复用。
    cached = provider.acquire("local", "kb_3")
    assert cached.knowledgebase is third.knowledgebase
    assert len(factory.created) == 3
    cached.release()


def test_release_closes_evicted_backend_outside_cache_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=1)
    first = provider.acquire("local", "kb_1")
    second = provider.acquire("local", "kb_2")
    close_without_lock: list[bool] = []

    def close() -> None:
        # 非阻塞探测，错误实现也不会让回归测试死锁。
        acquired = provider._lock.acquire(blocking=False)
        close_without_lock.append(acquired)
        if acquired:
            provider._lock.release()

    monkeypatch.setattr(first.knowledgebase, "close", close)
    first.release()
    first.release()

    assert close_without_lock == [True]
    assert second.knowledgebase.closed is False
    second.release()


def test_release_logs_close_failure_and_preserves_active_entries(
    caplog: pytest.LogCaptureFixture,
) -> None:
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=1)
    first = provider.acquire("local", "kb_1")
    second = provider.acquire("local", "kb_2")
    first.knowledgebase.close_error = RuntimeError("backend down")

    first.release()

    assert first.knowledgebase.closed is True
    assert "Failed to close knowledge base backend" in caplog.text
    assert "backend down" in caplog.text
    assert second.knowledgebase.closed is False
    reused = provider.acquire("local", "kb_2")
    assert reused.knowledgebase is second.knowledgebase
    reused.release()
    second.release()


def test_close_errors_do_not_break_eviction():
    factory = Factory()
    provider = KnowledgeBaseProvider(factory, cache_size=1)
    provider.acquire("local", "kb_1").release()
    factory.created[0].close_error = RuntimeError("backend down")

    provider.acquire("local", "kb_2")

    assert factory.created[0].closed is True
    assert factory.created[1].closed is False


def test_objects_without_close_are_evicted_silently():
    provider = KnowledgeBaseProvider(lambda **_: object(), cache_size=1)
    provider.acquire("local", "kb_1").release()

    provider.acquire("local", "kb_2")


def test_factory_failures_propagate_and_cache_nothing():
    def factory(**kwargs):
        raise RuntimeError("backend unavailable")

    provider = KnowledgeBaseProvider(factory, cache_size=1)

    with pytest.raises(RuntimeError, match="backend unavailable"):
        provider.acquire("local", "kb_1")
    assert provider._entries == {}


def test_concurrent_acquire_builds_the_backend_once():
    factory = Factory(delay=0.05)
    provider = KnowledgeBaseProvider(factory, cache_size=4)
    leases: list = []
    lock = threading.Lock()

    def worker() -> None:
        lease = provider.acquire("local", "kb_1")
        with lock:
            leases.append(lease)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(factory.calls) == 1
    assert len({id(lease.knowledgebase) for lease in leases}) == 1


def test_cache_size_must_be_positive():
    with pytest.raises(ValueError, match="cache_size"):
        KnowledgeBaseProvider(lambda **_: object(), cache_size=0)


def test_cache_size_from_env(monkeypatch):
    monkeypatch.delenv(CACHE_SIZE_ENV, raising=False)
    assert cache_size_from_env() == DEFAULT_CACHE_SIZE

    monkeypatch.setenv(CACHE_SIZE_ENV, "8")
    assert cache_size_from_env() == 8

    monkeypatch.setenv(CACHE_SIZE_ENV, "")
    assert cache_size_from_env() == DEFAULT_CACHE_SIZE


@pytest.mark.parametrize("value", ["many", "0", "-2"])
def test_invalid_cache_size_env_is_rejected(monkeypatch, value):
    monkeypatch.setenv(CACHE_SIZE_ENV, value)

    with pytest.raises(RuntimeError, match=CACHE_SIZE_ENV):
        cache_size_from_env()


def test_default_factory_builds_a_veadk_knowledgebase(monkeypatch):
    created = []

    class FakeVeadkKnowledgeBase:
        def __init__(self, *, backend, index):
            created.append((backend, index))
            self.backend = backend
            self.index = index

    import veadk.knowledgebase as veadk_kb

    monkeypatch.setattr(veadk_kb, "KnowledgeBase", FakeVeadkKnowledgeBase)
    provider = KnowledgeBaseProvider(cache_size=1)

    lease = provider.acquire("viking", "kb_default")

    assert created == [("viking", "kb_default")]
    assert lease.knowledgebase.index == "kb_default"


def test_module_level_builder_returns_veadk_instance(monkeypatch):
    sentinel = object()

    import veadk.knowledgebase as veadk_kb

    monkeypatch.setattr(veadk_kb, "KnowledgeBase", lambda **_: sentinel)

    assert kb_module._build_knowledgebase(backend="local", index="kb") is sentinel
