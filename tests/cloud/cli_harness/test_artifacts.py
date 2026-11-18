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

"""TOS artifact service, capture plugin, publish tool and cronjob contract."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from google.genai import types

from veadk.cloud.harness_app.server_types import HarnessOverrides
from veadk.cloud.harness_app.artifacts import (
    ArtifactCollector,
    ArtifactPlugin,
    ArtifactSettings,
    QuotaExceeded,
    TosArtifactService,
    current_invocation_id,
    is_sensitive_path,
    safe_filename,
)
from veadk.cloud.harness_app.artifact_tools import (
    allowed_roots,
    build_publish_tool,
    publish_bytes,
)
from veadk.cloud.harness_app.cronjobs.webhook import completion_payload
from veadk.cloud.harness_app.cronjobs.webhook_config import WebhookFields
from veadk.cloud.harness_app.cronjobs.engine import Engine
from veadk.cloud.harness_app.tos_client import build_client_factory, tos_endpoint


class FakeConflict(Exception):
    status_code = 409


class FakeNotFound(Exception):
    status_code = 404


class FakeResponse:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeObject:
    def __init__(self, key: str, size: int) -> None:
        self.key = key
        self.size = size


class FakeList:
    def __init__(self, contents) -> None:
        self.contents = contents
        self.is_truncated = False
        self.next_continuation_token = ""


class FakeTos:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail_keys: set[str] = set()
        self.presign_error = False

    def put_object(
        self, bucket, key, content, content_type, forbid_overwrite, meta=None
    ):
        if key in self.fail_keys:
            self.fail_keys.discard(key)
            raise FakeConflict()
        if forbid_overwrite and key in self.objects:
            raise FakeConflict()
        self.objects[key] = bytes(content)

    def get_object(self, bucket, key):
        if key not in self.objects:
            raise FakeNotFound()
        return FakeResponse(self.objects[key])

    def delete_object(self, bucket, key):
        if key not in self.objects:
            raise FakeNotFound()
        del self.objects[key]

    def list_objects_type2(self, bucket, prefix, max_keys, continuation_token):
        contents = [
            FakeObject(key, len(value))
            for key, value in sorted(self.objects.items())
            if key.startswith(prefix)
        ]
        return FakeList(contents)

    def head_bucket(self, bucket):
        return None

    def pre_signed_url(self, method, bucket, key, expires):
        if self.presign_error:
            raise RuntimeError("signing unavailable")
        return SimpleNamespace(signed_url=f"https://signed.example/{key}")


def make_settings(**overrides) -> ArtifactSettings:
    values = dict(
        adk_enabled=True,
        tos_bucket="bucket",
        tos_region="cn-beijing",
        tos_prefix="prefix/harness",
        max_session_bytes=10 * 1024 * 1024,
    )
    values.update(overrides)
    return ArtifactSettings(**values)


def make_service(**overrides):
    fake = FakeTos()
    settings = make_settings(**overrides)
    return TosArtifactService(settings, client_factory=lambda: fake), fake


def inline_part(data: bytes = b"hello", name: str = "a.txt") -> types.Part:
    return types.Part(
        inline_data=types.Blob(data=data, mime_type="text/plain", display_name=name)
    )


# ---------------------------------------------------------------------------
# settings / helpers
# ---------------------------------------------------------------------------
def test_settings_defaults_are_all_disabled():
    settings = ArtifactSettings.from_env({})
    assert not settings.any_enabled
    assert settings.link_ttl_seconds == 604800


def test_settings_require_storage_when_enabled():
    with pytest.raises(RuntimeError, match="HARNESS_ARTIFACT_TOS_BUCKET"):
        ArtifactSettings.from_env({"HARNESS_ARTIFACT_ADK_ENABLED": "true"})


def test_settings_reject_configured_but_disabled():
    with pytest.raises(RuntimeError, match="all disabled"):
        ArtifactSettings.from_env({"HARNESS_ARTIFACT_TOS_BUCKET": "bucket"})


def test_settings_validate_ttl_and_prefix():
    with pytest.raises(RuntimeError, match="LINK_TTL"):
        make_settings(link_ttl_seconds=10).validate()
    with pytest.raises(RuntimeError, match="must not contain"):
        make_settings(tos_prefix="a/../b").validate()
    with pytest.raises(RuntimeError, match="MAX_SESSION_BYTES"):
        make_settings(max_session_bytes=0).validate()


def test_safe_filename_and_sensitive_rules():
    assert safe_filename(" report.pptx ") == "report.pptx"
    for bad in ("", "..", "a/b", "a\\b", "a\x00b", "x" * 256):
        with pytest.raises(ValueError):
            safe_filename(bad)
    assert is_sensitive_path("secrets/token.txt")
    assert is_sensitive_path(".env")
    assert is_sensitive_path("id_rsa")
    assert not is_sensitive_path("outputs/report.pptx")


def test_tos_endpoint_and_client_factory(tmp_path: Path):
    assert tos_endpoint("cn-beijing") == "tos-cn-beijing.volces.com"
    assert tos_endpoint("sg", "byteplus") == "tos-sg.bytepluses.com"
    assert tos_endpoint("cn", "volcengine", "custom.example") == "custom.example"
    credential = tmp_path / "credential"
    credential.write_text(
        json.dumps({"access_key_id": "ak1", "secret_access_key": "sk1"})
    )
    created = []

    def factory(**kwargs):
        created.append(kwargs)
        return SimpleNamespace(**kwargs)

    client_factory = build_client_factory(
        region="cn-beijing",
        endpoint="tos-cn-beijing.volces.com",
        credential_path=credential,
        environ={},
        client_factory=factory,
    )
    first = client_factory()
    assert client_factory() is first
    assert len(created) == 1
    credential.write_text(
        json.dumps({"access_key_id": "ak2", "secret_access_key": "sk2"})
    )
    second = client_factory()
    assert second is not first
    assert len(created) == 2


def test_client_factory_requires_credentials(tmp_path: Path):
    credential = tmp_path / "missing"
    factory = build_client_factory(
        region="r",
        endpoint="e",
        credential_path=credential,
        environ={},
        client_factory=lambda **kwargs: None,
    )
    with pytest.raises(RuntimeError, match="IAM credentials"):
        factory()


# ---------------------------------------------------------------------------
# service
# ---------------------------------------------------------------------------
def test_save_load_versions_and_delete():
    service, fake = make_service()
    version = asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="a.txt",
            artifact=inline_part(),
        )
    )
    assert version == 1
    loaded = asyncio.run(
        service.load_artifact(
            app_name="app", user_id="u", session_id="s", filename="a.txt"
        )
    )
    assert loaded.inline_data.data == b"hello"
    assert loaded.inline_data.display_name == "a.txt"

    assert (
        asyncio.run(
            service.save_artifact(
                app_name="app",
                user_id="u",
                session_id="s",
                filename="a.txt",
                artifact=inline_part(b"world", "a.txt"),
            )
        )
        == 2
    )
    assert asyncio.run(
        service.list_versions(
            app_name="app", user_id="u", session_id="s", filename="a.txt"
        )
    ) == [1, 2]
    assert asyncio.run(
        service.list_artifact_keys(app_name="app", user_id="u", session_id="s")
    ) == ["a.txt"]
    meta = asyncio.run(
        service.get_artifact_version(
            app_name="app", user_id="u", session_id="s", filename="a.txt", version=1
        )
    )
    assert meta.version == 1
    assert meta.mime_type == "text/plain"

    asyncio.run(
        service.delete_artifact(
            app_name="app", user_id="u", session_id="s", filename="a.txt"
        )
    )
    assert not any(
        key.startswith("prefix/harness/app/u/s/adk/a.txt") for key in fake.objects
    )
    assert (
        asyncio.run(
            service.load_artifact(
                app_name="app", user_id="u", session_id="s", filename="a.txt"
            )
        )
        is None
    )


def test_text_and_file_data_parts_round_trip():
    service, _ = make_service()
    asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="note.txt",
            artifact=types.Part(text="hello"),
        )
    )
    loaded = asyncio.run(
        service.load_artifact(
            app_name="app", user_id="u", session_id="s", filename="note.txt"
        )
    )
    assert loaded.text == "hello"

    asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="remote.bin",
            artifact=types.Part(
                file_data=types.FileData(
                    file_uri="gs://bucket/x", mime_type="image/png"
                )
            ),
        )
    )
    loaded = asyncio.run(
        service.load_artifact(
            app_name="app", user_id="u", session_id="s", filename="remote.bin"
        )
    )
    assert loaded.file_data.file_uri == "gs://bucket/x"


def test_user_scoped_artifact_layout():
    service, fake = make_service()
    version = asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="user:avatar.png",
            artifact=inline_part(b"x", "avatar.png"),
        )
    )
    assert version == 1
    assert "prefix/harness/app/u/s/user/avatar.png/v1" in fake.objects
    assert asyncio.run(
        service.list_artifact_keys(app_name="app", user_id="u", session_id="s")
    ) == ["user:avatar.png"]


def test_version_conflict_retries():
    service, fake = make_service()
    fake.fail_keys.add("prefix/harness/app/u/s/adk/a.txt/v1")
    version = asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="a.txt",
            artifact=inline_part(),
        )
    )
    assert version == 2
    assert "prefix/harness/app/u/s/adk/a.txt/v2" in fake.objects


def test_session_quota():
    service, _ = make_service(max_session_bytes=5)
    with pytest.raises(QuotaExceeded):
        asyncio.run(
            service.save_artifact(
                app_name="app",
                user_id="u",
                session_id="s",
                filename="a.txt",
                artifact=inline_part(b"123456", "a.txt"),
            )
        )


def test_presign_fallback():
    service, fake = make_service()
    fake.presign_error = True
    assert asyncio.run(service.presign_key("k")) == "tos://bucket/k"


def test_publish_bytes_and_file(tmp_path: Path):
    service, _ = make_service(publish_enabled=True)
    item = asyncio.run(
        service.publish_bytes(
            app_name="app",
            user_id="u",
            session_id="s",
            data=b"abc",
            filename="out.txt",
        )
    )
    assert item.url.startswith("https://signed.example/")
    assert item.key.endswith("/published/out.txt/v1")
    source = tmp_path / "deck.pptx"
    source.write_bytes(b"deck")
    file_item = asyncio.run(
        service.publish_file(app_name="app", user_id="u", session_id="s", path=source)
    )
    assert file_item.name == "deck.pptx"
    assert file_item.version == 1


def test_publish_conflict_and_quota():
    service, fake = make_service(publish_enabled=True, publish_max_file_bytes=2)
    with pytest.raises(QuotaExceeded):
        asyncio.run(
            service.publish_bytes(
                app_name="app",
                user_id="u",
                session_id="s",
                data=b"abc",
                filename="x.txt",
            )
        )
    fake.fail_keys.add("prefix/harness/app/u/s/published/x.txt/v1")
    item = asyncio.run(
        service.publish_bytes(
            app_name="app",
            user_id="u",
            session_id="s",
            data=b"a",
            filename="x.txt",
        )
    )
    assert item.version == 2


def test_list_session_artifacts_filters_and_limits():
    service, _ = make_service(adk_enabled=True, publish_enabled=True)
    token = current_invocation_id.set("inv-1")
    try:
        asyncio.run(
            service.save_artifact(
                app_name="app",
                user_id="u",
                session_id="s",
                filename="a.txt",
                artifact=inline_part(),
            )
        )
        asyncio.run(
            service.publish_bytes(
                app_name="app",
                user_id="u",
                session_id="s",
                data=b"p",
                filename="p.txt",
            )
        )
    finally:
        current_invocation_id.reset(token)
    items = asyncio.run(
        service.list_session_artifacts(
            app_name="app", user_id="u", session_id="s", invocation_id="inv-1"
        )
    )
    assert {item.source for item in items} == {"adk", "published"}
    assert (
        asyncio.run(
            service.list_session_artifacts(
                app_name="app", user_id="u", session_id="s", invocation_id="other"
            )
        )
        == []
    )
    assert (
        asyncio.run(
            service.list_session_artifacts(
                app_name="app", user_id="u", session_id="s", since=time.time() + 3600
            )
        )
        == []
    )
    assert (
        len(
            asyncio.run(
                service.list_session_artifacts(
                    app_name="app", user_id="u", session_id="s", limit=1
                )
            )
        )
        == 1
    )


# ---------------------------------------------------------------------------
# plugin
# ---------------------------------------------------------------------------
def _context(runtime: str = "adk", invocation_id: str = "inv"):
    session = SimpleNamespace(app_name="app", user_id="u", id="s")
    agent = SimpleNamespace(runtime=runtime, name="agent")
    return SimpleNamespace(invocation_id=invocation_id, session=session, agent=agent)


def _event(delta=None, text="done", final=True, thought=False):
    part = SimpleNamespace(text=text, thought=thought)
    return SimpleNamespace(
        actions=SimpleNamespace(artifact_delta=delta or {}),
        content=SimpleNamespace(parts=[part]),
        is_final_response=lambda: final,
    )


def test_plugin_appends_names_only_marker():
    service, _ = make_service()
    plugin = ArtifactPlugin(make_settings(), service)
    context = _context()
    event = _event(delta={"report.pptx": 1})
    asyncio.run(plugin.before_run_callback(invocation_context=context))
    asyncio.run(plugin.on_event_callback(invocation_context=context, event=event))
    text = event.content.parts[0].text
    assert "```agentkit-artifacts" in text
    assert "report.pptx (adk): tos://bucket/" in text
    assert "https://signed" not in text
    asyncio.run(plugin.after_run_callback(invocation_context=context))


def test_plugin_skips_thought_and_missing_text():
    service, _ = make_service()
    plugin = ArtifactPlugin(make_settings(), service)
    context = _context()
    thought = _event(delta={"a.txt": 1}, thought=True)
    asyncio.run(plugin.on_event_callback(invocation_context=context, event=thought))
    assert thought.content.parts[0].text == "done"
    empty = _event(delta={"a.txt": 1}, text="")
    asyncio.run(plugin.on_event_callback(invocation_context=context, event=empty))
    assert empty.content.parts[0].text == ""


def test_collector_materialises_links():
    service, _ = make_service()
    collector = ArtifactCollector()
    collector.add(name="a.txt", source="adk", key="k/v1", version=1)
    items = asyncio.run(service.items_for_entries(collector.entries()))
    assert items[0].name == "a.txt"
    assert items[0].url.startswith("https://signed.example/")


# ---------------------------------------------------------------------------
# publish tool
# ---------------------------------------------------------------------------
def _tool_context(tmp_path: Path, session_id: str = "s"):
    session = SimpleNamespace(app_name="app", user_id="u", id=session_id)
    agent = SimpleNamespace(runtime="adk", name="agent")
    return SimpleNamespace(
        _invocation_context=SimpleNamespace(session=session, agent=agent)
    )


def test_publish_tool_saves_file(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("VEADK_SKILLS_WORK_DIR", str(tmp_path))
    outputs = tmp_path / "s" / "outputs"
    outputs.mkdir(parents=True)
    target = outputs / "deck.pptx"
    target.write_bytes(b"deck")
    settings = make_settings(publish_enabled=True)
    service, _ = make_service(publish_enabled=True)
    tool = build_publish_tool(settings, service)
    result = asyncio.run(tool(str(target), tool_context=_tool_context(tmp_path)))
    assert result["status"] == "saved"
    assert result["artifacts"][0]["url"].startswith("tos://bucket/")
    assert result["artifacts"][0]["source"] == "published"


def test_publish_tool_rejects_escape_sensitive_and_oversize(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("VEADK_SKILLS_WORK_DIR", str(tmp_path))
    outputs = tmp_path / "s" / "outputs"
    outputs.mkdir(parents=True)
    settings = make_settings(publish_enabled=True, publish_max_file_bytes=2)
    service, _ = make_service(publish_enabled=True, publish_max_file_bytes=2)
    tool = build_publish_tool(settings, service)
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"x")
    assert (
        asyncio.run(tool(str(outside), tool_context=_tool_context(tmp_path)))["status"]
        == "error"
    )
    secret = outputs / ".env"
    secret.write_bytes(b"x")
    assert (
        asyncio.run(tool(str(secret), tool_context=_tool_context(tmp_path)))["status"]
        == "error"
    )
    big = outputs / "big.txt"
    big.write_bytes(b"abc")
    assert (
        asyncio.run(tool(str(big), tool_context=_tool_context(tmp_path)))["status"]
        == "error"
    )


def test_publish_tool_requires_context():
    settings = make_settings(publish_enabled=True)
    service, _ = make_service(publish_enabled=True)
    tool = build_publish_tool(settings, service)
    assert asyncio.run(tool("/tmp/x"))["status"] == "error"


def test_publish_bytes_api():
    service, _ = make_service(publish_enabled=True)
    item = asyncio.run(
        publish_bytes(
            service,
            app_name="app",
            user_id="u",
            session_id="s",
            data=b"x",
            filename="x.bin",
        )
    )
    assert item.source == "published"


# ---------------------------------------------------------------------------
# cronjob contract
# ---------------------------------------------------------------------------
def _run(**updates):
    run = {
        "cronjob_id": "a" * 32,
        "run_id": "1700000000000-" + "b" * 20,
        "task_name": "task",
        "schedule": {"at": "2026-01-01T00:00:00Z", "timezone": "Asia/Shanghai"},
        "scheduled_at": "2026-01-01T00:00:00Z",
        "started_at": "2026-01-01T00:00:01Z",
        "completed_at": "2026-01-01T00:00:02Z",
        "state": "succeeded",
        "output": "ok",
        "error": "",
        "user_id": "u",
    }
    run.update(updates)
    return run


def test_webhook_payload_artifacts_are_opt_in():
    run = _run(webhook={"url": "https://example/hook", "include_artifacts": False})
    payload = completion_payload(run, {"enabled": True, "next_run_at": None})
    assert "artifacts" not in payload["result"]

    run = _run(
        webhook={"url": "https://example/hook", "include_artifacts": True},
        artifacts=[
            {
                "name": "a.pptx",
                "url": "https://signed",
                "mime_type": "text/plain",
                "size": 3,
                "source": "adk",
                "version": 1,
            }
        ],
    )
    payload = completion_payload(run, {"enabled": True, "next_run_at": None})
    assert payload["result"]["artifacts"][0]["mimeType"] == "text/plain"


def test_webhook_config_accepts_include_artifacts():
    fields = WebhookFields(url="https://example/hook", include_artifacts=True)
    assert fields.include_artifacts is True


def test_engine_terminal_stores_artifacts():
    run = {}
    Engine._terminal(
        run,
        "succeeded",
        __import__("datetime").datetime.now(),
        artifacts=[{"name": "a"}],
    )
    assert run["artifacts"] == [{"name": "a"}]


def test_engine_invocation_result_accepts_tuple():
    engine = Engine(store=None, invoke=None, lease_seconds=60, poll_seconds=1)

    async def scenario():
        async def work():
            return "answer", [{"name": "a"}]

        task = asyncio.create_task(work())
        return await engine._invocation_result(
            task, {"timeout_seconds": 5}, asyncio.Event()
        )

    state, output, error, artifacts = asyncio.run(scenario())
    assert state == "succeeded"
    assert output == "answer"
    assert artifacts == [{"name": "a"}]


# ---------------------------------------------------------------------------
# branch coverage for helpers and failure paths
# ---------------------------------------------------------------------------
def test_env_int_rejects_non_integer_and_negative():
    base = {
        "HARNESS_ARTIFACT_ADK_ENABLED": "true",
        "HARNESS_ARTIFACT_TOS_BUCKET": "b",
        "HARNESS_ARTIFACT_TOS_REGION": "r",
        "HARNESS_ARTIFACT_TOS_PREFIX": "p",
    }
    with pytest.raises(RuntimeError, match="integer"):
        ArtifactSettings.from_env({**base, "HARNESS_ARTIFACT_LINK_TTL_SECONDS": "x"})
    with pytest.raises(RuntimeError, match="non-negative"):
        ArtifactSettings.from_env({**base, "HARNESS_ARTIFACT_LINK_TTL_SECONDS": "-1"})


def test_validate_retention_and_endpoint_property(monkeypatch):
    monkeypatch.setenv("AGENTKIT_CLOUD_PROVIDER", "byteplus")
    assert make_settings(tos_region="sg").endpoint == "tos-sg.bytepluses.com"


def test_safe_segment():
    from veadk.cloud.harness_app.artifacts import _safe_segment

    assert _safe_segment("!!!").startswith("x-")
    assert _safe_segment("a/b").count("-") >= 1


def test_artifact_item_serialisation():
    from veadk.cloud.harness_app.artifacts import ArtifactItem

    item = ArtifactItem(name="a", key="k", source="adk", version=1, url="u", size=2)
    assert item.to_dict()["mime_type"] is None
    assert item.to_webhook_dict()["mimeType"] is None


def test_collector_merge_len_and_quota():
    from veadk.cloud.harness_app.artifacts import ArtifactItem, _RunQuota

    collector = ArtifactCollector()
    collector.merge([ArtifactItem(name="a", key="k", source="adk")])
    assert len(collector) == 1
    with pytest.raises(QuotaExceeded, match="count"):
        _RunQuota(0, 10).take(1)
    with pytest.raises(QuotaExceeded, match="bytes"):
        _RunQuota(10, 0).take(1)


def test_is_conflict_by_exception_name():
    from veadk.cloud.harness_app.artifacts import _is_conflict

    class ConflictError(Exception):
        pass

    assert _is_conflict(ConflictError())
    assert not _is_conflict(RuntimeError())


def test_tos_primitives_and_errors():
    service, fake = make_service()
    service._put("k", b"x", content_type="text/plain", forbid=False, meta={"a": "b"})
    assert fake.objects["k"] == b"x"
    fake.get_object = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        service._get("missing")
    fake.delete_object = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        service._delete("missing")
    service.check_bucket()


def test_list_keys_pagination(monkeypatch):
    service, fake = make_service()
    fake.objects["p/a"] = b"x"
    calls = []

    def listing(bucket, prefix, max_keys, continuation_token):
        calls.append(continuation_token)
        if not continuation_token:
            return SimpleNamespace(
                contents=[FakeObject("p/a", 1)],
                is_truncated=True,
                next_continuation_token="next",
            )
        return SimpleNamespace(
            contents=[FakeObject("p/b", 1)],
            is_truncated=False,
            next_continuation_token="",
        )

    fake.list_objects_type2 = listing
    assert [item.key for item in service._list_keys("p/")] == ["p/a", "p/b"]
    assert calls == ["", "next"]


def test_json_part_round_trip_and_unknown_kind():
    service, _ = make_service()
    part = types.Part(function_call=types.FunctionCall(name="f", args={"a": 1}))
    asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="call.json",
            artifact=part,
        )
    )
    loaded = asyncio.run(
        service.load_artifact(
            app_name="app", user_id="u", session_id="s", filename="call.json"
        )
    )
    assert loaded.function_call.name == "f"
    with pytest.raises(ValueError, match="unknown"):
        service._deserialise_part({"part_kind": "nope"}, b"")


def test_describe_missing_and_cache():
    service, fake = make_service()
    assert asyncio.run(service.describe("missing")) is None
    asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="a.txt",
            artifact=inline_part(),
        )
    )
    before = len(fake.objects)
    assert asyncio.run(service.session_bytes("app", "u", "s")) > 0
    service.session_bytes  # cache hit path
    asyncio.run(service.session_bytes("app", "u", "s"))
    assert len(fake.objects) == before


def test_version_allocation_sidecar_conflict():
    service, fake = make_service()
    fake.fail_keys.add("prefix/harness/app/u/s/adk/a.txt/v1.json")
    version = asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="a.txt",
            artifact=inline_part(),
        )
    )
    assert version == 2


def test_load_artifact_missing_versions_and_meta():
    service, _ = make_service()
    assert (
        asyncio.run(
            service.load_artifact(
                app_name="app", user_id="u", session_id="s", filename="none"
            )
        )
        is None
    )
    asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="a.txt",
            artifact=inline_part(),
        )
    )
    assert (
        asyncio.run(
            service.load_artifact(
                app_name="app", user_id="u", session_id="s", filename="a.txt", version=9
            )
        )
        is None
    )


def test_save_artifact_size_limit():
    service, _ = make_service(adk_max_file_bytes=2)
    with pytest.raises(QuotaExceeded):
        asyncio.run(
            service.save_artifact(
                app_name="app",
                user_id="u",
                session_id="s",
                filename="a.txt",
                artifact=inline_part(b"abc", "a.txt"),
            )
        )


def test_list_artifact_versions_and_published_versions():
    service, _ = make_service(publish_enabled=True)
    asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="a.txt",
            artifact=inline_part(),
        )
    )
    versions = asyncio.run(
        service.list_artifact_versions(
            app_name="app", user_id="u", session_id="s", filename="a.txt"
        )
    )
    assert [item.version for item in versions] == [1]
    asyncio.run(
        service.publish_bytes(
            app_name="app", user_id="u", session_id="s", data=b"a", filename="p.txt"
        )
    )
    assert (
        asyncio.run(
            service.publish_bytes(
                app_name="app", user_id="u", session_id="s", data=b"b", filename="p.txt"
            )
        ).version
        == 2
    )


def test_list_session_artifacts_malformed_sidecar():
    service, fake = make_service()
    fake.objects["prefix/harness/app/u/s/bad.json"] = b"not-json"
    assert (
        asyncio.run(
            service.list_session_artifacts(app_name="app", user_id="u", session_id="s")
        )
        == []
    )


def test_plugin_no_session_and_empty_marker():
    service, _ = make_service()
    plugin = ArtifactPlugin(make_settings(), service)
    context = SimpleNamespace(
        invocation_id="x", session=None, agent=SimpleNamespace(runtime="adk")
    )
    asyncio.run(plugin.before_run_callback(invocation_context=context))
    assert plugin.marker_text(ArtifactCollector()) == ""
    asyncio.run(plugin.after_run_callback(invocation_context=context))


def test_plugin_merges_published_artifacts():
    from veadk.cloud.harness_app.artifacts import ArtifactItem

    service, _ = make_service()
    plugin = ArtifactPlugin(make_settings(), service)
    context = _context()
    context._harness_published_artifacts = [
        ArtifactItem(name="p.txt", key="k", source="published", version=1)
    ]
    event = _event()
    asyncio.run(plugin.on_event_callback(invocation_context=context, event=event))
    assert "p.txt (published)" in event.content.parts[0].text


def test_publish_tool_rejects_symlink(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("VEADK_SKILLS_WORK_DIR", str(tmp_path))
    outputs = tmp_path / "s" / "outputs"
    outputs.mkdir(parents=True)
    target = outputs / "real.txt"
    target.write_bytes(b"x")
    link = outputs / "link.txt"
    link.symlink_to(target)
    settings = make_settings(publish_enabled=True)
    service, _ = make_service(publish_enabled=True)
    tool = build_publish_tool(settings, service)
    assert (
        asyncio.run(tool(str(link), tool_context=_tool_context(tmp_path)))["status"]
        == "error"
    )


def test_publish_file_api(tmp_path: Path):
    from veadk.cloud.harness_app.artifact_tools import publish_file

    service, _ = make_service(publish_enabled=True)
    path = tmp_path / "a.txt"
    path.write_bytes(b"x")
    item = asyncio.run(
        publish_file(service, app_name="app", user_id="u", session_id="s", path=path)
    )
    assert item.name == "a.txt"


def test_env_int_valid_value_and_zero_retention():
    settings = ArtifactSettings.from_env(
        {
            "HARNESS_ARTIFACT_ADK_ENABLED": "true",
            "HARNESS_ARTIFACT_TOS_BUCKET": "b",
            "HARNESS_ARTIFACT_TOS_REGION": "r",
            "HARNESS_ARTIFACT_TOS_PREFIX": "p",
            "HARNESS_ARTIFACT_LINK_TTL_SECONDS": "60",
        }
    )
    assert settings.link_ttl_seconds == 60
    settings.validate()


def test_safe_filename_rejects_control_characters():
    with pytest.raises(ValueError, match="control"):
        safe_filename("a\x01b")


def test_list_keys_truncated_without_token():
    service, fake = make_service()
    fake.list_objects_type2 = lambda **kwargs: SimpleNamespace(
        contents=[], is_truncated=True, next_continuation_token=""
    )
    assert service._list_keys("p/") == []


def test_describe_malformed_sidecar():
    service, fake = make_service()
    fake.objects["prefix/harness/app/u/s/adk/a.txt/v1.json"] = b"not-json"
    assert asyncio.run(service.describe("prefix/harness/app/u/s/adk/a.txt/v1")) is None


def test_session_bytes_counts_payloads_and_can_be_disabled():
    service, fake = make_service()
    asyncio.run(
        service.save_artifact(
            app_name="app",
            user_id="u",
            session_id="s",
            filename="a.txt",
            artifact=inline_part(),
        )
    )
    service._usage.clear()
    assert asyncio.run(service.session_bytes("app", "u", "s")) == 5
    disabled, _ = make_service(max_session_bytes=0)
    assert asyncio.run(disabled.session_bytes("app", "u", "s")) == 0


def test_allocation_propagates_non_conflict_and_exhausts():
    service, fake = make_service()

    def boom(**kwargs):
        raise RuntimeError("boom")

    fake.put_object = boom
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(
            service.save_artifact(
                app_name="app",
                user_id="u",
                session_id="s",
                filename="a.txt",
                artifact=inline_part(),
            )
        )
    service2, fake2 = make_service()

    def always_conflict(**kwargs):
        raise FakeConflict()

    fake2.put_object = always_conflict
    with pytest.raises(Exception, match="allocate"):
        asyncio.run(
            service2.save_artifact(
                app_name="app",
                user_id="u",
                session_id="s",
                filename="a.txt",
                artifact=inline_part(),
            )
        )


def test_get_artifact_version_missing_versions():
    service, _ = make_service()
    assert (
        asyncio.run(
            service.get_artifact_version(
                app_name="app", user_id="u", session_id="s", filename="missing"
            )
        )
        is None
    )


def test_publish_path_rejects_directory(tmp_path: Path):
    from veadk.cloud.harness_app.artifact_tools import resolve_publish_path

    with pytest.raises(ValueError, match="regular file"):
        resolve_publish_path(str(tmp_path), [tmp_path])


def test_list_session_artifacts_invalid_create_time():
    service, fake = make_service()
    fake.objects["prefix/harness/app/u/s/adk/a/v1"] = b"x"
    fake.objects["prefix/harness/app/u/s/adk/a/v1.json"] = json.dumps(
        {"name": "a", "source": "adk", "create_time": "bad"}
    ).encode()
    assert (
        asyncio.run(
            service.list_session_artifacts(
                app_name="app", user_id="u", session_id="s", since=1
            )
        )
        == []
    )


# ---------------------------------------------------------------------------
# wiring
# ---------------------------------------------------------------------------
def test_wiring_runner_kwargs_and_links():
    from veadk.cloud.harness_app.artifact_wiring import (
        artifact_links,
        request_plugin,
        runner_artifact_kwargs,
    )

    service, _ = make_service()
    assert request_plugin(make_settings(), None) is None
    plugin = request_plugin(make_settings(), service)
    assert runner_artifact_kwargs(make_settings(adk_enabled=False), service, None) == {}
    kwargs = runner_artifact_kwargs(make_settings(), service, plugin)
    assert kwargs["artifact_service"] is service
    assert kwargs["plugins"] == [plugin]
    assert asyncio.run(artifact_links(service, None)) == []
    assert asyncio.run(artifact_links(None, ArtifactCollector())) == []
    collector = ArtifactCollector()
    collector.add(name="a", source="adk", key="prefix/k/v1", version=1)
    links = asyncio.run(artifact_links(service, collector))
    assert links[0].name == "a"


def test_install_artifacts_disabled_and_enabled(tmp_path: Path):
    from google.adk.agents.base_agent import BaseAgent

    from veadk.cloud.harness_app.artifact_wiring import install_artifacts

    class DummyAgent(BaseAgent):
        tools: list = []

        async def _run_async_impl(self, ctx):
            if False:  # pragma: no cover
                yield None

        async def _run_live_impl(self, ctx):
            if False:  # pragma: no cover
                yield None

    agent = DummyAgent(name="agent")
    entry, service, plugin = install_artifacts(make_settings(adk_enabled=False), agent)
    assert entry is agent and service is None and plugin is None

    created = []

    def factory(settings):
        service = make_service(
            **{
                "adk_enabled": settings.adk_enabled,
                "publish_enabled": settings.publish_enabled,
            }
        )[0]
        created.append(service)
        return service

    entry, service, plugin = install_artifacts(
        make_settings(publish_enabled=True), agent, service_factory=factory
    )
    assert service is created[0]
    assert plugin is not None
    assert entry.root_agent is agent
    assert entry.plugins == [plugin]
    assert agent.tools and callable(agent.tools[0])


def test_install_adk_service_requires_attribute():
    from types import SimpleNamespace

    from veadk.cloud.harness_app.artifact_wiring import install_adk_service

    settings = make_settings()
    install_adk_service(SimpleNamespace(server=None), settings, None)
    with pytest.raises(RuntimeError, match="artifact_service"):
        install_adk_service(SimpleNamespace(server=object()), settings, object())
    api = SimpleNamespace(artifact_service=None)
    service = object()
    install_adk_service(SimpleNamespace(server=api), settings, service)
    assert api.artifact_service is service


def test_build_artifacts_handler():
    from fastapi import HTTPException

    from veadk.cloud.harness_app.artifact_wiring import build_artifacts_handler

    handler = build_artifacts_handler(None, None)
    assert asyncio.run(handler(SimpleNamespace(query_params={}))) == {"artifacts": []}

    service, _ = make_service()
    asyncio.run(
        service.publish_bytes(
            app_name="harness_agent",
            user_id="u",
            session_id="s",
            data=b"x",
            filename="p.txt",
        )
    )
    handler = build_artifacts_handler(service, SimpleNamespace())
    request = SimpleNamespace(query_params={"session_id": "s", "user_id": "u"})
    result = asyncio.run(handler(request))
    assert result["artifacts"][0]["name"] == "p.txt"

    with pytest.raises(HTTPException):
        asyncio.run(handler(SimpleNamespace(query_params={})))
    with pytest.raises(HTTPException):
        asyncio.run(
            handler(SimpleNamespace(query_params={"session_id": "s", "since": "bad"}))
        )
    identity_server = SimpleNamespace(
        _request_identity_context=lambda: SimpleNamespace(user_sub="identity-user")
    )
    handler = build_artifacts_handler(service, identity_server)
    assert asyncio.run(handler(SimpleNamespace(query_params={"session_id": "s"}))) == {
        "artifacts": []
    }


def test_place_harness_routes_before_mount():
    from veadk.cloud.harness_app.artifact_wiring import place_harness_routes_before_a2a

    class Mount:
        path = "/"

    app = SimpleNamespace(
        routes=[
            SimpleNamespace(path="/harness/invoke"),
            SimpleNamespace(path="/apps"),
            Mount(),
            SimpleNamespace(path="/harness/artifacts"),
        ]
    )
    place_harness_routes_before_a2a(app)
    paths = [getattr(route, "path", None) for route in app.routes]
    assert paths[-1] == "/"
    assert paths.index("/harness/invoke") < paths.index("/")
    assert paths.index("/harness/artifacts") < paths.index("/")


def test_list_session_artifacts_keeps_latest_version_only():
    service, _ = make_service()
    for payload in (b"v1", b"v2"):
        asyncio.run(
            service.save_artifact(
                app_name="app",
                user_id="u",
                session_id="s",
                filename="a.txt",
                artifact=inline_part(payload, "a.txt"),
            )
        )
    items = asyncio.run(
        service.list_session_artifacts(app_name="app", user_id="u", session_id="s")
    )
    assert len(items) == 1
    assert items[0].version == 2


def test_check_bucket_falls_back_to_prefix_for_403():
    service, fake = make_service()
    seen = []

    def head(bucket):
        raise type("Denied", (Exception,), {"status_code": 403})()

    fake.head_bucket = head
    fake.list_objects_type2 = lambda **kwargs: seen.append(kwargs) or FakeList([])
    service.check_bucket()
    assert seen and seen[0]["prefix"] == "prefix/harness/"


def test_check_bucket_reports_missing_bucket():
    service, fake = make_service()

    def head(bucket):
        raise type("Missing", (Exception,), {"status_code": 404})()

    fake.head_bucket = head
    with pytest.raises(RuntimeError, match="does not exist"):
        service.check_bucket()


def test_artifacts_handler_registered_with_request_injection():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from veadk.cloud.harness_app.artifact_wiring import build_artifacts_handler

    app = FastAPI()
    app.get("/harness/artifacts")(build_artifacts_handler(None, None))
    client = TestClient(app)
    response = client.get("/harness/artifacts", params={"session_id": "s"})
    assert response.status_code == 200
    assert response.json() == {"artifacts": []}


def test_plugin_records_into_an_empty_fixed_collector():
    service, _ = make_service()
    collector = ArtifactCollector()
    plugin = ArtifactPlugin(make_settings(), service, collector=collector)
    context = _context()
    event = _event(delta={"fixed.txt": 1})
    asyncio.run(plugin.before_run_callback(invocation_context=context))
    asyncio.run(plugin.on_event_callback(invocation_context=context, event=event))
    assert len(collector) == 1
    assert collector.entries()[0]["name"] == "fixed.txt"
    asyncio.run(plugin.after_run_callback(invocation_context=context))


def test_published_registry_bridges_different_invocation_contexts():
    from veadk.cloud.harness_app.artifacts import ArtifactItem, record_published

    service, _ = make_service()
    collector = ArtifactCollector()
    plugin = ArtifactPlugin(make_settings(), service, collector=collector)
    record_published(
        "registry-inv",
        ArtifactItem(
            name="p.txt", key="prefix/published/p.txt/v1", source="published", version=1
        ),
    )
    # The plugin sees a different InvocationContext object with the same id.
    context = _context(invocation_id="registry-inv")
    event = _event(text="done")
    asyncio.run(plugin.before_run_callback(invocation_context=context))
    asyncio.run(plugin.on_event_callback(invocation_context=context, event=event))
    assert any(entry["name"] == "p.txt" for entry in collector.entries())
    assert "p.txt (published)" in event.content.parts[0].text
    asyncio.run(plugin.after_run_callback(invocation_context=context))


def test_wiring_overrides_are_request_scoped():
    from veadk.cloud.harness_app.artifact_wiring import artifact_overrides
    from veadk.cloud.harness_app.server_types import InvokeHarnessRequest

    request = InvokeHarnessRequest(
        prompt="p",
        harness_name="h",
        run_agent_request={"user_id": "u", "session_id": "s"},
    )
    assert artifact_overrides(make_settings(), request) is None
    explicit = HarnessOverrides(system_prompt="x")
    request.harness = explicit
    assert artifact_overrides(make_settings(), request) is explicit


def test_settings_parse_publish_options():
    settings = ArtifactSettings.from_env(
        {
            "HARNESS_ARTIFACT_PUBLISH_ENABLED": "yes",
            "HARNESS_ARTIFACT_TOS_BUCKET": "b",
            "HARNESS_ARTIFACT_TOS_REGION": "r",
            "HARNESS_ARTIFACT_TOS_PREFIX": "p",
            "HARNESS_ARTIFACT_PUBLISH_ALLOWED_ROOTS": "/a, /b",
            "HARNESS_ARTIFACT_PUBLISH_MAX_FILES_PER_RUN": "3",
            "HARNESS_ARTIFACT_PUBLISH_MAX_FILE_BYTES": "1024",
            "HARNESS_ARTIFACT_PUBLISH_MAX_RUN_BYTES": "4096",
        }
    )
    assert settings.publish_enabled
    assert settings.publish_allowed_roots == ("/a", "/b")
    assert settings.publish_max_files_per_run == 3
    assert settings.publish_max_file_bytes == 1024
    assert settings.publish_max_run_bytes == 4096


def test_allowed_roots_include_skills_outputs(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("VEADK_SKILLS_WORK_DIR", str(tmp_path))
    settings = make_settings(publish_enabled=True, publish_allowed_roots=("/extra",))
    context = SimpleNamespace(
        session=SimpleNamespace(app_name="app", user_id="u", id="s"),
        agent=SimpleNamespace(runtime="adk", name="agent"),
    )
    roots = allowed_roots(settings, context)
    assert (tmp_path / "s" / "outputs").resolve() in roots
    assert Path("/extra").resolve() in roots


def test_plugin_flush_merges_published_registry():
    from veadk.cloud.harness_app.artifacts import ArtifactItem, record_published

    service, _ = make_service()
    collector = ArtifactCollector()
    plugin = ArtifactPlugin(make_settings(), service, collector=collector)
    record_published(
        "flush-inv",
        ArtifactItem(
            name="f.txt", key="prefix/published/f.txt/v1", source="published", version=1
        ),
    )
    context = _context(invocation_id="flush-inv")
    asyncio.run(plugin.before_run_callback(invocation_context=context))
    asyncio.run(plugin.flush())
    assert any(entry["name"] == "f.txt" for entry in collector.entries())


def test_plugin_flush_without_states_is_noop():
    service, _ = make_service()
    plugin = ArtifactPlugin(make_settings(), service)
    asyncio.run(plugin.flush())
