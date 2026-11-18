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

"""User-generated artifact capture backed by TOS.

Three independent sources feed the same object layout:

* ``adk``       — the ADK ``ArtifactService`` used by ppt/image/custom tools.
* ``published`` — files an agent explicitly hands over with ``publish_artifact``.

Every source has its own ``enabled`` switch.  When none is enabled this module
is never wired into the server, so the legacy behaviour (in-memory ADK
artifacts, no TOS calls) is preserved exactly.

The service deliberately keeps the *object store* concerns (versions, presign,
quotas) here and the *presentation* concerns (answer marker, CLI links) in the
plugin/tool layers.
"""

from __future__ import annotations

import asyncio
import contextvars
import fnmatch
import hashlib
import json
import logging
import mimetypes
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from google.adk.artifacts.base_artifact_service import (
    ArtifactVersion,
    BaseArtifactService,
)
from google.adk.plugins.base_plugin import BasePlugin
from google.genai import types

from .tos_client import build_client_factory, tos_endpoint

logger = logging.getLogger("harness.artifacts")

# Process-local invocation marker. ADK tools and plugins run in the same async
# task as the invocation, so a ContextVar is enough to attribute writes without
# threading an invocation id through every tool signature.
current_invocation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "harness_artifact_invocation", default=""
)

SENSITIVE_NAME_PATTERNS = (
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "*.p12",
    "*.pfx",
    "*.jks",
    "id_rsa*",
    "id_ed25519*",
    "id_ecdsa*",
    ".npmrc",
    ".netrc",
    ".pgpass",
    "credentials",
    "credentials.*",
    "*credential*",
    "*secret*",
)
SENSITIVE_PATH_PARTS = {".ssh", ".aws", ".config/gcloud", "secrets"}
MAX_LIST_PAGES = 20
_LIST_PAGE_SIZE = 1000


class QuotaExceeded(RuntimeError):
    """Raised when a per-run or per-session artifact quota is exceeded."""


class ArtifactConflict(RuntimeError):
    """Raised when a conditional TOS write lost a version race."""


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_int(environ: dict[str, str], name: str, default: int) -> int:
    raw = (environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be an integer") from None
    if value < 0:
        raise RuntimeError(f"{name} must be non-negative")
    return value


def _env_csv(value: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in (value or "").split(",") if item.strip())


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_segment(value: str, *, limit: int = 64) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("._-")
    if not cleaned:
        cleaned = "x"
    if cleaned != str(value):
        cleaned = f"{cleaned[: limit - 9]}-{hashlib.sha256(str(value).encode()).hexdigest()[:8]}"
    return cleaned[:limit]


def safe_filename(value: str) -> str:
    """Return a single path segment suitable for an object key."""
    name = str(value or "").strip()
    if not name or name in {".", ".."}:
        raise ValueError("filename must be a non-empty file name")
    if "/" in name or "\\" in name or "\x00" in name:
        raise ValueError("filename must not contain path separators")
    if any(ord(char) < 32 or ord(char) == 127 for char in name):
        raise ValueError("filename must not contain control characters")
    if len(name) > 255:
        raise ValueError("filename must be at most 255 characters")
    return name


def is_sensitive_path(name: str) -> bool:
    """Best-effort denylist for files that must never leave the workspace."""
    normalised = name.replace("\\", "/")
    parts = normalised.split("/")
    if any(part in SENSITIVE_PATH_PARTS for part in parts):
        return True
    basename = parts[-1]
    return any(
        fnmatch.fnmatch(basename, pattern) or fnmatch.fnmatch(normalised, pattern)
        for pattern in SENSITIVE_NAME_PATTERNS
    )


def _mime_for(name: str, fallback: str | None = None) -> str | None:
    guessed, _ = mimetypes.guess_type(name)
    return guessed or fallback


@dataclass(frozen=True)
class ArtifactSettings:
    """Resolved ``artifacts`` block. Both sources default to disabled."""

    adk_enabled: bool = False
    publish_enabled: bool = False

    tos_bucket: str = ""
    tos_region: str = ""
    tos_prefix: str = ""
    tos_endpoint: str = ""
    link_ttl_seconds: int = 604800
    max_session_bytes: int = 2 * 1024**3
    adk_max_file_bytes: int = 500 * 1024**2

    publish_allowed_roots: tuple[str, ...] = ()
    publish_max_files_per_run: int = 10
    publish_max_file_bytes: int = 100 * 1024**2
    publish_max_run_bytes: int = 500 * 1024**2

    @property
    def any_enabled(self) -> bool:
        return self.adk_enabled or self.publish_enabled

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "ArtifactSettings":
        env = dict(os.environ if environ is None else environ)
        settings = cls(
            adk_enabled=_truthy(env.get("HARNESS_ARTIFACT_ADK_ENABLED")),
            publish_enabled=_truthy(env.get("HARNESS_ARTIFACT_PUBLISH_ENABLED")),
            tos_bucket=(env.get("HARNESS_ARTIFACT_TOS_BUCKET") or "").strip(),
            tos_region=(env.get("HARNESS_ARTIFACT_TOS_REGION") or "").strip(),
            tos_prefix=(env.get("HARNESS_ARTIFACT_TOS_PREFIX") or "")
            .strip()
            .strip("/"),
            tos_endpoint=(env.get("HARNESS_ARTIFACT_TOS_ENDPOINT") or "").strip(),
            link_ttl_seconds=_env_int(env, "HARNESS_ARTIFACT_LINK_TTL_SECONDS", 604800),
            max_session_bytes=_env_int(
                env, "HARNESS_ARTIFACT_MAX_SESSION_BYTES", 2 * 1024**3
            ),
            adk_max_file_bytes=_env_int(
                env, "HARNESS_ARTIFACT_ADK_MAX_FILE_BYTES", 500 * 1024**2
            ),
            publish_allowed_roots=_env_csv(
                env.get("HARNESS_ARTIFACT_PUBLISH_ALLOWED_ROOTS")
            ),
            publish_max_files_per_run=_env_int(
                env, "HARNESS_ARTIFACT_PUBLISH_MAX_FILES_PER_RUN", 10
            ),
            publish_max_file_bytes=_env_int(
                env, "HARNESS_ARTIFACT_PUBLISH_MAX_FILE_BYTES", 100 * 1024**2
            ),
            publish_max_run_bytes=_env_int(
                env, "HARNESS_ARTIFACT_PUBLISH_MAX_RUN_BYTES", 500 * 1024**2
            ),
        )
        settings.validate(
            configured=any(key.startswith("HARNESS_ARTIFACT_") for key in env)
        )
        return settings

    def validate(self, *, configured: bool = True) -> None:
        if configured and not self.any_enabled:
            raise RuntimeError(
                "artifacts is configured but adk/publish are all disabled"
            )
        if not self.any_enabled:
            return
        missing = [
            name
            for name, value in (
                ("HARNESS_ARTIFACT_TOS_BUCKET", self.tos_bucket),
                ("HARNESS_ARTIFACT_TOS_REGION", self.tos_region),
                ("HARNESS_ARTIFACT_TOS_PREFIX", self.tos_prefix),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "artifacts requires "
                + ", ".join(missing)
                + " when any source is enabled"
            )
        if ".." in self.tos_prefix.split("/"):
            raise RuntimeError("HARNESS_ARTIFACT_TOS_PREFIX must not contain '..'")
        if not 60 <= self.link_ttl_seconds <= 604800:
            raise RuntimeError(
                "HARNESS_ARTIFACT_LINK_TTL_SECONDS must be between 60 and 604800"
            )
        if self.max_session_bytes <= 0:
            raise RuntimeError("HARNESS_ARTIFACT_MAX_SESSION_BYTES must be positive")

    @property
    def endpoint(self) -> str:
        provider = os.getenv("AGENTKIT_CLOUD_PROVIDER", "volcengine")
        return tos_endpoint(self.tos_region, provider, self.tos_endpoint)


@dataclass(frozen=True)
class ArtifactItem:
    name: str
    key: str
    source: str
    version: int | str | None = None
    url: str = ""
    mime_type: str | None = None
    size: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "url": self.url,
            "mime_type": self.mime_type,
            "size": self.size,
            "source": self.source,
            "version": self.version,
        }

    def to_webhook_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "url": self.url,
            "mimeType": self.mime_type,
            "size": self.size,
            "source": self.source,
            "version": self.version,
        }


# The publish tool and the plugin do not always share the same InvocationContext
# object (VeADK's non-streaming runner is one case).  A tiny registry keyed by
# invocation id keeps published artifacts from being lost between them.
_PUBLISHED: dict[str, list["ArtifactItem"]] = {}


def record_published(invocation_id: str | None, item: "ArtifactItem") -> None:
    _PUBLISHED.setdefault(str(invocation_id or "default"), []).append(item)


def peek_published(invocation_id: str | None) -> list["ArtifactItem"]:
    return list(_PUBLISHED.get(str(invocation_id or "default"), []))


def take_published(invocation_id: str | None) -> list["ArtifactItem"]:
    return _PUBLISHED.pop(str(invocation_id or "default"), [])


class ArtifactCollector:
    """Per-invocation accumulator; deduplicates by (source, key, version)."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str, Any], dict[str, Any]] = {}

    def add(
        self,
        *,
        name: str,
        source: str,
        key: str,
        version: int | str | None = None,
        mime_type: str | None = None,
        size: int | None = None,
    ) -> None:
        identity = (source, key, version)
        self._entries[identity] = {
            "name": name,
            "source": source,
            "key": key,
            "version": version,
            "mime_type": mime_type,
            "size": size,
        }

    def merge(self, items: Iterable[ArtifactItem]) -> None:
        for item in items:
            self.add(
                name=item.name,
                source=item.source,
                key=item.key,
                version=item.version,
                mime_type=item.mime_type,
                size=item.size,
            )

    def __len__(self) -> int:
        return len(self._entries)

    def entries(self) -> list[dict[str, Any]]:
        return list(self._entries.values())

    def has_entries(self) -> bool:
        return bool(self._entries)


class _RunQuota:
    """Simple per-invocation file/byte budget."""

    def __init__(self, max_files: int, max_bytes: int) -> None:
        self.max_files = max_files
        self.max_bytes = max_bytes
        self.files = 0
        self.bytes = 0

    def take(self, size: int) -> None:
        if self.files + 1 > self.max_files:
            raise QuotaExceeded(f"artifact count exceeds {self.max_files} per run")
        if self.bytes + size > self.max_bytes:
            raise QuotaExceeded(f"artifact bytes exceed {self.max_bytes} per run")
        self.files += 1
        self.bytes += size


def _is_conflict(error: BaseException) -> bool:
    status = getattr(error, "status_code", None)
    if status in {409, 412}:
        return True
    return "conflict" in type(error).__name__.lower()


class TosArtifactService(BaseArtifactService):
    """ADK artifact service with versioned TOS objects and presigned reads."""

    def __init__(
        self,
        settings: ArtifactSettings,
        client_factory=None,
        *,
        clock=time.time,
        usage_ttl: float = 30.0,
    ) -> None:
        self.settings = settings
        self.client_factory = client_factory or build_client_factory(
            region=settings.tos_region,
            endpoint=settings.endpoint,
            provider=os.getenv("AGENTKIT_CLOUD_PROVIDER", "volcengine"),
        )
        self._clock = clock
        self._usage_ttl = usage_ttl
        self._usage: dict[tuple[str, str, str], tuple[float, int]] = {}

    # -- TOS primitives -------------------------------------------------
    async def _run(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    def _put(
        self, key: str, content: bytes, *, content_type: str, forbid: bool, meta=None
    ):
        kwargs = {
            "bucket": self.settings.tos_bucket,
            "key": key,
            "content": content,
            "content_type": content_type,
            "forbid_overwrite": forbid,
        }
        if meta:
            kwargs["meta"] = meta
        self.client_factory().put_object(**kwargs)

    def _get(self, key: str) -> bytes | None:
        try:
            response = self.client_factory().get_object(
                bucket=self.settings.tos_bucket, key=key
            )
        except Exception as error:  # noqa: BLE001 - 404 is a normal miss
            if getattr(error, "status_code", None) == 404:
                return None
            raise
        data = response.read()
        return bytes(data)

    def _delete(self, key: str) -> None:
        try:
            self.client_factory().delete_object(
                bucket=self.settings.tos_bucket, key=key
            )
        except Exception as error:  # noqa: BLE001 - deleting a missing key is fine
            if getattr(error, "status_code", None) != 404:
                raise

    def _list_keys(self, prefix: str, *, max_keys: int = _LIST_PAGE_SIZE) -> list[Any]:
        keys: list[Any] = []
        token = ""
        for _ in range(MAX_LIST_PAGES):
            response = self.client_factory().list_objects_type2(
                bucket=self.settings.tos_bucket,
                prefix=prefix,
                max_keys=max_keys,
                continuation_token=token,
            )
            keys.extend(response.contents or [])
            if not response.is_truncated:
                break
            token = response.next_continuation_token or ""
            if not token:
                break
        return keys

    def check_bucket(self) -> None:
        """Fail fast when the configured bucket/prefix is unusable.

        The Runtime role is scoped to the artifact prefix, so a bucket-level
        HeadBucket can legitimately return 403 even though prefix access works.
        In that case verify the prefix with a one-key listing instead, which is
        exactly the permission the policy grants.
        """
        try:
            self.client_factory().head_bucket(self.settings.tos_bucket)
            return
        except Exception as error:  # noqa: BLE001
            status = getattr(error, "status_code", None)
            if status == 404:
                raise RuntimeError(
                    f"artifact bucket '{self.settings.tos_bucket}' does not exist"
                ) from error
            if status != 403:
                raise
        self._list_keys(f"{self.prefix}/", max_keys=1)

    # -- keys -----------------------------------------------------------
    @property
    def prefix(self) -> str:
        return self.settings.tos_prefix

    def _base(self, app_name: str, user_id: str, session_id: str | None) -> str:
        session = session_id or "default"
        return "/".join(
            part
            for part in (
                self.prefix,
                _safe_segment(app_name),
                _safe_segment(user_id),
                _safe_segment(session),
            )
            if part
        )

    def tos_uri(self, key: str) -> str:
        return f"tos://{self.settings.tos_bucket}/{key}"

    def _split_adk_name(self, filename: str) -> tuple[str, str]:
        if filename.startswith("user:"):
            return "user", safe_filename(filename.removeprefix("user:"))
        return "adk", safe_filename(filename)

    def adk_key(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
        filename: str,
        version: int,
    ) -> str:
        namespace, name = self._split_adk_name(filename)
        return (
            f"{self._base(app_name, user_id, session_id)}/{namespace}/{name}/v{version}"
        )

    def published_key(
        self,
        app_name: str,
        user_id: str,
        session_id: str | None,
        filename: str,
        version: int,
    ) -> str:
        return (
            f"{self._base(app_name, user_id, session_id)}/published/"
            f"{safe_filename(filename)}/v{version}"
        )

    @staticmethod
    def _sidecar(key: str) -> str:
        return f"{key}.json"

    # -- Part serialisation ---------------------------------------------
    @staticmethod
    def _serialise_part(part: types.Part) -> tuple[bytes, dict[str, Any]]:
        if part.inline_data is not None:
            payload = bytes(part.inline_data.data or b"")
            return payload, {
                "part_kind": "inline_data",
                "mime_type": part.inline_data.mime_type,
                "display_name": part.inline_data.display_name,
            }
        if part.text is not None:
            return part.text.encode("utf-8"), {
                "part_kind": "text",
                "mime_type": "text/plain; charset=utf-8",
                "display_name": None,
            }
        if part.file_data is not None:
            return b"", {
                "part_kind": "file_data",
                "mime_type": part.file_data.mime_type,
                "display_name": part.file_data.display_name,
                "file_uri": part.file_data.file_uri,
            }
        return json.dumps(part.model_dump(exclude_none=True)).encode("utf-8"), {
            "part_kind": "json",
            "mime_type": "application/json",
            "display_name": None,
            "raw_part": part.model_dump(exclude_none=True),
        }

    @staticmethod
    def _deserialise_part(meta: dict[str, Any], payload: bytes) -> types.Part:
        kind = meta.get("part_kind")
        if kind == "inline_data":
            return types.Part(
                inline_data=types.Blob(
                    data=payload,
                    mime_type=meta.get("mime_type"),
                    display_name=meta.get("display_name"),
                )
            )
        if kind == "text":
            return types.Part(text=payload.decode("utf-8"))
        if kind == "file_data":
            return types.Part(
                file_data=types.FileData(
                    file_uri=meta.get("file_uri", ""),
                    mime_type=meta.get("mime_type"),
                    display_name=meta.get("display_name"),
                )
            )
        if kind == "json":
            return types.Part.model_validate(meta.get("raw_part") or {})
        raise ValueError(f"unknown artifact part kind: {kind}")

    # -- metadata -------------------------------------------------------
    def _meta_key(self, key: str) -> str:
        return self._sidecar(key)

    def _read_meta_sync(self, key: str) -> dict[str, Any] | None:
        raw = self._get(self._meta_key(key))
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            return None

    async def describe(self, key: str) -> dict[str, Any] | None:
        return await self._run(self._read_meta_sync, key)

    # -- capacity -------------------------------------------------------
    def _session_bytes_sync(self, app_name: str, user_id: str, session_id: str) -> int:
        base = self._base(app_name, user_id, session_id)
        total = 0
        for item in self._list_keys(f"{base}/"):
            if not str(item.key).endswith(".json"):
                total += int(getattr(item, "size", 0) or 0)
        return total

    async def session_bytes(self, app_name: str, user_id: str, session_id: str) -> int:
        if self.settings.max_session_bytes <= 0:
            return 0
        cache_key = (app_name, user_id, session_id)
        cached = self._usage.get(cache_key)
        now = self._clock()
        if cached and cached[0] > now:
            return cached[1]
        total = await self._run(self._session_bytes_sync, app_name, user_id, session_id)
        self._usage[cache_key] = (now + self._usage_ttl, total)
        return total

    def _invalidate_usage(self, app_name: str, user_id: str, session_id: str) -> None:
        self._usage.pop((app_name, user_id, session_id), None)

    def _add_usage(
        self, app_name: str, user_id: str, session_id: str, delta: int
    ) -> None:
        key = (app_name, user_id, session_id)
        cached = self._usage.get(key)
        if cached is not None:
            self._usage[key] = (cached[0], cached[1] + delta)

    async def _ensure_capacity(
        self, app_name: str, user_id: str, session_id: str, extra: int
    ) -> None:
        if self.settings.max_session_bytes <= 0:
            return
        used = await self.session_bytes(app_name, user_id, session_id)
        if used + extra > self.settings.max_session_bytes:
            raise QuotaExceeded(
                f"session artifact bytes exceed {self.settings.max_session_bytes}"
            )

    # -- version allocation ---------------------------------------------
    def _versions_sync(
        self, app_name: str, user_id: str, session_id: str, filename: str
    ) -> list[int]:
        namespace, name = self._split_adk_name(filename)
        base = self._base(app_name, user_id, session_id)
        prefix = f"{base}/{namespace}/{name}/"
        versions: list[int] = []
        for item in self._list_keys(prefix):
            key = str(item.key)
            if not key.endswith(".json"):
                continue
            match = re.search(r"/v(\d+)\.json$", key)
            if match:
                versions.append(int(match.group(1)))
        return sorted(versions)

    def _allocate_sync(
        self,
        *,
        key_builder,
        payload: bytes,
        content_type: str,
        metadata: dict[str, Any],
        versions: Sequence[int],
    ) -> int:
        version = (max(versions) if versions else 0) + 1
        for _ in range(8):
            key = key_builder(version)
            try:
                self._put(key, payload, content_type=content_type, forbid=True)
            except Exception as error:  # noqa: BLE001
                if _is_conflict(error):
                    version += 1
                    continue
                raise
            meta = dict(metadata)
            meta["version"] = version
            try:
                self._put(
                    self._sidecar(key),
                    json.dumps(meta, ensure_ascii=False, default=str).encode("utf-8"),
                    content_type="application/json",
                    forbid=True,
                )
            except Exception as error:  # noqa: BLE001
                if _is_conflict(error):
                    version += 1
                    continue
                raise
            return version
        raise ArtifactConflict("unable to allocate an artifact version")

    # -- BaseArtifactService --------------------------------------------
    async def save_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        artifact: types.Part | dict[str, Any],
        session_id: str | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> int:
        part = (
            artifact
            if isinstance(artifact, types.Part)
            else types.Part.model_validate(artifact)
        )
        payload, meta = self._serialise_part(part)
        if len(payload) > self.settings.adk_max_file_bytes:
            raise QuotaExceeded(
                f"artifact exceeds {self.settings.adk_max_file_bytes} bytes"
            )
        namespace, name = self._split_adk_name(filename)
        meta.update(
            {
                "source": "adk",
                "name": filename,
                "size": len(payload),
                "create_time": _iso_now(),
                "invocation_id": current_invocation_id.get(""),
                "custom_metadata": custom_metadata or {},
                "mime_type": meta.get("mime_type") or _mime_for(name),
            }
        )
        await self._ensure_capacity(
            app_name, user_id, session_id or "default", len(payload)
        )
        versions = await self._run(
            self._versions_sync, app_name, user_id, session_id or "default", filename
        )
        version = await self._run(
            self._allocate_sync,
            key_builder=lambda value: f"{self._base(app_name, user_id, session_id)}/{namespace}/{name}/v{value}",
            payload=payload,
            content_type=meta.get("mime_type") or "application/octet-stream",
            metadata=meta,
            versions=versions,
        )
        self._add_usage(app_name, user_id, session_id or "default", len(payload))
        return version

    async def load_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> types.Part | None:
        versions = await self.list_versions(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id
        )
        if not versions:
            return None
        chosen = version if version is not None else max(versions)
        if chosen not in versions:
            return None
        key = self.adk_key(app_name, user_id, session_id, filename, chosen)
        meta = await self.describe(key)
        if meta is None:
            return None
        payload = await self._run(self._get, key)
        if payload is None:
            return None
        return self._deserialise_part(meta, payload)

    async def list_artifact_keys(
        self, *, app_name: str, user_id: str, session_id: str | None = None
    ) -> list[str]:
        base = self._base(app_name, user_id, session_id)
        names: list[str] = []
        for namespace in ("adk", "user"):
            for item in await self._run(self._list_keys, f"{base}/{namespace}/"):
                key = str(item.key)
                if not key.endswith(".json"):
                    continue
                match = re.search(rf"/{namespace}/([^/]+)/v\d+\.json$", key)
                if match:
                    name = match.group(1)
                    names.append(f"user:{name}" if namespace == "user" else name)
        return sorted(set(names))

    async def delete_artifact(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> None:
        for version in await self.list_versions(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id
        ):
            key = self.adk_key(app_name, user_id, session_id, filename, version)
            await self._run(self._delete, key)
            await self._run(self._delete, self._sidecar(key))
        self._invalidate_usage(app_name, user_id, session_id or "default")

    async def list_versions(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[int]:
        return await self._run(
            self._versions_sync, app_name, user_id, session_id or "default", filename
        )

    async def get_artifact_version(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> ArtifactVersion | None:
        versions = await self.list_versions(
            app_name=app_name, user_id=user_id, filename=filename, session_id=session_id
        )
        if not versions:
            return None
        chosen = version if version is not None else max(versions)
        if chosen not in versions:
            return None
        key = self.adk_key(app_name, user_id, session_id, filename, chosen)
        meta = await self.describe(key)
        if meta is None:
            return None
        return ArtifactVersion(
            version=chosen,
            canonicalUri=self.tos_uri(key),
            customMetadata=meta.get("custom_metadata") or {},
            createTime=time.time(),
            mimeType=meta.get("mime_type"),
        )

    async def list_artifact_versions(
        self,
        *,
        app_name: str,
        user_id: str,
        filename: str,
        session_id: str | None = None,
    ) -> list[ArtifactVersion]:
        versions: list[ArtifactVersion] = []
        for version in await self.list_versions(
            app_name=app_name,
            user_id=user_id,
            filename=filename,
            session_id=session_id,
        ):
            item = await self.get_artifact_version(
                app_name=app_name,
                user_id=user_id,
                filename=filename,
                session_id=session_id,
                version=version,
            )
            if item is not None:
                versions.append(item)
        return versions

    # -- published -----------------------------------------------------
    async def publish_file(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        path: Path,
        filename: str | None = None,
    ) -> ArtifactItem:
        name = safe_filename(filename or path.name)
        data = await asyncio.to_thread(path.read_bytes)
        return await self.publish_bytes(
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            data=data,
            filename=name,
            mime_type=_mime_for(name),
        )

    async def publish_bytes(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        data: bytes,
        filename: str,
        mime_type: str | None = None,
    ) -> ArtifactItem:
        name = safe_filename(filename)
        if len(data) > self.settings.publish_max_file_bytes:
            raise QuotaExceeded(
                f"published artifact exceeds {self.settings.publish_max_file_bytes} bytes"
            )
        await self._ensure_capacity(app_name, user_id, session_id, len(data))
        versions = await self._run(
            self._published_versions_sync, app_name, user_id, session_id, name
        )
        meta = {
            "source": "published",
            "name": name,
            "size": len(data),
            "mime_type": mime_type or _mime_for(name),
            "create_time": _iso_now(),
            "invocation_id": current_invocation_id.get(""),
        }
        version = await self._run(
            self._allocate_sync,
            key_builder=lambda value: self.published_key(
                app_name, user_id, session_id, name, value
            ),
            payload=data,
            content_type=meta["mime_type"] or "application/octet-stream",
            metadata=meta,
            versions=versions,
        )
        self._add_usage(app_name, user_id, session_id, len(data))
        key = self.published_key(app_name, user_id, session_id, name, version)
        return ArtifactItem(
            name=name,
            key=key,
            source="published",
            version=version,
            url=await self.presign_key(key),
            mime_type=meta["mime_type"],
            size=len(data),
        )

    def _published_versions_sync(
        self, app_name: str, user_id: str, session_id: str, name: str
    ) -> list[int]:
        prefix = f"{self._base(app_name, user_id, session_id)}/published/{safe_filename(name)}/"
        versions: list[int] = []
        for item in self._list_keys(prefix):
            match = re.search(r"/v(\d+)\.json$", str(item.key))
            if match:
                versions.append(int(match.group(1)))
        return sorted(versions)

    # -- read paths -----------------------------------------------------
    async def presign_key(self, key: str) -> str:
        try:
            import tos

            method = tos.HttpMethodType.Http_Method_Get
        except Exception:  # pragma: no cover - tos is a hard dependency
            method = "GET"
        try:
            output = await self._run(
                self.client_factory().pre_signed_url,
                method,
                bucket=self.settings.tos_bucket,
                key=key,
                expires=self.settings.link_ttl_seconds,
            )
            signed = getattr(output, "signed_url", "") or ""
            return signed or self.tos_uri(key)
        except Exception:  # noqa: BLE001 - the doc requires a tos:// fallback
            logger.warning("artifact presign failed for %s", key)
            return self.tos_uri(key)

    async def list_session_artifacts(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        invocation_id: str | None = None,
        since: float | None = None,
        limit: int = 50,
    ) -> list[ArtifactItem]:
        base = self._base(app_name, user_id, session_id)
        keys = await self._run(self._list_keys, f"{base}/")
        records: dict[tuple[str, str], tuple[str, str, dict[str, Any], str]] = {}
        items: list[ArtifactItem] = []
        for entry in keys:
            key = str(entry.key)
            if not key.endswith(".json"):
                continue
            sidecar = await self._run(self._get, key)
            if sidecar is None:
                continue
            try:
                meta = json.loads(sidecar)
            except ValueError:
                continue
            if invocation_id and meta.get("invocation_id") != invocation_id:
                continue
            if since is not None:
                try:
                    created = datetime.fromisoformat(
                        str(meta.get("create_time", "")).replace("Z", "+00:00")
                    ).timestamp()
                except ValueError:
                    created = 0.0
                if created < since:
                    continue
            payload_key = key[: -len(".json")]
            source = str(meta.get("source") or "adk")
            name = str(meta.get("name") or Path(payload_key).parent.name)
            # One entry per (source, name): older versions would otherwise fill
            # the 50-item response and duplicate names in the CLI.
            identity = (source, name)
            candidate = (
                str(meta.get("create_time") or ""),
                str(meta.get("version") or ""),
                meta,
                payload_key,
            )
            current = records.get(identity)
            if current is None or candidate[:2] > current[:2]:
                records[identity] = candidate
        for _, _, meta, payload_key in sorted(
            records.values(), key=lambda item: item[:2]
        ):
            source = str(meta.get("source") or "adk")
            name = str(meta.get("name") or Path(payload_key).parent.name)
            items.append(
                ArtifactItem(
                    name=name,
                    key=payload_key,
                    source=source,
                    version=meta.get("version"),
                    url=await self.presign_key(payload_key),
                    mime_type=meta.get("mime_type"),
                    size=meta.get("size"),
                )
            )
            if len(items) >= limit:
                break
        return items

    async def items_for_entries(
        self, entries: Iterable[dict[str, Any]]
    ) -> list[ArtifactItem]:
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for entry in entries:
            identity = (str(entry.get("source") or ""), str(entry.get("name") or ""))
            current = latest.get(identity)
            if current is None or str(entry.get("version") or "") > str(
                current.get("version") or ""
            ):
                latest[identity] = entry
        items: list[ArtifactItem] = []
        for entry in latest.values():
            meta = await self.describe(entry["key"])
            items.append(
                ArtifactItem(
                    name=str(entry.get("name") or ""),
                    key=str(entry["key"]),
                    source=str(entry.get("source") or ""),
                    version=entry.get("version"),
                    url=await self.presign_key(str(entry["key"])),
                    mime_type=(meta or {}).get("mime_type", entry.get("mime_type")),
                    size=(meta or {}).get("size", entry.get("size")),
                )
            )
        return items


@dataclass
class _InvocationState:
    collector: ArtifactCollector
    marker_done: bool = False
    token: Any = None


class ArtifactPlugin(BasePlugin):
    """ADK plugin that collects artifacts and appends the short marker.

    The marker intentionally contains names and ``tos://`` URIs only: the
    modified event is persisted in session history, so presigned URLs must not
    appear there.
    """

    def __init__(
        self,
        settings: ArtifactSettings,
        service: TosArtifactService,
        *,
        collector: ArtifactCollector | None = None,
    ) -> None:
        super().__init__(name="harness_artifacts")
        self.settings = settings
        self.service = service
        self._fixed_collector = collector
        self._states: dict[str, _InvocationState] = {}
        self._active: str = ""

    def _state(self, invocation_context) -> _InvocationState:
        invocation_id = str(getattr(invocation_context, "invocation_id", "") or "")
        if len(self._states) > 256:
            # after_run normally removes the state; a disconnected run_sse
            # stream can skip it. Bound the leak instead of growing forever.
            self._states.pop(next(iter(self._states)))
        state = self._states.get(invocation_id)
        if state is None:
            state = _InvocationState(
                collector=self._fixed_collector
                if self._fixed_collector is not None
                else ArtifactCollector()
            )
            self._states[invocation_id] = state
        return state

    async def before_run_callback(self, *, invocation_context) -> None:
        state = self._state(invocation_context)
        self._active = str(getattr(invocation_context, "invocation_id", "") or "")
        state.token = current_invocation_id.set(self._active)
        return None

    async def on_event_callback(self, *, invocation_context, event):
        state = self._state(invocation_context)
        if self.settings.adk_enabled:
            delta = (
                getattr(getattr(event, "actions", None), "artifact_delta", None) or {}
            )
            for filename, version in delta.items():
                key = self.service.adk_key(
                    invocation_context.session.app_name,
                    invocation_context.session.user_id,
                    invocation_context.session.id,
                    str(filename),
                    int(version),
                )
                state.collector.add(
                    name=str(filename), source="adk", key=key, version=int(version)
                )
        published = (
            getattr(invocation_context, "_harness_published_artifacts", None) or []
        )
        if published:
            state.collector.merge(published)
        registry_items = peek_published(
            getattr(invocation_context, "invocation_id", "")
        )
        if registry_items:
            state.collector.merge(registry_items)

        final = bool(getattr(event, "is_final_response", lambda: False)())
        if final and not state.marker_done and state.collector.has_entries():
            self._append_marker(event, state)
        return None

    async def after_run_callback(self, *, invocation_context) -> None:
        self._state(invocation_context)
        invocation_id = str(getattr(invocation_context, "invocation_id", "") or "")
        state = self._states.pop(invocation_id, None)
        if state is not None:
            state.collector.merge(peek_published(invocation_id))
            if state.token is not None:
                try:
                    current_invocation_id.reset(state.token)
                except ValueError:
                    pass
        take_published(invocation_id)

    async def flush(self) -> None:
        """Flush the published registry when a stream skipped after_run."""
        for invocation_id, state in list(self._states.items()):
            state.collector.merge(peek_published(invocation_id))
            take_published(invocation_id)
            self._states.pop(invocation_id, None)

    def _append_marker(self, event, state: _InvocationState) -> None:
        content = getattr(event, "content", None)
        parts = getattr(content, "parts", None) or []
        for part in parts:
            if getattr(part, "thought", False):
                continue
            text = getattr(part, "text", None)
            if not text:
                continue
            marker = self.marker_text(state.collector)
            if marker and "```agentkit-artifacts" not in text:
                part.text = f"{text}{marker}"
                state.marker_done = True
            return

    def marker_text(self, collector: ArtifactCollector) -> str:
        lines = []
        for entry in collector.entries():
            lines.append(
                f"- {entry['name']} ({entry['source']}): {self.service.tos_uri(entry['key'])}"
            )
        if not lines:
            return ""
        return "\n\n```agentkit-artifacts\n" + "\n".join(lines) + "\n```"
