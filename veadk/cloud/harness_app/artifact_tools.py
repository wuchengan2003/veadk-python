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

"""Explicit artifact hand-off for agents, skills and custom tools.

``publish_artifact`` is only injected when ``artifacts.publish.enabled`` is
true.  ``publish_bytes``/``publish_file`` are the Python-level equivalents for
custom code that already holds the content in memory.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from google.adk.tools import ToolContext

from .artifacts import (
    ArtifactItem,
    ArtifactSettings,
    QuotaExceeded,
    TosArtifactService,
    _RunQuota,
    is_sensitive_path,
    record_published,
)

logger = logging.getLogger("harness.artifacts")

_SKILLS_WORK_DIR_ENV = "VEADK_SKILLS_WORK_DIR"
_DEFAULT_SKILLS_WORK_DIR = Path("/home/gem/veadk_skills/sessions")


def _skills_outputs(session_id: str) -> Path:
    base = Path(os.getenv(_SKILLS_WORK_DIR_ENV) or _DEFAULT_SKILLS_WORK_DIR)
    return (base / session_id / "outputs").resolve()


def _scope(invocation_context) -> tuple[str, str, str]:
    session = invocation_context.session
    return (
        str(getattr(session, "app_name", "")),
        str(getattr(session, "user_id", "")),
        str(getattr(session, "id", "")),
    )


def allowed_roots(settings: ArtifactSettings, invocation_context) -> list[Path]:
    """Roots a publish call may read from for this invocation."""
    roots: list[Path] = [_skills_outputs(_scope(invocation_context)[2])]
    roots.extend(
        Path(item).expanduser().resolve() for item in settings.publish_allowed_roots
    )
    return roots


def resolve_publish_path(path: str, roots: list[Path]) -> Path:
    """Resolve and authorise a publish path, rejecting symlink escapes."""
    raw = Path(str(path)).expanduser()
    if raw.is_symlink():
        raise ValueError("publish path must not be a symlink")
    resolved = raw.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError("publish path must be a regular file")
    if not any(resolved.is_relative_to(root) for root in roots):
        raise ValueError("publish path is outside the allowed roots")
    return resolved


def _quota(invocation_context, settings: ArtifactSettings) -> _RunQuota:
    quota = getattr(invocation_context, "_harness_publish_quota", None)
    if quota is None:
        quota = _RunQuota(
            settings.publish_max_files_per_run, settings.publish_max_run_bytes
        )
        setattr(invocation_context, "_harness_publish_quota", quota)
    return quota


def _remember(invocation_context, item: ArtifactItem) -> None:
    published = list(
        getattr(invocation_context, "_harness_published_artifacts", []) or []
    )
    published.append(item)
    setattr(invocation_context, "_harness_published_artifacts", published)
    record_published(getattr(invocation_context, "invocation_id", ""), item)


def build_publish_tool(settings: ArtifactSettings, service: TosArtifactService):
    """Build the ADK tool closure; ``tool_context`` is injected by ADK."""

    async def publish_artifact(
        path: str, filename: str = "", tool_context: ToolContext = None
    ) -> dict[str, Any]:
        """Publish a generated file so the user can download it.

        Args:
            path: Absolute path of an existing file inside an allowed root
                (the current session's skills outputs or an operator-configured
                root).
            filename: Optional download name. Defaults to the file's basename.

        Returns:
            A status object with the published artifacts. On failure the
            ``status`` is ``error`` and ``error`` explains why.
        """
        context = getattr(tool_context, "_invocation_context", tool_context)
        if context is None:
            return {
                "status": "error",
                "error": "publish_artifact requires a tool context",
            }
        try:
            roots = allowed_roots(settings, context)
            resolved = resolve_publish_path(path, roots)
            relative = resolved.name if not roots else str(resolved)
            if is_sensitive_path(relative) or is_sensitive_path(resolved.name):
                raise ValueError("publish path matches a sensitive-file rule")
            size = resolved.stat().st_size
            if size > settings.publish_max_file_bytes:
                raise QuotaExceeded(
                    f"file exceeds {settings.publish_max_file_bytes} bytes"
                )
            _quota(context, settings).take(size)
            app_name, user_id, session_id = _scope(context)
            item = await service.publish_file(
                app_name=app_name,
                user_id=user_id,
                session_id=session_id,
                path=resolved,
                filename=filename or resolved.name,
            )
            _remember(context, item)
            return {
                "status": "saved",
                "artifacts": [
                    {
                        "name": item.name,
                        "url": service.tos_uri(item.key),
                        "mime_type": item.mime_type,
                        "size": item.size,
                        "source": "published",
                        "version": item.version,
                    }
                ],
            }
        except Exception as error:  # noqa: BLE001 - the model should see the reason
            logger.warning("publish_artifact rejected a path: %s", type(error).__name__)
            return {"status": "error", "error": str(error)}

    return publish_artifact


async def publish_bytes(
    service: TosArtifactService,
    *,
    app_name: str,
    user_id: str,
    session_id: str,
    data: bytes,
    filename: str,
    mime_type: str | None = None,
) -> ArtifactItem:
    """Python API for custom tools that already have the content in memory."""
    return await service.publish_bytes(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        data=data,
        filename=filename,
        mime_type=mime_type,
    )


async def publish_file(
    service: TosArtifactService,
    *,
    app_name: str,
    user_id: str,
    session_id: str,
    path: Path,
    filename: str | None = None,
) -> ArtifactItem:
    """Python API equivalent of the ``publish_artifact`` tool."""
    return await service.publish_file(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        path=path,
        filename=filename,
    )
