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

"""Harness 技能查询和工具集：空空间可跳过，查询失败必须保留错误。"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Literal, cast

from google.adk.skills.models import Skill as AdkSkill
from google.adk.tools.skill_toolset import SkillToolset
from veadk.skills.skill import Skill as RemoteSkill

logger = logging.getLogger("harness")


class HarnessSkillToolset(SkillToolset):
    """保留来源，供单次调用合并；不依赖 ADK 的私有技能存储。"""

    def __init__(self, entries: list[tuple[str, AdkSkill]]) -> None:
        unique: dict[str, tuple[str, AdkSkill]] = {}
        for source, skill in entries:
            previous = unique.get(skill.name)
            if previous is not None and previous[0] != source:
                raise ValueError(
                    f"Conflicting skill name '{skill.name}' from '{previous[0]}' and '{source}'."
                )
            unique[skill.name] = (source, skill)
        self.entries = tuple(unique.values())
        super().__init__(skills=[skill for _, skill in self.entries])


def merge_skill_toolsets(tools: list[Any], incoming: HarnessSkillToolset) -> list[Any]:
    """只修改副本，避免重复注册 list_skills 等同名工具。"""
    entries = [
        entry
        for tool in tools
        if isinstance(tool, HarnessSkillToolset)
        for entry in tool.entries
    ]
    merged = HarnessSkillToolset([*entries, *incoming.entries])
    return [tool for tool in tools if not isinstance(tool, HarnessSkillToolset)] + [
        merged
    ]


def _cloud_credentials() -> tuple[str, str, str]:
    """保持 VeADK 的环境变量优先、Runtime IAM 凭证回退顺序。"""
    access_key = os.getenv("VOLCENGINE_ACCESS_KEY", "")
    secret_key = os.getenv("VOLCENGINE_SECRET_KEY", "")
    if access_key and secret_key:
        return access_key, secret_key, os.getenv("VOLCENGINE_SESSION_TOKEN", "")
    from veadk.auth.veauth.utils import get_credential_from_vefaas_iam

    credential = get_credential_from_vefaas_iam()
    return (
        credential.access_key_id,
        credential.secret_access_key,
        credential.session_token,
    )


def _request_space(space_id: str) -> Any:
    """使用有超时且不共享签名全局变量的公开签名函数。"""
    from veadk.utils.volcengine_sign import volcengine_signed_request

    service = os.getenv("AGENTKIT_TOOL_SERVICE_CODE", "agentkit")
    byteplus = os.getenv("CLOUD_PROVIDER", "").lower() == "byteplus"
    region = os.getenv(
        "AGENTKIT_TOOL_REGION", "ap-southeast-1" if byteplus else "cn-beijing"
    )
    domain = "byteplusapi.com" if byteplus else "volcengineapi.com"
    host = os.getenv("AGENTKIT_SKILL_HOST", f"{service}.{region}.{domain}")
    scheme = os.getenv("AGENTKIT_TOP_SCHEME", "https").lower()
    if scheme not in {"http", "https"}:
        raise ValueError("AGENTKIT_TOP_SCHEME must be http or https")
    access_key, secret_key, token = _cloud_credentials()
    return volcengine_signed_request(
        request_body={"SkillSpaceId": space_id, "InnerTags": {"source": "sandbox"}},
        ak=access_key,
        sk=secret_key,
        service=service,
        region=region,
        host=host,
        path="/",
        query={"Action": "ListSkillsBySpaceId", "Version": "2025-10-30"},
        header={"X-Security-Token": token},
        scheme=cast(Literal["http", "https"], scheme),
        timeout=(5, 30),
    )


def _space_items(response: Any) -> list[dict[str, Any]]:
    """成功的空 Items 才代表空空间，错误响应和非法结构不能降级为空。"""
    if not isinstance(response, dict):
        raise ValueError("ListSkillsBySpaceId returned a non-object response")
    metadata = response.get("ResponseMetadata") or {}
    if metadata.get("Error"):
        error = metadata["Error"]
        raise RuntimeError(
            f"ListSkillsBySpaceId failed: {error.get('Code', 'Unknown')} "
            f"(request_id={metadata.get('RequestId', 'unknown')})"
        )
    result = response.get("Result", response)
    if not isinstance(result, dict) or "Items" not in result:
        raise ValueError("ListSkillsBySpaceId response is missing Items")
    items = result["Items"]
    if not isinstance(items, list) or not all(
        isinstance(item, dict) and item.get("Name") for item in items
    ):
        raise ValueError("ListSkillsBySpaceId returned invalid Items")
    return items


def load_skill_space(space_id: str) -> list[RemoteSkill]:
    """返回有效技能或空列表，所有查询失败均带操作名和空间上下文抛出。"""
    try:
        items = _space_items(_request_space(space_id))
        return [
            RemoteSkill(
                name=item["Name"],
                description=item.get("Description") or "",
                path=item.get("TosPath") or "",
                bucket_name=item.get("BucketName"),
                id=item.get("SkillId"),
                skill_space_id=space_id,
            )
            for item in items
        ]
    except Exception as error:
        raise RuntimeError(
            f"ListSkillsBySpaceId for '{space_id}' failed: {error}"
        ) from error


def load_space_skill_directories(
    spaces: list[str],
    selected: list[tuple[str, str]],
    directory: Path,
) -> list[tuple[str, Path]]:
    """同一空间只查询一次；显式指定的技能缺失仍然报错。"""
    from veadk.skills.materializer import materialize_remote_skill

    cache: dict[str, list[RemoteSkill]] = {}
    loaded: dict[str, Path] = {}
    refs = [(space, None) for space in spaces] + selected
    for space_id, skill_id in refs:
        if space_id not in cache:
            cache[space_id] = load_skill_space(space_id)
        matches = [
            skill
            for skill in cache[space_id]
            if skill_id is None or skill.id == skill_id
        ]
        if not matches:
            if skill_id is not None:
                raise RuntimeError(
                    f"Skill '{skill_id}' was not found in skill space '{space_id}'."
                )
            logger.warning(
                "No skills found in skill space '%s'; skipping empty space.", space_id
            )
            continue
        for skill in matches:
            source = f"{space_id}:{skill.id or skill.name}"
            if source not in loaded:
                try:
                    loaded[source] = materialize_remote_skill(
                        skill, cache_dir=directory
                    )
                except Exception as error:
                    raise RuntimeError(
                        f"Failed to load skill-space ref '{source}': {error}"
                    ) from error
    return list(loaded.items())
