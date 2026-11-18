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

"""覆盖技能空间为空、重复引用和单次调用隔离的回归场景。"""

from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from google.adk.agents import LlmAgent

from veadk.cloud.harness_app.harness_agent import (
    _build_skill_toolset,
    spawn_harness_agent,
)
from veadk.cloud.harness_app.server_types import HarnessOverrides


@pytest.fixture
def local_skill(tmp_path: Path) -> Path:
    directory = tmp_path / "example-skill"
    directory.mkdir()
    (directory / "SKILL.md").write_text(
        "---\nname: example-skill\ndescription: Regression fixture\n---\nFixture body.\n"
    )
    return directory


@pytest.fixture
def remote_query(monkeypatch: pytest.MonkeyPatch, local_skill: Path) -> Mock:
    # 网络和凭证获取是替身，保留真实解析、物化结果加载与工具集构造。
    from veadk.skills import utils
    from veadk.skills import materializer
    from veadk.utils import volcengine_sign

    query = Mock(return_value={"Result": {"Items": []}})
    monkeypatch.setattr(utils, "ve_request", query)
    monkeypatch.setattr(volcengine_sign, "volcengine_signed_request", query)
    monkeypatch.setenv("VOLCENGINE_ACCESS_KEY", uuid4().hex)
    monkeypatch.setenv("VOLCENGINE_SECRET_KEY", uuid4().hex)
    monkeypatch.setattr(utils, "_get_cloud_credentials", lambda: ("test", "test", ""))
    monkeypatch.setattr(
        materializer, "materialize_remote_skill", lambda *args, **kwargs: local_skill
    )
    return query


def skill_item() -> dict[str, str]:
    return {
        "Name": "example-skill",
        "Description": "Fixture",
        "SkillId": "s-example",
        "BucketName": "test",
        "TosPath": "example.zip",
    }


def test_empty_space_does_not_abort_override(
    remote_query: Mock, tmp_path: Path
) -> None:
    base = LlmAgent(name="fixture", model="unused")
    result = spawn_harness_agent(base, HarnessOverrides(skills="ss-empty"), tmp_path)
    assert result is not base
    assert result.tools == base.tools == []


def test_empty_space_preserves_other_valid_skills(
    remote_query: Mock,
    local_skill: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "veadk.cloud.harness_app.harness_agent._download_and_extract_skill",
        lambda *args: local_skill,
    )
    result = _build_skill_toolset(
        ["clawhub/example/skill"], ["ss-empty"], download_dir=tmp_path
    )
    assert result is not None


@pytest.mark.parametrize(
    "payload",
    [
        {"ResponseMetadata": {"Error": {"Code": "AccessDenied", "Message": "denied"}}},
        {"Result": {"Items": "invalid"}},
    ],
)
def test_query_failure_is_not_reported_as_empty(
    remote_query: Mock, tmp_path: Path, payload: dict
) -> None:
    remote_query.return_value = payload
    with pytest.raises(RuntimeError) as error:
        _build_skill_toolset([], ["ss-example"], download_dir=tmp_path)
    assert "No skills found" not in str(error.value)
    assert "ListSkillsBySpaceId" in str(error.value)


def test_duplicate_space_and_selected_skill_load_once(
    remote_query: Mock, tmp_path: Path
) -> None:
    remote_query.return_value = {"Result": {"Items": [skill_item()]}}
    result = _build_skill_toolset(
        [], ["ss-example"], [("ss-example", "s-example")], tmp_path
    )
    assert result is not None
    assert remote_query.call_count == 1


def test_repeated_hub_reference_loads_once(
    local_skill: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    download = Mock(return_value=local_skill)
    monkeypatch.setattr(
        "veadk.cloud.harness_app.harness_agent._download_and_extract_skill", download
    )
    result = _build_skill_toolset(["clawhub/example/skill"] * 2, download_dir=tmp_path)
    assert result is not None
    assert download.call_count == 1


def test_override_merges_into_one_toolset_without_mutating_base(
    local_skill: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "veadk.cloud.harness_app.harness_agent._download_and_extract_skill",
        lambda *args: local_skill,
    )
    original = _build_skill_toolset(["clawhub/example/skill"], download_dir=tmp_path)
    base = LlmAgent(name="fixture", model="unused", tools=[original])
    result = spawn_harness_agent(
        base, HarnessOverrides(skills="clawhub/example/skill"), tmp_path
    )
    assert len(result.tools) == 1
    assert len(base.tools) == 1
    assert result.tools[0] is not base.tools[0]
