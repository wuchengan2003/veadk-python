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

"""Assemble a single VeADK agent for the harness server from the environment.

Decoupled from ``veadk.cloud``: this module reads only the runtime environment
and uses VeADK's public component classes for core agent assembly. AgentKit
skill-space refs are resolved by VeADK's skills loader.

**All VeADK imports are deferred** into :func:`_assemble_agent`, which runs lazily
on the first invocation (see ``server.py``) — never at import time. That keeps the
container's startup (and its ``:8000`` readiness probe) independent of which VeADK
build is installed: a missing/renamed symbol surfaces as an invoke-time error, not
a container that never boots. Tool loading is best-effort for the same reason.

Environment variables:
    MODEL_NAME              Reasoning model name. Default: VeADK default model.
    MODEL_PROVIDER          Model provider (optional)
    MODEL_API_BASE          Model API base URL (optional)
    MODEL_API_KEY           Model API key for direct mode (optional)
    SYSTEM_PROMPT           Agent instruction. Default: VeADK default instruction.
    TOOLS                   Comma-separated built-in tool names, e.g. "web_search".
    MCP_SERVERS             JSON list of remote MCP protocol/endpoint/api_key settings
    SKILLS                  Comma-separated Skill Hub slugs, skill-space ids, or
                            one skill in a space, e.g. "clawhub/a/b", "ss-xxx",
                            or "ss-xxx:s-yyy".
    RUNTIME                 Agent runtime backend: "adk" (default) or "codex".
    HARNESS_NAME            App/index name for the knowledge base and long-term
                            memory (also the served harness name). Default: "harness_app".
    KNOWLEDGEBASE_TYPE      Knowledge base backend (e.g. "viking"). Unset disables it.
    LONG_TERM_MEMORY_TYPE   Long-term memory backend (e.g. "viking"). Unset disables it.
    SHORT_TERM_MEMORY_TYPE  Short-term memory backend (e.g. "sqlite"). Default: "local".
    HARNESS_KB_CACHE_SIZE   Upper bound on cached knowledge base backends. Default: 64.
    HARNESS_SANDBOX_TOOL_ID        AgentKit Sandbox tool id. Unset disables the sandbox;
                                   requests may still supply their own ``sandbox.tool_id``.
    HARNESS_SANDBOX_TOOL_ID_ALLOWLIST  Comma-separated tool ids a request may select.
    HARNESS_SANDBOX_WORKSPACE      Fixed container working directory. Default: /home/gem.
    HARNESS_SANDBOX_TTL            Default AgentKit session TTL in seconds. Default: 86400.
"""

import copy
import io
import logging
import os
import re
import shutil
import tempfile
import threading
import zipfile
from pathlib import Path
from typing import Any, TYPE_CHECKING

from .server_types import HARNESS_APP_NAME, HarnessConfig, HarnessOverrides

if TYPE_CHECKING:
    from .skills import HarnessSkillToolset

logger = logging.getLogger("harness")

# Skill hub download endpoint. A skill name is the path after `/download/`,
# e.g. "clawhub/lgwventrue/system-file-handler".
SKILL_HUB_DOWNLOAD_URL = os.getenv(
    "SKILL_HUB_DOWNLOAD_URL", "https://skills.volces.com/v1/skills/download"
)
_LEGACY_SKILL_SPACE_PREFIX = "space:"
_SKILL_SPACE_REF_RE = re.compile(r"^(ss-[A-Za-z0-9]+)(?::(s-[A-Za-z0-9]+))?$")

# Maps HarnessConfig field names to their environment variables. `app_name` is
# populated via its "name" alias. Only variables that are set are passed.
_ENV_FIELDS = {
    "model_name": "MODEL_NAME",
    "model_provider": "MODEL_PROVIDER",
    "model_api_base": "MODEL_API_BASE",
    "model_api_key": "MODEL_API_KEY",
    "model_credential_mode": "MODEL_CREDENTIAL_MODE",
    "model_target_alias": "MODEL_TARGET_ALIAS",
    "tools": "TOOLS",
    "mcp_servers": "MCP_SERVERS",
    "skills": "SKILLS",
    "system_prompt": "SYSTEM_PROMPT",
    "description": "DESCRIPTION",
    "runtime": "RUNTIME",
    "name": "HARNESS_NAME",
    "knowledgebase_type": "KNOWLEDGEBASE_TYPE",
    "longterm_memory_type": "LONG_TERM_MEMORY_TYPE",
    "shortterm_memory_type": "SHORT_TERM_MEMORY_TYPE",
    "max_llm_calls": "MAX_LLM_CALLS",
    "structured_tool_calls": "STRUCTURED_TOOL_CALLS",
    "include_tools_every_turn": "INCLUDE_TOOLS_EVERY_TURN",
    "registry_type": "REGISTRY_TYPE",
    "registry_space_id": "REGISTRY_SPACE_ID",
    "registry_endpoint": "REGISTRY_ENDPOINT",
    "registry_region": "REGISTRY_REGION",
    "registry_top_k": "REGISTRY_TOP_K",
}


def split_csv(value: str) -> list[str]:
    """``"web_search, web_fetch"`` -> ``["web_search", "web_fetch"]``; ``""`` -> ``[]``."""
    return [item.strip() for item in value.split(",") if item.strip()]


def config_from_env() -> HarnessConfig:
    """Parse the environment into a :class:`HarnessConfig` (validated by pydantic)."""
    kwargs: dict[str, Any] = {
        field: os.environ[env]
        for field, env in _ENV_FIELDS.items()
        if env in os.environ
    }
    return HarnessConfig(**kwargs)


def _download_and_extract_skill(skill: str, dest_dir: Path) -> Path:
    """Download a skill zip from the hub and extract it into ``dest_dir``."""
    import httpx
    import frontmatter

    name = skill.strip("/")
    url = f"{SKILL_HUB_DOWNLOAD_URL.rstrip('/')}/{name}"
    logger.info(f"Downloading skill '{skill}' from {url}")
    response = httpx.get(url, timeout=60, follow_redirects=True)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to download skill '{skill}': HTTP {response.status_code}"
        )

    staging = dest_dir / f"{name.split('/')[-1]}__staging"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    staging_root = staging.resolve()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for member in zf.namelist():
            if not (staging / member).resolve().is_relative_to(staging_root):
                raise RuntimeError(f"Unsafe path in skill '{skill}' zip: {member}")
        zf.extractall(staging)

    skill_md = staging / "SKILL.md"
    if not skill_md.exists():
        skill_md = staging / "skill.md"
    if not skill_md.exists():
        raise RuntimeError(f"Skill '{skill}' has no SKILL.md")
    declared_name = frontmatter.loads(
        skill_md.read_text(encoding="utf-8")
    ).metadata.get("name")
    if not declared_name:
        raise RuntimeError(f"Skill '{skill}' SKILL.md has no 'name' in frontmatter")

    skill_dir = dest_dir / str(declared_name)
    if skill_dir.exists():
        shutil.rmtree(skill_dir)
    staging.rename(skill_dir)
    logger.info(f"Extracted skill '{skill}' (name='{declared_name}') to {skill_dir}")
    return skill_dir


def _partition_skill_references(
    skills: list[str],
) -> tuple[list[str], list[str], list[tuple[str, str]]]:
    """Split refs into Skill Hub slugs, whole spaces, and selected space skills."""
    skillhub: list[str] = []
    spaces: list[str] = []
    selected: list[tuple[str, str]] = []

    for raw in skills:
        ref = raw.strip()
        candidate = ref.removeprefix(_LEGACY_SKILL_SPACE_PREFIX).strip()
        match = _SKILL_SPACE_REF_RE.fullmatch(candidate)
        if match:
            space_id, skill_id = match.groups()
            if skill_id:
                selected.append((space_id, skill_id))
            else:
                spaces.append(space_id)
            continue
        if candidate.startswith("ss-") or ref.startswith(_LEGACY_SKILL_SPACE_PREFIX):
            raise ValueError(
                f"Invalid skill-space reference '{raw}'. Expected ss-<space-id> "
                "or ss-<space-id>:s-<skill-id>."
            )
        skillhub.append(ref)

    return skillhub, spaces, selected


def _build_skill_toolset(
    skills: list[str],
    skill_space_ids: list[str] | None = None,
    selected_space_skills: list[tuple[str, str]] | None = None,
    download_dir: Path | None = None,
) -> "HarnessSkillToolset | None":
    """加载并去重技能；空空间容错，查询或物化失败保留错误。"""
    try:
        from google.adk.skills import load_skill_from_dir
        from .skills import HarnessSkillToolset, load_space_skill_directories
    except ImportError as error:
        logger.warning("Skill extras unavailable (%s); ignoring SKILLS.", error)
        return None

    if download_dir is None:
        download_dir = Path(tempfile.mkdtemp(prefix="harness_skills_"))
    entries = []
    for skill in dict.fromkeys(ref.strip("/") for ref in skills):
        try:
            directory = _download_and_extract_skill(skill, download_dir)
            entries.append((skill, load_skill_from_dir(directory)))
        except Exception as error:
            logger.warning("Skipping skill '%s': %s", skill, error)
    for source, directory in load_space_skill_directories(
        skill_space_ids or [],
        selected_space_skills or [],
        download_dir,
    ):
        entries.append((source, load_skill_from_dir(directory)))
    if not entries:
        logger.warning("No skills loaded successfully; skipping skill toolset.")
        return None
    return HarnessSkillToolset(entries)


def _load_builtin_tools(names: list[str]) -> list:
    """Resolve built-in tools by name. Best-effort: tools the installed VeADK
    build doesn't expose are skipped with a warning rather than crashing."""
    if not names:
        return []
    try:
        from veadk.tools import get_builtin_tool
    except Exception as e:
        logger.warning(
            f"veadk.tools.get_builtin_tool unavailable ({e}); skipping TOOLS {names}."
        )
        return []
    tools = []
    for name in names:
        try:
            if name == "write_memory":
                from .memory import write_memory

                tools.append(write_memory)
            else:
                tools.append(get_builtin_tool(name))
        except Exception as e:
            logger.warning(f"Skipping tool '{name}': {e}")
    return tools


def _bounded_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be an integer") from None
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}")
    return value


def _session_database_kwargs(backend: str) -> dict[str, int]:
    if backend not in {"mysql", "postgresql"}:
        return {}
    return {
        "pool_size": _bounded_env_int("HARNESS_SESSION_DB_POOL_SIZE", 5, 1, 100),
        "max_overflow": _bounded_env_int("HARNESS_SESSION_DB_MAX_OVERFLOW", 3, 0, 100),
        "pool_timeout": _bounded_env_int(
            "HARNESS_SESSION_DB_POOL_TIMEOUT_SECONDS", 10, 1, 120
        ),
        "pool_recycle": _bounded_env_int(
            "HARNESS_SESSION_DB_POOL_RECYCLE_SECONDS", 300, 30, 3600
        ),
    }


def _assemble_agent(config: HarnessConfig, runtime_identity=None):
    """Build an agent and its short-term memory from a :class:`HarnessConfig`.

    All VeADK imports happen here (lazily), so importing this module never does.
    An empty backend string disables the knowledge base / long-term memory.
    """
    from veadk import Agent
    from veadk.memory.short_term_memory import ShortTermMemory

    tools = _load_builtin_tools(split_csv(config.tools))
    from .mcp import HarnessMcpToolset, mcp_tool_error_fallback

    tools.append(HarnessMcpToolset(config.mcp_servers))

    if config.registry_type == "agentkit_a2a":
        from veadk.a2a.registry_client import AgentKitA2ARegistryConfig
        from veadk.tools.builtin_tools.a2a_registry import build_a2a_registry_tools

        registry_config = AgentKitA2ARegistryConfig(
            space_id=config.registry_space_id,
            endpoint=config.registry_endpoint or AgentKitA2ARegistryConfig.endpoint,
            region=config.registry_region or AgentKitA2ARegistryConfig.region,
            top_k=config.registry_top_k,
        )
        tools.extend(build_a2a_registry_tools(registry_config))

    skillhub_skills, skill_space_ids, selected_space_skills = (
        _partition_skill_references(split_csv(config.skills))
    )

    if skillhub_skills or skill_space_ids or selected_space_skills:
        logger.info(
            "Loading Skill Hub/space/selected refs %s / %s / %s for harness.",
            skillhub_skills,
            skill_space_ids,
            selected_space_skills,
        )
        toolset = _build_skill_toolset(
            skillhub_skills,
            skill_space_ids=skill_space_ids,
            selected_space_skills=selected_space_skills,
        )
        if toolset is not None:
            tools.append(toolset)

    knowledgebase = None
    if config.knowledgebase_type:
        from veadk.knowledgebase import KnowledgeBase

        logger.info(
            f"Initializing knowledge base: backend={config.knowledgebase_type} index={config.app_name}"
        )
        knowledgebase = KnowledgeBase(
            backend=config.knowledgebase_type,  # type: ignore[arg-type]
            app_name=config.app_name,
        )

    long_term_memory = None
    if config.longterm_memory_type:
        from veadk.memory.long_term_memory import LongTermMemory

        logger.info(
            f"Initializing long-term memory: backend={config.longterm_memory_type} index={config.app_name}"
        )
        long_term_memory = LongTermMemory(
            backend=config.longterm_memory_type,  # type: ignore[arg-type]
            app_name=config.app_name,
        )
        from .memory import write_memory

        if not any(_tool_name(tool) == "write_memory" for tool in tools):
            tools.append(write_memory)

    logger.info(
        f"Initializing short-term memory: backend={config.shortterm_memory_type}"
    )
    short_term_memory = ShortTermMemory(
        backend=config.shortterm_memory_type,  # type: ignore[arg-type]
        db_kwargs=_session_database_kwargs(config.shortterm_memory_type),
    )

    agent_kwargs: dict[str, Any] = {
        "name": HARNESS_APP_NAME,
        "tools": tools,
        "on_tool_error_callback": mcp_tool_error_fallback,
        "skills": [],
        "runtime": config.runtime,
        "description": config.description,
        "structured_tool_calls": config.structured_tool_calls,
        "include_tools_every_turn": config.include_tools_every_turn,
        "knowledgebase": knowledgebase,
        "long_term_memory": long_term_memory,
        "short_term_memory": short_term_memory,
    }
    if config.model_credential_mode == "obo_broker":
        if config.model_api_key.get_secret_value():
            raise RuntimeError("MODEL_API_KEY is forbidden in obo_broker mode")
        token_for = (
            getattr(runtime_identity, "_token_for", None)
            if runtime_identity is not None
            else None
        )
        if not callable(token_for):
            raise RuntimeError("Managed model requires an active Runtime identity")
        if not all(
            str(value or "").strip()
            for value in (
                config.model_provider,
                config.model_name,
                config.model_api_base,
            )
        ):
            raise RuntimeError(
                "Managed model provider, name, and api_base are required"
            )
        if config.runtime != "adk":
            raise RuntimeError("Managed model currently requires runtime=adk")
        from .concurrency import ModelAdmissionController
        from .managed_model import ManagedIdentityLiteLlm

        admission = ModelAdmissionController(
            max_inflight=_bounded_env_int("HARNESS_MODEL_MAX_INFLIGHT", 12, 1, 1000),
            max_inflight_per_user=_bounded_env_int(
                "HARNESS_MODEL_MAX_INFLIGHT_PER_USER", 1, 1, 100
            ),
            queue_timeout_seconds=_bounded_env_int(
                "HARNESS_MODEL_QUEUE_TIMEOUT_SECONDS", 45, 1, 600
            ),
        )
        managed_model = ManagedIdentityLiteLlm(
            model=f"{config.model_provider}/{config.model_name}",
            runtime_identity=runtime_identity,
            target_alias=config.model_target_alias,
            api_base=config.model_api_base,
            admission=admission,
            request_timeout_seconds=_bounded_env_int(
                "HARNESS_MODEL_REQUEST_TIMEOUT_SECONDS", 120, 1, 900
            ),
        )
        from google.genai.types import GenerateContentConfig

        agent_kwargs.update(
            {
                "model": managed_model,
                "model_name": config.model_name,
                "model_provider": config.model_provider,
                "model_api_base": config.model_api_base,
                # A non-secret sentinel prevents VeADK from calling ListApiKeys /
                # GetRawApiKey during Agent initialization.
                "model_api_key": "request-obo",
                "generate_content_config": GenerateContentConfig(
                    max_output_tokens=_bounded_env_int(
                        "HARNESS_MODEL_MAX_OUTPUT_TOKENS", 1024, 128, 16_384
                    )
                ),
            }
        )
    else:
        # Unset fields keep VeADK's provider, endpoint, and IAM credential defaults
        for field in ("model_name", "model_provider", "model_api_base"):
            value = getattr(config, field)
            if field in config.model_fields_set and value:
                agent_kwargs[field] = value
        if config.model_api_key.get_secret_value():
            agent_kwargs["model_api_key"] = config.model_api_key.get_secret_value()
    if config.system_prompt:
        agent_kwargs["instruction"] = config.system_prompt

    agent = Agent(**agent_kwargs)
    if config.registry_type == "agentkit_a2a":
        setattr(agent, "_agentkit_a2a_registry_config", registry_config)
    return agent, short_term_memory


def _tool_name(tool) -> str:
    return str(getattr(tool, "name", None) or getattr(tool, "__name__", ""))


_KB_PROVIDER = None
_KB_PROVIDER_LOCK = threading.Lock()


def _knowledgebase_provider():
    """Process-wide cache of knowledge base backends selected by requests."""
    global _KB_PROVIDER
    if _KB_PROVIDER is None:
        from .knowledgebase import KnowledgeBaseProvider, cache_size_from_env

        with _KB_PROVIDER_LOCK:
            if _KB_PROVIDER is None:
                _KB_PROVIDER = KnowledgeBaseProvider(cache_size=cache_size_from_env())
    return _KB_PROVIDER


def _apply_knowledgebase_override(cloned, overrides: HarnessOverrides):
    """Point the cloned agent at the requested index/backend.

    The deployment's backend and index are the defaults; only fields present in
    the request change. Returns the lease the caller must release.
    """
    override = overrides.knowledgebase
    if override is None or not override.model_fields_set:
        return None

    base = getattr(cloned, "knowledgebase", None)
    backend = override.type
    if backend is None:
        backend = getattr(base, "backend", "") if base is not None else ""
    if not isinstance(backend, str) or not backend:
        raise ValueError(
            "knowledgebase.type is required when the deployment has no knowledge base backend"
        )
    index = override.index or (getattr(base, "index", "") if base is not None else "")
    if not index:
        raise ValueError(
            "knowledgebase.index is required when the deployment has no knowledge base index"
        )

    lease = _knowledgebase_provider().acquire(backend, str(index))
    from veadk.tools.builtin_tools.load_knowledgebase import LoadKnowledgebaseTool

    cloned.tools = [
        tool for tool in cloned.tools if _tool_name(tool) != "load_knowledgebase"
    ]
    cloned.tools.append(LoadKnowledgebaseTool(knowledgebase=lease.knowledgebase))
    cloned.knowledgebase = lease.knowledgebase
    logger.info("Knowledge base override active: backend=%s index=%s", backend, index)
    return lease


def _apply_sandbox_override(
    cloned,
    overrides: HarnessOverrides,
    *,
    user_id: str,
    session_id: str,
    agent_name: str,
):
    """Mount an AgentKit sandbox toolset on the cloned agent, if one is configured.

    The request tool id wins, then ``HARNESS_SANDBOX_TOOL_ID``. Without either,
    the agent keeps its ordinary tool set.
    """
    from . import sandbox as sandbox_module

    request = overrides.sandbox
    tool_id = (request.tool_id if request is not None else None) or (
        sandbox_module.configured_tool_id()
    )
    if not tool_id:
        return None
    if getattr(cloned, "runtime", "adk") != "adk":
        # The codex runtime ships its own workspace and native file/shell tools.
        logger.info(
            "Skipping AgentKit sandbox for runtime=%s", getattr(cloned, "runtime", "")
        )
        return None
    if not user_id or not session_id:
        raise ValueError("sandbox requires user_id and session_id")

    workspace = (
        os.getenv(sandbox_module.WORKSPACE_ENV, "").strip()
        or sandbox_module.DEFAULT_WORKSPACE
    )
    ttl = (
        request.ttl
        if request is not None and request.ttl is not None
        else _bounded_env_int(
            sandbox_module.TTL_ENV, sandbox_module.DEFAULT_TTL_SECONDS, 60, 86400
        )
    )
    environment = sandbox_module.AgentkitSandboxEnvironment(
        tool_id=tool_id,
        tool_user_session_id=f"{agent_name}_{user_id}_{session_id}",
        workspace=workspace,
        ttl=ttl,
        ready_timeout_seconds=_bounded_env_int(
            sandbox_module.READY_TIMEOUT_ENV,
            sandbox_module.DEFAULT_READY_TIMEOUT_SECONDS,
            1,
            3600,
        ),
    )
    # Stock ADK toolset: Execute keeps ADK's fixed 30s budget, which keeps the
    # tool contract identical to LocalEnvironment and forces long commands to
    # run as background processes.
    from google.adk.tools.environment import EnvironmentToolset

    toolset = EnvironmentToolset(
        environment=environment,
        max_output_chars=_bounded_env_int(
            sandbox_module.MAX_OUTPUT_CHARS_ENV,
            sandbox_module.DEFAULT_MAX_OUTPUT_CHARS,
            1000,
            1_000_000,
        ),
    )
    cloned.tools.append(toolset)
    logger.info(
        "AgentKit sandbox mounted: tool_id=%s session=%s ttl=%s",
        tool_id,
        f"{agent_name}_{user_id}_{session_id}",
        ttl,
    )
    return toolset


async def close_request_resources(run_agent) -> None:
    """Release per-request toolsets and knowledge base leases."""
    for toolset in getattr(run_agent, "_harness_request_toolsets", None) or []:
        try:
            await toolset.close()
        except Exception:  # cleanup must never mask the invocation result
            logger.warning("Failed to close request toolset", exc_info=True)
    lease = getattr(run_agent, "_harness_kb_lease", None)
    if lease is not None:
        try:
            lease.release()
        except Exception:
            logger.warning("Failed to release knowledge base lease", exc_info=True)


def spawn_harness_agent(
    base_agent,
    overrides: HarnessOverrides,
    download_dir: Path,
    *,
    user_id: str = "",
    session_id: str = "",
    agent_name: str = HARNESS_APP_NAME,
):
    """Clone the deployed agent and apply only fields set in this request."""
    set_fields = overrides.model_fields_set
    update: dict[str, Any] = {}
    if "system_prompt" in set_fields:
        update["instruction"] = overrides.system_prompt
    if "runtime" in set_fields:
        update["runtime"] = overrides.runtime
    model_fields = {"model_name", "model_provider", "model_api_base", "model_api_key"}
    for field in model_fields & set_fields:
        value = getattr(overrides, field)
        update[field] = value.get_secret_value() if field == "model_api_key" else value
    cloned = base_agent.clone(update=update)

    if "mcp_servers" in set_fields:
        from .mcp import HarnessMcpToolset

        cloned.tools = [
            tool for tool in cloned.tools if not isinstance(tool, HarnessMcpToolset)
        ]
        cloned.tools.append(HarnessMcpToolset(overrides.mcp_servers))

    if model_fields & set_fields:
        # A fresh client applies credentials to actual requests and keeps the
        # deployed client's connection state isolated from this invocation
        model_options = copy.deepcopy(cloned.model_extra_config)
        if cloned.enable_responses:
            model_options["enable_responses_cache"] = cloned.enable_responses_cache
        cloned.model = type(base_agent.model)(
            model=f"{cloned.model_provider}/{cloned.model_name}",
            api_key=cloned.model_api_key,
            api_base=cloned.model_api_base,
            **model_options,
        )

    if "tools" in set_fields:
        existing = {_tool_name(tool) for tool in cloned.tools}
        for tool in _load_builtin_tools(split_csv(overrides.tools)):
            if _tool_name(tool) not in existing:
                cloned.tools.append(tool)
                existing.add(_tool_name(tool))

    if "skills" in set_fields:
        skillhub, space_ids, selected_space_skills = _partition_skill_references(
            split_csv(overrides.skills)
        )
        if skillhub or space_ids or selected_space_skills:
            toolset = _build_skill_toolset(
                skillhub,
                skill_space_ids=space_ids,
                selected_space_skills=selected_space_skills,
                download_dir=download_dir,
            )
            if toolset is not None:
                from .skills import merge_skill_toolsets

                cloned.tools = merge_skill_toolsets(cloned.tools, toolset)

    registry_fields = {
        "registry_space_id",
        "registry_endpoint",
        "registry_region",
        "registry_top_k",
    }
    if registry_fields & set_fields:
        from dataclasses import replace

        from veadk.a2a.registry_client import AgentKitA2ARegistryConfig
        from veadk.tools.builtin_tools.a2a_registry import build_a2a_registry_tools

        config = (
            getattr(base_agent, "_agentkit_a2a_registry_config", None)
            or AgentKitA2ARegistryConfig()
        )
        updates = {
            field.removeprefix("registry_"): getattr(overrides, field)
            for field in registry_fields & set_fields
        }
        config = replace(config, **updates)
        registry_tool_names = {
            "a2a_registry_search_agent_cards",
            "a2a_registry_task_create",
            "a2a_registry_task_poll",
        }
        cloned.tools = [
            tool for tool in cloned.tools if _tool_name(tool) not in registry_tool_names
        ]
        cloned.tools.extend(build_a2a_registry_tools(config))
        setattr(cloned, "_agentkit_a2a_registry_config", config)

    lease = _apply_knowledgebase_override(cloned, overrides)
    try:
        toolset = _apply_sandbox_override(
            cloned,
            overrides,
            user_id=user_id,
            session_id=session_id,
            agent_name=agent_name,
        )
    except Exception:
        if lease is not None:
            lease.release()
        raise
    if lease is not None:
        setattr(cloned, "_harness_kb_lease", lease)
    if toolset is not None:
        setattr(cloned, "_harness_request_toolsets", [toolset])

    return cloned


def init_harness_agent(runtime_identity=None):
    """Create the agent on first use by reading the environment. Returns
    ``(agent, short_term_memory)``."""
    return _assemble_agent(config_from_env(), runtime_identity=runtime_identity)
