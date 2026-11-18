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

import asyncio
import json

from google.adk.tools import ToolContext


async def write_memory(memory: str, tool_context: ToolContext) -> str:
    """Write a durable long-term memory for the current user.

    Args:
        memory: The concise fact, preference, or note to store.
    """
    invocation_context = getattr(tool_context, "_invocation_context", None)
    agent = getattr(invocation_context, "agent", None)
    long_term_memory = getattr(agent, "long_term_memory", None)
    if long_term_memory is None:
        return "Long-term memory is not configured."

    session = getattr(invocation_context, "session", None)
    user_id = (
        getattr(session, "user_id", None)
        or getattr(session, "userId", None)
        or getattr(invocation_context, "user_id", None)
        or getattr(invocation_context, "userId", None)
        or getattr(long_term_memory, "user_id", "")
    )
    if not user_id:
        return "Unable to write memory because the current user is unknown."

    app_name = (
        getattr(session, "app_name", None)
        or getattr(session, "appName", None)
        or getattr(invocation_context, "app_name", None)
        or getattr(invocation_context, "appName", None)
        or getattr(long_term_memory, "app_name", "")
    )
    session_id = getattr(session, "id", "") or ""

    try:
        memory_text = memory.strip()
        if not memory_text:
            raise ValueError("memory text is required")

        # The SDK exposes session auto-save but no explicit text-write API.
        # Adapt its existing backend here, without extending public memory classes.
        save_call = {
            "user_id": str(user_id),
            "event_strings": [
                json.dumps(
                    {"role": "user", "parts": [{"text": memory_text}]},
                    ensure_ascii=False,
                )
            ],
            "app_name": str(app_name or ""),
            "session_id": str(session_id),
        }
        if long_term_memory._uses_openviking_backend():
            saved = await asyncio.to_thread(
                long_term_memory._backend.save_memory, **save_call
            )
        else:
            saved = long_term_memory._backend.save_memory(**save_call)
    except Exception as exc:
        return f"Failed to write memory: {type(exc).__name__}: {str(exc)[:200]}"

    return "Memory saved." if saved else "Memory was not saved."


write_memory.custom_metadata = {"source": "long_term_memory"}
