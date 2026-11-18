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

"""AgentKit sandbox environment mapped onto the AIO data plane."""

import asyncio
import base64
import json

import httpx
import pytest
from google.adk.environment import ExecutionResult
from google.adk.tools.environment import EnvironmentToolset

from veadk.cloud.harness_app.sandbox import (
    BASH_EXEC_ROUTE,
    DEFAULT_WORKSPACE,
    FILE_WRITE_ROUTE,
    AgentkitSandboxEnvironment,
    service_url,
)

ENDPOINT = "http://sandbox.local/session/abc?token=secret"


class RecordingTransport:
    """MockTransport handler that records requests and replays queued responses."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, httpx.Response):
            return response
        if isinstance(response, Exception):
            raise response
        return httpx.Response(200, json=response)


def make_environment(
    responses, **kwargs
) -> tuple[AgentkitSandboxEnvironment, RecordingTransport]:
    recorder = RecordingTransport(responses)
    environment = AgentkitSandboxEnvironment(
        tool_id="tool-1",
        tool_user_session_id="agent_user_session",
        endpoint=ENDPOINT,
        transport=httpx.MockTransport(recorder),
        **kwargs,
    )
    return environment, recorder


def payload(transport: RecordingTransport, index: int = 0) -> dict:
    return json.loads(transport.requests[index].content)


def test_service_url_appends_route_and_keeps_session_auth():
    assert (
        service_url("http://sandbox.local/session/abc?token=secret", "/v1/bash/exec")
        == "http://sandbox.local/session/abc/v1/bash/exec?token=secret"
    )
    assert (
        service_url("http://sandbox.local/session/abc/?token=secret", "/v1/file/read")
        == "http://sandbox.local/session/abc/v1/file/read?token=secret"
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tool_id": ""},
        {"tool_user_session_id": ""},
        {"workspace": "relative/path"},
    ],
)
def test_constructor_rejects_invalid_bindings(kwargs):
    values = {
        "tool_id": "tool-1",
        "tool_user_session_id": "session",
        "endpoint": ENDPOINT,
    }
    values.update(kwargs)

    with pytest.raises(ValueError):
        AgentkitSandboxEnvironment(**values)


def test_working_dir_is_the_fixed_workspace():
    environment, _ = make_environment([], workspace="/home/gem/project")

    assert str(environment.working_dir) == "/home/gem/project"


def test_default_workspace_matches_the_deployment_image():
    environment, _ = make_environment([])

    assert str(environment.working_dir) == DEFAULT_WORKSPACE


def test_initialize_resolves_the_endpoint_once_under_concurrency():
    calls = []

    def resolver() -> str:
        calls.append(1)
        return ENDPOINT

    environment = AgentkitSandboxEnvironment(
        tool_id="tool-1",
        tool_user_session_id="session",
        endpoint_resolver=resolver,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
    )

    async def run():
        await asyncio.gather(*[environment.initialize() for _ in range(5)])

    asyncio.run(run())

    assert len(calls) == 1


def test_initialize_skips_a_session_opened_by_a_concurrent_caller():
    import threading

    resolved: list[int] = []
    release = threading.Event()
    started = asyncio.Event()
    loop: list[asyncio.AbstractEventLoop] = []

    def resolver() -> str:
        resolved.append(1)
        loop[0].call_soon_threadsafe(started.set)
        release.wait(2)
        return ENDPOINT

    environment = AgentkitSandboxEnvironment(
        tool_id="tool-1",
        tool_user_session_id="session",
        endpoint_resolver=resolver,
    )

    async def run() -> None:
        loop.append(asyncio.get_running_loop())
        first = asyncio.create_task(environment.initialize())
        await started.wait()
        second = asyncio.create_task(environment.initialize())
        await asyncio.sleep(0.05)  # second caller now waits on the init lock
        release.set()
        await asyncio.gather(first, second)

    asyncio.run(run())

    assert resolved == [1]
    assert environment._endpoint == ENDPOINT


def test_post_requires_a_resolved_endpoint(monkeypatch):
    environment = AgentkitSandboxEnvironment(
        tool_id="tool-1",
        tool_user_session_id="session",
        endpoint_resolver=lambda: ENDPOINT,
    )

    async def noop() -> None:
        return

    monkeypatch.setattr(environment, "initialize", noop)

    with pytest.raises(RuntimeError, match="not initialized"):
        asyncio.run(environment.execute("true"))


def test_initialize_rejects_an_empty_endpoint():
    environment = AgentkitSandboxEnvironment(
        tool_id="tool-1",
        tool_user_session_id="session",
        endpoint_resolver=lambda: "",
    )

    with pytest.raises(RuntimeError, match="no session endpoint"):
        asyncio.run(environment.initialize())


def test_default_resolver_calls_veadk_session_helper(monkeypatch):
    recorded = {}

    def ensure(**kwargs):
        recorded.update(kwargs)
        return ENDPOINT

    import veadk.tools.builtin_tools._agentkit as helpers

    monkeypatch.setattr(helpers, "ensure_agentkit_session_endpoint", ensure)
    environment = AgentkitSandboxEnvironment(
        tool_id="tool-9",
        tool_user_session_id="agent_user_session",
        ttl=123,
        ready_timeout_seconds=7,
    )

    asyncio.run(environment.initialize())

    assert recorded == {
        "tool_id": "tool-9",
        "tool_user_session_id": "agent_user_session",
        "ttl": 123,
        "prefer_internal_endpoint": True,
        "wait_until_ready": True,
        "ready_timeout": 7.0,
    }


def test_execute_maps_a_completed_bash_call():
    environment, transport = make_environment(
        [
            {
                "data": {
                    "status": "completed",
                    "exit_code": 0,
                    "stdout": "hello\n",
                    "stderr": "",
                }
            }
        ]
    )

    result = asyncio.run(environment.execute("echo hello"))

    assert result == ExecutionResult(
        exit_code=0, stdout="hello\n", stderr="", timed_out=False
    )
    assert transport.requests[0].url.path.endswith(BASH_EXEC_ROUTE)
    assert payload(transport) == {
        "command": "echo hello",
        "exec_dir": "/home/gem",
        "timeout": 30,
        "hard_timeout": 30,
    }


def test_execute_honours_the_caller_timeout():
    environment, transport = make_environment(
        [{"data": {"status": "completed", "exit_code": 0}}]
    )

    asyncio.run(environment.execute("sleep 1", timeout=42))

    assert payload(transport)["timeout"] == 42
    assert payload(transport)["hard_timeout"] == 42


def test_execute_rejects_a_non_positive_timeout():
    environment, transport = make_environment(
        [{"data": {"status": "completed", "exit_code": 0}}]
    )

    asyncio.run(environment.execute("true", timeout=0))

    assert payload(transport)["timeout"] == 1


def test_execute_marks_timed_out_and_non_zero_exits():
    environment, transport = make_environment(
        [
            {
                "data": {
                    "status": "timed_out",
                    "exit_code": -1,
                    "stdout": "",
                    "stderr": "killed",
                }
            },
            {"data": {"status": "completed", "exit_code": 2, "stderr": "boom"}},
        ]
    )

    timed_out = asyncio.run(environment.execute("sleep 100"))
    failed = asyncio.run(environment.execute("false"))

    assert timed_out.timed_out is True
    assert timed_out.exit_code == -1
    assert failed.exit_code == 2
    assert failed.stderr == "boom"
    assert failed.timed_out is False


def test_execute_rejects_a_command_that_is_still_running():
    environment, _ = make_environment([{"data": {"status": "running"}}])

    with pytest.raises(RuntimeError, match="still running after 30s"):
        asyncio.run(environment.execute("npm run dev"))


def test_read_file_returns_utf8_content():
    environment, transport = make_environment(
        [{"success": True, "data": {"content": "name,value\n"}}]
    )

    content = asyncio.run(environment.read_file("report.csv"))

    assert content == b"name,value\n"
    assert payload(transport) == {"file": "/home/gem/report.csv"}


def test_read_file_maps_not_found_to_file_not_found_error():
    environment, _ = make_environment(
        [
            {
                "success": False,
                "message": "Failed to read file",
                "data": {"error_type": "not_found", "errno_name": "ENOENT"},
            }
        ]
    )

    with pytest.raises(FileNotFoundError, match="Failed to read file"):
        asyncio.run(environment.read_file("missing.txt"))


def test_read_file_maps_other_failures_to_runtime_error():
    environment, _ = make_environment(
        [
            {
                "success": False,
                "data": {"error_type": "permission_denied", "message": "denied"},
            }
        ]
    )

    with pytest.raises(RuntimeError, match="denied"):
        asyncio.run(environment.read_file("secret.txt"))


def test_write_file_sends_text_as_utf8():
    environment, transport = make_environment([{"success": True, "data": {}}])

    asyncio.run(environment.write_file("notes/todo.md", "hello"))

    assert transport.requests[0].url.path.endswith(FILE_WRITE_ROUTE)
    assert payload(transport) == {
        "file": "/home/gem/notes/todo.md",
        "content": "hello",
        "encoding": "utf-8",
        "append": False,
        "trailing_newline": False,
    }


def test_write_file_sends_bytes_as_base64():
    environment, transport = make_environment([{"success": True, "data": {}}])

    asyncio.run(environment.write_file("/home/gem/logo.png", b"\x89PNG"))

    assert payload(transport)["encoding"] == "base64"
    assert base64.b64decode(payload(transport)["content"]) == b"\x89PNG"


def test_paths_outside_the_workspace_are_rejected():
    environment, _ = make_environment([])

    with pytest.raises(ValueError, match="must stay inside"):
        asyncio.run(environment.read_file("../../etc/passwd"))
    with pytest.raises(ValueError, match="must stay inside"):
        asyncio.run(environment.read_file("/etc/passwd"))


def test_absolute_paths_inside_the_workspace_are_allowed():
    environment, transport = make_environment(
        [{"success": True, "data": {"content": ""}}]
    )

    asyncio.run(environment.read_file("/home/gem/sub/file.txt"))

    assert payload(transport) == {"file": "/home/gem/sub/file.txt"}


def test_http_errors_are_reported_with_the_route():
    environment, _ = make_environment([httpx.Response(503, text="unavailable")])

    with pytest.raises(RuntimeError, match="HTTP 503"):
        asyncio.run(environment.execute("ls"))


def test_non_json_responses_are_reported():
    environment, _ = make_environment([httpx.Response(200, text="not json")])

    with pytest.raises(RuntimeError, match="non-JSON"):
        asyncio.run(environment.execute("ls"))


def test_non_object_responses_are_reported():
    environment, _ = make_environment([httpx.Response(200, json=["unexpected"])])

    with pytest.raises(RuntimeError, match="unexpected payload"):
        asyncio.run(environment.execute("ls"))


def test_transport_errors_are_wrapped():
    environment, _ = make_environment([httpx.ConnectError("refused")])

    with pytest.raises(RuntimeError, match="request failed: ConnectError"):
        asyncio.run(environment.execute("ls"))


def test_plain_payloads_without_data_are_returned_as_is():
    environment, _ = make_environment([{"status": "completed", "exit_code": 0}])

    result = asyncio.run(environment.execute("true"))

    assert result.exit_code == 0


def test_scalar_data_is_wrapped():
    environment, transport = make_environment([{"data": "ok"}])

    result = asyncio.run(environment.execute("true"))

    assert result.stdout == ""
    assert payload(transport)["command"] == "true"


def test_close_releases_the_client_and_is_idempotent():
    environment, _ = make_environment([{"data": {"status": "completed"}}])
    asyncio.run(environment.execute("true"))
    client = environment._client
    assert client is not None

    asyncio.run(environment.close())
    asyncio.run(environment.close())

    assert environment._client is None
    assert client.is_closed


class FakeToolContext:
    def __init__(self) -> None:
        self.state = {}


def test_toolset_exposes_the_stock_adk_tools():
    environment, _ = make_environment([{"data": {"status": "completed"}}])
    toolset = EnvironmentToolset(environment=environment, max_output_chars=100)

    tools = asyncio.run(toolset.get_tools())

    assert [tool.name for tool in tools] == [
        "Execute",
        "ReadFile",
        "EditFile",
        "WriteFile",
    ]


def test_toolset_initializes_the_environment_once():
    calls = []

    def resolver() -> str:
        calls.append(1)
        return ENDPOINT

    environment = AgentkitSandboxEnvironment(
        tool_id="tool-1",
        tool_user_session_id="session",
        endpoint_resolver=resolver,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": {"status": "completed"}})
        ),
    )
    toolset = EnvironmentToolset(environment=environment)

    asyncio.run(toolset.get_tools())
    asyncio.run(toolset.get_tools())

    assert len(calls) == 1
    asyncio.run(toolset.close())
    assert environment._client is None


def test_execute_tool_keeps_adk_fixed_timeout():
    environment, transport = make_environment(
        [{"data": {"status": "completed", "exit_code": 0, "stdout": "done"}}]
    )
    toolset = EnvironmentToolset(environment=environment)

    async def run():
        tools = await toolset.get_tools()
        return await tools[0].run_async(
            args={"command": "make"}, tool_context=FakeToolContext()
        )

    result = asyncio.run(run())

    assert payload(transport)["timeout"] == 30
    assert payload(transport)["hard_timeout"] == 30
    assert result == {"status": "ok", "stdout": "done"}


def test_execute_tool_returns_environment_errors_to_the_model():
    environment, _ = make_environment([httpx.ConnectError("refused")])
    toolset = EnvironmentToolset(environment=environment)

    async def run():
        tools = await toolset.get_tools()
        return await tools[0].run_async(
            args={"command": "ls"}, tool_context=FakeToolContext()
        )

    result = asyncio.run(run())

    assert result["status"] == "error"
    assert "request failed" in result["error"]
