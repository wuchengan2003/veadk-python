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

"""Opt-in real Nginx + ASGI transport checks; no model or Work requests."""

import importlib.util
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("runtime_launch", HERE / "launch.py")
launch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launch)

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_NGINX_SMOKE") != "1",
    reason="Set RUN_NGINX_SMOKE=1 with Nginx installed",
)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def proxy(tmp_path_factory):
    import httpx

    directory = tmp_path_factory.mktemp("proxy")
    app_file = directory / "fixture_app.py"
    app_file.write_text("""
import asyncio
import os
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import StreamingResponse
app = FastAPI()
@app.api_route('/echo', methods=['GET', 'POST'])
async def echo(request: Request):
    return {'path': request.url.path, 'root': request.scope['root_path'],
            'query': request.url.query, 'body': (await request.body()).decode(),
            'auth': request.headers.get('authorization'), 'host': request.headers['host'],
            'backend': os.environ['TEST_BACKEND'],
            'target': request.headers.get('x-agentkit-target'),
            'affinity': request.headers.get('x-session-id')}
@app.get('/slash/')
async def slash(): return {'ok': True}
@app.get('/events')
async def events():
    async def stream():
        yield 'data: first\\n\\n'
        await asyncio.sleep(2)
        yield 'data: last\\n\\n'
    return StreamingResponse(stream(), media_type='text/event-stream')
@app.websocket('/ws')
async def ws(socket: WebSocket):
    await socket.accept()
    await socket.send_text(await socket.receive_text())
    await socket.close()
""")
    port, ma_port, studio_port = free_port(), free_port(), free_port()
    rendered = directory / "rendered-nginx.conf"
    launch.render_nginx_config(
        HERE / "nginx.conf", rendered, {"harness": True, "studio": True}
    )
    config = rendered.read_text()
    config = config.replace("listen 8000;", f"listen {port};")
    config = config.replace("127.0.0.1:8001", f"127.0.0.1:{ma_port}")
    config = config.replace("127.0.0.1:8002", f"127.0.0.1:{studio_port}")
    config = config.replace("/tmp/agentkit-studio-", str(directory / "nginx-"))
    conf = directory / "nginx.conf"
    conf.write_text(config)
    log = (directory / "process.log").open("w+")
    processes = []
    try:
        for backend_port, backend in ((ma_port, "default"), (studio_port, "studio")):
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "uvicorn",
                        "fixture_app:app",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(backend_port),
                        "--no-access-log",
                    ],
                    cwd=directory,
                    env=dict(os.environ, TEST_BACKEND=backend),
                    stdout=log,
                    stderr=log,
                )
            )
        processes.append(
            subprocess.Popen(
                [
                    shutil.which("nginx") or "/usr/sbin/nginx",
                    "-c",
                    str(conf),
                    "-g",
                    "daemon off;",
                ],
                stdout=log,
                stderr=log,
            )
        )
        base = f"http://127.0.0.1:{port}"
        with httpx.Client(trust_env=False) as client:
            deadline = time.monotonic() + 20
            while True:
                try:
                    if all(
                        client.get(base + "/echo", headers=headers).status_code == 200
                        for headers in ({}, {"X-AgentKit-Target": "studio"})
                    ):
                        break
                except httpx.TransportError:
                    pass
                if (
                    any(p.poll() is not None for p in processes)
                    or time.monotonic() > deadline
                ):
                    log.seek(0)
                    pytest.fail(log.read())
                time.sleep(0.05)
            yield client, base
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        log.close()


@pytest.mark.parametrize(
    "target,backend", [(None, "default"), ("studio", "studio"), ("unknown", "default")]
)
def test_routes_body_query_and_auth(proxy, target, backend):
    client, base = proxy
    headers = {"Authorization": "Bearer fixture", "X-Session-Id": "affinity-fixture"}
    if target is not None:
        headers["X-AgentKit-Target"] = target
    response = client.post(base + "/echo?q=a%2Fb", content="payload", headers=headers)
    assert response.status_code == 200
    assert response.json() == {
        "path": "/echo",
        "root": "",
        "query": "q=a%2Fb",
        "body": "payload",
        "auth": "Bearer fixture",
        "host": base.removeprefix("http://"),
        "backend": backend,
        "target": target,
        "affinity": "affinity-fixture",
    }


def test_redirects_and_schema_use_root_paths(proxy):
    client, base = proxy
    headers = {"X-AgentKit-Target": "studio", "X-Forwarded-Proto": "https"}
    response = client.get(base + "/slash", headers=headers)
    assert response.headers["location"] == base.replace("http:", "https:") + "/slash/"
    assert not client.get(base + "/openapi.json", headers=headers).json().get("servers")
    assert client.get(base + "/studio/echo", headers=headers).status_code == 404


def test_sse_arrives_before_response_finishes(proxy):
    client, base = proxy
    for headers in ({}, {"X-AgentKit-Target": "studio"}):
        start = time.monotonic()
        with client.stream("GET", base + "/events", headers=headers) as response:
            lines = response.iter_lines()
            assert next(lines) == "data: first"
            assert time.monotonic() - start < 1.5
            assert "data: last" in list(lines)


def test_websocket_upgrade(proxy):
    from websockets.sync.client import connect

    _, base = proxy
    for headers in ({}, {"X-AgentKit-Target": "studio"}):
        with connect(
            base.replace("http:", "ws:") + "/ws", additional_headers=headers, proxy=None
        ) as socket:
            socket.send("echo")
            assert socket.recv(timeout=5) == "echo"
