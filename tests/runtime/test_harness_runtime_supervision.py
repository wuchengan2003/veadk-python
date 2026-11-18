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

"""Exercise Harness Runtime Server/proxy processes and signals."""

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="Runtime supervision uses Linux process groups; exercised in the Linux image gate",
)

ROOT = Path(__file__).resolve().parents[2]
SUPERVISOR = ROOT / "docker/harness-runtime/supervise.py"


def wait_for(predicate, process, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        assert process.poll() is None, process.communicate()
        time.sleep(0.02)
    raise AssertionError("Timed out waiting for child processes")


@pytest.mark.parametrize(
    "stop_role,exit_code,signum",
    [
        ("app", 23, None),
        ("proxy", 23, None),
        ("proxy", 0, None),
        (None, None, signal.SIGTERM),
        (None, None, signal.SIGINT),
    ],
)
def test_runtime_stops_siblings_and_forwards_signals(
    tmp_path, stop_role, exit_code, signum
):
    child = tmp_path / "child.py"
    child.write_text(
        "import os,signal,sys,time\nfrom pathlib import Path\n"
        "root=Path(sys.argv[1]); role=sys.argv[2]\n"
        "def stop(sig,frame):\n"
        " (root/(role+'.stopped')).write_text(str(sig))\n"
        " sys.exit(0)\n"
        "signal.signal(signal.SIGTERM,stop)\n"
        "(root/(role+'.pid')).write_text(str(os.getpid()))\n"
        "while not (root/(role+'.exit')).exists(): time.sleep(0.01)\n"
        "sys.exit(int((root/(role+'.exit')).read_text()))\n"
    )
    services = [
        (role, [sys.executable, str(child), str(tmp_path), role], str(tmp_path), {})
        for role in ("app", "proxy")
    ]
    launcher = (
        "import importlib.util,json,sys; "
        "spec=importlib.util.spec_from_file_location('supervisor',sys.argv[1]); "
        "module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); "
        "sys.exit(module.supervise(json.loads(sys.argv[2]),0.5))"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", launcher, str(SUPERVISOR), json.dumps(services)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(
            lambda: all(
                (tmp_path / f"{role}.pid").exists() for role in ("app", "proxy")
            ),
            process,
        )
        if signum is not None:
            process.send_signal(signum)
            expected = 128 + signum
        else:
            (tmp_path / f"{stop_role}.exit").write_text(str(exit_code))
            expected = exit_code or 1
        assert process.wait(timeout=5) == expected, process.communicate()
        for role in ("app", "proxy"):
            with pytest.raises(ProcessLookupError):
                os.kill(int((tmp_path / f"{role}.pid").read_text()), 0)
            if role != stop_role:
                assert (tmp_path / f"{role}.stopped").read_text() == str(signal.SIGTERM)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        for role in ("app", "proxy"):
            pid_file = tmp_path / f"{role}.pid"
            if pid_file.exists():
                try:
                    os.killpg(int(pid_file.read_text()), signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_supervisor_kills_uncooperative_service_and_cleans_up_failed_start(tmp_path):
    spec = importlib.util.spec_from_file_location("runtime_supervisor", SUPERVISOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(60)",
        ],
        start_new_session=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        start = time.monotonic()
        module.stop_services([process], 0.2)
        assert time.monotonic() - start < 2
        assert process.returncode == -signal.SIGKILL
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()

    # A missing second executable must also clean up the first service.
    started = []
    real_popen = module.subprocess.Popen

    def record(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        started.append(process)
        return process

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(module.subprocess, "Popen", record)
        with pytest.raises(FileNotFoundError):
            module.supervise(
                [
                    (
                        "app",
                        [sys.executable, "-c", "import time; time.sleep(60)"],
                        str(tmp_path),
                        {},
                    ),
                    ("proxy", [str(tmp_path / "missing")], str(tmp_path), {}),
                ],
                0.2,
            )
    assert len(started) == 1
    assert started[0].poll() is not None
