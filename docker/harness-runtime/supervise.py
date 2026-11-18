"""Supervise all enabled resident processes in Harness Runtime."""

import os
import signal
import subprocess
import time


def stop_services(processes, grace_seconds):
    """Signal whole process groups, including tools spawned by either service."""

    def send(signum):
        for process in processes:
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass

    send(signal.SIGTERM)
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        for process in processes:
            process.poll()
        alive = False
        for process in processes:
            try:
                os.killpg(process.pid, 0)
                alive = True
            except ProcessLookupError:
                pass
        if not alive:
            break
        time.sleep(0.05)
    send(signal.SIGKILL)
    for process in processes:
        process.wait()


def supervise(services, grace_seconds):
    processes = []
    stop_signal = None

    def request_stop(signum, _frame):
        nonlocal stop_signal
        stop_signal = signum

    old_handlers = {
        signum: signal.signal(signum, request_stop)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        for name, command, cwd, env in services:
            if stop_signal is not None:
                return 128 + stop_signal
            process = subprocess.Popen(
                command, cwd=cwd, env=env, start_new_session=True
            )
            processes.append(process)
            print(f"harness-runtime: started {name} pid={process.pid}", flush=True)
        while stop_signal is None:
            for (name, *_), process in zip(services, processes):
                code = process.poll()
                if code is not None:
                    print(f"harness-runtime: {name} exited code={code}", flush=True)
                    # Both services are resident: even a clean early exit is a
                    # container failure, so the platform can restart the pair.
                    return (128 - code if code < 0 else code) or 1
            time.sleep(0.05)
        return 128 + stop_signal
    finally:
        stop_services(processes, grace_seconds)
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
