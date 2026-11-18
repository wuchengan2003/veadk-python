"""Emit fixed shell exports before Python sitecustomize initializes OTel.

Run with python -S. No application imports, environment values, endpoint URLs,
or credentials are emitted. Explicit non-OTLP exporters remain untouched.
"""

import os
import socket
import time
from urllib.parse import urlsplit


def collector_reachable(endpoint):
    for attempt in range(3):
        try:
            with socket.create_connection(endpoint, timeout=0.25):
                return True
        except OSError:
            if attempt < 2:
                time.sleep(0.1)
    return False


def disabled_exporters(env):
    disabled = []
    collectors = {}
    for signal in ("TRACES", "METRICS", "LOGS"):
        key = f"OTEL_{signal}_EXPORTER"
        selection = env.get(key, "").strip()
        endpoint = env.get(f"OTEL_EXPORTER_OTLP_{signal}_ENDPOINT") or env.get(
            "OTEL_EXPORTER_OTLP_ENDPOINT"
        )
        if not selection and not endpoint:
            disabled.append(key)
            continue
        # Mixed or custom explicit exporters belong to the caller.
        if selection not in ("", "otlp", "otlp_proto_http", "otlp_proto_grpc"):
            continue
        protocol = env.get(f"OTEL_EXPORTER_OTLP_{signal}_PROTOCOL") or env.get(
            "OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"
        )
        endpoint = endpoint or (
            "http://localhost:4318"
            if protocol == "http/protobuf" or selection == "otlp_proto_http"
            else "http://localhost:4317"
        )
        try:
            parsed = urlsplit(endpoint if "://" in endpoint else "//" + endpoint)
            if parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
                continue
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            address = parsed.hostname, port
            if address not in collectors:
                collectors[address] = collector_reachable(address)
            if not collectors[address]:
                disabled.append(key)
        except ValueError:
            # Invalid explicit config should retain its normal validation error.
            continue
    return disabled


if __name__ == "__main__":
    for key in disabled_exporters(os.environ):
        print(f"export {key}=none")
