#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${MANAGED_AGENTS_PYTHON:-python}"
case "${MANAGED_AGENT_HEALTH_ENABLED:-true}" in
    true|false) ;;
    *) echo "MANAGED_AGENT_HEALTH_ENABLED must be true or false" >&2; exit 2 ;;
esac
# -S prevents injected sitecustomize from creating exporters before we can
# validate the local collector. Output contains fixed export statements only.
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if ! telemetry_exports="$("$PYTHON_BIN" -S "$script_dir/configure-telemetry.py" 2>/dev/null)"; then
    telemetry_exports=""
    echo "Managed telemetry filter failed; keeping the original exporter configuration" >&2
fi
eval "$telemetry_exports"
listener_pid=""
worker_pid=""
cleanup() {
    trap - EXIT INT TERM
    for child_pid in "$listener_pid" "$worker_pid"; do
        if [[ -n "$child_pid" ]]; then
            kill "$child_pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export MANAGED_AGENT_WORK_SCOPE="${MANAGED_AGENT_WORK_SCOPE:-account}"
export MA_AGENT_LOOP_RUNTIME_TYPE="${MA_AGENT_LOOP_RUNTIME_TYPE:-agentkit_runtime}"
export MANAGED_AGENT_TOOL_EXECUTION="${MANAGED_AGENT_TOOL_EXECUTION:-remote}"
# Packaged Workers restore conversation history from the SessionStore. Never
# switch them back to model-side Responses context through inherited env vars.
export MANAGED_AGENT_USE_RESPONSES=false
# Some managed launchers supply process environments without Docker image ENV.
# Keep the image's worker capacity at the executable entrypoint as well. Use
# '-' (not ':-') so explicit empty/invalid values still fail worker validation.
export MANAGED_AGENT_WORK_CONCURRENCY="${MANAGED_AGENT_WORK_CONCURRENCY-500}"

case "${MANAGED_AGENT_ENTRYPOINT_MODE:-worker}" in
    worker) mode="--managed-agent-worker" ;;
    claimed) mode="--managed-agent-work-item" ;;
    *) echo "MANAGED_AGENT_ENTRYPOINT_MODE must be worker or claimed" >&2; exit 2 ;;
esac

# Clear the prior process's readiness before the listener can accept probes.
if [[ -e "${MANAGED_AGENT_READY_FILE:-/tmp/managed-agent-worker-ready}" ]]; then
    unlink -- "${MANAGED_AGENT_READY_FILE:-/tmp/managed-agent-worker-ready}"
fi
if [[ "${MANAGED_AGENT_HEALTH_ENABLED:-true}" == false ]]; then
    exec "$PYTHON_BIN" -m veadk.runtime.managed_agents.worker "$mode" "$@"
fi
"$PYTHON_BIN" -m veadk.runtime.managed_agents.health &
listener_pid=$!
"$PYTHON_BIN" -m veadk.runtime.managed_agents.worker "$mode" "$@" &
worker_pid=$!

# Failure or completion of either process must stop its sibling.
wait -n "$listener_pid" "$worker_pid"
