# Managed Agents Worker build

Private infrastructure addresses are supplied by the build environment. Optional
proxies use Docker `--build-arg HTTP_PROXY` / `--build-arg HTTPS_PROXY`. The optional
ByteAST CI helper requires `BYTEAST_API_URL` and `BYTEAST_REPORT_URL` alongside its
existing credentials; these service addresses are not embedded in the repository.

[中文版](README.zh.md)

This image runs the installed `veadk.runtime.managed_agents.worker` package and
the AgentKit health listener. The dispatcher and Session adapters ship inside
VeADK; no neighboring repository, private SDK wheel, frontend build, or cloud
deployment is required to build the image.

## Build and verify locally

From the repository root:

```bash
uv sync --extra dev --extra sandbox --extra extensions
uv run --extra dev --extra sandbox pytest tests/runtime/harness_agents tests/integrations/mpa
bash docker/managed-agents/build.sh veadk-managed-agents-worker:local
docker run --rm --entrypoint python veadk-managed-agents-worker:local \
  -m veadk.runtime.managed_agents.worker --help
```

The build script defaults to `--load`; publication requires an explicit
`--push` argument and a registry/tag selected by the operator. Building an image
does not update an AgentKit Runtime, Tool, Session, or Kubernetes Deployment.
`MANAGED_AGENTS_PLATFORM` overrides the default `linux/amd64` build platform.

## Dependency and SDK pitfalls

Older source images copied a wheel from
`examples/16_self_host_sandbox/.docker-dist/anthropic-*.whl`, built from an adjacent
SDK checkout, and imported `anthropic.lib.environments._dispatcher`. A normal
Anthropic 1.3.0 installation provides Managed Sessions and native tools but does
not provide that private dispatcher. An installed package could import successfully
yet fail to start a Worker. The new dispatcher uses generated Work APIs and the
existing account model-work polling protocol instead; the Docker build validates
its import and native-tool availability before the image is considered built.

Images default to `https://pypi.org/simple`. If an approved package mirror does
not contain every version in `uv.lock`, use a complete mirror through build args;
do not loosen pins, remove hashes, or regenerate the lock to hide missing packages:

```bash
PIP_INDEX_URL=https://packages.example.com/simple \
UV_DEFAULT_INDEX=https://packages.example.com/simple \
  bash docker/managed-agents/build.sh veadk-managed-agents-worker:local
```

Indexes are build inputs, not runtime environment variables. This helper accepts
unauthenticated mirror URLs and rejects user info, query strings and fragments
before invoking Docker. Authenticated mirrors require a separately configured
BuildKit secret integration; never put credentials in build arguments, image tags,
source, or logs. Frozen export preserves dependency hashes. The component uses
its own named, locked BuildKit cache rather than sharing an unrelated image
build's cache. This dedicated Worker image installs the sandbox and codex extras plus the
pinned Pi 0.80.6 binary. Feishu and optional data integrations use a separate
SDK installation with the extensions extra; development packages are not included.

## Backend selection

Each retrieved Session selects its own backend: top-level `session.agent_loop`,
then frozen `session.agent.agent_loop`, then `MANAGED_AGENT_BACKEND`, then `adk`.
Supported backends are `adk`, `codex` and `piagent`; invalid values fail without silently
falling back. The environment default is checked when the Worker starts.
Both continuous Worker and claimed Work modes apply the same selection.

DSH is rejected before Agent construction on every Managed entrypoint. The pinned
SDK inherits the full Worker environment and `sdk-minimal` includes a native
shell, so it cannot enforce remote sandbox isolation. DSH remains available for
standalone Python Agents only.

AKX Session creation accepts `agent_loop` as an optional SDK extension. An explicit
value overrides the Harness selection and is frozen into the Session snapshot;
when omitted, new Sessions inherit the Harness selection (default `adk`).
Existing Sessions without either field retain the environment fallback.
One Worker can concurrently serve Sessions using different backends, without a
rollout between Sessions. A Session keeps its original backend; create a new
Session to change it. Workers consuming legacy Sessions must share the same
environment default. Model, endpoint, tools and credentials remain Session-scoped.

```python
session = await client.beta.sessions.create(
    agent=agent_id,
    environment_id=environment_id,
    extra_body={"agent_loop": "codex"},
)
```

ADK continues through the remote Ark response reference. Codex and Pi restore
user/assistant messages and tool calls/results from the durable Session event
ledger on every turn, excluding the current and later queued inputs. The CLI
conversation state is not required for Worker replacement. Codex uses the existing
VeADK SDK adapter and Responses shim; Pi uses the existing RPC adapter. Each runner
disables native CLI tools through instance configuration so calls use the Session-authorized tool callbacks.
The image preinstalls the locked Codex SDK/CLI and checksum-verified Pi binary;
no CLI download occurs at runtime. A bare Python installation also needs the
`codex` extra for Codex and an installed `PIAGENT_BINARY` for Pi.

```bash
MANAGED_AGENT_BACKEND=codex python -m veadk.runtime.managed_agents.worker --managed-agent-worker
MANAGED_AGENT_BACKEND=piagent python -m veadk.runtime.managed_agents.worker --managed-agent-worker
```

## Runtime configuration

Packaged ADK Workers use Chat Completions and restore conversation history from
the configured PostgreSQL SessionStore. The image entrypoint forces
`MANAGED_AGENT_USE_RESPONSES=false`, so inherited environment settings cannot
enable Responses or `previous_response_id` continuation. Configure
`DATABASE_POSTGRESQL_*` and `MANAGED_AGENT_SESSION_STORE_REQUIRED=true` for durable
history and strict PG admission. Without PG, local ADK history lasts only for the
lifetime of its in-memory Session service. The control plane's Session/Event/Work
storage remains separate.

Skill names, descriptions and their instruction snapshot are persisted in Session
state as `managed_agent_skill_snapshot`. A matching snapshot restored from PG is
reused without another Skill API call. Missing or outdated snapshots are filled
from the pinned version metadata endpoint; the Loop does not download archives.
Skill files remain in the Sandbox and can be read there through tools.


### Native ADK history compaction

Chat Workers enable ADK `EventsCompactionConfig` on the Runner's native App.
ADK summarizes older events using the Session model, saves the compaction event
through the same SessionStore, and applies it when assembling later requests.
Original events remain stored; a replacement Worker reads and uses the saved
summary. Codex, Pi, DSH and legacy Responses calls keep their existing behavior.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `MANAGED_AGENT_COMPACTION_TOKEN_THRESHOLD` | `192000` | Native token trigger |
| `MANAGED_AGENT_COMPACTION_INTERVAL` | `10` | Fallback interval in invocations |
| `MANAGED_AGENT_COMPACTION_OVERLAP` | `2` | Previous invocations included in the next summary |
| `MANAGED_AGENT_COMPACTION_RETENTION` | `4` | Recent raw events retained for token compaction |

Harness `Model.CompactThreshold` is frozen as `agent.model.compact_threshold`
in each Session. ADK Chat runners use this positive integer (1–9007199254740991)
before `MANAGED_AGENT_COMPACTION_TOKEN_THRESHOLD`, which defaults to `192000`.
Fresh Workers reuse the frozen value. Harness updates affect new Sessions only.
This setting is not sent to the model API and does not apply to other backends or Responses. The token trigger uses ADK's latest
observed prompt usage, or its native text
estimate when usage is unavailable. It is not an exact preflight tokenizer for a
new oversized message. A single oversized user message can still exceed the
model limit.
At INFO level, `managed_compaction enabled=true` shows configuration and
`managed_session_store op=compact ... persisted=true` confirms a saved summary.
Actual input token reduction must be verified from the following model request.

The HTTP health listener defaults to port `8000` and serves `/ping`. Explicit port configuration uses `AGENTKIT_RUNTIME_PORT`, then `MA_RUNTIME_PORT`, then `PORT`. Configure the container port and health probes to match the effective listener port.

Configure gateway/credential/identity settings through the platform's secret and
environment mechanisms. A minimal worker needs `ANTHROPIC_BASE_URL`, a Runtime
Runtime API key injected as `ANTHROPIC_API_KEY` (the only accepted key source;
no Runtime Identity lookup or refresh), and its
Work scope. Account workers default to `MANAGED_AGENT_WORK_SCOPE=account` and
`MA_AGENT_LOOP_RUNTIME_TYPE=agentkit_runtime`; environment workers must set
`MANAGED_AGENT_WORK_SCOPE=environment` and `ANTHROPIC_ENVIRONMENT_ID` explicitly.
Session creation additionally requires a configured Agent ID. Session snapshots
remain authoritative for model, tools, Skills, MCP and credentials.

`MANAGED_AGENT_WORK_CONCURRENCY` defaults to 500 in the image and in
`run-runtime.sh` when absent from the process environment. Managed launchers may
supply their own environment without preserving image ENV; the entrypoint
exports the same default before starting Python. Explicit values, including 1,
remain authoritative; empty or invalid values still fail worker validation.
It counts Work
handlers, including waits, and does not enable simultaneous turns in one Session.
Direct Python invocation defaults to 1 when the variable is absent. The
`MANAGED_AGENT_WORKER_READY` log includes the effective `max_concurrency`; image
defaults and Runtime HTTP concurrency settings do not establish this value.
With `MANAGED_AGENT_POLL_TRACE=true`, `dispatcher_started`, `capacity_wait`,
`admission_started` and `admission_completed` distinguish local capacity/admission
waits from the existing `poll_started`/`poll_completed` request traces. These
records contain counts and configuration only, without credentials.
The worker ready file appears only after a valid successful Work poll, and is
removed while draining. HTTP health endpoints alone do not prove worker readiness.
The entry point supervises worker/listener together; failure of either stops both.

For an AgentKit Tool executing an already claimed Work, the image also retains
`/opt/gem/run.sh`. It selects `MANAGED_AGENT_ENTRYPOINT_MODE=claimed`, uses injected
Work/Session/lease configuration, and defaults tool execution to local inside the
Tool's isolation. The regular image command selects continuous worker mode.

The process runs as UID/GID 10001. Mount a writable `/workspace` and `/tmp` when
using a read-only filesystem. Native bash must execute within the deployment's OS
isolation; path-confined file tools do not provide a shell security sandbox.

The [Kubernetes example](k8s/worker.yaml) references an operator-created Secret;
replace the image placeholder before applying. No secret values or real deployment
identifiers are supplied. Kubernetes, AgentKit and live model acceptance require
separate deployment authorization and are not proven by local tests/builds.

## Official SDK extension strategy

The current image installs the unchanged official PyPI `anthropic==1.3.0` selected
by `uv.lock`; it does **not** install the historical adjacent custom wheel.
The locked SHA256 matches the [official release](https://pypi.org/project/anthropic/1.3.0/).
The installer validates artifact hashes, and `verify_anthropic_sdk.py` checks every
installed SDK Python file against wheel RECORD, rejecting changed, missing and
unrecorded files. RECORD checking is an integrity check, not an independent origin
proof if the wheel and its RECORD are both replaced; the lock hash supplies that boundary.

VeADK uses composition rather than subclassing or globally patching the SDK:

| Behavior | Implementation |
| --- | --- |
| environment Work poll | official `beta.environments.work.with_raw_response.poll`; inspect HTTP 204 before validating Work |
| account model Work poll | public `AsyncAnthropic.get` for `/v1/model-work/poll`, a VeADK gateway extension, not a Claude API endpoint |
| ack / heartbeat / stop and Session events | official generated SDK APIs |
| account/runtime routing headers | public `default_headers` / `extra_headers` |
| Gateway authentication | public HTTP client request hooks; use the injected key as `x-api-key` alongside Work-scoped Bearer |
| native tool execution | official `agent_toolset` factory and public `ToolError` |

The official SDK documents [raw responses, custom requests and HTTP clients](https://platform.claude.com/docs/en/cli-sdks-libraries/sdks/python#advanced-usage).
Its `EnvironmentWorker` has no arbitrary VeADK handler callback; subclassing would
require overriding private execution methods. No SDK source files or methods are
replaced. The per-Work client is a copy with inherited auth cleared; setting its
public `api_key=None` is necessary because SDK 1.3.0 `copy(api_key=None)` inherits
the parent key.

Two SDK internals remain version-sensitive: `_skills._download_and_extract` is
shipped by the official package and preserves fail-fast Skill download semantics
(the public helper skips failures); the tool decorator's `_context_manager` is
read for additive cleanup. VeADK performs cleanup locally to avoid the SDK helper
logging raw exception text. Real SDK archive, tool, auth and dispatcher tests cover
these boundaries. A locked official SDK does not require a custom SDK fork.

For a local VKE Worker with no Session Identity reference, explicitly set
`MANAGED_AGENT_MODEL_CREDENTIAL_SOURCE=environment` and supply `MODEL_AGENT_API_KEY`
through a dedicated Kubernetes Secret. The default is `session`; AgentKit and any
Session with a provider reference always require Identity and never fall back.
This choice verifies configured model credentials, not cloud Identity delivery.

See [Python runtime selection](../../docs/runtime-backends.md) for DSH configuration, Session boundaries, and real-model acceptance.

### Cancellation and execution ownership

The Cloud model worker reads the current item through the standard GetWork API
every second during execution. A `stopping` or `stopped` state cancels the model
and tool tasks and waits for cleanup. Heartbeats still renew the lease using the
server TTL. A heartbeat with `lease_extended=false` requests shutdown; 412 means
the lease was lost, so cleanup must not force-stop another holder. 409 remains
retryable under the Anthropic protocol.

When AKX returns a valid `lease_epoch`, the execution client sends
`X-MA-Work-Id` and `X-MA-Lease-Epoch`. These headers bind output to an execution;
they do not replace gateway authentication or turn a shared Environment key into
an unforgeable per-Work credential. Native Anthropic items without an epoch keep
the existing credential flow. A local cancellation guard prevents fallback model
messages, tool events, and previews after cancellation, even if a runner catches
the cancellation exception. Error-only model-span cleanup remains allowed; AKX
emits the final idle after cancellation is acknowledged.

See the [validation record](../../docs/verification/2026-11-05-managed-interrupt-cancellation.md)
for test scope and existing test limitations.
