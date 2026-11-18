# Harness Runtime

The public checkout does not embed a private Studio repository URL. Supply
`--studio-repository` when invoking `build.py` or `prepare_studio.py`, or configure
`studio.repository` in a local `sources.json` before building.

`docker/harness-runtime/Dockerfile` is the only Harness Runtime image recipe. It
contains the repository-owned Harness App, the Managed Agent Worker, Studio
Harness, Headroom, PiAgent and Nginx. Runtime switches select which components
start; separate two-in-one and three-in-one images are not maintained.

```text
port 8000 -> Nginx -> default                 Harness App :8001
                   -> X-AgentKit-Target:studio Studio      :8002

optional MA Worker -> Harness Infra Work polling (outbound, no listening port)
```

The standalone Worker image under `docker/managed-agents/` is a separate product
and release path.

## Source ownership and build

- Harness App and MA Worker come from the current `veadk-agent-loop` commit and
  share `/opt/veadk/.venv`.
- Studio Harness is fetched from the repository/ref configured in `sources.json`.
  The ref is resolved once to an exact commit for each build, recorded in image
  metadata, installed in `/opt/studio/.venv`, and run as UID/GID 10001.
- Studio and Headroom use the pinned Python 3.12.12 installation under
  `/opt/studio-python`.
- The pinned `aks-ma-runtime` image is used only to extract the PiAgent archive;
  it does not supply Harness, Studio or the MA Worker.

The build helper requires Python 3.12, Git access to Studio, Docker Buildx and
access to the configured registries. Run it from a clean, committed checkout:

```bash
python3.12 docker/harness-runtime/build.py \
  --python-image 'ghcr.io/astral-sh/uv:python3.12-bookworm-slim@sha256:<digest>' \
  --tag harness-runtime:local
```

`--studio-checkout /path/to/agentkit-studio` may reuse local Git objects without
changing that checkout. The configured remote is still fetched, so stale local
objects are never used silently. `--studio-repository` and `--studio-ref` override
the configured source; a branch, tag or full commit can be selected.
`--source-metadata-file /path/sources.json` records the resolved Runtime and Studio
commits and build inputs. `--print` prints the resolved `docker buildx build`
command without executing it. `--output-tarball /path/image.tar` writes a Docker
archive instead of loading the image locally.

`prepare_studio.py` is the Python 3.9-compatible CI preparation step. It fetches
and resolves the configured Studio ref, then safely exports only
`servers/harness_app_server` from that exact commit into the ignored
`.studio-source/` build directory. The resolved repository, ref and commit are
attached to the final image. The Dockerfile then builds all runtime stages in one
graph.

## Component switches

All values are case-insensitive `true` or `false` with surrounding whitespace
ignored. Empty or other values fail startup.

| Variable | Default | Component |
| --- | --- | --- |
| `HARNESS_APP_ENABLED` | `true` | Repository-owned Harness App |
| `STUDIO_ENABLED` | `true` | Studio Harness |
| `MANAGED_AGENT_WORKER_ENABLED` | `false` | Managed Agent Work consumer |

At least one component must be enabled. Supported combinations include
Harness-only, Studio-only, MA-only, Harness + MA, Studio + MA, Harness + Studio,
and all three components.

Nginx starts whenever either HTTP service is enabled. A request for a disabled
HTTP service returns HTTP 503 instead of reaching an absent upstream:

- Missing or non-`studio` `X-AgentKit-Target` selects Harness App.
- `X-AgentKit-Target: studio` selects Studio Harness.

Nginx preserves root API paths, request bodies, query strings, authorization,
streaming and WebSocket upgrades. It disables buffering and upstream retries.

## Startup and health

The complete command is stored in image `Config.Cmd`:

```text
tini -> launch.py -> enabled services -> supervise.py
```

Any enabled resident process exiting stops all other enabled processes. SIGTERM
and SIGINT stop every process group, including child tools. Shutdown grace is
controlled by `DUAL_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS` and defaults to 30 seconds.

The Docker healthcheck validates every enabled component:

- Harness: `GET /health` through Nginx.
- Studio: `GET /ping` through Nginx with `X-AgentKit-Target: studio`.
- MA Worker: the configured `MANAGED_AGENT_READY_FILE`, defaulting to
  `/tmp/managed-agent-worker-ready`.

When enabling the MA Worker, supply its credentials, runtime identity and Work
configuration as documented in `docker/managed-agents/README.md`. Studio storage
configuration remains independent. The presence of credentials never enables a
component automatically.

## Pipeline and versioning

`.codebase/pipelines/harness-runtime.yaml` is the single Harness Runtime release
pipeline. It:

1. Resolves the image version.
2. Fetches the configured Studio ref and exports the resolved commit.
3. Builds this Dockerfile for linux/amd64 and uploads a Docker archive.
4. Pushes `zn-datapath-cn-beijing.cr.volces.com/sandbox/veadk-harness-runtime:<tag>`.
5. Verifies the registry manifest and scans the final image with ByteAST.

`VERSION` controls releases from `main`; non-main builds use the full Git commit
SHA. The pipeline does not deploy the image or update a mutable `latest` tag.

## Tests

```bash
pytest docker/harness-runtime/test_runtime.py \
  docker/harness-runtime/test_pack_runtime.py \
  tests/runtime/harness_agents/test_runtime_supervision.py

RUN_NGINX_SMOKE=1 pytest docker/harness-runtime/test_proxy.py
```

The offline image build also validates the installed repository-owned Harness
startup, health, discovery, session and invocation routes without invoking a
model or consuming Work.
