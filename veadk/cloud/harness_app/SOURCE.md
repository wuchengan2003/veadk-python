# Harness HTTP service baseline

The runtime service in `server.py` is maintained in this repository. It was migrated
from `veadk-agent-loop`, commit `3d95681195a73b9ac38dc1ffbfbb9ab159f2c317`.
That service's original baseline was `agentkit/agentkit-cli`, commit
`b1a9e558a719a23ef8ac4210d32ea60f4db03542`, directory `harness/harness_app`.
The source repositories use Apache-2.0.

This is a source migration, not a dependency on the agentkit-cli checkout or its
container image. Agent assembly uses this repository's installed VeADK package.

## Entry point and modules

- `server.py`: AgentKit HTTP app, `/run_sse`, `/harness/invoke`, artifacts and cron wiring.
- `harness_agent.py`: environment configuration and request-scoped Agent assembly.
- `memory.py`: Harness-only `write_memory`, using the existing SDK memory backend.
- `server_types.py`: CLI-compatible request/response models; upstream `types.py`
  was renamed to avoid replacing schemas already consumed by the MA worker.
- `sandbox.py`, `skills.py`, `mcp.py`, `knowledgebase.py`: runtime integrations.
- `identity.py`, `managed_model.py`, `concurrency.py`: optional shared identity mode.
- `artifacts.py`, `artifact_wiring.py`, `artifact_tools.py`, `tos_client.py`: artifacts.
- `cronjobs/`: opt-in scheduling, storage and completion webhooks.

All upstream service Python modules were brought over, except its empty package
initializer. Relative imports now refer to `server_types`; executable module paths
refer to `veadk.cloud.harness_app.server`. Formatting follows this repository.
The local Skills session helper now honors `VEADK_SKILLS_WORK_DIR`, as expected
by the CLI baseline, while retaining this repository's default directory.
No API-key removal or other intentional protocol redesign is part of this baseline.
In particular, the CLI request model still accepts `harness.model_api_key`.

The older SDK modules (`app.py`, `agent.py`, `types.py`, `utils.py`, `env_mapping.py`,
`agentkit_resources.py`, `harness_plugins.py`, `metrics.py`) remain for existing
VeADK CLI and MA consumers. They are not the harness-runtime image's HTTP service.
Make subsequent HTTP service changes in `server.py` and its modules above.

## SDK adaptation boundary

The migrated service adapts to the target SDK. Public `Agent`, database settings,
memory classes, the global built-in tool catalog, Studio declarations and the older
SDK Harness modules retain the target implementation. The migration began at
`622e7a21e`; the feature branch was synchronized to `1763e5a5` before submission.

`harness_agent.py` mounts `write_memory` only for the migrated service when
`LONG_TERM_MEMORY_TYPE` is configured. It also resolves that tool locally for
`TOOLS` and request overrides, without registering it in `veadk.tools`.
The adapter in `memory.py` takes the current invocation's user, app and session,
then uses the SDK memory instance's existing backend `save_memory` operation.
It preserves the backend's success/failure result and existing OpenViking thread
dispatch. The SDK's memory search and automatic session saving are unchanged.

The service does not expose the source SDK/Worker's explicit `root_uri`, `store_path`
or context-binding extensions. Those source features are not added to the target
SDK's database configuration or OpenViking backend as part of this migration.

## Dependencies and validation

Install the `harness-server`, `sandbox` and `codex` extras. The runtime lock uses
AgentKit SDK 0.8.9 from the baseline's KeyAuth manifest, while retaining the target
Google ADK constraint `>=2.7.0,<2.12.0` and its locked version `2.11.0`;
VeADK itself is installed from this checkout, not from its PyPI 1.1.26 dependency.
The alternative upstream Identity wheel manifest is not imported. Optional Identity
code is retained; enabling it requires the corresponding SDK identity capabilities
and deployment configuration.

The adapted upstream tests live in `tests/cloud/cli_harness/`, including local fake
model, MCP, sandbox and storage tests. Imports use the installed package namespace;
webhook schemas are copied under that test directory. The TypeScript CLI subprocess
test `test_cronjob_local_e2e.py` is excluded because the TS CLI remains in its source
repository. No test here requires cloud model credentials or a production Work queue.

## Delivery status

This branch contains the service migration and the copied runtime build files.
The separate stage-1 checkout's two-target wheel/Studio-mock build is not included
here. The copied builder still needs an explicitly configured Studio repository.
Its Dockerfile, build-context allowlist, two-target image workflow and older
two-target examples need alignment before the formal image build can be accepted.
The Managed Agent Worker is an importable placeholder, not a running worker.

Service validation passed 473 imported scenarios and four Harness memory adapter
cases. Existing SDK and Studio catalog checks passed after restoring their source
and tests. Python 3.10 passed the four adapter cases. Five container HTTP checks
covered file write/read and continuation, explicit memory, streaming, cancellation
and Studio-mock routing using the migrated wheel installed into an existing mock
framework image. Server, Agent, Runner and tools were real; external model and
cloud services were HTTP fixtures. This does not validate this checkout's complete
Docker build or real cloud providers.

Public SDK release scripts remain unchanged. Private Studio, proxy and scan-service
addresses are supplied through explicit build/CI configuration rather than stored
in the copied files. No image publication is performed by a feature-branch push.

Before submission on 2026-11-18, the regression run on the synchronized branch
passed 9,868 tests, with 143 skips, four expected failures and six passing subtests.
All-file pre-commit checks passed. Linux runtime packing and process supervision
passed nine checks in a container; the packing tests require Linux filesystem
attribute APIs and do not pass on the macOS host.
