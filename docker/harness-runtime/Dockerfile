ARG PYTHON_IMAGE=python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE} AS veadk-build

ARG UV_DEFAULT_INDEX=https://pypi.org/simple
ARG VENV_COMPILE_BYTECODE=0
WORKDIR /opt/veadk
COPY --from=ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa /uv /opt/build-tools/uv
COPY --chmod=0644 pyproject.toml uv.lock README.md LICENSE ./
COPY veadk ./veadk

# Harness App and the MA Worker intentionally share the repository-owned venv.
RUN --mount=type=cache,id=veadk-harness-runtime-python312,target=/root/.cache/uv,sharing=locked \
    /opt/build-tools/uv export --quiet --frozen --no-dev --extra sandbox --extra harness-server --extra codex \
      --no-emit-project --output-file /tmp/requirements.txt \
    && /opt/build-tools/uv venv --python /usr/local/bin/python /opt/veadk/.venv \
    && UV_COMPILE_BYTECODE="$VENV_COMPILE_BYTECODE" UV_LINK_MODE=copy UV_SYSTEM_PYTHON=false UV_INDEX_URL="$UV_DEFAULT_INDEX" \
       /opt/build-tools/uv pip install --python /opt/veadk/.venv/bin/python \
       --no-deps --require-hashes -r /tmp/requirements.txt \
    && UV_COMPILE_BYTECODE="$VENV_COMPILE_BYTECODE" UV_LINK_MODE=copy UV_SYSTEM_PYTHON=false UV_INDEX_URL="$UV_DEFAULT_INDEX" \
       /opt/build-tools/uv pip install --python /opt/veadk/.venv/bin/python --no-deps .

COPY --chmod=0644 docker/managed-agents/verify_anthropic_sdk.py /tmp/verify_anthropic_sdk.py
RUN /opt/veadk/.venv/bin/python /tmp/verify_anthropic_sdk.py \
    && /opt/veadk/.venv/bin/python -c "import veadk.runtime.managed_agents.worker"

COPY --chmod=0644 docker/harness-runtime/verify_app.py /tmp/verify_app.py
RUN --network=none /opt/veadk/.venv/bin/python /tmp/verify_app.py


FROM ${PYTHON_IMAGE} AS runtime-base
ARG VCS_REF=unknown
LABEL org.opencontainers.image.revision="${VCS_REF}"
RUN apt-get update \
    && apt-get install -y --no-install-recommends nginx tini util-linux \
    && rm -rf /var/lib/apt/lists/*
COPY --from=veadk-build /opt/veadk/.venv /opt/veadk/.venv
COPY --chmod=0755 docker/managed-agents/run-runtime.sh /opt/veadk/run-runtime.sh
COPY --chmod=0644 docker/managed-agents/configure-telemetry.py /opt/veadk/configure-telemetry.py
COPY --chmod=0644 docker/harness-runtime/supervise.py /opt/veadk/runtime/supervise.py
COPY --chmod=0644 docker/harness-runtime/VERSION /opt/veadk/VERSION
RUN mkdir -p /workspace
ENV PYTHONUNBUFFERED=1 \
    PATH=/opt/veadk/.venv/bin:$PATH \
    VIRTUAL_ENV=/opt/veadk/.venv \
    UV_SYSTEM_PYTHON=false
WORKDIR /opt/veadk


# The external image is only an immutable carrier for the pinned PiAgent archive.
FROM agentkit-studio-cn-beijing.cr.volces.com/agentkit/aks-ma-runtime@sha256:e131130e60769669dd664113f3e77ae891bf8af5b4dd9c5b4effc3b25707c4a3 AS piagent-dist


FROM runtime-base AS studio-python
COPY --from=ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa /uv /opt/build-tools/uv
ARG STUDIO_PYTHON_VERSION=3.12.12
RUN /opt/build-tools/uv python install --no-bin --install-dir /opt/studio-python "${STUDIO_PYTHON_VERSION}"


FROM runtime-base AS studio-build
WORKDIR /opt/studio
ARG PIP_INDEX_URL=https://pypi.org/simple
ARG STUDIO_PYTHON_VERSION=3.12.12
ARG HF_ENDPOINT=https://hf-mirror.com
ENV UV_COMPILE_BYTECODE=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    TIKTOKEN_CACHE_DIR=/opt/agentkit-headroom/tiktoken-cache
COPY --from=ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa /uv /opt/build-tools/uv
COPY --from=studio-python /opt/studio-python /opt/studio-python
COPY docker/harness-runtime/.studio-source/requirements.lock docker/harness-runtime/.studio-source/sidecar-requirements.lock /tmp/studio-locks/
RUN --mount=type=cache,id=studio-harness-python312,target=/root/.cache/uv,sharing=locked \
    studio_python="/opt/studio-python/cpython-${STUDIO_PYTHON_VERSION}-linux-x86_64-gnu/bin/python3.12" \
    && "$studio_python" -c "import sys; assert sys.version_info[:2] == (3, 12)" \
    && "$studio_python" -m venv /opt/studio/.venv \
    && UV_LINK_MODE=copy UV_SYSTEM_PYTHON=false UV_INDEX_URL="$PIP_INDEX_URL" \
       /opt/build-tools/uv pip install --python /opt/studio/.venv/bin/python --no-deps --require-hashes -r /tmp/studio-locks/requirements.lock \
    && /opt/build-tools/uv pip check --python /opt/studio/.venv/bin/python \
    && "$studio_python" -m venv /opt/agentkit-headroom \
    && UV_LINK_MODE=copy UV_SYSTEM_PYTHON=false UV_INDEX_URL="$PIP_INDEX_URL" \
       /opt/build-tools/uv pip install --python /opt/agentkit-headroom/bin/python --no-deps --require-hashes -r /tmp/studio-locks/sidecar-requirements.lock \
    && /opt/build-tools/uv pip check --python /opt/agentkit-headroom/bin/python
RUN /opt/agentkit-headroom/bin/python -c "import tiktoken; tiktoken.get_encoding('cl100k_base'); tiktoken.get_encoding('o200k_base')" \
    && /opt/agentkit-headroom/bin/python -c "from pathlib import Path; from huggingface_hub import snapshot_download; revision='7683fea62db869066ddaff6a41d032262c490d4f'; snapshot=Path(snapshot_download(repo_id='deepseek-ai/deepseek-llm-7b-base', revision=revision, allow_patterns=['config.json', 'tokenizer.json', 'tokenizer_config.json'], cache_dir='/opt/agentkit-headroom/huggingface/hub')); ref=snapshot.parent.parent / 'refs' / 'main'; ref.parent.mkdir(exist_ok=True); ref.write_text(revision)"
COPY --from=piagent-dist /pi-linux-x64.tar.gz /tmp/pi-linux-x64.tar.gz
RUN mkdir /tmp/piagent-install \
    && TMPDIR=/tmp/piagent-install PIAGENT_INSTALL_DIR=/opt/piagent PIAGENT_BINARY_VERSION=1.4.0 \
       PIAGENT_BINARY_URL=file:///tmp/pi-linux-x64.tar.gz \
       PIAGENT_BINARY_SHA256=f2e4e4329603223d03260cdb8bd5033dbdc1f73ba58ab8939db2e56546e115d2 \
       /opt/studio/.venv/bin/python -c "from veadk.runtime.piagent.installer import resolve_or_install_piagent_binary; print(resolve_or_install_piagent_binary())" \
    && chmod -R a+rX /opt/piagent
COPY docker/harness-runtime/pack_runtime.py /tmp/pack_runtime.py
RUN /usr/local/bin/python /tmp/pack_runtime.py /opt/studio-runtime \
    /opt/studio/.venv /opt/agentkit-headroom /opt/studio-python


FROM runtime-base AS runtime-final
ARG STUDIO_REVISION=unknown
ARG STUDIO_SOURCE=unknown
ARG STUDIO_REF=unknown
LABEL com.agentkit.studio.revision="${STUDIO_REVISION}" \
      com.agentkit.studio.source="${STUDIO_SOURCE}" \
      com.agentkit.studio.ref="${STUDIO_REF}"
COPY --from=studio-build /opt/studio-runtime/opt/ /opt/
COPY --from=studio-build /opt/piagent /opt/piagent
COPY --chown=10001:10001 docker/harness-runtime/.studio-source/*.py docker/harness-runtime/.studio-source/LICENSE docker/harness-runtime/.studio-source/requirements.lock /opt/studio/harness_app_server/
COPY --chown=10001:10001 docker/harness-runtime/.studio-source/agent /opt/studio/harness_app_server/agent
COPY --chown=10001:10001 docker/harness-runtime/.studio-source/workflow /opt/studio/harness_app_server/workflow
COPY --chown=10001:10001 docker/harness-runtime/.studio-source/types /opt/studio/harness_app_server/types
COPY --chown=10001:10001 docker/harness-runtime/.studio-source/observability /opt/studio/harness_app_server/observability
COPY --chown=10001:10001 docker/harness-runtime/.studio-source/routes /opt/studio/harness_app_server/routes
COPY --chmod=0644 docker/harness-runtime/launch.py docker/harness-runtime/nginx.conf docker/harness-runtime/healthcheck.py /opt/veadk/runtime/
RUN mkdir -p /opt/studio/home/.cache/veadk \
    && ln -s /opt/piagent /opt/studio/home/.cache/veadk/piagent \
    && chown -R 10001:10001 /opt/studio/home

ENV HARNESS_APP_ENABLED=true \
    STUDIO_ENABLED=true \
    MANAGED_AGENT_WORKER_ENABLED=false
WORKDIR /app
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/python", "-S", "/opt/veadk/runtime/healthcheck.py"]
# AgentKit reads Config.Cmd but may ignore Config.Entrypoint. Keep the full command here.
ENTRYPOINT []
CMD ["/usr/bin/tini", "-s", "--", "/usr/local/bin/python", "-S", "/opt/veadk/runtime/launch.py"]
