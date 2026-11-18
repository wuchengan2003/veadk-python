# Harness Server 镜像

公开仓库不内置私有 Studio 仓库地址。运行 `build.py` 或 `prepare_studio.py` 时
传入 `--studio-repository`，或在本地 `sources.json` 中配置 `studio.repository` 后构建。

本目录从 `veadk-agent-loop` 的提交
`3d95681195a73b9ac38dc1ffbfbb9ab159f2c317` 迁入构建和运行链路。
服务实现位于 `veadk.cloud.harness_app.server`；`veadk.cloud.server` 导出同一个
ASGI app，并提供 Python 启动入口。原 SDK 的 `HarnessApp` 继续使用
`veadk.cloud.harness_app.app`。

## 本地启动 Server

```bash
uv sync --extra harness-server --extra sandbox --extra codex
MODEL_NAME=your-model MODEL_API_KEY=your-key MODEL_API_BASE=https://your-provider.example/v1 \
  uv run --no-sync python -m veadk.cloud.server
# 在仓库根目录也可以执行：
# uv run --no-sync python veadk/cloud/server.py
```

默认监听 `0.0.0.0:8000`，通过 `SERVER_HOST`、`SERVER_PORT` 覆盖。
模型、工具、技能、MCP、会话、产物和 cron 配置保留来源 Server 的行为。
`LONG_TERM_MEMORY_TYPE` 启用记忆及 `write_memory` 工具；这是部署配置，
不是 SDK 的请求级覆盖字段。迁入接口和配置见
`veadk/cloud/harness_app/SOURCE.md` 及 `harness_agent.py` 中的环境变量定义。

## 构建和运行镜像

使用 Python 3.12、`uv`、Docker 和 Buildx，在干净且已提交的工作区构建。
`build.py` 从自身路径定位仓库，构建一份 wheel，导出固定提交，并把 wheel
放入导出的 Docker 上下文。`dist/harness/wheel-build.json` 记录来源提交和
wheel SHA-256，两个目标复用同一份 wheel；提交或 wheel 变化时缓存失效。
镜像不再复制一份服务源码覆盖安装包。

```bash
PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df
for target in harness runtime; do
  python3 docker/harness-runtime/build.py --target "$target" \
    --python-image "$PYTHON_IMAGE" --tag "veadk-harness:$target" \
    --source-metadata-file "dist/harness/$target-build.json"
done
docker run --rm -p 8000:8000 --env-file /path/to/server.env veadk-harness:harness
```

`harness` 直接在 8000 运行 Server。`runtime` 基于同一构建阶段，Nginx 在
8000 接收请求并转发至 `127.0.0.1:8001` 的 Server。两个镜像都支持普通 HTTP
和逐片段 SSE。框架在异常或退出时清理服务进程组；容器退出宽限应超过
30 秒，例如 `docker stop --time 40`。

框架默认 `HARNESS_APP_ENABLED=true`、`STUDIO_ENABLED=false`、
`MANAGED_AGENT_WORKER_ENABLED=false`。镜像不包含 Studio 和 Worker 业务运行时。
启用任一未实装组件时，在启动进程前失败；请求头 `X-AgentKit-Target: studio`
返回 503，不代理到旧端口，也不回退 Harness。原 SDK 的其他运行时集成保留。

`--platform` 默认为 `linux/amd64`；`--output-tarball` 导出 Docker 归档而不加载
到本机；`--print` 打印构建命令，不执行 Docker。发布是
`.github/workflows/publish-harness-runtime.yaml` 中的显式操作，在两个镜像验收后执行。

## 验收

迁入行为复用来源服务测试，新增验证集中于实际适配和镜像边界。
未变 SDK 行为以 Git 差异审查确认，不另写一套测试。

```bash
uv sync --extra dev --extra harness-server --extra sandbox --extra codex --extra extensions
uv run --no-sync pytest tests/cloud/cli_harness -p no:xdist -rs
uv run --no-sync pytest docker/harness-runtime/test_runtime.py tests/runtime/test_harness_runtime_supervision.py -p no:xdist -rs
RUN_NGINX_SMOKE=1 uv run --no-sync pytest docker/harness-runtime/test_proxy.py -p no:xdist -rs
for target in harness runtime; do
  HARNESS_E2E_IMAGE="veadk-harness:$target" uv run --no-sync pytest docker/harness-runtime/test_image_e2e.py -p no:xdist -rs
done
```

进程监督测试需要 Linux，代理测试需要 Nginx。镜像测试使用默认启动命令，
不挂载源码；通过隔离的 HTTP 模型、AgentKit 和 OpenViking 测试服务，执行真实
Server、Agent、Runner 和工具。文件场景写入 `note.txt`、执行 `cat`，再在后续请求读回；
其他场景覆盖显式记忆写入、SSE 完成和断连，并检查副作用、安装路径及退出。
这些结果证明协议替身环境的行为，不代表真实云后端验证。
缺失 Docker 或镜像会报错，不以跳过测试作为验收通过。
