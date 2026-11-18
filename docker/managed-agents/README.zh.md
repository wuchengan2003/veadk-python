# Managed Agents Worker 构建

私有基础设施地址由构建环境提供。可选代理通过 Docker 的
`--build-arg HTTP_PROXY` / `--build-arg HTTPS_PROXY` 传入。可选的 ByteAST CI
辅助脚本除原有凭据外，还需配置 `BYTEAST_API_URL`、`BYTEAST_REPORT_URL`；
仓库不内置这些服务地址。

[English](README.md)

镜像运行安装后的 `veadk.runtime.managed_agents.worker` 包与 AgentKit 健康监听器。
dispatcher 和 Session 适配器包含在 VeADK 内，构建不依赖相邻仓库、私有 SDK wheel、
前端构建或线上部署。

## 本地构建与验证

在仓库根目录运行：

```bash
uv sync --extra dev --extra sandbox --extra extensions
uv run --extra dev --extra sandbox pytest tests/runtime/harness_agents tests/integrations/mpa
bash docker/managed-agents/build.sh veadk-managed-agents-worker:local
docker run --rm --entrypoint python veadk-managed-agents-worker:local \
  -m veadk.runtime.managed_agents.worker --help
```

构建脚本默认 `--load`。发布必须显式增加 `--push`，并由操作者选择镜像仓库/tag。
镜像构建不更新 AgentKit Runtime、Tool、Session 或 Kubernetes Deployment。
`MANAGED_AGENTS_PLATFORM` 可覆盖默认 `linux/amd64` 平台。

## 依赖与 SDK 注意事项

旧来源镜像复制相邻 SDK checkout 构建的
`examples/16_self_host_sandbox/.docker-dist/anthropic-*.whl`，并导入
`anthropic.lib.environments._dispatcher`。普通 Anthropic 1.3.0 安装包含 Managed
Sessions 与原生工具，但不包含该私有 dispatcher，导致包导入成功而 Worker 启动失败。
新 dispatcher 使用生成的 Work API 与既有账号级模型 Work 轮询协议；Docker 构建结束
前验证其导入与原生工具可用性。

镜像默认使用 `https://pypi.org/simple`。如果批准的镜像源没有 `uv.lock` 中全部版本，
通过 build args 使用完整镜像源；不能放宽版本、删除哈希或重建锁文件掩盖缺失：

```bash
PIP_INDEX_URL=https://packages.example.com/simple \
UV_DEFAULT_INDEX=https://packages.example.com/simple \
  bash docker/managed-agents/build.sh veadk-managed-agents-worker:local
```

包源只作为构建输入，不写入运行时环境。本脚本只接受无认证镜像源，调用 Docker
前拒绝用户名密码、查询字符串与 fragment。认证镜像源必须单独配置 BuildKit secret
集成；凭据不能写入 build args、镜像 tag、源码或日志。冻结导出保留依赖哈希。组件使用
独立命名且锁定的 BuildKit 缓存，避免与无关镜像构建共用缓存。专用 Worker 镜像
安装 sandbox、codex、dsh extras 与固定的 Pi 0.80.6 二进制。飞书和可选数据集成使用安装
extensions extra 的独立 SDK 环境；Worker 镜像不包含开发包。

## Backend 切换

每次领取 Work 后，按以下顺序为 Session 选择 backend：顶层 `session.agent_loop` →
冻结的 `session.agent.agent_loop` → `MANAGED_AGENT_BACKEND` → `adk`。
取值为 `adk`、`codex`、`piagent` 或 `dsh`，非法值直接报错，不静默回退；Worker 启动时也会校验
环境默认值。常驻 Worker 和 claimed Work 模式遵循相同选择规则。

AKX 创建 Session 时支持通过 SDK 扩展传入 `agent_loop`，显式值覆盖 Harness 的选择，
并冻结到 Session 快照；新 Session 省略时继承 Harness 的选择（默认 `adk`）。
旧 Session 两处字段都缺失时才使用环境默认值。
同一 Worker 可以并发执行不同 backend 的 Session，无需为每个 Session 滚动更新。
同一 Session 保持原 backend，变更时创建新 Session；消费旧 Session 的 Worker 保持相同
环境默认值。Session 快照仍决定模型、Endpoint、工具与凭据。

```python
session = await client.beta.sessions.create(
    agent=agent_id,
    environment_id=environment_id,
    extra_body={"agent_loop": "codex"},
)
```

ADK 使用远端 Ark response 引用续接；Codex 和 Pi 在每轮从远端 Session 事件恢复
用户/助手消息及工具调用/结果，排除当前及后续排队输入，Worker 更换不依赖 CLI 本地状态。
Codex 复用 VeADK SDK adapter 与 Responses shim，Pi 复用 RPC adapter。每个 runner 通过实例配置禁用
CLI 自带工具，全部调用走 Session 授权的工具回调。镜像预装锁定的 Codex SDK/CLI
与通过 SHA256 校验的 Pi，不在启动时下载。裸 Python 环境使用 Codex 时还需安装
`codex` extra，使用 Pi 时设置预装的 `PIAGENT_BINARY`。
Worker 镜像在未指定 `PIAGENT_BINARY` 和 `PIAGENT_INSTALL_DIR` 且默认缓存不存在时，
直接使用 `/opt/piagent/pi/pi`，无需平台透传镜像的环境变量。显式指定的二进制或安装目录
保持优先；构建会以运行用户在无网络、移除上述环境变量的条件下验证预装 Pi 可执行。

```bash
MANAGED_AGENT_BACKEND=codex python -m veadk.runtime.managed_agents.worker --managed-agent-worker
MANAGED_AGENT_BACKEND=piagent python -m veadk.runtime.managed_agents.worker --managed-agent-worker
MANAGED_AGENT_BACKEND=dsh python -m veadk.runtime.managed_agents.worker --managed-agent-worker
```

DSH 使用 `sdk-minimal`，禁用全局 DSH 配置对 Session 模型与凭据的覆盖。
配置、依赖、切换范围和排障见 [Python runtime 说明](../../docs/runtime-backends.md)。

## 运行时配置

镜像中的 ADK Worker 使用 Chat Completions，通过配置的 PostgreSQL SessionStore 恢复历史。
镜像入口强制设置 `MANAGED_AGENT_USE_RESPONSES=false`，继承的环境配置不能重新开启
Responses 或 `previous_response_id` 续接。持久化部署需配置 `DATABASE_POSTGRESQL_*`，
并通过 `MANAGED_AGENT_SESSION_STORE_REQUIRED=true` 开启严格 PG 准入。
无 PG 时，ADK 本地历史仅在内存 Session service 的生命周期内有效。
控制面的 Session/Event/Work 存储单独管理。

Skill 的名称、描述及指令快照保存在 Session state 的 `managed_agent_skill_snapshot` 中。
从 PG 恢复到匹配快照后直接复用，不再查询 Skill API；快照缺失或版本变化时，
通过固定版本的元数据接口补齐。Loop 不下载压缩包，Skill 文件由 Sandbox 提供，
需要时通过工具读取。


### ADK 原生历史压缩

Chat Worker 在 Runner 的原生 App 上启用 `EventsCompactionConfig`。
ADK 使用 Session 模型摘要较早的事件，通过同一个 SessionStore 保存压缩事件，
并在后续请求组装时应用摘要。原始事件仍保留在数据库中，替换 Worker 后可恢复摘要。
Codex、Pi、DSH 和旧版 Responses 调用保留各自的行为。

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `MANAGED_AGENT_COMPACTION_TOKEN_THRESHOLD` | `192000` | 原生 token 触发阈值 |
| `MANAGED_AGENT_COMPACTION_INTERVAL` | `10` | 按 invocation 数触发的兜底间隔 |
| `MANAGED_AGENT_COMPACTION_OVERLAP` | `2` | 下一次摘要重叠包含的历史 invocation 数 |
| `MANAGED_AGENT_COMPACTION_RETENTION` | `4` | token 压缩时保留的近期原始事件数 |

Harness `Model.CompactThreshold`（例如 `192000`）固定为 Session 快照的
`agent.model.compact_threshold`，在 ADK Chat runner 创建时优先应用；Worker 重建后仍读取该快照。
该字段必须为 1–9007199254740991 的整数。省略时使用
`MANAGED_AGENT_COMPACTION_TOKEN_THRESHOLD`，环境也未设置时默认为 `192000`。
更新 Harness 仅影响新 Session；此配置不传给模型 API。其他 backend 和 Responses 路径不应用此配置。
阈值表示 ADK 原生 token 触发条件，原有 invocation 间隔兜底仍可提前触发压缩。
阈值判断使用 ADK 最近一次观测到的 prompt 用量；缺少用量时使用原生文本估算。
这不是对新增超长消息的精确预检，单条超长用户输入仍可能超限。
INFO 日志 `managed_compaction enabled=true` 展示配置；
`managed_session_store op=compact ... persisted=true` 表示摘要已写入。
实际输入 token 是否下降，仍需核对下一次模型请求的用量。

HTTP 健康监听默认端口为 `8000`，健康路径为 `/ping`。显式端口按 `AGENTKIT_RUNTIME_PORT` → `MA_RUNTIME_PORT` → `PORT` 的优先级生效；容器端口与健康探针需匹配实际监听端口。

通过平台 Secret 和环境配置提供网关及云服务凭据。Session 模型使用 `ark_api_key_name`，按账号、地域和项目完整分页解析唯一名称后 `GetRawApiKey`。名称与实际 Key 内容只在进程内 LRU 缓存，使用标准库 `functools.lru_cache(maxsize=2000)`，命中不访问云服务，无额外 TTL；按容量淘汰，重启或 `cache_clear()` 后重新读取。缓存状态线程安全，并发未命中允许重复取钥，错误不缓存。缓存按 provider/可信账号/地域/项目/Name 隔离，明文不持久化或输出。最小 Worker 需要
`ANTHROPIC_BASE_URL`、控制面注入的 `ANTHROPIC_API_KEY` 与 Work 作用域。
仅接受 `ANTHROPIC_API_KEY`，它是 Runtime API Key，不是 Environment Key；
缺少 Key 时直接报错，不调用 Runtime Identity 获取或刷新。账号 Worker 默认
`MANAGED_AGENT_WORK_SCOPE=account` 和 `MA_AGENT_LOOP_RUNTIME_TYPE=agentkit_runtime`；
Environment Worker 必须显式设置 `MANAGED_AGENT_WORK_SCOPE=environment` 与
`ANTHROPIC_ENVIRONMENT_ID`。创建 Session 还需要配置 Agent ID。Session 快照是模型、
工具、Skills、MCP 与凭据的权威来源。

镜像默认 `MANAGED_AGENT_WORK_CONCURRENCY=500`。当平台启动环境未保留镜像 ENV 时，
`run-runtime.sh` 在启动 Python 前为缺失的变量导出相同的默认值 500。
显式值（包括 1）保持不变，空值或非法值仍由 Worker 校验拒绝。它限制包含等待时间的 Work handler
数量，不允许同一 Session 同时执行多个轮次。直接运行 Python 且未设置变量时默认值为 1。
`MANAGED_AGENT_WORKER_READY` 日志包含实际生效的 `max_concurrency`；镜像默认值与
Runtime HTTP 并发配置不能证明这个进程值。开启 `MANAGED_AGENT_POLL_TRACE=true` 后，
`dispatcher_started`、`capacity_wait`、`admission_started`、`admission_completed`
配合已有的 `poll_started`/`poll_completed` 可区分容量、准入等待与领取请求等待。
新增记录只含计数与配置，不含凭据。只有成功完成合法 Work 轮询后才创建
就绪文件，排空时删除。HTTP 健康端点不能单独证明 Worker 就绪。启动入口同时监督
Worker 与监听器，任一进程停止会终止另一个。

对于执行已领取 Work 的 AgentKit Tool，镜像保留 `/opt/gem/run.sh`，选择
`MANAGED_AGENT_ENTRYPOINT_MODE=claimed`，使用注入的 Work/Session/租约配置，并默认
在 Tool 隔离中本地执行工具。镜像常规命令选择持续 Worker 模式。

进程以 UID/GID 10001 运行。使用只读文件系统时，挂载可写 `/workspace` 与 `/tmp`。
原生 bash 必须在部署提供的 OS 隔离中执行；文件路径限制不能替代 shell 安全沙箱。

[Kubernetes 示例](k8s/worker.yaml) 引用操作者创建的 Secret；应用前替换镜像占位符。
模板不提供密钥值或真实部署标识。Kubernetes、AgentKit 和真实模型验收需要独立部署
授权，本地测试或构建不能证明线上验收。

## 官方 SDK 扩展方案

当前镜像安装 `uv.lock` 选定的 **未修改的官方 PyPI `anthropic==1.3.0`**，不会安装
历史相邻仓库的定制 wheel。锁定 SHA256 与[官方发行版](https://pypi.org/project/anthropic/1.3.0/)
一致。安装器验证发行物哈希，`verify_anthropic_sdk.py` 再将所有已安装 SDK Python
文件与 wheel RECORD 对比，拒绝文件被修改、缺失或额外覆盖。RECORD 是完整性校验，
若 wheel 与 RECORD 同时被替换则不能独立证明来源；此边界由锁定发行物哈希保障。

VeADK 采用组合，无需继承或全局修改 SDK 方法：

| 行为 | 实现 |
| --- | --- |
| 环境级 Work 轮询 | 官方 `beta.environments.work.with_raw_response.poll`，先检查 HTTP 204 再校验 Work |
| 账号级模型 Work 轮询 | 公开 `AsyncAnthropic.get` 请求 `/v1/model-work/poll`；这是 VeADK 网关扩展，非 Claude API 端点 |
| ack / heartbeat / stop 与 Session 事件 | 官方 SDK 生成接口 |
| 账号/Runtime 路由头 | 公开 `default_headers` / `extra_headers` |
| 网关认证 | 公开 HTTP client 请求 hooks，以注入的 Key 设置 `x-api-key`，保留 Work Bearer |
| 原生工具执行 | 官方 `agent_toolset` 工厂与公开 `ToolError` |

官方文档说明了[原始响应、自定义请求及 HTTP client](https://platform.claude.com/docs/en/cli-sdks-libraries/sdks/python#advanced-usage)。
官方 `EnvironmentWorker` 没有任意 VeADK handler 回调；继承需要覆盖私有执行方法。
本实现不覆盖 SDK 源码或方法。每个 Work 使用清除父认证的客户端副本；必须设置
副本公开属性 `api_key=None`，因为 SDK 1.3.0 的 `copy(api_key=None)` 会继承父 Key。

仍有两个版本敏感的 SDK 内部接口：官方发行包内的 `_skills._download_and_extract`
保留 Skill 下载失败即失败的语义，公开 helper 会跳过失败；工具 decorator 的
`_context_manager` 用于额外的退出清理。VeADK 在本地执行清理，避免 SDK helper
直接输出异常文本。真实 SDK 的归档、工具、认证与 dispatcher 测试覆盖这些边界。
锁定官方 SDK 版本不等于需要定制 SDK fork。

本地 VKE Worker 没有 Session 模型名称时，可显式设置
`MANAGED_AGENT_MODEL_CREDENTIAL_SOURCE=environment`，通过独立 Kubernetes Secret
提供 `MODEL_AGENT_API_KEY`。默认 `session`；AgentKit 及带 provider 引用的 Session
始终按 `ark_api_key_name` 解析模型 Key，不回退旧 ID 或 Identity provider。此路径证明配置的模型凭据，不代表云端取钥已验证。

## Runtime 轮询隔离

AgentKit 注入 `MODEL_AGENT_CLIENT_REQ_ID=agentkit/<RuntimeId>`。
`SelfHostSandboxClient` 复用现有 Runtime 身份解析，将 `agentkit/` 后的 ID 作为
`X-Runtime-Id` 传给账号级和环境级轮询，无需额外设置模板 ID。
已有 `AGENTKIT_RUNTIME_ID`、`MA_RUNTIME_ID` 的显式身份覆盖顺序保持不变。

配套 harness-infra 按 Session 固定规格中的 `ProviderConfig.RuntimeId` 筛选
领取和超时回收。不同 Runtime 隔离，同一 Runtime 绑定的多个模板可共享消费者。
Session 创建仍指定模板；Runtime ID 不是模板 ID，也不是独立鉴权凭据。

保留 `MA_RUNTIME_TEMPLATE_ID` 和构造参数作为没有 Runtime 身份时的兼容路径。
有 Runtime ID 时仅发送 Runtime 选择器；后端同时收到两个选择器时取交集。
模型 Worker 的 `MA_AGENT_LOOP_RUNTIME_TYPE=agentkit_runtime` 不变。
两个选择器均缺少时后端返回 400，SelfHosted 无类型工具 Worker 保持原协议。

需要先部署支持 Runtime 选择器的 harness-infra，再启用新版镜像。旧模板选择器
客户端继续兼容，resident 的原模板变量可保留，但新客户端不依赖它。

### 中断与执行归属

Cloud 模型 worker 执行期间每秒通过标准 GetWork 读取当前 Work 状态，看到
`stopping` / `stopped` 就取消当前模型与工具任务，并等待清理；心跳仍按服务端 TTL
续租，不靠修改 409 重试规则实现取消。心跳 `lease_extended=false` 表示控制面停止，
412 表示租约丢失；失去租约后不再 force-stop，避免停止新持有者。

AKX Work 带有效 `lease_epoch` 时，执行客户端在请求中绑定
`X-MA-Work-Id` / `X-MA-Lease-Epoch`。这些是执行归属信息，不代替网关认证，
也不使共享 Environment Key 具有不可伪造的单 Work 权限。原生无代次的 Anthropic Work
保持原有凭据流程。取消前先设置本地执行标记，阻止底层 runner 吞掉取消后继续发送
正常消息、工具事件和 preview；仅允许结束模型 span 的错误清理，最终 idle 由 AKX
在取消确认后生成。

验证范围与既有测试限制见[中断取消记录](../../docs/verification/2026-11-05-managed-interrupt-cancellation.md)。
