# smalux-server

`smalux-server` 是 Smalux 的服务端应用层。它负责启动 Axum/HTTP、装配 Agent gRPC
服务、连接 SeaORM 数据库、恢复 Server Noise 密钥环，并把注册和授权请求交给 Agent
领域服务处理。

协议消息、Noise 握手和 Session 的通用实现位于
[`smalux-protocol`](../smalux-protocol/README.md)；Server README 只说明应用层如何装配这些
能力。

## 当前状态

当前 Server 已经可以作为一个可运行的开发服务启动，已经包含：

- SQLite、PostgreSQL、MySQL 的 SeaORM 连接配置和启动迁移；
- `GET /api/v1/health` 普通 HTTP 健康检查；
- `/api/v1/grpc` 下的 `AgentTransport` gRPC unary 和长期双向流；
- 基于 Noise XXpsk3 的首次注册、pending/commit/committed 状态和 IK 后续授权；
- 数据库持久化的 Server keyring、Agent、注册 Token 和注册事务；
- 每 Agent 的数据库权威 Job catalog、在线完整对账、命令结果关联和本地策略/能力/插件过滤；
- `TaskReport` 幂等持久化，以及失败、超时、重试、取消等 `JobEvent` 追加历史；
- Plus Schema、inventory、runtime 配置持久化；Server 根据 `schema.pb` 动态把 JSON 编码为插件私有 Protobuf bytes；
- Agent 会话数、注册会话数、gRPC 消息大小限制；
- 通过本地 Named Pipe/Unix Socket 提供的 Server CLI 管理面；
- 注册 Token 签发、查询、吊销，以及 Agent、实时 Session 和 keyring 状态管理；
- `Ctrl+C` 取消通知和优雅关闭；
- `tracing` 控制台日志与按日期/大小滚动的文本日志。
- 默认关闭的 Web 登录基础：本地管理员初始化、Cookie 会话登录/恢复/退出、meta 和 `session.info`；Argon2id、Origin/CSRF、限流及脱敏安全事件。

仍未完成的应用能力包括：

- Web 写操作 API、用户管理/改密和完整操作审计；当前仅有 Agent/Job/Report/Event 只读查询子集；
- 面向 Web 页面的 Job 编辑 API、任务模板和批量 Agent 分配；
- Agent/Job 等 Web 业务页面适配、租户和资源级授权；
- Server 进程自身的 TLS listener（生产环境建议由 Nginx/Cloudflare 终止 TLS）；
- 多实例之间的注册表、Token 和业务数据一致性策略。

正式 Server CLI 已替代 Protocol Example 的 `token generate`。Example 仍只用于学习协议，不能
作为生产管理入口。

## 目录结构

```text
crates/smalux-server/
├── src/main.rs                 # 进程入口：初始化 tracing，调用 run_from_env
├── src/lib.rs                  # 可复用的 Server 启动入口
├── src/bootstrap.rs            # Runtime 装配、监听和优雅关闭
├── src/cli.rs                  # Clap 参数、子命令和配置覆盖
├── src/commands.rs             # CLI 到本地 IPC 请求的转换与输出
├── src/management/              # CLI/未来页面共用的本地管理边界
│   ├── mod.rs                   # 兼容性重导出和 IPC 装配
│   ├── protocol.rs              # 请求、响应、配置和安全状态 DTO
│   ├── service.rs               # AdminService 和管理业务分发
│   └── ipc.rs                   # Named Pipe/Unix Socket 有界 JSON 帧
├── src/config.rs               # ServerConfig 和 RuntimeConfig
├── src/route.rs                # 顶层 Axum Router、request ID 中间件
├── src/state.rs                # AppState、Agent 子状态和关闭令牌装配
├── src/controller/
│   ├── frontend.rs             # 普通 HTTP 健康检查
│   └── agent.rs                # Tonic AgentTransport 路由
├── src/service/agent/
│   ├── transport.rs            # HealthCheck/OpenSession 和 worker 生命周期
│   ├── transport/session/      # 注册、授权和加密业务循环
│   ├── agent_registry.rs       # Token、注册事务、Agent 激活与授权
│   ├── keyring_manager.rs      # 数据库 keyring 恢复、CAS 轮换和后台同步
│   └── state.rs                # Agent gRPC 共享状态和容量限制
└── src/database/
    ├── connection.rs           # SeaORM 连接池、配置校验和迁移入口
    ├── agent_registration.rs   # Token、注册事务、Agent 授权与吊销的原子持久化
    ├── entity/                 # agents、registration_tokens 等实体
    ├── migration/              # 数据库 schema
    └── keyring.rs              # Server keyring 快照读写
```

## Web 登录基础（可选）

首次在本地交互终端初始化管理员，命令直接使用数据库配置，不依赖运行中的 Server：

```powershell
cargo run -p smalux-server -- auth bootstrap --username admin
```

密码隐藏输入两次，要求 12–128 个字符且 UTF-8 不超过 512 字节；无默认密码、无公开注册接口，重复初始化拒绝。用户名为 3–64 个 ASCII 字母/数字/`.`/`_`/`-`，统一小写。先备份现有数据库；启动自动追加独立迁移，不改写旧迁移。新增 `web_users`、`web_sessions`、`web_auth_events` 和防并发重复初始化的 `web_bootstrap`。

开发环境必须让浏览器与 API 同源，例如同源开发反代监听 `127.0.0.1:5173`，转发 `/api/` 到 Server 的 `127.0.0.1:12345`：

```powershell
$env:SMALUX_WEB_ENABLED = "true"
$env:SMALUX_WEB_DEVELOPMENT = "true"
$env:SMALUX_WEB_ORIGIN = "http://127.0.0.1:5173"
cargo run -p smalux-server -- run
```

`SMALUX_WEB_ORIGIN` 是浏览器访问的规范 origin，不带末尾 `/`、路径或通配符。生产设置 `SMALUX_WEB_DEVELOPMENT=false`，origin 使用 `https://console.example.com`，由同机反代终止 TLS，Server 必须回环监听；反代应覆盖/丢弃客户端提供的转发头。Server 不信任 `Forwarded`/`X-Forwarded-*`，不开放跨域 CORS，也不自带 TLS listener。不要开启包含绑定参数的数据库调试日志或请求正文日志。

可选配置：`SMALUX_WEB_SESSION_TTL_SECONDS=86400`、`SMALUX_WEB_IDLE_TTL_SECONDS=1800`、`SMALUX_WEB_LOGIN_LIMIT_PER_MINUTE=30`。TTL 为正、idle 不超过 absolute，absolute 最长一年；登录限流 1–10000 次/分钟，另有 2 个并发哈希上限。限流是单进程内存状态，重启清空。

独立前端 `app-config.json` 设 `enableMock:false`、`transport:"http"`、`authApiBaseUrl:"/api/v1"`；保留其他已有配置，避免把 `/api` 再拼进认证路径。真实模式只显示登录与服务端身份/能力状态，不挂载尚未接通的 Mock 业务页面。默认 Mock 模式保持不变。

登录/退出要求 JSON、精确 `Origin` 和 `X-Smalux-Client: web`；退出另需 `X-CSRF-Token`。Cookie 为 HttpOnly/SameSite=Strict，生产带 Secure；所有认证响应 no-store。数据库仅存 Cookie 摘要，退出持久吊销并主动关闭该会话 WS；会话校验读取用户启用状态及绝对/空闲期限。启动和登录时清理过期/吊销超过 7 天的会话和超过 30 天的安全事件。完整权限管理、改密、MFA、Agent/Job/Report/Event 只读 RPC 已接入；用户管理、改密、Job 写入、Operation 和完整资源级授权仍未实现。

### CPU/内存快照与 WebSocket（Rust 已实现）

先用既有 CLI 创建 CPU 和内存采集 Job，再配置 Agent 到 Job 的只读绑定。例如下面 UUID 仅为格式示例，必须替换为当前 Agent 的真实 Job ID：

```powershell
$env:SMALUX_WEB_METRICS_BINDINGS = '[{"agentId":"agent-a","cpuJobId":"00000000-0000-4000-8000-000000000001","memoryJobId":"00000000-0000-4000-8000-000000000002"}]'
$env:SMALUX_WEB_METRICS_STALE_SECONDS = "60"
```

绑定默认 `[]`，最多 1000 项/256 KiB；Agent ID 为 1–128 个可打印非空白 ASCII 字符，Job ID 为规范小写带连字符 UUID。每组 Job 可省略，但 CPU/内存不能绑定同一个 Job；重复 Agent、未知字段和非法值启动时拒绝。stale 阈值 1–86400 秒。修改配置需重启；不自动创建 Job、不提供绑定管理写 API、不新增数据库迁移。

HTTP：登录后调用 `POST /api/v1/rpc`，附带同源 Cookie、Origin、JSON 和 `X-Smalux-Client: web`：

```json
{"jsonrpc":"2.0","id":"latest-1","method":"metrics.latest","params":{"agentIds":["agent-a"],"metrics":["cpu","memory"]}}
```

`metrics` 省略时查询两组；Agent 1–100 个且唯一，指标 1–2 个且唯一。未配置/不存在/吊销的 Agent 一律 FORBIDDEN；未绑定组、无报告返回 unknown/null，来源缺失/禁用/类型不符返回 unavailable/null。真实零值保留；过期样本保留值并标 stale，非法百分比、不安全整数字节或未来时间不能标 valid。来源只取当前启用 Job revision 的报告，按采样时间而非到达时间选最新，不暴露原始 payload。

WS：同源浏览器连接 `/api/v1/ws`（生产 wss；反代需转发 Upgrade），只用现有 HttpOnly Cookie + 精确 Origin，不把凭据放 URL，也不要求浏览器自定义认证头。发送：

```json
{"jsonrpc":"2.0","id":"sub-1","method":"stream.subscribe","params":{"topic":"metrics","agentIds":["agent-a"]}}
```

先收到 `{subscriptionId,streamEpoch,sequence:"0",snapshot:[...]}`；随后 `stream.notification` 的 `params.kind="metrics.update"`，`params.data={items:[...]}`。每 2 秒读库检查，仅变化时推送。`stream.unsubscribe` 传 subscriptionId/streamEpoch；`stream.ping` 传 `{}`。每次订阅均新 epoch 和全量快照；带旧 sinceCursor 时另发 resyncRequired，不提供历史重放。

快照、心跳和推送不续 idle TTL；独立 1 秒检查禁用/吊销/过期，读库超时 5 秒，失败关闭。上限为 32 连接/进程、16 订阅/连接、100 去重 Agent/连接、64 KiB 输入、1 MiB 输出、32 条发送队列、120 条文本控制/分钟；过载或发送超时关闭后须重新取快照。此为有界数据库轮询驱动的服务端推送，不是事件总线或已验收的生产容量。

本批没有修改独立前端，指标面板/WS 适配待下批接入。Report/Event 摘要只读 RPC 已实现；metrics.history、网络速率、operation topic 和 Job CRUD 仍未实现。协议细节见根目录 `WEB_API.md` §5.5。
### Agent/Job/Report/Event 只读 RPC（Rust 已实现）

默认 Web 登录开启后，`POST /api/v1/rpc` 还提供 `agent.list`、`agent.get`、`job.list`、`job.get`、`report.list`、`event.list`。它们复用 Server `AdminService`、Job catalog 与已有报告/事件表，无需新增迁移；查询有界、拒绝未知参数字段，报告/事件仅返回摘要，不暴露 payload。列表采用 Agent ID 游标或时间+记录 ID 的稳定游标；报告/事件时间过滤单位为 UTC Unix 毫秒，范围 `[fromMs,toMs)`。

`agent.list`/`agent.get` 当前只返回数据库已有的身份元数据与进程内在线状态，不伪造 region/labels/note 等未存字段；在线状态只反映当前 Server 进程观察到的已认证 Session。列表请求 `{status?,name?,online?,limit?,after?}`，使用 Agent ID 字典序游标。`job.list/get` 返回 Server 权威 Job catalog 与 revision；Job 仍使用 Protobuf `JobDefinition`，Web 只读 DTO 只提供 jobId/revision/enabled/taskKind 等 summary，不提供完整调度参数或插件私有配置。`report.list`/`event.list` 请求按 Agent、可选 Job、`fromMs/toMs` 半开时间窗和游标分页；Report 按 `received_at`，Event 按 `emitted_at`，均以时间和 ID 稳定倒序。列表只返回摘要、不含 payload；默认仅 admin/operator 可读，其他角色返回 FORBIDDEN。此批未实现 Job 写入、metadata 修改、Operation 状态、raw report payload 读取或细粒度 Agent ACL。

## 启动

在仓库根目录执行：

```powershell
cargo run -p smalux-server
# 等价的显式写法
cargo run -p smalux-server -- run
```

默认监听：

```text
http://127.0.0.1:12345
```

启动顺序是：

```text
初始化 tracing
  -> 按 CLI > 环境变量 > 默认值读取 ServerConfig
  -> 连接数据库并执行 SeaORM migration
  -> 恢复或创建 Server Noise keyring
  -> 创建 AgentRegistry、后台清理/同步任务和 AppState
  -> 启动仅本机可访问的管理 IPC
  -> 装配普通 HTTP 与 Agent gRPC 路由
  -> 监听 TCP
```

### 启动参数

```powershell
cargo run -p smalux-server -- run `
  --listen-address 127.0.0.1 `
  --listen-port 12345 `
  --max-agent-sessions 256 `
  --max-registration-sessions 32 `
  --max-grpc-message-bytes 1048576 `
  --shutdown-grace 15s
```

数据库可通过 `--database-url`、`--database-username`、`--database-password-file`、连接池
参数和可重复的 `--database-option KEY=VALUE` 覆盖。CLI 不提供明文
`--database-password`，避免密码进入命令历史或进程列表。

只解析并校验配置、不启动 Server：

```powershell
cargo run -p smalux-server -- config check --database-url sqlite::memory:
```

## 本地管理 CLI

管理子命令不会直接连接数据库，而是通过正在运行的 Server 调用 `AdminService`：

```text
CLI -> 本地 IPC -> AdminService -> Database / AgentRegistry / SessionRegistry / Keyring
未来页面 -> 管理 HTTP API -> 同一个 AdminService
```

Windows 默认端点为 `\\.\pipe\smalux-server`，拒绝远程 Pipe Client，并通过 ACL 只允许
System、管理员和对象所有者访问。Unix 默认端点为 `<data_dir>/server/control.sock`，权限为
`0600`。可通过全局 `--control-endpoint` 或 `SMALUX_SERVER_CONTROL_ENDPOINT` 覆盖。

常用命令：

```powershell
# 运行状态、最终生效配置和只读 keyring 状态
cargo run -p smalux-server -- status
cargo run -p smalux-server -- status --watch --interval 2s
cargo run -p smalux-server -- config show --output json
cargo run -p smalux-server -- keyring status

# 注册 Token 默认 30 分钟有效；期限支持 30m、24h、7d
cargo run -p smalux-server -- registration-token create --agent-name node-a --expires-in 24h
cargo run -p smalux-server -- registration-token create --credential-file token.txt
cargo run -p smalux-server -- registration-token list --status active
cargo run -p smalux-server -- registration-token show <TOKEN_ID>
cargo run -p smalux-server -- registration-token revoke <TOKEN_ID> --yes

# Agent 和实时 Session
cargo run -p smalux-server -- agent list --online
cargo run -p smalux-server -- agent rename <AGENT_ID> --name edge-node
cargo run -p smalux-server -- agent revoke <AGENT_ID> --yes
cargo run -p smalux-server -- session list --agent-id <AGENT_ID>
cargo run -p smalux-server -- session disconnect <SESSION_ID> --yes

# 每个文件是一条编码后的 JobDefinition；replace 后在线 Agent 立即完整对账
cargo run -p smalux-server -- job replace <AGENT_ID> --definition cpu-job.pb
cargo run -p smalux-server -- job list <AGENT_ID>
cargo run -p smalux-server -- job clear <AGENT_ID> --yes

# 插件 runtime JSON 由 Server 依据 schema.pb 动态编码；不需要安装插件 crate
cargo run -p smalux-server -- plugin runtime-replace <AGENT_ID> --file echo-runtime.json
cargo run -p smalux-server -- plugin runtime-list <AGENT_ID>

# 已持久化的成功数据和异常生命周期事件
cargo run -p smalux-server -- report --agent-id <AGENT_ID>
cargo run -p smalux-server -- event --agent-id <AGENT_ID>

# 使用现有优雅关闭流程停止 Server
cargo run -p smalux-server -- shutdown --yes
```

完整 `token_id.psk` 只在创建响应中出现一次。使用 `--credential-file` 时，CLI 会在请求前
以“文件必须不存在”的方式预留目标文件，成功后不再把凭据打印到控制台。Token 的
`list/show` DTO 不含 PSK。`agent revoke` 会先持久化吊销状态，再取消该 Agent 的活动
Session；`session disconnect` 只断开当前连接，Agent 仍可重新认证。

危险命令在交互终端要求输入 `yes`；脚本或其他非交互环境必须显式提供 `--yes`。

检查普通 HTTP 路由：

```powershell
Invoke-RestMethod http://127.0.0.1:12345/api/v1/health
```

返回示例：

```json
{"date":"2026-08-09"}
```

## 路由与协议

| 能力 | 路径 | 说明 |
| --- | --- | --- |
| HTTP health | `GET /api/v1/health` | 不读取业务数据，只返回 Server UTC 日期。 |
| gRPC HealthCheck | `/api/v1/grpc/smalux.agent.v1.AgentTransport/HealthCheck` | 未认证 unary RPC，适合检查路由和进程。 |
| Agent Session | `/api/v1/grpc/smalux.agent.v1.AgentTransport/OpenSession` | gRPC 双向流，Noise 握手和加密业务消息都在同一条流内。 |

Server 的 gRPC 路由使用 `tonic::service::Routes::into_axum_router()` 后再 `nest` 到
`/api/v1/grpc`。因此它可以和 Axum 普通 HTTP 路由共用端口，但普通 HTTP、WebSocket 和
gRPC 仍然由不同路径区分。正式 Server 保留 health 与 Agent gRPC；显式启用 Web 后额外提供
`/api/v1/auth/login`、`/api/v1/auth/session`、`/api/v1/auth/logout`、`/api/v1/meta`，以及 `/api/v1/rpc` 上的 `session.info`、CPU/内存 `metrics.latest`、Agent/Job 只读目录和 Report/Event 摘要查询；`/api/v1/ws` 提供 CPU/内存 metrics 订阅。写 API、完整资源级授权和 Web 业务页面仍未实现。

顶层 Router 会：

- 保留网关传入的 `x-request-id`；没有时生成 UUID；
- 把 `x-request-id` 返回给调用方；
- 让普通 HTTP 使用 `FrontendState`，让 gRPC 直接持有 `Arc<AgentState>`；
- 使用 gRPC 专用 TraceLayer 记录长期流的建立、结束和错误。

## 数据库

Server 支持 `sqlite`、`postgres`/`postgresql` 和 `mysql`。未设置
`SMALUX_DATABASE_URL` 时，会在公共数据目录下使用 `server.db` SQLite 文件。

公共目录由 `smalux-core::config::paths` 解析：设置 `SMALUX_HOME` 后使用：

```text
<SMALUX_HOME>/config
<SMALUX_HOME>/data
<SMALUX_HOME>/cache
<SMALUX_HOME>/data/logs
```

未设置时使用平台默认目录。数据库 URL、用户名和密码是独立配置，URL 不要嵌入凭据。

### 环境变量

```powershell
# PostgreSQL 示例；URL 中不要写 username/password
$env:SMALUX_DATABASE_URL = "postgres://127.0.0.1:5432/smalux"
$env:SMALUX_DATABASE_USERNAME = "smalux"
$env:SMALUX_DATABASE_PASSWORD = "change-me"

# 连接池通用参数
$env:SMALUX_DATABASE_MAX_CONNECTIONS = "20"
$env:SMALUX_DATABASE_MIN_CONNECTIONS = "2"
$env:SMALUX_DATABASE_CONNECT_TIMEOUT_SECONDS = "5"
$env:SMALUX_DATABASE_ACQUIRE_TIMEOUT_SECONDS = "5"
$env:SMALUX_DATABASE_SQLX_LOGGING = "false"
$env:SMALUX_DATABASE_RECORD_STMT_IN_SPANS = "false"
```

还支持 `SMALUX_DATABASE_IDLE_TIMEOUT_SECONDS` 和
`SMALUX_DATABASE_MAX_LIFETIME_SECONDS`。后端专属查询参数通过 `DatabaseConfig.options`
传入并在连接前校验；未知参数、凭据嵌入 URL、零值池参数和不匹配的后端都会直接拒绝。

启动连接成功后立即执行迁移。当前 schema 包含：

| 表 | 作用 |
| --- | --- |
| `server_keyrings` | 保存 current/next/previous Server Noise 身份和 CAS revision。 |
| `registration_tokens` | 保存公开 Token ID、32 字节 PSK、状态和过期时间。 |
| `agents` | 保存稳定 Agent ID、展示名称、Noise 公钥和授权状态。 |
| `agent_registrations` | 保存 XXpsk3 的 pending/committed 注册事务。 |

当前数据库中的 PSK 和 Noise 私钥仍是应用层字节字段，暂未做数据库字段加密。生产部署至少
需要限制数据库账号、文件权限、备份权限，并在后续接入 envelope encryption 或 KMS。

## Agent 注册和授权

首次注册不要求 Agent 预置 Server 公钥。标准凭据格式是：

```text
token_id.psk
```

其中 `token_id` 是 Noise 首帧携带的公开选择器，`psk` 是 32 字节秘密。完整流程如下：

```text
Agent 发送 XXpsk3 message 1 + token_id
  -> Server 根据 token_id 从数据库解析 PSK
  -> XXpsk3 完成，双方得到加密 Session
  -> Agent 发送加密 RegistrationRequest(token_id.psk)
  -> Server prepare：读取 Token 绑定的展示名称并保存注册事务，但不创建 active Agent
  -> Agent 保存身份、公钥、预分配 Agent ID、registration_id
  -> Agent 发送加密 RegistrationCommit
  -> Server 原子创建 active Agent、标记事务 committed、消费 Token
  -> Agent 直接复用当前 XX Session
```

后续重连不再使用 Token，而是使用保存的 Agent 私钥和 Server 公钥执行 IK。Server 通过
Agent 公钥查询 `agents.status`，只有 `active` Agent 可以进入业务循环；`revoked` 或未知
公钥会收到加密授权错误。

pending 注册默认保留 10 分钟，后台任务每 60 秒清理过期且尚未 commit 的事务。同一 Token、
同一 Agent 公钥的重试保持注册事务幂等；同一 Token 绑定不同公钥会被拒绝。
展示名称在 Server 签发 Token 时可选；未指定时，prepare 使用新生成的 `agent_id` 作为默认值。
Agent 不在注册请求中上报名称，因此不能自行覆盖 Server 的管理信息。

## Agent Job 策略

远程 Job 黑名单由 Agent 本地持久化并通过 Noise 密文同步。Server 在每次注册或 IK 授权
成功后发送 `AgentJobPolicyQuery`，接受 Agent 主动或应答发送的完整快照并返回 revision ACK；Server
不为策略建立数据库持久化副本。能力和插件 inventory 会写入数据库，用于管理查询；当前会话仍使用新鲜快照做下发判断。

业务循环获得策略、能力、插件 inventory 与 runtime ACK 前不会主动下发 Job。完成后，数据库
`AgentJobCatalogProvider` 读取该 Agent 的权威完整目录，过滤本地黑名单、能力、插件 inventory、
已 ACK runtime 和 Worker 暂停状态，再发送 `ReplaceAllJobs`。本地 CLI 或未来页面替换目录/runtime
后会唤醒该 Agent 的在线 Session；离线 Agent 在下次连接获得最新完整快照。

目录和 Plus runtime 使用独立的 Session 通知通道。修改普通 Job 不会重启插件 Worker；runtime
变更则必须先等待 Agent 的 runtime ACK，再重新过滤和下发插件 Job。控制面写入支持可选
`expected_revision`：传入时执行数据库 CAS，冲突返回 `conflict` 且不会删除旧目录；省略时保留
旧 CLI 的无条件替换行为，但仍由当前 Server 进程写锁串行化。

Agent 重连后会先发送 `AgentReconcileSummary`。Server 比较目录/runtime revision 和 BLAKE2s-256
摘要，匹配时跳过对应快照；摘要缺失、摘要不一致或检测到新的 Agent 进程实例时执行完整同步。
Job catalog 还保留 `agent_job_versions` 历史定义，用于校验延迟到达的 TaskReport/JobEvent。
TaskReport 按执行身份幂等保存，JobEvent 按 `agent_id + instance_id + sequence` 幂等保存，并
记录序号缺口和重复 payload 冲突。

策略 ACK 仅表示 Server 接收了该会话的快照，不表示 Provider 已经重新下发 Job。完整线序、
revision 规则和 Agent 本地停用语义见
[`PROTOCOL_FLOW.md`](../smalux-protocol/PROTOCOL_FLOW.md#11-agent-job-策略同步)。

## 资源限制和关闭

```powershell
$env:SMALUX_AGENT_MAX_SESSIONS = "256"
$env:SMALUX_AGENT_MAX_REGISTRATION_SESSIONS = "32"
$env:SMALUX_AGENT_MAX_MESSAGE_BYTES = "1048576"
$env:SMALUX_SERVER_SHUTDOWN_GRACE_SECONDS = "15"
```

默认值分别为 256 条 Agent 流、32 条注册业务并发和 1 MiB 单消息上限。超过会话容量返回
gRPC `RESOURCE_EXHAUSTED`；超过注册容量返回加密 `SecureError`。这些限制只保护 Server 资源，
不替代上游网关的连接数和速率限制。

按 `Ctrl+C` 后，Server 会：

1. 取消共享 `CancellationToken`；
2. 通知握手、注册、长期业务流、keyring 同步和注册清理任务退出；
3. 在 `SMALUX_SERVER_SHUTDOWN_GRACE_SECONDS` 内等待；
4. 宽限期结束后停止等待并退出。

## 日志

Server 只在 `main` 初始化一次公共 tracing subscriber。日志同时输出到控制台和：

```text
<data_dir>/logs/<component>/smalux.log
```

文件按日期和 10 MiB 大小滚动，旧文件由滚动策略清理。常用设置：

```powershell
$env:RUST_LOG = "smalux_server=info,smalux_protocol=info"
$env:SMALUX_LOG_COMPONENT = "server"
```

日志不会记录数据库密码、Token PSK、Noise 私钥或业务明文。排查单帧和心跳时可短时间使用
`smalux_protocol=trace`，生产环境应按需要过滤 Agent ID、endpoint 和 request ID。

## 测试与开发

```powershell
cargo check -p smalux-server --all-targets
cargo test -p smalux-server --lib
cargo clippy -p smalux-server --all-targets -- -D warnings
```

工作区完整检查：

```powershell
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Server 测试默认使用 `sqlite::memory:`，不会连接外部数据库。Agent Client 的一个端到端错误
测试被标记为 ignored，需要先手动启动 Server，再运行：

```powershell
$env:SMALUX_SERVER_ENDPOINT = "http://127.0.0.1:12345"
cargo test -p smalux-agent client_reports_unencrypted_frame_error -- --ignored --nocapture
```

## 相关文档

- [Protocol README](../smalux-protocol/README.md)：Noise、gRPC、Session、rekey 和协议方法；
- [协议完整流程](../smalux-protocol/PROTOCOL_FLOW.md)：注册和重连的调用时序；
- [Server 安装边界](../../website/docs/installation/server.md)：部署前的待完成能力；
- [反向代理与单端口](../../website/docs/deployment/reverse-proxy.md)：Nginx/Cloudflare 路径和 HTTP/2 要求。
