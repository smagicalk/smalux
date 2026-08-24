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
- Agent Job 策略的会话内 Query、Snapshot、ACK 和 Job Provider 扩展边界；
- Agent 会话数、注册会话数、gRPC 消息大小限制；
- 通过本地 Named Pipe/Unix Socket 提供的 Server CLI 管理面；
- 注册 Token 签发、查询、吊销，以及 Agent、实时 Session 和 keyring 状态管理；
- `Ctrl+C` 取消通知和优雅关闭；
- `tracing` 控制台日志与按日期/大小滚动的文本日志。

仍未完成的应用能力包括：

- 面向 Web 页面的管理 HTTP API、用户登录和操作审计；
- Job 管理 API、TaskReport 持久化和结果查询；
- Web 管理端和租户/用户授权；
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
gRPC 仍然由不同路径区分。当前正式 Server 只装配 health 和 Agent gRPC；完整 REST/WebSocket
对照流程请运行 Protocol Example。

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
成功后发送 `AgentJobPolicyQuery`，接受 Agent 主动或应答发送的完整快照并返回 revision ACK。
Server 只在当前会话保存最新快照，不为它创建数据库表；断线后的新会话会重新查询。

业务循环获得快照前不会主动下发 Job。每次接受新 revision 后调用
`AgentJobCatalogProvider`，传入 `agent_id` 和当前策略；Provider 可以返回权威
`ReplaceAllJobs`。当前默认 Provider 只记录日志并返回 `None`，因此本阶段完成策略闭环，
但不提供正式 Job 数据来源。后续接入数据库或管理服务时只需替换 Provider。

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
