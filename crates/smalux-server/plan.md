# smalux-server 实现计划

本文档用于指导 `smalux-server` 从当前目录骨架逐步实现为可运行的服务端。计划按“先跑通 agent 上报闭环，再补 REST API，再补前端实时和管理功能”的顺序推进，避免一开始把数据库、前端、加密、远程命令全部揉在一起。

当前 server 目标：

- 接收 `smalux-agent` 主连接和上报数据。
- 通过 REST API 提供 agent 列表、latest 状态、命令下发和结果查询。
- 支持前端实时推送，用于 dashboard live update。
- 支持 React/Vite 前端独立运行、目录托管和可选二进制内嵌。
- 复用 `smalux-protocol` 的 frame、wire、secure 逻辑，不在 server 重复实现协议。
- 使用 SQLite + SeaORM 做持久化，首版先以 latest state 为主，历史数据后续再扩展。

## 总体边界

server 当前按下面几个模块分工：

```text
main.rs
  -> 启动入口，后续串联日志、CLI、配置、数据库、HTTP server。

cli/
  -> 只解析启动参数和环境变量，不做业务逻辑。

config/
  -> 运行配置模型、默认值、校验和 CLI 到配置的转换。

auth/
  -> agent 认证、secure_psk secret 查找、管理后台 session 和权限判断。

http/
  -> axum 路由、REST handler、agent WebSocket、前端实时通道、前端静态资源和中间件。

ingest/
  -> agent frame 解码后的业务分发，处理 snapshot、delta、heartbeat 和控制响应。

service/
  -> 业务编排，管理 agent 连接、latest 查询、dashboard 聚合、命令调度和后台清理。

storage/
  -> 数据库 entity、migration、repository 和内存/SQLite 存储适配。
```

已经删除 `query/` 模块。前端查询读模型不单独占目录：

- agent 列表、latest report、在线状态放在 `service/agent.rs`。
- dashboard 聚合数据放在 `service/dashboard.rs`。
- REST handler 放在 `http/rest.rs`。
- 数据库存取放在 `storage/repository.rs`。

## 路由规划

首版固定三类入口：

```text
/agent/v1/connect
  -> Smalux agent 主 WebSocket。

/api/v1/*
  -> 前端和管理端 REST API。

/live/v1/dashboard
  -> 前端 dashboard 实时订阅，后续可以用 WebSocket 或 SSE。
```

建议首版端点：

```text
GET  /agent/v1/connect
GET  /api/v1/health
GET  /api/v1/agents
GET  /api/v1/agents/{agent_id}
GET  /api/v1/agents/{agent_id}/latest
POST /api/v1/agents/{agent_id}/commands
GET  /api/v1/commands/{command_id}
GET  /live/v1/dashboard
```

端点职责：

- `/agent/v1/connect` 只处理 agent 主连接、认证、wire 解包、frame 分发和 server frame 下发。
- `/api/v1/*` 只处理前端 REST 请求，不承载 agent 主连接。
- `/live/v1/*` 只处理前端实时订阅，不接收 agent 上报。
- React 静态资源放在 fallback，不能抢占 `/api/v1/*`、`/agent/v1/connect`、`/live/v1/*`。

## 阶段 0：保持骨架可编译

目标：每次结构调整后，server crate 都保持可编译。

涉及文件：

- `src/main.rs`
- `src/http.rs`
- `src/service.rs`
- 各模块入口文件
- `README.md`
- `plan.md`

实现内容：

1. 保持 `foo.rs + foo/` 模块风格，不使用 `mod.rs`。
2. 空目录不保留；只有真正需要子模块时才创建目录。
3. 每个新增 `.rs` 文件先写清楚中文模块注释。
4. `main.rs` 只声明真实存在的模块。

验证：

```powershell
cargo fmt --all --check
cargo check -p smalux-server
```

检查点：

- 没有 `mod query`。
- 没有旧 REST 文件命名残留。
- 没有旧 WebSocket 聚合文件或目录残留。
- 没有空目录。

## 阶段 1：CLI 和配置

目标：server 可以从启动参数得到稳定运行配置，所有参数有默认值，重要参数可由 CLI 覆盖。

涉及文件：

- `src/cli.rs`
- `src/cli/args.rs`
- `src/config.rs`
- `src/config/model.rs`
- `src/main.rs`

建议配置项：

```text
server.bind_addr
  -> 默认 127.0.0.1:3000。

server.public_base_url
  -> 可选，用于生成前端展示链接或回调地址。

database.url
  -> 默认 sqlite://smalux-server.db。

auth.agent_plain_token
  -> 可选，只用于 binary_plain 开发或兼容模式。

auth.allow_plain_none
  -> 默认 false，只有本地开发可打开。

auth.secure_psk_enabled
  -> 默认 true。

frontend.mode
  -> Disabled | Dir | Embedded，默认 Disabled。

frontend.dir
  -> 默认 apps/smalux-web/dist，只在 Dir 模式使用。

frontend.spa_fallback
  -> 默认 true。

realtime.enabled
  -> 默认 true。

limits.max_agent_connections
  -> 默认 1024。

limits.max_frame_bytes
  -> 默认按协议安全上限设置，防止超大 frame。

limits.command_ack_timeout_ms
  -> 默认 3000。

limits.command_result_timeout_ms
  -> 默认 30000。
```

实现步骤：

1. 在 `cli/args.rs` 定义 `ServerArgs`。
2. 使用 `clap` derive，给常用参数加长参数和短参数。
3. 在 `config/model.rs` 定义 `ServerConfig`、`HttpConfig`、`DatabaseConfig`、`AuthConfig`、`FrontendConfig`、`LimitConfig`。
4. 给配置实现 `Default`。
5. 实现 `ServerConfig::from_args(args)`。
6. 实现配置校验，例如端口格式、数据库 URL 非空、`frontend.mode=Dir` 时 `frontend.dir` 非空。
7. `main.rs` 只调用解析和校验，暂不启动 HTTP。

测试：

- 默认参数可以生成有效配置。
- CLI 参数可以覆盖默认值。
- `frontend.mode=Dir` 且目录为空时报错。
- `secure_psk_enabled=false` 且 `allow_plain_none=true` 只允许开发标记下使用。

## 阶段 2：日志和启动入口

目标：server 启动时初始化日志，输出清晰启动信息，失败时有明确错误。

涉及文件：

- `src/main.rs`
- `src/config/model.rs`
- `smalux-core/src/log.rs`

实现步骤：

1. `main.rs` 改为 `#[tokio::main] async fn main() -> anyhow::Result<()>`。
2. 解析 CLI。
3. 根据配置初始化日志。
4. 打印启动摘要，日志内容使用英文。
5. 不打印 token、secret、Authorization header、完整 URL query。
6. 调用后续 bootstrap 函数。

日志建议：

```text
info: server starting
info: http bind address configured
info: database backend configured
info: frontend mode configured
debug: cli arguments parsed
debug: server config validated
```

测试：

- 启动配置解析失败会返回错误。
- 敏感字段不会出现在 Debug 输出里。

## 阶段 3：存储接口和内存实现

目标：先不用 SQLite 也能跑通 agent 上报和 REST 查询，降低首轮实现复杂度。

涉及文件：

- `src/storage.rs`
- `src/storage/repository.rs`
- `src/storage/entity.rs`
- `src/storage/migration.rs`
- `src/service/agent.rs`
- `src/service/command.rs`

核心数据：

```text
AgentRecord
  agent_id
  hostname
  online_state
  last_seen_at
  first_seen_at
  latest_sequence
  latest_report_json
  delta_base_sequence
  public_ip_status

PendingCommand
  command_id
  agent_id
  sequence
  command_type
  payload_json
  status
  created_at
  acked_at
  finished_at
  error_json

RemoteTaskResult
  task_id
  agent_id
  status
  stdout
  stderr
  exit_code
  started_at
  finished_at

RemoteProbeResult
  task_id
  agent_id
  probe_type
  status
  result_json
  started_at
  finished_at
```

实现步骤：

1. 定义 `Storage` 或 `Repository` trait。
2. 先实现 `MemoryRepository`。
3. 支持 `apply_snapshot(agent_id, sequence, report)`。
4. 支持 `apply_delta(agent_id, sequence, base_sequence, patch)`。
5. 支持 `touch_heartbeat(agent_id, sequence, heartbeat)`。
6. 支持 `list_agents()`。
7. 支持 `get_agent_latest(agent_id)`。
8. 支持 `create_pending_command()`、`mark_command_acked()`、`mark_command_failed()`、`mark_command_finished()`。

并发要求：

- 内存实现可以用 `tokio::sync::RwLock`。
- 写入 snapshot/delta 时必须按 `agent_id` 串行更新，避免 delta base 被并发覆盖。
- 读 agent 列表不能长时间持有写锁。

测试：

- snapshot 可以创建 agent latest。
- 新 snapshot 覆盖旧 latest。
- heartbeat 只更新 `last_seen_at`，不覆盖 latest report。
- delta base 匹配时合并。
- delta base 不匹配时返回需要 `snapshot_request`。
- command ack 和 result 可以按 `command_id` 更新。

## 阶段 4：SQLite + SeaORM

目标：把内存 latest 状态落到 SQLite，server 重启后能恢复 latest、命令状态和任务结果。

涉及文件：

- `src/storage/entity.rs`
- `src/storage/migration.rs`
- `src/storage/repository.rs`
- `src/config/model.rs`

建议表：

```text
agents
  id
  agent_id
  hostname
  online_state
  first_seen_at
  last_seen_at
  latest_sequence
  delta_base_sequence
  latest_report_json
  public_ip_status
  created_at
  updated_at

agent_events
  id
  agent_id
  event_type
  sequence
  payload_json
  received_at

pending_commands
  id
  command_id
  agent_id
  sequence
  command_type
  payload_json
  status
  created_at
  acked_at
  finished_at
  error_json

remote_task_results
  id
  task_id
  agent_id
  status
  stdout
  stderr
  exit_code
  started_at
  finished_at
  updated_at

remote_probe_results
  id
  task_id
  agent_id
  probe_type
  status
  result_json
  started_at
  finished_at
  updated_at
```

实现步骤：

1. 写 SeaORM entity。
2. 写 migration。
3. 启动时自动运行 migration。
4. 实现 `SqliteRepository`。
5. 把 `MemoryRepository` 保留为测试或开发 fallback。
6. 在 `config` 中选择存储后端，首版默认 SQLite。

测试：

- migration 可重复执行。
- SQLite repository 和 Memory repository 通过同一组行为测试。
- upsert snapshot 不产生重复 agent。
- command/result upsert 幂等。

## 阶段 5：agent 认证

目标：让 agent 连接进入 ingest 前完成身份识别，认证逻辑集中在 `auth/agent.rs`。

涉及文件：

- `src/auth.rs`
- `src/auth/agent.rs`
- `src/config/model.rs`
- `src/http/agent.rs`
- `src/ingest/frame.rs`

认证模式：

```text
binary_plain + bearer token
  -> Authorization: Bearer <token>

binary_plain + query token
  -> 只用于兼容或开发，不推荐生产使用。

binary_plain + none
  -> 只允许本地开发或可信内网。

secure_psk
  -> agent Hello 携带 key_id，server 查 secret，Noise 握手成功后认证通过。
```

实现步骤：

1. 定义 `AgentAuthContext`，包含 `agent_id`、`auth_mode`、`key_id`、`connection_id`。
2. 定义 `AgentSecretStore` trait，用于 `key_id -> secret` 查询。
3. 实现开发期静态 token 校验。
4. 实现 bearer token 解析。
5. 对 query token 做单独分支，日志中只记录是否存在，不打印 token。
6. secure_psk 使用 `smalux-protocol` 提供的 secure 模块，不在 server 重写 HKDF/Noise。
7. 认证失败返回清晰 close reason 或 HTTP upgrade 前拒绝。

测试：

- bearer token 正确时通过。
- bearer token 错误时拒绝。
- query token 在禁用时拒绝。
- `allow_plain_none=false` 时 none 拒绝。
- secure_psk 不能叠加明文 token。
- 日志脱敏方法不会泄露 token。

## 阶段 6：agent WebSocket 主连接

目标：agent 可以连接 `/agent/v1/connect`，server 能读写 WebSocket，连接状态可注册和清理。

涉及文件：

- `src/http/agent.rs`
- `src/service/agent.rs`
- `src/service/command.rs`
- `src/ingest/frame.rs`
- `src/ingest/report.rs`

连接状态：

```text
accepted
  -> authenticating
  -> wire_negotiating
  -> ready
  -> closing
  -> disconnected
```

实现步骤：

1. 在 `http/router.rs` 注册 `/agent/v1/connect`。
2. 在 `http/agent.rs` 完成 WebSocket upgrade。
3. 生成 `connection_id`。
4. 调用 `auth::agent` 完成连接级认证。
5. 注册到 `service::agent` 的在线连接表。
6. 拆分 WebSocket read/write。
7. read loop 收到 binary/text 后交给 `ingest::frame`。
8. write loop 从 agent 专属发送队列读取 `ServerFrame`。
9. close、读失败、写失败都清理连接。
10. 同一个 `agent_id` 新连接进入时，旧连接应关闭或标记 replaced，避免命令发到旧连接。

队列建议：

```text
agent outbound queue
  -> 用于 server 发给单个 agent 的控制命令。
  -> 容量可以从配置读取。
  -> 默认丢弃最旧的低优先级消息，但 command 类消息不能静默丢弃，应标记失败或拒绝入队。

realtime broadcast queue
  -> 用于前端 dashboard 实时推送。
  -> 可以丢弃旧事件，因为前端可通过 REST 拉取最新状态补齐。
```

测试：

- agent 连接成功后 online。
- agent 断开后 offline。
- 同 agent 新连接替换旧连接。
- 写队列满时 command 不被静默丢弃。
- close 帧和 None 消息都能正常清理。

## 阶段 7：wire/frame 解包和 ingest

目标：server 复用 `smalux-protocol` 解包 `ClientFrame`，并按 payload 类型更新状态。

涉及文件：

- `src/ingest/frame.rs`
- `src/ingest/report.rs`
- `src/service/agent.rs`
- `src/service/command.rs`
- `src/storage/repository.rs`

处理流程：

```text
websocket message
  -> decode wire packet
  -> secure 模式先解密
  -> decode ClientFrame
  -> validate common fields
  -> dispatch by payload type
  -> update service/storage
  -> optionally enqueue ServerFrame
```

支持 payload：

```text
snapshot
  -> 校验 AgentReport，保存完整 latest。

delta
  -> 校验 base_sequence，匹配则合并，不匹配则请求 snapshot。

heartbeat
  -> 更新 last_seen_at，不覆盖 latest report。

ack
  -> 标记 server 下发命令已 ack。

error
  -> 标记 server 下发命令失败。

remote_task_result
  -> upsert remote task result。

remote_probe_result
  -> upsert remote probe result。

unknown
  -> 记录 warning 后忽略，不让单个未知类型打断连接。
```

测试：

- 合法 snapshot 入库成功。
- agent_id 和连接身份不一致时拒绝。
- schema/version 不支持时返回错误或请求 snapshot。
- delta base mismatch 会生成 `snapshot_request`。
- heartbeat 不改 latest report。
- unknown payload 不 panic。

## 阶段 8：server frame 下发和命令调度

目标：REST API 可以创建命令，server 通过 agent 主连接下发，agent 返回 ack/result 后可查询状态。

涉及文件：

- `src/service/command.rs`
- `src/service/agent.rs`
- `src/http/rest.rs`
- `src/storage/repository.rs`
- `src/ingest/frame.rs`

命令类型：

```text
snapshot_request
config_patch
remote_task_run
remote_probe_run
remote_shell_open
remote_shell_input
remote_shell_resize
remote_shell_close
ping
```

实现步骤：

1. 在 `service/command.rs` 定义命令创建接口。
2. 每个命令生成 `command_id` 和 server outbound sequence。
3. 命令先写入 `pending_commands`。
4. 查找 agent 当前在线连接。
5. 在线则入队发送。
6. 离线时按命令类型处理：
   - `config_patch` 可保存为 desired config。
   - 一次性命令默认拒绝或标记 waiting_offline。
   - shell 类命令必须在线。
7. agent ack 后更新 `acked_at`。
8. agent error 后更新失败状态。
9. agent result 后更新完成状态。

REST 返回建议：

```json
{
  "command_id": "uuid",
  "agent_id": "agent-id",
  "status": "pending",
  "created_at": "2026-06-08T00:00:00Z"
}
```

默认不阻塞等待命令完成。后续如需要同步等待，可增加：

```text
POST /api/v1/agents/{agent_id}/commands?wait=ack&timeout_ms=3000
```

测试：

- 在线 agent 创建命令后入队。
- 离线 agent 下发 shell 被拒绝。
- config_patch 可以保存 desired config。
- ack/result/error 幂等。
- command timeout 后状态正确。

## 阶段 9：REST API

目标：提供前端和管理端可用的 REST API，handler 简短，只做请求/响应转换。

涉及文件：

- `src/http/rest.rs`
- `src/http/router.rs`
- `src/service/agent.rs`
- `src/service/dashboard.rs`
- `src/service/command.rs`
- `src/auth/session.rs`

首版 API：

```text
GET /api/v1/health
  -> 返回 server 状态、版本、数据库连通状态。

GET /api/v1/agents
  -> 返回 agent 列表、在线状态、hostname、public_ip_status、last_seen_at。

GET /api/v1/agents/{agent_id}
  -> 返回 agent 基础信息和 latest 摘要。

GET /api/v1/agents/{agent_id}/latest
  -> 返回完整 latest report。

POST /api/v1/agents/{agent_id}/commands
  -> 创建控制命令。

GET /api/v1/commands/{command_id}
  -> 查询命令状态。

GET /api/v1/dashboard/summary
  -> 返回 dashboard 汇总。
```

DTO 放置建议：

- REST 请求 DTO 可以先放 `http/rest.rs`。
- 如果文件变长，再拆成 `http/rest/agent.rs`、`http/rest/command.rs`、`http/rest/dashboard.rs`。
- 不提前创建过多小文件。

测试：

- handler 返回 JSON。
- 错误统一为结构化错误。
- 404、400、401、403、500 语义清楚。
- REST handler 不直接访问 WebSocket sink。
- REST handler 不直接写 SQL。

## 阶段 10：前端实时通道

目标：前端可以订阅 dashboard 实时变化，但不依赖实时通道保证完整性。

涉及文件：

- `src/http/realtime.rs`
- `src/service/dashboard.rs`
- `src/service/agent.rs`
- `src/service/command.rs`

事件类型：

```text
agent_connected
agent_disconnected
report_updated
command_acked
command_finished
command_failed
snapshot_requested
```

设计原则：

- 实时事件是通知，不是唯一数据来源。
- 前端收到事件后可以用 REST 拉 latest 补齐。
- 队列满时可以丢弃旧 dashboard 事件。
- 命令结果不能只靠 realtime 保存，必须落库后再广播。

实现步骤：

1. 在 service 层创建 broadcast channel。
2. agent 状态变化时发事件。
3. report 更新后发 `report_updated`。
4. command 状态变化后发事件。
5. `http/realtime.rs` 提供 WebSocket 或 SSE。
6. 前端连接断开时清理订阅。

测试：

- report 更新会产生事件。
- 慢订阅者不会阻塞 agent ingest。
- 队列满时不会导致 server panic。
- 前端断开后订阅清理。

## 阶段 11：React/Vite 前端服务

目标：支持前端独立运行、目录托管和可选内嵌。

涉及文件：

- `src/http/frontend.rs`
- `src/http/router.rs`
- `src/config/model.rs`
- `Cargo.toml`

模式：

```text
Disabled
  -> 只服务 API 和 agent 通道。

Dir
  -> 使用 tower-http ServeDir 服务 dist。

Embedded
  -> 可选 feature，把 dist 打进二进制。
```

实现步骤：

1. 在 `config/model.rs` 定义 `FrontendMode` enum。
2. `http/frontend.rs` 实现 `frontend_service(config)`。
3. `Dir` 模式使用 `tower_http::services::ServeDir`。
4. SPA fallback 返回 `index.html`。
5. `Embedded` 模式先作为 feature 预留；需要时再加 `rust-embed`。
6. `http/router.rs` 确保 API/agent/live 路由优先，frontend fallback 最后。

测试：

- Disabled 模式不注册静态资源。
- Dir 模式能返回 `index.html`。
- SPA fallback 不影响 `/api/v1/health`。
- 不存在文件返回 fallback 或 404 的规则符合配置。

## 阶段 12：管理后台 session

目标：为前端管理 API 增加用户 session，不影响 agent 认证。

涉及文件：

- `src/auth/session.rs`
- `src/http/middleware.rs`
- `src/http/rest.rs`
- `src/storage/repository.rs`

实现步骤：

1. 使用 `tower-sessions` 建立 session middleware。
2. 定义登录接口。
3. 定义登出接口。
4. 定义当前用户接口。
5. REST 管理接口加 session 校验。
6. agent `/agent/v1/connect` 不使用管理后台 session。

测试：

- 未登录访问管理 API 返回 401。
- 登录后 session 可用。
- 登出后 session 失效。
- agent 连接不被 session middleware 误拦截。

## 阶段 13：Komari 兼容

目标：兼容逻辑可插拔，未来可以整体删除，不污染主协议路径。

建议新增：

```text
compat.rs
compat/
  komari.rs
```

实现原则：

- Smalux 自有协议是主路径。
- Komari 请求进入 `compat/komari.rs` 后转换成 server 内部命令或 report 应用。
- 不让 Komari 类型散落到 `service/agent.rs`、`ingest/report.rs` 或 `http/rest.rs`。
- 如果兼容远程 exec，应转换成通用 `remote_task` 或 shell command，不新写一套执行管线。

测试：

- Komari basic info 可转换为 agent 基础信息。
- Komari report 可更新 latest。
- Komari exec 可转换为通用命令。
- 删除 `compat/` 后主协议不受影响。

## 阶段 14：安全加固

目标：避免 token 泄露、明文误用、超大请求、未授权控制命令和日志泄密。

检查项：

1. 日志不打印 token、secret、PSK、Authorization header、带 token 的完整 URL。
2. `secure_psk` 模式不允许叠加 query/bearer token。
3. `binary_plain + none` 默认禁用。
4. WebSocket frame 有最大大小限制。
5. REST body 有最大大小限制。
6. 命令下发必须校验用户权限。
7. shell 和 remote task 必须有明确开关。
8. shell/task 是否允许只能由 CLI 启动参数决定，不能被 server 动态配置打开。
9. command id、task id、connection id 使用 server 生成 UUID。
10. 所有外部输入 DTO 都要校验。

测试：

- 带 token 的 URL 不进入日志。
- 超大 frame 被拒绝。
- 未认证 agent 被拒绝。
- 未登录用户不能下发命令。
- shell disabled 时任何 shell 命令都拒绝。

## 阶段 15：性能和并发

目标：agent 高频上报时 server 不被慢前端、慢数据库或慢命令阻塞。

设计要求：

- agent read loop 不等待前端 realtime 发送完成。
- command 类消息不能被静默丢弃。
- dashboard realtime 可以丢弃旧事件，因为 REST 可补最新状态。
- SQLite 写入应串行或受控并发，避免锁竞争。
- report latest 更新优先保证最新状态正确，历史写入可以后续异步化。
- 同 agent 多连接时，必须明确选择最新连接或拒绝新连接。

建议队列：

```text
agent_command_queue
  -> 单 agent 下行命令。
  -> 满时 command 创建失败或标记失败，不静默丢弃。

realtime_event_queue
  -> 前端实时事件。
  -> 满时允许丢弃旧事件。

storage_write_queue
  -> 如果 SQLite 写入压力大，再引入。
  -> 首版可先直接 repository 写入。
```

测试：

- 慢前端不会阻塞 agent 上报。
- agent 快速 snapshot 不导致内存无限增长。
- 队列满时行为符合规则。
- 同 agent 重连后命令不会发给旧连接。

## 阶段 16：测试策略

每个阶段完成后至少跑：

```powershell
cargo fmt --all --check
cargo check -p smalux-server
cargo test -p smalux-server
```

推荐测试分层：

```text
unit tests
  -> config 校验、auth 校验、ingest 分发、repository 行为。

integration tests
  -> axum router、REST handler、WebSocket agent 连接。

protocol tests
  -> 使用 smalux-protocol 测试向量验证 secure_psk 和 frame decode。

storage tests
  -> MemoryRepository 和 SqliteRepository 共用同一行为测试。
```

首批必须补的测试：

1. 默认配置可用。
2. CLI 覆盖配置。
3. agent 认证成功/失败。
4. snapshot 写入 latest。
5. heartbeat 只更新时间。
6. delta base mismatch 触发 snapshot_request。
7. REST `GET /api/v1/agents` 返回列表。
8. REST 创建 command 后 pending。
9. agent disconnect 后 online 状态改变。
10. frontend fallback 不抢 API 路由。

## 阶段 17：文档同步

每完成一个阶段，必须同步：

- `crates/smalux-server/README.md`
- `crates/smalux-server/plan.md`
- 如果协议变化，同步 `crates/smalux-protocol/README.md`
- 如果 agent 调用方式变化，同步 `crates/smalux-agent/README.md`

文档检查项：

- agent 主连接路径统一使用 `/agent/v1/connect`，不要再写旧路径。
- 前端 REST 使用 `/api/v1/*`。
- 前端实时使用 `/live/v1/*`。
- 不再把 `query/` 作为 server 源码目录。
- `sea-query` 只能作为 SQL builder 依赖出现。
- URL query token 说明要写清楚是认证传参，不是源码模块。

## 多轮检查流程

每次实现或重构后按三轮检查：

### 第 1 轮：结构检查

```powershell
rg --files crates/smalux-server/src
rg -n -F 'mod query' crates/smalux-server/src
rg -n -F 'http/api' crates/smalux-server/src crates/smalux-server/README.md
rg -n -F 'http/ws' crates/smalux-server/src crates/smalux-server/README.md
rg -n -F '/agent/v1/connect' crates/smalux-server/src crates/smalux-server/README.md crates/smalux-server/plan.md
```

目标：

- 模块文件和目录一致。
- 没有旧命名残留。
- agent 主连接路径统一为 `/agent/v1/connect`。
- 没有空目录。

### 第 2 轮：编译和测试

```powershell
cargo fmt --all --check
cargo check -p smalux-server
cargo test -p smalux-server
```

目标：

- 格式化通过。
- server crate 编译通过。
- 测试通过。

### 第 3 轮：文档和安全检查

```powershell
rg -n -F 'query/' crates/smalux-server/src crates/smalux-server/README.md
rg -n -F 'token' crates/smalux-server/README.md crates/smalux-server/plan.md
rg -n -F 'secret' crates/smalux-server/README.md crates/smalux-server/plan.md
```

目标：

- `query/` 只允许出现在“已删除”或“不要使用”的说明中；`query/bearer` 是 URL query token 认证说明，不是源码目录。
- token/secret 说明必须强调脱敏。
- secure_psk 不能和 query/bearer token 混用。

## 推荐实现顺序总览

```text
1. CLI + config
2. 日志 + main bootstrap
3. MemoryRepository
4. storage 行为测试
5. auth agent
6. http router
7. agent websocket
8. binary_plain frame ingest
9. snapshot/heartbeat latest
10. REST agents/latest
11. command pending/ack/result
12. secure_psk
13. SQLite/SeaORM migration
14. realtime dashboard
15. frontend ServeDir
16. session auth
17. Komari compat
18. 历史指标和高级查询
```

## 当前下一步建议

当前 server 还只是骨架。最适合马上开始的是：

1. 实现 `cli/args.rs` 和 `config/model.rs`。
2. 把 `main.rs` 改为 async bootstrap。
3. 实现 `MemoryRepository`。
4. 实现 `GET /api/v1/health`。
5. 实现 `/agent/v1/connect` 的最小 WebSocket upgrade。

这样可以最快得到一个可运行 server，然后再逐步接 agent frame、REST 查询和数据库。
