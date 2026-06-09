# smalux-server 实现计划

本文档用于指导 `smalux-server` 从当前目录骨架逐步实现为可运行的服务端。计划按“先跑通 agent 上报闭环，再补 REST API，再补前端实时和管理功能”的顺序推进，避免一开始把数据库、前端、加密、远程命令全部揉在一起。

当前 server 目标：

- 接收 `smalux-agent` 主连接和上报数据。
- 通过 REST API 提供 agent 列表、latest 状态、命令下发和结果查询。
- 支持前端实时推送，用于 dashboard live update。
- 支持 React/Vite 前端独立运行、目录托管和可选二进制内嵌。
- 复用 `smalux-protocol` 的 frame、wire、secure 逻辑，不在 server 重复实现协议。
- 使用 SQLite / PostgreSQL / MySQL + SeaORM 做持久化，首版先以 latest state 为主，历史数据后续再扩展。

## 如何阅读和执行

这份计划分成三层：

1. `阶段 0-17` 是架构路线，说明 server 最终要有哪些能力，以及每个大阶段的边界。
2. `Milestone 详细执行清单` 是可交付版本，说明每个里程碑做到什么程度可以进入下一步。
3. `任务包级施工计划` 是编码顺序，后续真正开发时优先按任务包推进。

实际执行时不要从头到尾一次实现。推荐只盯住当前任务包：

```text
当前任务包
  -> 涉及文件
  -> 具体步骤
  -> 完成标准
  -> 测试和检查
  -> README 同步
```

阅读顺序建议：

```text
当前状态
  -> 总体边界
  -> 路由规划
  -> 当前下一步建议
  -> 执行矩阵
  -> 当前任务包
```

如果只是确认模块放哪里，优先看 `总体边界` 和 `最终目录快照`。

如果要开始写代码，优先看 `任务包级施工计划`。

如果要确认 server 和 agent 怎么交互，优先看 `交互流程施工图`。

如果要提交前检查，优先看 `每轮实现后的固定复盘` 和 `优先提交边界`。

## 当前完成度定义

当前 server 只算“结构准备阶段”，还不能算 agent server 已完成。

完成度按下面标准判断：

```text
0. 结构准备
  -> 目录、依赖、文档和模块边界准备好。

1. 可启动
  -> CLI/config/bootstrap/health 完成，server 能监听 HTTP。

2. 可接入
  -> agent 能连接 /agent/v1/connect，连接状态可注册和清理。

3. 可上报
  -> snapshot/heartbeat 能进入 latest state，REST 能查 latest。

4. 可控制
  -> REST 能创建命令，server 能下发，agent ack/result 能回收。

5. 可生产化
  -> secure_psk、SQLite、session、限流、日志脱敏和错误处理完成。

6. 可扩展
  -> realtime、frontend hosting、Komari compat、历史指标等扩展完成。
```

当前下一步只应该推进到 `1. 可启动`，不要提前做 secure、agent 凭据管理或 Komari。

## 当前状态

已经完成：

- server crate 依赖已配置。
- `query/` 模块已删除，前端读模型职责下沉到 `service/agent.rs` 和 `service/dashboard.rs`。
- 自有协议 agent 主连接路径统一为 `/agent/v1/connect`。
- 已预建 `bootstrap.rs`、`state.rs`、`cli/`、`config/`、`auth/`、`http/`、`ingest/`、`service/`、`storage/` 骨架。
- 前端托管规则已确定：是否内嵌由 `frontend-embed` 编译 feature 决定，运行时只配置 `frontend.enabled`、`frontend.dir`、`frontend.spa_fallback`。

当前不做：

- 不实现业务逻辑前不再新增更深目录。
- 不提前实现 Komari server 兼容。
- 不引入 gRPC、OpenAPI、密码哈希、外部 HTTP client。
- 不把前端是否 embedded 做成运行时字符串参数。

下一步优先级：

1. 实现 `cli/args.rs`、`cli/startup.rs`、`config/model.rs`、`config/validation.rs`。
2. 把 `main.rs` 改成只调用 `bootstrap::run().await`。
3. 实现 `MemoryRepository` 和最小 `GET /api/v1/health`。
4. 实现 `/agent/v1/connect` 最小 WebSocket upgrade。
5. 接入 binary_plain snapshot/heartbeat 到 latest state。

## 总体边界

server 当前按下面几个模块分工：

```text
main.rs
  -> 启动入口，后续串联日志、CLI、配置、数据库、HTTP server。

bootstrap.rs
  -> 启动编排，负责把日志、CLI、配置、数据库、HTTP server 和后台任务串起来。

state.rs
  -> 共享状态，负责定义 axum handler 和后台服务共用的 AppState。

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
  -> 数据库 entity、migration、repository 和内存/数据库存储适配。
```

最终目录快照：

```text
src/
  main.rs
  bootstrap.rs
  state.rs
  cli.rs
  cli/
    args.rs
    startup.rs      # 后续实现 CLI -> StartupOptions 时再创建
  config.rs
  config/
    defaults.rs
    model.rs
    validation.rs
  auth.rs
  auth/
    agent.rs
    session.rs
    permission.rs   # 后续实现用户权限时再创建
  http.rs
  http/
    router.rs
    rest.rs
    rest/           # REST handler 变长后再创建
      agent.rs
      command.rs
      dashboard.rs
      health.rs
    agent.rs
    realtime.rs
    frontend.rs
    middleware.rs
  ingest.rs
  ingest/
    frame.rs
    report.rs
    control.rs      # ack/error/remote result 逻辑变长后再创建
  service.rs
  service/
    agent.rs
    command.rs
    connection.rs
    dashboard.rs
    event.rs
  storage.rs
  storage/
    entity.rs
    memory.rs
    migration.rs
    repository.rs
  compat.rs         # 需要第三方 server 兼容时再创建
  compat/
    komari.rs
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
- `src/cli/startup.rs`
- `src/config.rs`
- `src/config/defaults.rs`
- `src/config/model.rs`
- `src/config/validation.rs`
- `src/main.rs`

建议配置项：

```text
server.bind_addr
  -> 默认 127.0.0.1:3000。

database.url
  -> 默认 sqlite://smalux-server.db；支持 sqlite://、postgres://、postgresql:// 和 mysql://。

auth.agent_credentials
  -> 不属于启动配置；添加 agent 时由 server 动态生成 token/key 并保存到数据库。

frontend.enabled
  -> 默认 false；是否由 server 托管前端。

frontend.dir
  -> 默认 apps/smalux-web/dist；没有编译 frontend-embed 且 frontend.enabled=true 时使用。

frontend.spa_fallback
  -> 默认 true。

log.file
  -> 默认 logs/smalux-server.log；日志级别仍由 RUST_LOG 控制。

log.retention_files
  -> 默认 14。

log.max_size_mb
  -> 默认 64。
```

最终 server CLI 只保留下面 8 个参数：

```text
--bind, -b
--database-url, -d
--serve-frontend
--frontend-dir
--frontend-spa-fallback
--log-file
--log-retention-files, -L
--log-max-size-mb
```

不要加入到首版 server CLI：

```text
agent token/key/secure key path
remote task/shell/probe allow 开关
public_base_url
max_agent_connections
max_request_body_bytes
agent outbound queue capacity
dashboard realtime queue capacity
```

原因：

- agent 凭据是业务数据，应在添加 agent 时生成并存数据库，不能作为 server 进程级固定参数。
- remote task/shell/probe 是否可用由 agent 启动授权、管理端权限和 agent 当前状态共同决定，不是 server 启动开关。
- 连接数、请求大小和队列容量要等对应 middleware/registry/queue 真正实现后再加配置，避免 CLI 先承诺未实现行为。
- `public_base_url` 目前没有 server 运行时必需场景，部署层或前端需要时再单独设计。

实现步骤：

1. 在 `cli/args.rs` 定义 `ServerArgs`。
2. 使用 `clap` derive，给常用参数加长参数和短参数。
3. 在 `cli/startup.rs` 定义 `StartupOptions`，只做 CLI 到启动输入的转换。
4. 在 `config/model.rs` 定义 `ServerConfig`、`HttpConfig`、`DatabaseConfig`、`FrontendConfig`、`LogConfig`、`AuthConfig`。
5. 在 `config/defaults.rs` 集中默认值。
6. 给配置实现 `Default`。
7. 实现 `ServerConfig::from_startup_options(options)`。
8. 在 `config/validation.rs` 实现配置校验，例如端口格式、数据库 URL 非空且 scheme 支持、`frontend.enabled=true` 且没有内置前端时 `frontend.dir` 非空、日志滚动参数大于 0。
9. `main.rs` 只调用 bootstrap，暂不直接解析 CLI。

测试：

- 默认参数可以生成有效配置。
- CLI 参数可以覆盖默认值。
- `frontend.enabled=true` 且没有内置前端时，目录配置为空时报错。
- agent token/key 不从 CLI 读取，后续由添加 agent 接口生成并存库。

## 阶段 2：日志和启动入口

目标：server 启动时初始化日志，输出清晰启动信息，失败时有明确错误。

涉及文件：

- `src/main.rs`
- `src/bootstrap.rs`
- `src/config/model.rs`
- `smalux-core/src/log.rs`

实现步骤：

1. `main.rs` 改为 `#[tokio::main] async fn main() -> anyhow::Result<()>`。
2. `main.rs` 只调用 `bootstrap::run().await`。
3. `bootstrap::run()` 解析 CLI。
4. `bootstrap::run()` 生成并校验配置。
5. 根据配置初始化日志。
6. 打印启动摘要，日志内容使用英文。
7. 不打印 token、secret、Authorization header、完整 URL query。
8. 后续再接数据库和 HTTP server。

日志建议：

```text
info: server starting
info: http bind address configured
info: database backend configured
info: frontend hosting configured
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
- `src/storage/memory.rs`
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

## 阶段 4：数据库持久化 + SeaORM

目标：把内存 latest 状态落到数据库，server 重启后能恢复 latest、命令状态和任务结果。首版可以先实现 SQLite，PostgreSQL/MySQL 通过同一 repository 边界继续扩展。

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
6. 在 `config` 中根据 `database.url` 识别数据库后端，默认 SQLite。

测试：

- migration 可重复执行。
- 数据库 repository 和 Memory repository 通过同一组行为测试。
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

secure_psk
  -> agent Hello 携带 key_id，server 查 secret，Noise 握手成功后认证通过。
```

实现步骤：

1. 定义 `AgentAuthContext`，包含 `agent_id`、`auth_mode`、`key_id`、`connection_id`。
2. 定义 `AgentSecretStore` trait，用于 `key_id -> secret` 查询。
3. 从数据库或内存 repository 查询已登记 agent 的 token/key。
4. 实现 bearer token 解析，token 来源是添加 agent 时生成的记录。
5. 对 query token 做单独分支，日志中只记录是否存在，不打印 token。
6. secure_psk 使用 `smalux-protocol` 提供的 secure 模块，不在 server 重写 HKDF/Noise。
7. 认证失败返回清晰 close reason 或 HTTP upgrade 前拒绝。

测试：

- bearer token 正确时通过。
- bearer token 错误时拒绝。
- query token 对应的 agent 未登记时拒绝。
- 未登记 agent 或凭据不匹配时拒绝。
- secure_psk 不能叠加明文 token。
- 日志脱敏方法不会泄露 token。

## 阶段 6：agent WebSocket 主连接

目标：agent 可以连接 `/agent/v1/connect`，server 能读写 WebSocket，连接状态可注册和清理。

涉及文件：

- `src/http/agent.rs`
- `src/state.rs`
- `src/service/agent.rs`
- `src/service/connection.rs`
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
- `src/service/event.rs`

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
frontend.enabled=false
  -> 不服务前端，只服务 API、agent 通道和前端实时通道。

frontend.enabled=true + 没有编译 frontend-embed
  -> 使用 tower-http ServeDir 服务 frontend.dir。

frontend.enabled=true + 编译了 frontend-embed
  -> 使用编译进 server 二进制的前端资源。
```

实现步骤：

1. 在 `config/model.rs` 定义 `FrontendConfig { enabled, dir, spa_fallback }`。
2. `http/frontend.rs` 实现 `frontend_service(config)`。
3. 未编译 `frontend-embed` 时使用 `tower_http::services::ServeDir`。
4. SPA fallback 返回 `index.html`。
5. `frontend-embed` 作为编译 feature 预留；需要时再加 `rust-embed`。
6. `http/router.rs` 确保 API/agent/live 路由优先，frontend fallback 最后。

测试：

- `frontend.enabled=false` 时不注册静态资源。
- `frontend.enabled=true` 且未编译内置前端时，目录托管能返回 `index.html`。
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

另外手动确认没有旧的 CLI 嵌套目录，也没有旧的 agent 主连接路径，避免文档自身被搜索命中后误判。

目标：

- 模块文件和目录一致。
- 没有旧命名残留。
- `cli` 保持顶层模块，不嵌入配置目录。
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

按 milestone 推进，避免每个阶段都同时碰 HTTP、存储和协议。

```text
Milestone 1: 可启动
  1. CLI + StartupOptions
  2. ServerConfig + defaults + validation
  3. bootstrap::run()
  4. GET /api/v1/health

Milestone 2: agent 最小闭环
  5. MemoryRepository
  6. agent auth 最小实现
  7. /agent/v1/connect WebSocket upgrade
  8. binary_plain ClientFrame decode
  9. snapshot/heartbeat -> latest state

Milestone 3: 前端 REST 查询
  10. GET /api/v1/agents
  11. GET /api/v1/agents/{agent_id}/latest
  12. dashboard summary

Milestone 4: server 控制 agent
  13. pending command
  14. snapshot_request
  15. config_patch
  16. ack/error/result

Milestone 5: 安全和持久化
  17. secure_psk
  18. SQLite/SeaORM migration
  19. SqliteRepository

Milestone 6: UI 和扩展
  20. realtime dashboard
  21. frontend ServeDir / frontend-embed
  22. session auth
  23. Komari compat
  24. 历史指标和高级查询
```

## 当前下一步建议

当前 server 还是骨架。下一步只做 Milestone 1：

1. 实现 `cli/args.rs`、`cli/startup.rs`。
2. 实现 `config/model.rs`、`config/defaults.rs`、`config/validation.rs`。
3. 把 `main.rs` 改为 async，并调用 `bootstrap::run().await`。
4. 在 `bootstrap.rs` 串联 CLI、配置校验和日志初始化。
5. 在 `state.rs` 定义最小 `AppState`。
6. 在 `http/router.rs` 和 `http/rest.rs` 增加最小 `GET /api/v1/health`。

完成 Milestone 1 后再进入 `MemoryRepository` 和 `/agent/v1/connect`，不要在第一步同时实现数据库、secure_psk 或前端托管。

## 关键决策记录

这些决策会影响后续实现，除非重新评估，否则编码时按这里执行。

| 决策 | 当前选择 | 原因 | 后续如果要改 |
| --- | --- | --- | --- |
| 模块风格 | `foo.rs + foo/`，不使用 `mod.rs` | 和 agent 保持一致，文件入口清晰 | 全项目统一评估后再改 |
| agent 主连接路径 | `/agent/v1/connect` | 和 REST `/api/v1/*` 分开，避免职责混淆 | agent/server 文档和代码必须一起改 |
| 前端 REST 路径 | `/api/v1/*` | 只服务前端和管理端 API | 新版本用 `/api/v2/*`，不要覆盖 v1 |
| 前端实时路径 | `/live/v1/dashboard` | 和 REST、agent 主连接分开 | 如果改 SSE/WS 细节，路径可不变 |
| 前端托管 | `frontend.enabled` + `frontend.dir` + 编译 feature | 运行时不拼字符串模式，减少无效组合 | 如果引入多前端，再扩展配置结构 |
| 自有协议优先级 | Smalux 自有协议是主路径 | Komari 只是兼容，不污染主流程 | 新兼容协议放 `compat/` |
| 加密实现 | 复用 `smalux-protocol` | server 和 agent 不重复实现 Noise/HKDF | 如果协议变化，先改 protocol crate |
| agent 凭据 | 添加 agent 时动态生成 token/key 并存库 | 避免启动参数写死凭据，后续可管理和轮换 | 如要改为外部密钥源，需要先设计 secret store |
| shell/task 开关 | 只能 CLI 启动时开启，server 动态配置不能打开 | 避免远程配置提升执行能力 | 如需放开必须重新做权限模型 |
| 数据库 | SQLite / PostgreSQL / MySQL + SeaORM | SQLite 适合单机部署，PostgreSQL/MySQL 适合后续多实例或更高写入压力 | 先通过 `database.url` scheme 区分后端，不在 CLI 增加单独数据库类型参数 |
| latest state | 首版优先保存 latest | 先保证可用，再考虑历史指标 | 历史表在 SQLite 阶段后扩展 |
| realtime | 通知，不是事实来源 | 慢前端不能影响 agent ingest | 前端收到事件后用 REST 补数据 |

## 风险和排查索引

| 问题 | 优先检查 | 可能原因 | 处理方式 |
| --- | --- | --- | --- |
| server 启动失败 | `bootstrap.rs`、`config/validation.rs` | 配置非法、bind 地址被占用、文件不可读 | fail-fast，日志打印字段名，不打印敏感值 |
| `/api/v1/health` 404 | `http/router.rs` | route 没注册或被 frontend fallback 抢占 | API route 必须先注册，fallback 放最后 |
| agent 连不上 | `http/agent.rs`、`auth/agent.rs` | 路径错误、认证拒绝、WebSocket upgrade 失败 | 先确认 `/agent/v1/connect` 和认证配置 |
| agent 在线但没有 latest | `ingest/frame.rs`、`ingest/report.rs` | frame 解码失败、snapshot 未分发、agent_id 不一致 | 打印 frame 类型和脱敏 agent id |
| heartbeat 覆盖了 snapshot | `storage/repository.rs` | heartbeat 路径误写 latest report | heartbeat 只更新 `last_seen_at` |
| delta 后数据错乱 | `storage/repository.rs` | base sequence 未校验或并发覆盖 | base mismatch 请求 snapshot，不覆盖 latest |
| 命令创建成功但 agent 没收到 | `service/command.rs`、`service/connection.rs` | agent 离线、连接被替换、queue 满 | command 不静默丢弃，状态写 failed/expired |
| ack/result 找不到命令 | `ingest/frame.rs`、`service/command.rs` | command id 不一致、重复连接、旧结果 | warning 后忽略或标记 orphan event |
| 前端实时卡住 agent | `service/event.rs`、`http/realtime.rs` | broadcast 队列使用错误，等待慢订阅者 | realtime 只能通知，不能阻塞 ingest |
| token 出现在日志 | `auth/agent.rs`、日志调用点 | 打印完整 URL/header/config Debug | 用脱敏工具，禁止打印完整 query |
| Komari 影响主协议 | `compat/komari.rs`、`http/router.rs` | 兼容 DTO 散落到 service/ingest | Komari 只通过内部 service 边界转换 |

## 错误响应和状态码规范

REST API 首版统一返回结构化错误，避免 handler 各写各的。

建议错误 JSON：

```json
{
  "error": {
    "code": "agent_not_found",
    "message": "Agent not found",
    "request_id": "optional-request-id",
    "details": {
      "agent_id": "agent-001"
    }
  }
}
```

状态码建议：

| 状态码 | 场景 | 示例 |
| --- | --- | --- |
| 400 | 请求格式错误 | JSON body 非法、字段类型错误 |
| 401 | 未认证 | 管理 API 未登录、agent token 错误 |
| 403 | 无权限或能力未开启 | remote shell 未通过 CLI 开启 |
| 404 | 资源不存在 | agent 不存在、command 不存在 |
| 409 | 状态冲突 | agent 离线但命令要求在线 |
| 413 | 请求过大 | REST body 超过限制、frame 超过限制 |
| 422 | 语义校验失败 | delta base 缺失、command type 不支持 |
| 500 | 内部错误 | repository 写入失败 |
| 503 | 服务暂不可用 | 数据库未连接、server 正在关闭 |

错误处理规则：

- 对外 `message` 使用英文，方便日志和 API 客户端统一处理。
- 文档说明使用中文。
- `details` 只能放非敏感字段。
- 不返回 token、secret、PSK、Authorization header。
- 内部错误日志可以更详细，但仍要脱敏。

WebSocket/agent 错误规则：

```text
认证失败
  -> upgrade 前返回 401；upgrade 后 close。

协议解码失败
  -> warning + close，避免继续处理未知状态。

unknown payload
  -> warning + ignore，不影响连接。

agent_id 不一致
  -> warning + close。

delta base mismatch
  -> 不关闭连接，发送 snapshot_request。
```

## Milestone 详细执行清单

### Milestone 1：可启动

目标：server 能解析启动参数、生成配置、初始化日志，并提供最小健康检查。此阶段不连接数据库，不接 agent，不做认证。

涉及文件：

```text
src/main.rs
src/bootstrap.rs
src/state.rs
src/cli.rs
src/cli/args.rs
src/cli/startup.rs
src/config.rs
src/config/defaults.rs
src/config/model.rs
src/config/validation.rs
src/http.rs
src/http/router.rs
src/http/rest.rs
```

具体步骤：

1. 在 `config/defaults.rs` 定义默认监听地址、数据库 URL 和前端目录。
2. 在 `cli/args.rs` 定义 `ServerArgs`，使用 `clap::Parser`。
3. CLI 首批参数只实现启动必需项：
   - `--bind`, `-b`
   - `--database-url`, `-d`
   - `--serve-frontend`
   - `--frontend-dir`
   - `--frontend-spa-fallback`
   - `--log-file`
   - `--log-retention-files`, `-L`
   - `--log-max-size-mb`
4. 在 `cli/startup.rs` 定义 `StartupOptions`，负责从 `ServerArgs` 转成启动输入。
5. 在 `config/model.rs` 定义 `ServerConfig`、`HttpConfig`、`DatabaseConfig`、`AuthConfig`、`FrontendConfig`、`LogConfig`。
6. 在 `config/validation.rs` 实现 `validate_server_config()`。
7. 在 `bootstrap.rs` 实现 `run()`：
   - 解析 CLI。
   - 转成 `StartupOptions`。
   - 生成 `ServerConfig`。
   - 校验配置。
   - 初始化日志。
   - 构建 `AppState`。
   - 构建 axum router。
   - 启动 HTTP server。
8. 在 `state.rs` 定义最小 `AppState`，首版只持有 `Arc<ServerConfig>`。
9. 在 `http/router.rs` 注册 `/api/v1/health`。
10. 在 `http/rest.rs` 实现 health handler，返回版本、状态和当前时间。
11. `main.rs` 只保留模块声明和 `bootstrap::run().await`。

验收标准：

- `cargo run -p smalux-server -- --help` 能显示参数。
- `cargo run -p smalux-server -- -b 127.0.0.1:3000` 能启动。
- `GET /api/v1/health` 返回 JSON。
- 启动日志不打印 token、secret、Authorization header。
- `frontend.enabled=false` 时不注册前端 fallback。
- `frontend.enabled=true` 且未编译内置前端时，会校验 `frontend.dir`。

测试和检查：

```powershell
cargo fmt --all --check
cargo check -p smalux-server
cargo test -p smalux-server
cargo run -p smalux-server -- --help
```

推荐测试：

- 默认 CLI 能生成有效配置。
- `--bind` 能覆盖默认监听地址。
- `--serve-frontend` 能打开前端托管标记。
- token 和 secret 字段 Debug 输出脱敏。
- health handler 返回 `status=ok`。

### Milestone 2：agent 最小闭环

目标：agent 可以通过 `/agent/v1/connect` 连上 server，server 能接收 `binary_plain` snapshot/heartbeat 并写入 latest state。

涉及文件：

```text
src/http/agent.rs
src/http/router.rs
src/auth/agent.rs
src/ingest/frame.rs
src/ingest/report.rs
src/service/agent.rs
src/service/connection.rs
src/storage/repository.rs
src/storage/memory.rs
src/state.rs
```

具体步骤：

1. 在 `storage/repository.rs` 定义 `AgentRepository` trait。
2. 在 `storage/memory.rs` 实现 `MemoryRepository`。
3. 在 `service/connection.rs` 定义 `AgentConnectionRegistry`。
4. 在 `service/agent.rs` 定义 `AgentService`，封装 latest 读写和在线状态。
5. 在 `auth/agent.rs` 实现 `binary_plain` 的最小认证：
   - bearer/query token 只做最小校验。
   - token 来源是添加 agent 时生成并保存的记录。
6. 在 `http/agent.rs` 实现 WebSocket upgrade。
7. 连接建立后注册 connection id。
8. read loop 接收 WebSocket binary。
9. `ingest/frame.rs` 调用 `smalux-protocol` 解 `WirePacket` 和 `ClientFrame`。
10. `ingest/report.rs` 处理 `snapshot` 和 `heartbeat`。
11. snapshot 写入 latest state。
12. heartbeat 只更新 `last_seen_at`。
13. 断开连接时清理在线状态。

验收标准：

- agent 使用 `smalux_json + binary_plain` 可以连上。
- server 收到 snapshot 后 latest state 有数据。
- heartbeat 不覆盖 latest report。
- 同 agent 新连接会替换旧连接或明确拒绝旧连接，不能双活发送命令。
- unknown frame type 不 panic。

测试和检查：

```powershell
cargo test -p smalux-server
cargo test -p smalux-agent export_endpoint_derives_wss_from_https_base_url
rg -n -F '/api/agents/connect' crates/smalux-agent crates/smalux-server/src crates/smalux-server/README.md
```

推荐测试：

- WebSocket upgrade 成功。
- 未认证连接被拒绝。
- snapshot 写入 latest。
- heartbeat 只更新时间。
- agent disconnect 后 online=false。

### Milestone 3：前端 REST 查询

目标：前端可以通过 REST 查询 agent 列表、单个 agent latest 和 dashboard summary。

涉及文件：

```text
src/http/rest.rs
src/service/agent.rs
src/service/dashboard.rs
src/storage/repository.rs
src/state.rs
```

具体步骤：

1. 在 `http/rest.rs` 注册 `/api/v1/agents`。
2. 实现 `GET /api/v1/agents`。
3. 实现 `GET /api/v1/agents/{agent_id}`。
4. 实现 `GET /api/v1/agents/{agent_id}/latest`。
5. 在 `service/dashboard.rs` 实现 dashboard summary。
6. 实现统一 REST error response。
7. 如果 `http/rest.rs` 超过可读范围，再拆 `http/rest/agent.rs`、`dashboard.rs`、`health.rs`。

验收标准：

- REST handler 不直接写 SQL。
- REST handler 不直接访问 WebSocket sink。
- response DTO 不暴露 token/secret。
- 空列表、未知 agent、无 latest 都有明确响应。

推荐测试：

- agent 列表为空时返回 `[]`。
- snapshot 后 agent 列表包含该 agent。
- latest endpoint 返回完整 latest report。
- 未知 agent 返回 404。

### Milestone 4：server 控制 agent

目标：server 可以通过 REST 创建控制命令，经 agent 主连接下发，并接收 ack/error/result。

涉及文件：

```text
src/http/rest.rs
src/service/command.rs
src/service/connection.rs
src/ingest/frame.rs
src/storage/repository.rs
```

具体步骤：

1. 定义 `CommandRecord` 和 command status。
2. 实现 `POST /api/v1/agents/{agent_id}/commands`。
3. 创建 command 时先写 pending。
4. 找在线 connection。
5. 在线则下发 `ServerFrame`。
6. 离线时按命令类型拒绝或保存 desired state。
7. 处理 agent `ack`。
8. 处理 agent `error`。
9. 处理 `remote_task_result` 和 `remote_probe_result`。
10. 实现 `GET /api/v1/commands/{command_id}`。

验收标准：

- command 不会静默丢弃。
- ack/result/error 幂等。
- shell/task/probe 权限按 CLI 静态开关控制。
- config_patch 不作为普通一次性命令长期排队。

### Milestone 5：安全和持久化

目标：支持 `secure_psk` 和 SQLite 持久化，server 重启后可恢复 latest 与命令状态。

具体步骤：

1. 在 `auth/agent.rs` 接 `key_id -> secret`。
2. 复用 `smalux-protocol::secure` 实现 Noise responder。
3. 禁止 `secure_psk` 同时使用 query/bearer token。
4. 在 `storage/entity.rs` 定义 SeaORM entity。
5. 在 `storage/migration.rs` 定义 migration。
6. 实现 `SqliteRepository`。
7. MemoryRepository 和 SqliteRepository 共用行为测试。

验收标准：

- HKDF/Noise 参数和 protocol README 测试向量一致。
- token、secret、PSK 不进日志。
- migration 可重复执行。
- snapshot upsert 不产生重复 agent。

### Milestone 6：UI 和扩展

目标：补前端实时、前端托管、session 和第三方兼容。

具体步骤：

1. 在 `service/event.rs` 定义 dashboard event。
2. 在 `http/realtime.rs` 实现 WebSocket 或 SSE。
3. 在 `http/frontend.rs` 实现 `frontend.enabled` 规则。
4. 未编译 `frontend-embed` 时使用 `frontend.dir`。
5. 编译 `frontend-embed` 时使用内置资源。
6. 在 `auth/session.rs` 接管理后台 session。
7. 需要兼容第三方时再创建 `compat.rs` 和 `compat/komari.rs`。

验收标准：

- 慢前端不阻塞 agent ingest。
- 前端 fallback 不抢 `/api/v1/*`、`/agent/v1/connect`、`/live/v1/*`。
- session 不影响 agent 主连接。
- 删除 `compat/` 不影响自有协议。

## 任务包级施工计划

下面是后续真正编码时的最小执行单元。每个任务包都应该独立完成、独立验证、独立提交或至少独立记录变更，避免一次修改跨太多模块导致难以排查。

执行规则：

1. 每个任务包开始前先确认涉及文件是否仍符合当前目录结构。
2. 每个任务包只处理自己的边界，不顺手实现下一阶段功能。
3. 每个任务包完成后至少运行 `cargo fmt --all --check` 和 `cargo check -p smalux-server`。
4. 涉及 handler、service、repository 行为时同步补测试。
5. 涉及协议、路径、CLI 参数、JSON 响应时同步更新 README。
6. 日志内容使用英文，代码注释和文档说明使用中文。
7. 所有 token、secret、Authorization header、带 token 的 URL query 都必须脱敏。

### 任务包统一执行模板

每个任务包都按下面顺序执行，不因为任务小就跳过验证。

```text
1. 读文件
  -> 读取涉及文件。
  -> 确认当前模块声明和文件位置。
  -> 确认是否有用户未提交改动。

2. 写最小测试
  -> 能写单元测试就先写单元测试。
  -> handler/service/repository 行为必须有测试。
  -> 纯骨架任务至少补编译检查。

3. 做最小实现
  -> 只实现当前任务包完成标准需要的代码。
  -> 不提前实现下一任务包。
  -> 不引入暂时用不到的新目录。

4. 补错误路径
  -> 明确非法输入怎么返回。
  -> 明确队列满、连接断开、解码失败怎么处理。
  -> 日志必须脱敏。

5. 补文档
  -> CLI 参数变化更新 README。
  -> API/JSON 变化更新 README。
  -> 协议变化更新 smalux-protocol README。

6. 验证
  -> cargo fmt --all --check
  -> cargo check -p smalux-server
  -> cargo test -p smalux-server
  -> 有 endpoint 时手动 curl 或 router 测试。

7. 复盘
  -> 检查文件是否放对位置。
  -> 检查是否有旧命名残留。
  -> 检查是否需要拆文件，但不硬拆。
```

每个任务包完成后记录：

```text
完成了什么：
  -> 类型/函数/endpoint/测试。

没有做什么：
  -> 明确没做的下一阶段能力。

验证结果：
  -> fmt/check/test 是否通过。

下一步：
  -> 下一个任务包。
```

### 每步编码的最小循环

开发时每个小步骤按下面循环，不要一口气写完整阶段：

```text
1. 添加或更新一个类型/函数
2. 编译当前 crate
3. 补一个对应测试
4. 运行该测试
5. 再继续下一个类型/函数
```

推荐的检查粒度：

```powershell
cargo check -p smalux-server
cargo test -p smalux-server <test_name>
cargo test -p smalux-server
```

如果一个任务包里出现三次以上“为了继续需要顺便实现别的模块”，说明边界拆得不对，应暂停重新拆任务。

### 执行矩阵

| 任务包 | 前置条件 | 主要产出 | 可验证能力 | 是否可并行 |
| --- | --- | --- | --- | --- |
| 1. 启动参数模型 | 当前骨架可编译 | `ServerArgs`、`StartupOptions` | `--help` 正常输出 | 不建议并行 |
| 2. 配置模型和默认值 | 任务包 1 | `ServerConfig` 和默认值 | 默认配置可生成 | 不建议并行 |
| 3. 配置校验 | 任务包 2 | `validate_server_config()` | 错误配置 fail-fast | 不建议并行 |
| 4. bootstrap 和最小启动 | 任务包 1-3 | `bootstrap::run()` | server 可启动 | 不建议并行 |
| 5. 最小 health API | 任务包 4 | `/api/v1/health` | 浏览器/curl 可探活 | 可和任务包 6 准备并行 |
| 6. 内存存储接口 | 任务包 2 | repository trait + memory | latest 行为测试 | 可和任务包 5 并行 |
| 7. agent service 和连接注册 | 任务包 6 | `AgentService`、连接 registry | 在线状态可维护 | 不建议并行 |
| 8. agent 认证最小实现 | 任务包 2、4 | `AgentAuthContext` | 未认证会拒绝 | 可和任务包 7 部分并行 |
| 9. agent WebSocket 主连接 | 任务包 7、8 | `/agent/v1/connect` | agent 可连接/断开 | 不建议并行 |
| 10. snapshot ingest | 任务包 6、7、9 | snapshot 写 latest | latest 可查询 | 不建议并行 |
| 11. heartbeat/delta ingest | 任务包 10 | heartbeat/delta 应用 | 局部更新可用 | 不建议并行 |
| 12. agent 查询 REST | 任务包 6、10 | agent/latest REST | 前端可查 latest | 可和任务包 11 部分并行 |
| 13. 命令创建和下发 | 任务包 7、9、12 | pending command + outbound | server 可下发命令 | 不建议并行 |
| 14. ack/error/result 回收 | 任务包 13 | command 状态回写 | 结果可查询 | 不建议并行 |
| 15. secure_psk 接入 | 任务包 9、10 | secure 握手/加解密 | 加密连接可用 | 独立阶段 |
| 16. SQLite 和 SeaORM | 任务包 6、10、14 | `SqliteRepository` | 重启恢复 latest | 独立阶段 |
| 17. 前端实时事件 | 任务包 10、14 | dashboard event stream | 前端可订阅变化 | 可和任务包 18 并行 |
| 18. 前端静态资源托管 | 任务包 4、5 | ServeDir/embed fallback | 前端资源可访问 | 可和任务包 17 并行 |
| 19. 管理后台 session | 任务包 12、18 | session middleware | REST 权限可控 | 独立阶段 |
| 20. Komari 兼容 | 任务包 10、13、14 | `compat/komari.rs` | Komari 可兼容接入 | 独立阶段 |

最小可运行闭环：

```text
任务包 1 -> 2 -> 3 -> 4 -> 5
```

agent 最小上报闭环：

```text
任务包 1 -> 2 -> 3 -> 4 -> 6 -> 7 -> 8 -> 9 -> 10 -> 12
```

server 控制 agent 闭环：

```text
任务包 13 -> 14
```

安全和生产化闭环：

```text
任务包 15 -> 16 -> 19
```

### Milestone 1 函数级交付物

Milestone 1 结束时，建议已经具备下面这些明确函数或类型。名称可以按实际代码微调，但职责不要变。

```text
cli::args::ServerArgs
  -> clap 参数结构，只负责从命令行读取原始输入。

cli::startup::StartupOptions
  -> 启动输入结构，只承接 CLI/env 解析后的值。

cli::startup::StartupOptions::from_args(args)
  -> 把 ServerArgs 转换为 StartupOptions。

config::model::ServerConfig
  -> 运行时最终配置。

config::model::ServerConfig::from_startup_options(options)
  -> 合并默认值和启动输入。

config::validation::validate_server_config(config)
  -> 启动前校验配置，返回结构化错误。

state::AppState
  -> axum handler 和后续 service 共享状态。

state::AppState::new(config)
  -> 构建最小共享状态。

http::router::build_router(state)
  -> 组合所有 HTTP route。

http::rest::health(state)
  -> 最小健康检查 handler。

bootstrap::run()
  -> 启动总编排。
```

Milestone 1 不应该出现：

- agent WebSocket read/write loop。
- repository trait 的完整实现。
- SeaORM migration。
- secure_psk 握手。
- Komari route。
- 远程 shell 或 remote task 逻辑。

### Milestone 1 建议测试清单

单元测试：

1. `ServerConfig::default()` 可通过校验。
2. `--bind` 能覆盖默认监听地址。
3. `--database-url` 能覆盖默认数据库地址。
4. `--serve-frontend` 能打开前端托管标记。
5. `frontend.enabled=true` 且 `frontend.dir` 为空时校验失败。
6. token、secret 类字段不出现在 CLI Debug 输出中。

router 测试：

1. `GET /api/v1/health` 返回 200。
2. health JSON 中 `status=ok`。
3. health JSON 中不包含 token 或 secret。
4. 未注册 agent endpoint 前，`/agent/v1/connect` 不应该被 frontend fallback 接管。

手动检查：

```powershell
cargo run -p smalux-server -- --help
cargo run -p smalux-server -- -b 127.0.0.1:3000
```

### Milestone 1 实际实施清单

Milestone 1 建议拆成 7 个小步骤完成，每一步都能单独编译。

#### Step 1：CLI 模块可编译

改动文件：

```text
src/cli.rs
src/cli/args.rs
src/cli/startup.rs
src/main.rs
```

实现内容：

1. `cli.rs` 只声明子模块。
2. `args.rs` 定义 `ServerArgs`。
3. `startup.rs` 定义 `StartupOptions`。
4. `main.rs` 暂时不需要使用这些类型，但模块必须能编译。

检查：

```powershell
cargo check -p smalux-server
```

完成后不应该有：

- HTTP server。
- config 校验。
- health route。

#### Step 2：配置模型可编译

改动文件：

```text
src/config.rs
src/config/defaults.rs
src/config/model.rs
src/config/validation.rs
```

实现内容：

1. `defaults.rs` 放默认常量。
2. `model.rs` 放配置结构。
3. `validation.rs` 先提供 `validate_server_config()`。
4. `ServerConfig::default()` 能构建默认配置。

检查：

```powershell
cargo check -p smalux-server
cargo test -p smalux-server default_server_config_is_valid
```

完成后不应该有：

- 文件加载逻辑。
- 数据库连接逻辑。
- WebSocket 逻辑。

#### Step 3：CLI 合并到配置

改动文件：

```text
src/cli/startup.rs
src/config/model.rs
```

实现内容：

1. `StartupOptions::from_args()` 保留 CLI 原始输入。
2. `ServerConfig::from_startup_options()` 合并默认值和启动输入。
3. CLI 未传字段使用默认值。
4. CLI 传了字段只覆盖对应配置。

检查：

```powershell
cargo test -p smalux-server cli_bind_overrides_default_bind
cargo test -p smalux-server cli_database_url_overrides_default_database_url
```

完成后不应该有：

- handler。
- router。
- storage。

#### Step 4：bootstrap 串起来

改动文件：

```text
src/main.rs
src/bootstrap.rs
src/state.rs
```

实现内容：

1. `main.rs` 改为 async。
2. `main.rs` 只调用 `bootstrap::run().await`。
3. `bootstrap::run()` 完成 CLI -> StartupOptions -> ServerConfig -> validate。
4. `state.rs` 定义 `AppState`。
5. 暂时可以先不启动 server，或者启动空 router。

检查：

```powershell
cargo check -p smalux-server
cargo run -p smalux-server -- --help
```

完成后不应该有：

- agent route。
- command service。
- SQLite migration。

#### Step 5：最小 router 和 health

改动文件：

```text
src/http.rs
src/http/router.rs
src/http/rest.rs
src/state.rs
src/bootstrap.rs
```

实现内容：

1. `http.rs` 声明 `router` 和 `rest`。
2. `router.rs` 提供 `build_router(state)`。
3. `rest.rs` 提供 health handler。
4. `bootstrap.rs` 启动 axum server。

检查：

```powershell
cargo check -p smalux-server
cargo test -p smalux-server health_returns_ok
```

手动检查：

```powershell
cargo run -p smalux-server -- -b 127.0.0.1:3000
```

然后访问：

```text
GET http://127.0.0.1:3000/api/v1/health
```

#### Step 6：日志和脱敏检查

改动文件：

```text
src/bootstrap.rs
src/config/model.rs
smalux-core/src/log.rs
```

实现内容：

1. bootstrap 初始化日志。
2. 启动日志打印 bind、database、frontend。
3. `Debug` 或日志摘要不打印 token/secret。
4. 使用已有 core 日志工具，不在方法里临时拼 tracing 初始化。

检查：

```powershell
cargo test -p smalux-server sensitive_config_debug_is_redacted
```

#### Step 7：README 同步

改动文件：

```text
crates/smalux-server/README.md
crates/smalux-server/plan.md
```

实现内容：

1. README 写清 CLI 参数。
2. README 写清 `/api/v1/health` 响应。
3. README 写清当前还不支持 agent 接入。
4. plan.md 标记 Milestone 1 已完成。

最终检查：

```powershell
cargo fmt --all --check
cargo check -p smalux-server
cargo test -p smalux-server
rg -n -F '/api/agents/connect' crates/smalux-agent crates/smalux-server/src crates/smalux-server/README.md
```

### Milestone 2 函数级交付物

Milestone 2 结束时，建议具备这些能力：

```text
storage::repository::AgentRepository
  -> latest、heartbeat、command 状态的抽象接口。

storage::memory::MemoryRepository
  -> 首版内存实现。

service::connection::AgentConnectionRegistry
  -> 当前在线连接表。

service::connection::AgentConnectionHandle
  -> 单个 agent 当前连接和下行队列。

service::agent::AgentService
  -> agent latest、online/offline、heartbeat 的业务入口。

auth::agent::authenticate_agent()
  -> 连接级认证。

http::agent::agent_connect()
  -> `/agent/v1/connect` upgrade handler。

ingest::frame::handle_client_message()
  -> 入站 WebSocket 消息到 ClientFrame。

ingest::report::apply_snapshot()
  -> snapshot 应用。

ingest::report::apply_heartbeat()
  -> heartbeat 应用。
```

Milestone 2 的关键断言：

- 连接不是数据来源，`ClientFrame` 才是数据来源。
- agent_id 必须来自认证上下文和 frame 校验结果，不能只相信 URL query。
- write loop 只负责发送，不负责决定业务状态。
- read loop 只负责收包、解码、分发，不直接写 SQL。
- 最新状态由 repository/service 维护，不散落在 WebSocket handler 局部变量里。

### Milestone 2 实际实施清单

Milestone 2 拆成 6 个小步骤：

#### Step 1：MemoryRepository

先实现 repository trait 和内存 latest。完成后必须能测试 snapshot/heartbeat，不需要 WebSocket。

检查：

```powershell
cargo test -p smalux-server apply_snapshot_creates_agent_latest
cargo test -p smalux-server touch_heartbeat_updates_last_seen_only
```

#### Step 2：AgentService

把 repository 包起来，提供上层服务接口。REST 和 ingest 后续都只能调用 service。

检查：

```powershell
cargo test -p smalux-server agent_service_applies_snapshot_through_repository
```

#### Step 3：ConnectionRegistry

实现连接注册、替换、清理。重点测试旧连接断开不能清掉新连接。

检查：

```powershell
cargo test -p smalux-server old_connection_disconnect_does_not_clear_new_connection
```

#### Step 4：AgentAuth

先做 binary_plain 的 bearer/query/none 识别。secure_psk 只保留接口。

检查：

```powershell
cargo test -p smalux-server bearer_token_accepts_matching_token
cargo test -p smalux-server plain_none_rejected_by_default
```

#### Step 5：WebSocket connect

接 `/agent/v1/connect`，完成 upgrade、注册连接、断开清理。此时可以还不解 snapshot。

检查：

```powershell
cargo test -p smalux-server agent_connect_registers_connection_after_auth
cargo test -p smalux-server disconnect_clears_current_connection
```

#### Step 6：snapshot/heartbeat ingest

WebSocket read loop 收到 binary 后走 `ingest::frame`，解出 snapshot/heartbeat 并写 latest。

检查：

```powershell
cargo test -p smalux-server decode_plain_snapshot_updates_latest
cargo test -p smalux-server heartbeat_updates_last_seen_only
```

Milestone 2 完成后，README 必须写清：

- agent 主连接路径。
- 当前支持的 wire/auth mode。
- snapshot/heartbeat 已支持。
- delta、secure_psk、command 是否已支持。

### Milestone 3 函数级交付物

Milestone 3 结束时，建议具备这些 REST DTO 和 handler：

```text
http::rest::ListAgentsResponse
http::rest::AgentSummaryResponse
http::rest::AgentLatestResponse
http::rest::DashboardSummaryResponse
http::rest::list_agents()
http::rest::get_agent()
http::rest::get_agent_latest()
http::rest::dashboard_summary()
service::dashboard::build_dashboard_summary()
```

REST 响应原则：

- 列表接口返回摘要，避免一次返回完整大 JSON。
- latest 接口才返回完整 report。
- 404 用于未知 agent。
- 没有 latest 的已知 agent 可以返回 204 或结构化 `latest=null`，实现前二选一并写入 README。
- REST DTO 不直接暴露 repository 内部锁、连接 ID、token、secret。

### Milestone 3 实际实施清单

Milestone 3 拆成 4 个小步骤：

#### Step 1：Agent 查询 service

实现 `list_agents()`、`get_agent_summary()`、`get_agent_latest()`，只返回前端需要的数据。

检查：

```powershell
cargo test -p smalux-server list_agents_returns_empty_array
cargo test -p smalux-server get_unknown_agent_returns_404
```

#### Step 2：REST DTO 和错误结构

实现统一错误响应和 agent 查询 DTO，先不加 session。

检查：

```powershell
cargo test -p smalux-server rest_error_does_not_expose_sensitive_fields
```

#### Step 3：REST route

注册：

```text
GET /api/v1/agents
GET /api/v1/agents/{agent_id}
GET /api/v1/agents/{agent_id}/latest
GET /api/v1/dashboard/summary
```

检查：

```powershell
cargo test -p smalux-server list_agents_route_returns_json
cargo test -p smalux-server latest_route_returns_latest_report
```

#### Step 4：README 同步

README 写清 REST 路径、响应字段、错误结构和暂不支持的功能。

Milestone 3 完成后，前端应该可以只靠 REST 读取 agent latest，不依赖 realtime。

### Milestone 4 函数级交付物

Milestone 4 结束时，建议具备这些能力：

```text
service::command::CommandService
service::command::create_command()
service::command::dispatch_command()
service::command::handle_ack()
service::command::handle_error()
service::command::handle_remote_task_result()
service::command::handle_remote_probe_result()
http::rest::create_agent_command()
http::rest::get_command()
```

命令状态建议：

```text
created
queued
sent
acked
running
finished
failed
cancelled
expired
```

状态流转约束：

- `created -> queued -> sent -> acked -> running -> finished`
- `created/queued/sent/acked/running -> failed`
- `created/queued -> expired`
- `finished/failed/cancelled/expired` 是终态。
- 重复 ack/result 不应破坏终态，最多刷新 `updated_at` 或忽略。

### Milestone 4 实际实施清单

Milestone 4 拆成 5 个小步骤：

#### Step 1：Command 存储模型

定义 command record、status、result record。MemoryRepository 先实现。

检查：

```powershell
cargo test -p smalux-server create_command_stores_pending_command
```

#### Step 2：CommandService 创建命令

实现命令校验、能力开关校验、command id 生成。

检查：

```powershell
cargo test -p smalux-server create_remote_task_rejected_when_disabled
```

#### Step 3：下发到 agent

在线 agent 走 connection registry 入队；离线 agent 按命令类型拒绝或保存 desired config。

检查：

```powershell
cargo test -p smalux-server create_command_for_online_agent_queues_frame
cargo test -p smalux-server create_shell_command_for_offline_agent_rejected
```

#### Step 4：ack/error/result 回收

ingest 识别 ack/error/result，更新 command 状态和 result。

检查：

```powershell
cargo test -p smalux-server ack_marks_command_acked
cargo test -p smalux-server remote_task_result_marks_command_finished
```

#### Step 5：REST 查询 command

实现：

```text
POST /api/v1/agents/{agent_id}/commands
GET  /api/v1/commands/{command_id}
```

检查：

```powershell
cargo test -p smalux-server get_command_returns_command_status
```

Milestone 4 完成后，README 必须写清：

- 支持哪些 command type。
- 哪些命令要求 agent 在线。
- shell/task/probe 由 CLI 静态开关控制。
- REST 创建命令默认不等待执行完成。

### Milestone 5 函数级交付物

Milestone 5 结束时，建议具备这些能力：

```text
auth::agent::AgentSecretStore
auth::agent::StaticAgentSecretStore
ingest::frame::SecureFrameSession
storage::repository::SqliteRepository
storage::migration::Migrator
```

secure_psk 注意点：

- server 只通过 `key_id` 查 secret，不能把 `key_id` 当成认证成功。
- Noise 握手成功后才建立可信会话。
- secure 会话状态应挂在 connection 上，不放全局。
- 明文 bearer/query token 和 secure_psk 不能叠加使用。
- 加解密失败应关闭连接，不进入业务分发。

SQLite 注意点：

- latest state upsert 应幂等。
- event history 可以后续再做，不要拖慢首版 latest。
- migration 应可重复执行。
- repository 行为测试要同时覆盖 memory 和 sqlite。

### Milestone 6 函数级交付物

Milestone 6 结束时，建议具备这些能力：

```text
service::event::DashboardEvent
service::event::EventPublisher
http::realtime::dashboard_stream()
http::frontend::frontend_service()
auth::session::SessionAuth
compat::komari::router()
```

扩展边界：

- 前端 realtime 是通知，不是事实来源。
- 前端静态资源 fallback 必须最后注册。
- session 只保护管理 API，不保护 agent 主连接。
- Komari 兼容必须通过内部 service/repository 边界，不直接改 storage 内部结构。

### 交互流程施工图

#### 启动流程

```text
main()
  -> bootstrap::run()
  -> cli::args::ServerArgs::parse()
  -> cli::startup::StartupOptions::from_args()
  -> config::model::ServerConfig::from_startup_options()
  -> config::validation::validate_server_config()
  -> smalux_core::log 初始化日志
  -> storage 初始化，首版 MemoryRepository，后续 SqliteRepository
  -> service 初始化，注入 repository、connection registry、event publisher
  -> state::AppState::new()
  -> http::router::build_router()
  -> axum serve
```

启动流程输入：

- CLI 参数。
- 环境变量，首版只通过 clap/env 支持必要项。
- 可选 token file。
- 可选 frontend dir。

启动流程输出：

- 一个监听中的 HTTP server。
- 一个共享 `AppState`。
- 可访问的 `/api/v1/health`。
- 清晰的英文启动日志。

启动失败条件：

- bind 地址非法。
- 必要文件路径非法或不可读。
- 限制参数为 0 或明显无效。
- 前端托管启用但目录配置不满足当前 feature 规则。
- 数据库初始化失败，SQLite 接入后启用。

#### agent 上报流程

```text
agent
  -> GET /agent/v1/connect
  -> http::agent::agent_connect()
  -> auth::agent::authenticate_agent()
  -> service::connection::register()
  -> websocket read loop
  -> ingest::frame::handle_client_message()
  -> smalux_protocol decode wire packet
  -> plain decode 或 secure decrypt + decode
  -> ClientFrame payload dispatch
  -> ingest::report::apply_snapshot/apply_delta/apply_heartbeat()
  -> service::agent 更新业务状态
  -> storage::repository 写 latest/heartbeat
  -> service::event 发布 report_updated
```

agent 上报流程输入：

- WebSocket binary frame。
- 认证上下文。
- 当前 connection id。
- 当前 sequence。

agent 上报流程输出：

- latest state 更新。
- last_seen_at 更新。
- dashboard event。
- 必要时生成 `snapshot_request` 下行命令。

agent 上报失败处理：

- frame 过大：关闭连接或返回 protocol error。
- 解码失败：记录 warning，必要时关闭连接。
- agent_id 不一致：关闭连接。
- delta base 不匹配：不覆盖 latest，生成 snapshot request。
- unknown payload：记录 warning 后忽略，不 panic。

#### REST 查询流程

```text
frontend/admin
  -> GET /api/v1/agents
  -> http::rest handler
  -> auth::session 校验，首版可暂不启用
  -> service::agent/list latest summary
  -> storage::repository 查询
  -> response DTO
```

REST 查询流程输入：

- path 参数。
- query 参数。
- session 身份，管理后台阶段启用。

REST 查询流程输出：

- agent list。
- single agent summary。
- latest report。
- dashboard summary。

REST 查询约束：

- handler 不直接写 SQL。
- handler 不访问 WebSocket sink。
- response DTO 不暴露 token、secret、connection sender。
- 404、400、401、403、500 使用统一错误结构。

#### 命令下发流程

```text
frontend/admin
  -> POST /api/v1/agents/{agent_id}/commands
  -> http::rest::create_agent_command()
  -> auth::session + permission 校验
  -> service::command::create_command()
  -> storage::repository::create_command()
  -> service::connection 查当前在线连接
  -> smalux_protocol encode ServerFrame
  -> agent outbound queue
  -> websocket write loop
  -> agent
```

命令回收流程：

```text
agent
  -> ack/error/remote_task_result/remote_probe_result
  -> websocket read loop
  -> ingest::frame
  -> service::command::handle_ack/handle_error/handle_result()
  -> storage::repository 更新 command/result
  -> service::event 发布 command event
  -> GET /api/v1/commands/{command_id} 可查询
```

命令流程约束：

- command id 由 server 生成。
- REST 默认不等待执行完成。
- command 入队失败不能静默丢弃。
- shell/task/probe 只能由 CLI 启动参数允许，不能被 server 动态打开。
- config_patch 后续可以走 desired config，不一定作为普通一次性命令排队。

#### 前端实时流程

```text
service::event 发布 DashboardEvent
  -> broadcast channel
  -> http::realtime::dashboard_stream()
  -> frontend
  -> frontend 收到事件后用 REST 拉最新状态
```

前端实时约束：

- realtime 是通知，不是事实来源。
- 慢前端不能阻塞 agent ingest。
- 队列满时可以丢弃旧 dashboard event。
- command result 必须先落 repository，再广播事件。

#### 前端静态资源流程

```text
browser
  -> /*
  -> http::router 先匹配 /api/v1/*、/agent/v1/connect、/live/v1/*
  -> http::frontend fallback
  -> ServeDir 或 embedded asset
```

前端托管约束：

- `frontend.enabled=false` 时不注册 fallback。
- `frontend.enabled=true` 且编译 `frontend-embed` 时使用内置资源。
- `frontend.enabled=true` 且未编译 `frontend-embed` 时使用 `frontend.dir`。
- fallback 永远最后注册。

### 任务包 1：启动参数模型

目标：只把 CLI 参数解析出来，不启动 HTTP server，不做业务逻辑。

改动文件：

```text
src/cli.rs
src/cli/args.rs
src/cli/startup.rs
```

步骤：

1. 在 `cli.rs` 声明 `pub mod args;` 和 `pub mod startup;`。
2. 在 `cli/args.rs` 定义 `ServerArgs`。
3. 给 `ServerArgs` 实现 `clap::Parser`。
4. 添加基础参数：
   - `--bind`, `-b`
   - `--database-url`, `-d`
   - `--serve-frontend`
   - `--frontend-dir`
   - `--frontend-spa-fallback`
   - `--log-file`
   - `--log-retention-files`, `-L`
   - `--log-max-size-mb`
5. `--serve-frontend` 支持两种写法：
   - `--serve-frontend`
   - `--serve-frontend true|false`
6. `--frontend-spa-fallback` 默认 `true`，需要关闭时传 `--frontend-spa-fallback false`。
7. 不添加 agent token/key 参数，这些凭据由添加 agent 流程动态生成并保存到数据库。
8. 不添加 remote task/shell/probe 开关；这是 agent 能力和管理权限，不是 server 启动参数。
9. 在 `cli/startup.rs` 定义 `StartupOptions`。
10. 实现 `StartupOptions::from_args(args)`。
11. 不在 `StartupOptions` 中做复杂校验，只做路径、字符串、开关的原样承接。

完成标准：

- `cargo run -p smalux-server -- --help` 能看到全部参数。
- `ServerArgs` 中没有业务状态字段。
- `StartupOptions` 不依赖 axum、storage、service。

文件级执行顺序：

1. 先改 `src/cli.rs`：
   - 只声明 `pub mod args;`
   - 只声明 `pub mod startup;`
   - 不写业务函数。
2. 再改 `src/cli/args.rs`：
   - 写 `ServerArgs`。
   - 按分类组织字段：HTTP、database、frontend、log。
   - 字段名使用清晰全称，不使用 `pri`、`cfg` 这种缩写。
3. 再改 `src/cli/startup.rs`：
   - 写 `StartupOptions`。
   - 写 `impl From<ServerArgs> for StartupOptions` 或 `from_args()`。
   - 保持原始输入，不做复杂校验。
4. 最后临时调整 `bootstrap.rs` 或测试入口：
   - 只验证 `ServerArgs::parse()` 能编译。
   - 不提前启动 HTTP server。

字段命名建议：

```text
bind_addr
database_url
serve_frontend
frontend_dir
frontend_spa_fallback
log_file
log_retention_files
log_max_size_mb
```

暂时不要加入：

- 管理员用户名/密码。
- session cookie secret。
- OpenAPI 开关。
- gRPC 地址。
- Komari server token。
- 历史指标保留周期。

这些参数都等对应任务包开始时再加，避免 CLI 先膨胀。

### 任务包 2：配置模型和默认值

目标：把启动输入转换成稳定的 `ServerConfig`，所有配置都有默认值。

改动文件：

```text
src/config.rs
src/config/defaults.rs
src/config/model.rs
src/config/validation.rs
src/cli/startup.rs
```

步骤：

1. 在 `config.rs` 声明 `defaults`、`model`、`validation`。
2. 在 `config/defaults.rs` 定义默认常量。
3. 默认监听地址使用 `127.0.0.1:3000`。
4. 默认数据库地址使用 `sqlite://smalux-server.db`。
5. 默认 `frontend.enabled=false`。
6. 默认 `frontend.dir=apps/smalux-web/dist`。
7. 默认 `frontend.spa_fallback=true`。
8. agent token/key 不设置默认值，添加 agent 时由 server 动态生成并存库。
9. remote task/shell/probe 不在 server CLI 中配置，后续由 agent 能力和管理权限决定。
10. 在 `config/model.rs` 定义：
    - `ServerConfig`
    - `HttpConfig`
    - `DatabaseConfig`
    - `AuthConfig`
    - `FrontendConfig`
11. 给模型实现 `Default`。
12. 实现 `ServerConfig::from_startup_options(options)`。
13. token、secret 类字段的 `Debug` 输出必须脱敏。

完成标准：

- 默认配置可直接通过校验。
- CLI 覆盖只影响对应字段。
- 日志或 Debug 不泄露敏感值。

文件级执行顺序：

1. 先改 `src/config/defaults.rs`：
   - 集中默认值常量。
   - 常量名使用 `DEFAULT_*`。
   - 不引用 clap 或 axum。
2. 再改 `src/config/model.rs`：
   - 定义配置结构。
   - 按 `http`、`database`、`frontend`、`log`、`auth` 分组。
   - 敏感字段使用 `secrecy` 或自定义脱敏 wrapper。
3. 再改 `src/config/validation.rs`：
   - 先只写函数签名和基础校验。
   - 不在这里加载文件内容。
4. 最后回到 `src/cli/startup.rs`：
   - 确认 CLI 字段能映射到配置字段。
   - 缺失项使用默认值。

配置结构建议：

```text
ServerConfig
  http: HttpConfig
  database: DatabaseConfig
  frontend: FrontendConfig
  log: LogConfig
  auth: AuthConfig

HttpConfig
  bind_addr

DatabaseConfig
  url

FrontendConfig
  enabled
  dir
  spa_fallback

LogConfig
  file
  retention_files
  max_size_mb

AuthConfig
  # 不包含启动时写死的 agent token/key。
  # agent 凭据由添加 agent 流程生成，并通过 storage/auth 查询。

```

注意：

- agent token/key 不是启动配置，不在 CLI 或默认值中出现。
- 默认配置允许 server 启动，但未添加 agent 时不会有任何 agent 凭据可用。
- agent 认证后续从数据库中读取已登记 agent 的 token/key。

### 任务包 3：配置校验

目标：启动前 fail-fast，避免错误配置进入运行态。

改动文件：

```text
src/config/validation.rs
src/config/model.rs
```

步骤：

1. 校验 bind 地址可解析为 `SocketAddr`。
2. 校验 database URL 非空，并且 scheme 是 `sqlite`、`postgres`、`postgresql` 或 `mysql`。
3. 校验 `frontend.enabled=true` 且未编译 `frontend-embed` 时，`frontend.dir` 非空。
4. 校验 `log.retention_files > 0`。
5. 校验 `log.max_size_mb > 0`。
6. agent token/key 不属于启动配置，不在这里校验。
7. 返回结构化错误，错误信息不要包含 token 或 secret。

完成标准：

- 错误配置启动前失败。
- 校验错误有明确字段名。
- 测试覆盖默认配置、非法监听地址、非法数据库 URL、前端目录缺失和非法日志参数。

文件级执行顺序：

1. 在 `config/validation.rs` 定义 `ConfigValidationError`。
2. 实现 `validate_server_config(config)`。
3. 先校验纯内存字段：
   - bind 地址。
   - URL 非空。
   - 布尔开关组合。
4. 再校验路径字段：
   - `frontend.dir`。
5. 路径存在性校验只判断启动必须条件：
   - 前端目录只有 `frontend.enabled=true` 且未编译内置资源时必需。
   - agent token/key 由添加 agent 流程生成并存库，不做启动路径校验。
6. 编写测试覆盖每个错误分支。

错误信息规则：

- 可以打印字段名，例如 `frontend.dir`。
- 可以打印文件路径。
- 不打印文件内容。
- 不打印 token、secret、PSK。
- 不打印完整带 query 的 URL。

### 任务包 4：bootstrap 和最小启动

目标：server 能启动并绑定 HTTP 地址，入口保持干净。

改动文件：

```text
src/main.rs
src/bootstrap.rs
src/state.rs
src/http.rs
src/http/router.rs
src/http/rest.rs
```

步骤：

1. `main.rs` 改成 `#[tokio::main] async fn main() -> anyhow::Result<()>`。
2. `main.rs` 只调用 `bootstrap::run().await`。
3. `bootstrap::run()` 解析 `ServerArgs`。
4. 转成 `StartupOptions`。
5. 生成 `ServerConfig`。
6. 调用配置校验。
7. 初始化日志。
8. 构建 `AppState`。
9. 构建 axum router。
10. 绑定监听地址。
11. 打印启动摘要：
    - bind address
    - database backend
    - frontend enabled
    - secure mode enabled
    - database backend
12. 启动 axum server。

完成标准：

- `cargo run -p smalux-server -- -b 127.0.0.1:3000` 能启动。
- 启动日志清晰，且不包含敏感值。
- `main.rs` 不直接出现配置解析、路由构建、数据库初始化细节。

文件级执行顺序：

1. 先改 `src/main.rs`：
   - 保留模块声明。
   - `main()` 只调用 `bootstrap::run().await`。
2. 再改 `src/bootstrap.rs`：
   - 解析 CLI。
   - 构建 config。
   - 校验 config。
   - 初始化日志。
   - 构建 state。
   - 构建 router。
   - bind server。
3. 再改 `src/state.rs`：
   - 定义 `AppState`。
   - 首版只持有 `Arc<ServerConfig>` 和 `started_at`。
4. 再改 `src/http/router.rs`：
   - 暴露 `build_router(state)`。
   - 先只接 health route。
5. 最后手动启动验证。

启动日志建议：

```text
info: server starting
debug: cli arguments parsed
debug: server config validated
info: http listener binding
info: frontend hosting disabled
info: agent credential store configured
info: remote task disabled
info: remote shell disabled
info: server started
```

注意：

- 日志不要打印完整 `ServerConfig`。
- 可以打印脱敏摘要。
- `bootstrap.rs` 可以使用 `anyhow::Result`。
- 内部模块错误后续再用 `thiserror` 细分。

### 任务包 5：最小 health API

目标：先提供一个可探活的 REST endpoint。

改动文件：

```text
src/http/router.rs
src/http/rest.rs
src/state.rs
```

步骤：

1. 在 `http/router.rs` 定义 `build_router(state)`。
2. 注册 `GET /api/v1/health`。
3. 在 `http/rest.rs` 定义 `HealthResponse`。
4. health response 返回：
   - `status`
   - `version`
   - `started_at`
   - `now`
   - `database`
   - `frontend_enabled`
5. 首版 database 可以返回 `not_configured` 或 `not_connected`。
6. handler 只读取 `AppState`，不做复杂业务。

完成标准：

- `GET /api/v1/health` 返回 JSON。
- 不需要 agent 连接也能访问。
- health handler 有单元测试或 router 测试。

文件级执行顺序：

1. 在 `http/rest.rs` 定义 `HealthResponse`。
2. 在 `http/rest.rs` 实现 `health()` handler。
3. 在 `http/router.rs` 注册 `/api/v1/health`。
4. 在 `state.rs` 增加 health 需要的只读字段。
5. 写 router 测试。

建议 JSON：

```json
{
  "status": "ok",
  "version": "0.1.0",
  "started_at": "2026-06-09T00:00:00Z",
  "now": "2026-06-09T00:00:01Z",
  "database": {
    "configured": true,
    "connected": false,
    "backend": "sqlite"
  },
  "frontend": {
    "enabled": false
  }
}
```

注意：

- health 不应该返回 token、secret。
- 首版数据库未连接时可以 `connected=false`，不要伪装成功。
- SQLite 接入后再把 `connected` 改为真实状态。

### 任务包 6：内存存储接口

目标：先定义 repository 边界，让 service 不关心内存还是 SQLite。

改动文件：

```text
src/storage.rs
src/storage/entity.rs
src/storage/repository.rs
src/storage/memory.rs
```

步骤：

1. 在 `storage/entity.rs` 定义内部存储模型：
   - `AgentRecord`
   - `AgentOnlineState`
   - `CommandRecord`
   - `CommandStatus`
   - `RemoteTaskResultRecord`
   - `RemoteProbeResultRecord`
2. 在 `storage/repository.rs` 定义 repository trait。
3. repository trait 包含 latest 写入方法：
   - `apply_snapshot`
   - `apply_delta`
   - `touch_heartbeat`
4. repository trait 包含查询方法：
   - `list_agents`
   - `get_agent`
   - `get_agent_latest`
5. repository trait 包含命令方法：
   - `create_command`
   - `mark_command_acked`
   - `mark_command_failed`
   - `mark_command_finished`
   - `get_command`
6. 在 `storage/memory.rs` 用 `RwLock` 实现内存版本。
7. 同一个 agent 的 snapshot/delta 更新必须保证顺序一致。
8. delta base 不匹配时返回需要 snapshot 的结果，不直接覆盖 latest。

完成标准：

- repository trait 不依赖 axum。
- memory 实现通过行为测试。
- 后续替换 SQLite 不需要改 handler。

文件级执行顺序：

1. 先改 `src/storage/entity.rs`：
   - 定义纯业务存储模型。
   - 不使用 SeaORM derive。
   - 不依赖 axum。
2. 再改 `src/storage/repository.rs`：
   - 定义 trait。
   - 明确返回错误类型。
   - 明确 snapshot/delta/heartbeat 方法语义。
3. 再改 `src/storage/memory.rs`：
   - 用 `RwLock<HashMap<AgentId, AgentRecord>>` 实现 agent latest。
   - command/result 可以先定义结构，后续任务包再完整实现。
4. 最后在 `src/storage.rs` 暴露模块。

最小测试：

1. `apply_snapshot_creates_agent_latest`。
2. `apply_snapshot_replaces_old_latest`。
3. `touch_heartbeat_updates_last_seen_only`。
4. `apply_delta_rejects_mismatched_base_sequence`。
5. `list_agents_returns_summary_without_full_report_if_needed`。

失败处理：

- agent id 为空：返回 validation error。
- sequence 倒退：返回 stale sequence error。
- delta base 不匹配：返回 `NeedSnapshot`，不覆盖 latest。
- repository 内部锁异常：转换为 storage error。

暂时不要做：

- SeaORM entity。
- migration。
- 历史事件表。
- dashboard 聚合查询。

### 任务包 7：agent service 和连接注册

目标：把 agent 在线状态、latest 操作、连接替换规则集中到 service。

改动文件：

```text
src/service.rs
src/service/agent.rs
src/service/connection.rs
src/state.rs
```

步骤：

1. 在 `service/connection.rs` 定义 `ConnectionId`。
2. 定义 `AgentConnectionHandle`，包含：
   - `agent_id`
   - `connection_id`
   - `connected_at`
   - server outbound sender
3. 定义 `AgentConnectionRegistry`。
4. 实现注册连接。
5. 实现按 agent_id 查询当前连接。
6. 实现断开清理。
7. 明确同 agent 重连规则：默认新连接替换旧连接。
8. 旧连接被替换时需要通知 write loop 关闭。
9. 在 `service/agent.rs` 定义 `AgentService`。
10. `AgentService` 封装 online/offline、snapshot、heartbeat、latest 查询。

完成标准：

- REST 和 ingest 都通过 `AgentService` 操作 agent。
- 没有 handler 直接改连接表。
- 同 agent 双连接测试通过。

文件级执行顺序：

1. 先改 `src/service/connection.rs`：
   - 定义 `ConnectionId`。
   - 定义 `AgentConnectionHandle`。
   - 定义 `AgentConnectionRegistry`。
2. 实现连接注册：
   - 输入 `agent_id`、`connection_id`、outbound sender。
   - 返回是否替换旧连接。
3. 实现连接查找：
   - `current_connection(agent_id)`。
   - 只返回发送所需 handle，不暴露内部 map。
4. 实现断开清理：
   - 只有当前 connection id 匹配时才清理。
   - 防止旧连接断开误清新连接。
5. 再改 `src/service/agent.rs`：
   - 定义 `AgentService`。
   - 注入 repository 和 connection registry。
   - 封装 online/offline/latest 操作。
6. 最后改 `state.rs`：
   - AppState 持有 `AgentService` 或 service handles。

最小测试：

1. `register_first_connection_marks_online`。
2. `register_new_connection_replaces_old_connection`。
3. `old_connection_disconnect_does_not_clear_new_connection`。
4. `current_connection_returns_latest_connection`。
5. `agent_service_applies_snapshot_through_repository`。

失败处理：

- 注册时 agent id 为空：拒绝。
- outbound queue 创建失败：拒绝连接。
- 旧连接替换：通知旧连接关闭，不能继续接收命令。
- 清理连接时 connection id 不匹配：忽略并记录 debug。

### 任务包 8：agent 认证最小实现

目标：让 `/agent/v1/connect` 进入业务前完成身份识别。

改动文件：

```text
src/auth.rs
src/auth/agent.rs
src/config/model.rs
src/http/agent.rs
```

步骤：

1. 在 `auth/agent.rs` 定义 `AgentAuthContext`。
2. 定义 `AgentAuthMode`：
   - `BinaryPlainBearer`
   - `BinaryPlainQuery`
   - `BinaryPlainNone`
   - `SecurePsk`
3. 定义认证输入 `AgentAuthRequest`。
4. 从 HTTP header 解析 bearer token。
5. 从 URL query 解析 token，但日志只记录是否存在。
6. 根据 token 或 key_id 查询已登记 agent。
7. 未登记 agent 或凭据不匹配时拒绝连接。
8. 认证成功返回 `AgentAuthContext`。
9. 认证失败在 upgrade 前返回 401，或 upgrade 后尽快关闭。
10. `secure_psk` 先保留接口，真正握手放到后续任务包。

完成标准：

- bearer 正确通过。
- bearer 错误拒绝。
- 未登记 agent 拒绝。
- 凭据不匹配拒绝。
- 日志不泄露 token。

文件级执行顺序：

1. 先改 `src/auth/agent.rs`：
   - 定义 `AgentAuthContext`。
   - 定义 `AgentAuthMode`。
   - 定义 `AgentAuthError`。
2. 定义认证输入：
   - headers。
   - query token 是否存在。
   - peer addr，可选。
   - 当前配置。
3. 实现 bearer 解析：
   - 支持 `Authorization: Bearer xxx`。
   - 大小写按 HTTP 标准处理。
   - 不打印 token。
4. 实现 query token 解析：
   - 只用于 `binary_plain`。
   - 日志只打印 `query_token_present=true/false`。
5. 根据 token 或 key_id 查询已登记 agent。
6. 未登记 agent 或凭据不匹配时拒绝连接。
7. secure_psk：
   - 只保留模式和错误提示。
   - 真正握手在任务包 15。
8. 在 `src/auth.rs` 暴露 `agent` 模块。

最小测试：

1. `bearer_token_accepts_matching_token`。
2. `bearer_token_rejects_wrong_token`。
3. `query_token_rejected_when_agent_not_registered`。
4. `unregistered_agent_is_rejected`。
5. `credential_mismatch_is_rejected`。
6. `auth_debug_does_not_include_token_value`。

失败处理：

- 未登记 agent：返回 unauthorized。
- token 错误：返回 unauthorized。
- secure_psk 请求进入但未配置 key store：返回 unauthorized 或 protocol close。
- 日志使用英文，例如 `agent authentication failed`。

### 任务包 9：agent WebSocket 主连接

目标：agent 能连接 `/agent/v1/connect`，server 能读写 WebSocket。

改动文件：

```text
src/http/router.rs
src/http/agent.rs
src/service/connection.rs
src/ingest/frame.rs
```

步骤：

1. 在 router 注册 `GET /agent/v1/connect`。
2. 在 `http/agent.rs` 完成 WebSocket upgrade。
3. upgrade 前调用认证。
4. 建立 agent outbound queue。
5. 注册连接。
6. 拆分 read half 和 write half。
7. read loop 读取 binary/text/close。
8. binary 交给 `ingest::frame`。
9. text 默认只在兼容模式处理，自有协议优先 binary。
10. write loop 从 outbound queue 接收 server frame。
11. write 失败时清理连接。
12. read 返回 `None` 时按正常断开处理。
13. close frame 到达时等待短时间完成清理，不无限等待。

完成标准：

- 成功连接后 agent online。
- 断开后 agent offline。
- 新连接替换旧连接。
- write queue 满时有明确错误路径。

文件级执行顺序：

1. 先改 `src/http/router.rs`：
   - 注册 `/agent/v1/connect`。
   - 确保 `/api/v1/*` 和 agent route 都早于 frontend fallback。
2. 再改 `src/http/agent.rs`：
   - 写 upgrade handler。
   - 从 request 中提取 header/query。
   - 调用 `auth::agent`。
3. 建立连接上下文：
   - `agent_id`。
   - `connection_id`。
   - auth mode。
   - peer addr。
   - connected_at。
4. 创建 outbound queue。
5. 调用 connection registry 注册。
6. split WebSocket。
7. 启动 read loop：
   - binary -> ingest。
   - text -> 自有协议默认拒绝或只记录 warning。
   - close -> 正常清理。
   - None -> 正常清理。
8. 启动 write loop：
   - 从 outbound queue 取 server frame。
   - encode 后发送 binary。
   - 发送失败触发清理。
9. read/write 任一结束后统一 cleanup。
10. cleanup 时只清理当前 connection id。

最小测试：

1. `agent_connect_rejects_unauthorized_request`。
2. `agent_connect_registers_connection_after_auth`。
3. `disconnect_clears_current_connection`。
4. `new_connection_replaces_old_connection`。
5. `old_connection_cleanup_does_not_remove_new_connection`。

失败处理：

- upgrade 前认证失败：HTTP 401。
- upgrade 后协议错误：close connection。
- write queue 满：command 路径标记失败，连接本身不一定关闭。
- read loop panic 风险：不要 unwrap 外部输入。

### 任务包 10：wire/frame 解码和 snapshot ingest

目标：server 能处理 agent 上报的 snapshot。

改动文件：

```text
src/ingest.rs
src/ingest/frame.rs
src/ingest/report.rs
src/service/agent.rs
```

步骤：

1. 在 `ingest/frame.rs` 定义 `handle_websocket_message()`。
2. 调用 `smalux-protocol` 解 `WirePacket`。
3. 根据 wire format 判断是否 plain 或 secure。
4. plain 模式直接解 `ClientFrame`。
5. secure 模式先留接口，后续接入解密。
6. 校验 frame 中的 `agent_id` 和认证上下文一致。
7. 校验 sequence 单调性。
8. snapshot payload 交给 `ingest/report.rs`。
9. `ingest/report.rs` 校验 report 基础字段。
10. 调用 `AgentService::apply_snapshot()`。
11. 更新 latest state。
12. 产生 `report_updated` 事件。

完成标准：

- 合法 snapshot 可写入 latest。
- agent_id 不一致会拒绝。
- unknown payload 记录 warning 后忽略。
- snapshot 测试覆盖成功和失败路径。

文件级执行顺序：

1. 先改 `src/ingest/frame.rs`：
   - 定义 `FrameIngestContext`。
   - 包含 `agent_id`、`connection_id`、auth mode、sequence 状态引用。
2. 定义 `handle_client_message(context, bytes)`：
   - 输入 WebSocket binary bytes。
   - 输出 ingest 结果或需要下发的 server frame。
3. 调用 `smalux-protocol` 解 wire packet。
4. plain 模式解 `ClientFrame`。
5. secure 模式先返回 unsupported 或走预留接口。
6. 校验 frame agent id。
7. 校验 sequence。
8. 按 payload 分发到 `ingest/report.rs`。
9. 在 `src/ingest/report.rs` 实现 snapshot 分支。
10. snapshot 校验通过后调用 `AgentService`。

最小测试：

1. `decode_plain_snapshot_updates_latest`。
2. `snapshot_rejected_when_agent_id_mismatch`。
3. `unknown_payload_is_ignored_without_panic`。
4. `invalid_wire_packet_returns_decode_error`。
5. `stale_sequence_is_rejected`。

失败处理：

- wire decode error：warning，必要时关闭连接。
- frame agent id 不一致：关闭连接。
- sequence 重放：拒绝该 frame。
- payload schema 不支持：warning 后忽略或返回协议错误。

### 任务包 11：heartbeat 和 delta ingest

目标：支持轻量心跳和局部更新。

改动文件：

```text
src/ingest/report.rs
src/service/agent.rs
src/storage/repository.rs
```

步骤：

1. heartbeat 只更新 `last_seen_at`。
2. heartbeat 不覆盖 latest report。
3. delta 必须带 `base_sequence`。
4. repository 判断 base 是否匹配。
5. base 匹配则合并 patch。
6. base 不匹配则返回 `NeedSnapshot`。
7. `NeedSnapshot` 由 command service 生成 `snapshot_request`。
8. delta 成功后产生 `report_updated` 事件。

完成标准：

- heartbeat 测试不改变 latest。
- delta base 匹配测试通过。
- delta base 不匹配触发 snapshot request。

文件级执行顺序：

1. 在 `ingest/report.rs` 增加 heartbeat 分支。
2. heartbeat 调用 `AgentService::touch_heartbeat()`。
3. `touch_heartbeat()` 调用 repository。
4. 确认 heartbeat 不读取或覆盖 latest report。
5. 在 `ingest/report.rs` 增加 delta 分支。
6. 校验 delta 必须携带 base sequence。
7. 调用 repository apply_delta。
8. base 匹配时合并并更新 latest。
9. base 不匹配时返回 `NeedSnapshot`。
10. `NeedSnapshot` 交给 command service 生成 snapshot request。
11. 如果 command service 还未实现，先返回明确 TODO result，不静默忽略。

最小测试：

1. `heartbeat_updates_last_seen_only`。
2. `delta_applies_when_base_matches`。
3. `delta_rejects_missing_base_sequence`。
4. `delta_requests_snapshot_when_base_mismatch`。
5. `delta_does_not_update_latest_on_mismatch`。

失败处理：

- heartbeat agent id 不一致：关闭连接。
- delta patch 非法：返回 validation error。
- delta base mismatch：请求 snapshot。
- snapshot request 入队失败：记录 command failure，不影响 latest 旧数据。

### 任务包 12：agent 查询 REST

目标：前端可以查询 agent 列表和 latest。

改动文件：

```text
src/http/rest.rs
src/service/agent.rs
src/service/dashboard.rs
```

步骤：

1. 定义统一 REST error。
2. 实现 `GET /api/v1/agents`。
3. 实现 `GET /api/v1/agents/{agent_id}`。
4. 实现 `GET /api/v1/agents/{agent_id}/latest`。
5. 实现 `GET /api/v1/dashboard/summary`。
6. response DTO 不返回敏感配置。
7. 空数据返回空列表或 404，不返回内部错误。

完成标准：

- REST 查询不直接访问 repository，走 service。
- handler 保持短小。
- 文件变长后再拆 `http/rest/` 子模块。

文件级执行顺序：

1. 先改 `service/agent.rs`：
   - 提供 `list_agents()`。
   - 提供 `get_agent_summary(agent_id)`。
   - 提供 `get_agent_latest(agent_id)`。
2. 再改 `service/dashboard.rs`：
   - 提供 `dashboard_summary()`。
   - 首版只统计 agent 总数、在线数、离线数。
3. 再改 `http/rest.rs`：
   - 定义 response DTO。
   - handler 只做参数提取和 response 转换。
4. 再改 `http/router.rs`：
   - 注册 REST routes。
5. 如果 `http/rest.rs` 太长，再拆：
   - `http/rest/agent.rs`
   - `http/rest/dashboard.rs`
   - `http/rest/health.rs`

最小测试：

1. `list_agents_returns_empty_array`。
2. `list_agents_returns_snapshot_agent`。
3. `get_unknown_agent_returns_404`。
4. `get_agent_latest_returns_latest_report`。
5. `dashboard_summary_counts_online_agents`。

失败处理：

- agent 不存在：404。
- agent 存在但没有 latest：返回 `latest=null` 或 204，二选一后写 README。
- service error：500，返回结构化错误。
- session 未接入前不要伪造权限判断。

### 任务包 13：命令创建和下发

目标：REST 创建命令，server 经 agent 主连接下发。

改动文件：

```text
src/http/rest.rs
src/service/command.rs
src/service/connection.rs
src/storage/repository.rs
```

步骤：

1. 定义 command request DTO。
2. 校验命令类型。
3. 校验远程能力开关。
4. 创建 `command_id`。
5. 写入 pending command。
6. 查询 agent 当前连接。
7. agent 在线则编码 `ServerFrame`。
8. 入 agent outbound queue。
9. 入队失败时更新 command 状态。
10. agent 离线时按命令类型决定拒绝或保存 desired config。
11. REST 默认立即返回 command 状态，不等待执行完成。

完成标准：

- command 不会静默丢弃。
- 离线 shell/task 默认拒绝。
- config_patch 可作为 desired config 后续补齐。

文件级执行顺序：

1. 先改 `storage/entity.rs`：
   - 定义 `CommandRecord`。
   - 定义 `CommandStatus`。
2. 再改 `storage/repository.rs`：
   - 添加 command CRUD 方法。
3. 再改 `storage/memory.rs`：
   - 实现 pending command 存储。
4. 再改 `service/command.rs`：
   - 定义 `CommandService`。
   - 定义 `CreateCommandRequest`。
   - 校验 command type。
   - 校验 agent capability 和管理端权限。
5. 查找 connection：
   - 在线：编码 server frame 并入队。
   - 离线：按命令类型拒绝或保存 desired config。
6. 再改 `http/rest.rs`：
   - 实现 `POST /api/v1/agents/{agent_id}/commands`。
7. 再改 `http/router.rs` 注册 route。

最小测试：

1. `create_command_for_online_agent_queues_frame`。
2. `create_shell_command_for_offline_agent_rejected`。
3. `create_remote_task_rejected_when_disabled`。
4. `create_config_patch_can_be_stored_when_agent_offline`。
5. `queue_full_marks_command_failed`。

失败处理：

- agent 不存在：404。
- agent 离线且命令不可离线：409 或 422。
- agent capability 或管理端权限不允许：403。
- outbound queue 满：命令状态更新为 failed。
- frame encode 失败：命令状态更新为 failed。

### 任务包 14：ack、error、result 回收

目标：agent 执行结果可以回写 server 并被 REST 查询。

改动文件：

```text
src/ingest/frame.rs
src/service/command.rs
src/storage/repository.rs
src/http/rest.rs
```

步骤：

1. ingest 识别 `ack`。
2. `ack` 更新 `acked_at`。
3. ingest 识别 `error`。
4. `error` 更新 command failed。
5. ingest 识别 `remote_task_result`。
6. 保存 stdout、stderr、exit_code、status。
7. ingest 识别 `remote_probe_result`。
8. 保存 probe result JSON。
9. 实现 `GET /api/v1/commands/{command_id}`。
10. command 状态变化产生实时事件。

完成标准：

- ack/error/result 幂等。
- 未知 command_id 有 warning，不 panic。
- result 不只存在内存队列，必须进 repository。

文件级执行顺序：

1. 先改 `ingest/frame.rs`：
   - 识别 ack payload。
   - 识别 error payload。
   - 识别 remote task result。
   - 识别 remote probe result。
2. 再改 `service/command.rs`：
   - 实现 `handle_ack()`。
   - 实现 `handle_error()`。
   - 实现 `handle_remote_task_result()`。
   - 实现 `handle_remote_probe_result()`。
3. 再改 `storage/repository.rs`：
   - 添加 result upsert 方法。
   - command 状态更新必须幂等。
4. 再改 `storage/memory.rs`：
   - 实现 ack/error/result 更新。
5. 再改 `http/rest.rs`：
   - 实现 `GET /api/v1/commands/{command_id}`。
6. 状态变化后发布 event。

最小测试：

1. `ack_marks_command_acked`。
2. `duplicate_ack_is_idempotent`。
3. `error_marks_command_failed`。
4. `remote_task_result_marks_command_finished`。
5. `unknown_command_result_is_logged_and_ignored`。
6. `get_command_returns_command_status`。

失败处理：

- 未知 command id：warning，不 panic。
- result agent id 和 command agent id 不一致：拒绝。
- 已终态 command 收到 ack：忽略。
- 已终态 command 收到不同 result：保留首次终态，记录 warning。

### 任务包 15：secure_psk 接入

目标：自有协议支持加密传输，server 不重复实现协议加密细节。

改动文件：

```text
src/auth/agent.rs
src/ingest/frame.rs
src/service/connection.rs
src/config/model.rs
```

步骤：

1. 定义 server 侧 `AgentSecretStore`。
2. 根据 `key_id` 查 secret。
3. 使用 `smalux-protocol` secure 模块完成 responder 握手。
4. 握手成功后 connection 进入 ready。
5. 后续入站 frame 先解密再 decode。
6. 出站 server frame 先 encode 再加密。
7. 禁止 secure_psk 同时使用 bearer/query token。
8. 握手失败关闭连接。
9. 日志只打印 key_id 的脱敏摘要。

完成标准：

- secure_psk 测试向量通过。
- plain 和 secure 两条路径都可运行。
- 加密逻辑仍集中复用 `smalux-protocol`。

### 任务包 16：SQLite 和 SeaORM

目标：把 latest、command、result 持久化。

改动文件：

```text
src/storage/entity.rs
src/storage/migration.rs
src/storage/repository.rs
src/config/model.rs
src/bootstrap.rs
```

步骤：

1. 定义 SeaORM entity。
2. 定义 migration。
3. 启动时连接 SQLite。
4. 启动时自动运行 migration。
5. 实现 `SqliteRepository`。
6. 把 repository 作为 trait object 或 enum 注入 `AppState`。
7. MemoryRepository 保留为测试使用。
8. 同一组 repository 行为测试同时跑 memory 和 sqlite。

完成标准：

- server 重启后 latest 可恢复。
- command 状态可恢复。
- migration 可重复执行。
- SQLite 写入失败有明确日志和错误。

### 任务包 17：前端实时事件

目标：前端可以订阅变化通知，但完整数据仍通过 REST 补齐。

改动文件：

```text
src/service/event.rs
src/http/realtime.rs
src/http/router.rs
src/state.rs
```

步骤：

1. 定义 `DashboardEvent`。
2. AppState 中加入 broadcast sender。
3. agent online/offline 发送事件。
4. report 更新发送事件。
5. command 状态变化发送事件。
6. `GET /live/v1/dashboard` 建立 SSE 或 WebSocket。
7. 慢订阅者不阻塞 ingest。
8. 队列满时 dashboard 事件允许丢弃旧事件。

完成标准：

- 事件只是通知，不是唯一事实来源。
- 前端断开后订阅清理。
- 慢前端不会影响 agent 上报。

### 任务包 18：前端静态资源托管

目标：支持独立前端、目录托管和可选内嵌。

改动文件：

```text
src/http/frontend.rs
src/http/router.rs
src/config/model.rs
Cargo.toml
```

步骤：

1. `frontend.enabled=false` 时不注册前端 fallback。
2. 未编译 `frontend-embed` 且 `frontend.enabled=true` 时使用 `frontend.dir`。
3. 编译 `frontend-embed` 且 `frontend.enabled=true` 时使用内置资源。
4. `frontend.spa_fallback=true` 时未知路径返回 `index.html`。
5. API、agent、live 路由必须优先注册。
6. fallback 放最后。

完成标准：

- `/api/v1/health` 不被前端 fallback 抢占。
- `/agent/v1/connect` 不被前端 fallback 抢占。
- 没有前端资源时错误清晰。

### 任务包 19：管理后台 session

目标：给前端管理 API 加 session，不影响 agent。

改动文件：

```text
src/auth/session.rs
src/http/middleware.rs
src/http/rest.rs
src/storage/repository.rs
```

步骤：

1. 引入 session middleware。
2. 实现登录接口。
3. 实现登出接口。
4. 实现当前用户接口。
5. 管理 REST API 加 session 校验。
6. agent `/agent/v1/connect` 不挂 session middleware。
7. live dashboard 是否需要 session 由前端权限策略决定。

完成标准：

- 未登录访问管理 API 返回 401。
- 登录后可访问。
- 登出后 session 失效。
- agent 连接不受影响。

### 任务包 20：Komari 兼容

目标：第三方兼容可以整体删除，不污染自有协议。

改动文件：

```text
src/compat.rs
src/compat/komari.rs
src/http/router.rs
```

步骤：

1. 需要兼容时再创建 `compat.rs`。
2. 在 `compat/komari.rs` 放 Komari 路由、DTO、转换逻辑。
3. Komari 上报转换为内部 report 更新。
4. Komari exec 转换为通用 remote task 或 shell command。
5. Komari 输出走自己的 encoder。
6. 不把 Komari DTO 放到 `service/agent.rs`。
7. 不把 Komari 路由混入自有 `/agent/v1/connect`。

完成标准：

- 删除 `compat/` 后自有协议仍可编译。
- Komari 只通过 service/repository 边界影响 server 状态。
- 文档明确 Komari 是兼容路径，不是主协议。

## 每轮实现后的固定复盘

每完成一个任务包，都按下面顺序复盘：

1. 结构复盘：
   - 文件是否放在对应模块。
   - 有没有新建不必要目录。
   - 有没有超过职责边界的代码。
   - 有没有旧命名残留。
2. 行为复盘：
   - 本任务包的输入是什么。
   - 本任务包的输出是什么。
   - 错误路径是否清楚。
   - 并发路径是否有锁、队列、断开清理说明。
3. 安全复盘：
   - 是否打印 token。
   - 是否打印 secret。
   - 是否打印完整带 query 的 URL。
   - 是否允许未授权控制命令。
4. 性能复盘：
   - 是否让 agent ingest 等慢前端。
   - 是否让 WebSocket read loop 等数据库长写入。
   - 是否有无限增长队列。
   - 队列满时行为是否明确。
5. 文档复盘：
   - README 是否同步 CLI 参数。
   - README 是否同步路由。
   - README 是否同步 JSON 格式。
   - server 交互文档是否足够写客户端或前端。

## 优先提交边界

推荐提交顺序：

```text
commit 1: server skeleton and plan
commit 2: cli config bootstrap health
commit 3: memory repository and agent service
commit 4: agent websocket and binary_plain ingest
commit 5: rest query endpoints
commit 6: command dispatch and result ingest
commit 7: secure_psk
commit 8: sqlite repository
commit 9: realtime and frontend hosting
commit 10: session and compat
```

每次提交前检查：

```powershell
cargo fmt --all --check
cargo check -p smalux-server
cargo test -p smalux-server
rg -n -F '/api/agents/connect' crates/smalux-agent crates/smalux-server/src crates/smalux-server/README.md
rg -n -F 'config/cli' crates/smalux-server/src crates/smalux-server/README.md
rg -n -F 'FrontendMode' crates/smalux-server/src crates/smalux-server/README.md
rg -n -F 'frontend.mode' crates/smalux-server/src crates/smalux-server/README.md
```
