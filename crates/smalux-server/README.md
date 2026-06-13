# smalux-server

`smalux-server` 是接收、存储和查询 agent 上报数据的服务端进程。

## 当前职责

- 当前保留 server crate 依赖配置、目录骨架、CLI 参数结构和实现文档。
- `src/` 当前只有最小启动入口和模块骨架，`main.rs` 已交给 `bootstrap::run()`，业务代码由你后续继续编写。
- 后续接收 `smalux-agent` 上报的 `ClientFrame`，完成校验、标准化、持久化和查询。

## 目录结构

当前 `src/` 保留最小可编译入口、CLI 参数解析入口和按 agent 风格预建的目录骨架。`crates/smalux-server` 已在 workspace `members` 中，后续可以直接在这些目录里继续写 server 业务代码。

当前预留结构：

```text
src/
  main.rs          # 最小入口，声明模块并交给 bootstrap 启动
  bootstrap.rs     # 启动编排：日志、CLI、配置、数据库、HTTP server
  state.rs         # axum handler 和后台服务共享的 AppState
  cli/             # server 启动参数、环境变量和命令行默认值解析
    args.rs        # clap 参数结构
  config/          # server 运行配置模型、默认值和校验
    defaults.rs    # 默认值常量
    model.rs       # 配置模型
    validation.rs  # 配置校验
  auth/            # agent 认证、secure_psk 识别、管理后台 session 和授权边界
    agent.rs       # agent token、key_id、secure_psk 认证
    session.rs     # 管理后台 session 和用户登录态
  http/            # axum router、REST API、agent 接入、前端实时通道、静态前端资源和中间件
    router.rs      # REST、agent 接入、前端实时通道、静态资源和中间件的路由组合
    rest.rs        # 前端 REST API handler 和 route 组织
    agent.rs       # agent WebSocket upgrade、连接生命周期和控制帧发送
    realtime.rs    # 前端 dashboard live update、事件订阅和管理端实时反馈
    frontend.rs    # 静态前端资源、SPA fallback 和前端入口页面服务
    middleware.rs  # axum/tower middleware，例如 trace、CORS、限流、request id
  ingest/          # agent 上报接入、协议分发和校验
    frame.rs       # ClientFrame/ServerFrame 分发和序列号语义
    report.rs      # AgentReport snapshot/delta/heartbeat 校验与应用
  storage/         # 持久化接口、内存状态和数据库适配
    entity.rs      # SeaORM entity
    memory.rs      # 内存存储，用于首版开发和测试
    migration.rs   # SeaORM migration
    repository.rs  # latest state、pending command、remote result 仓储
  service/         # server 后台服务编排、连接状态和控制命令调度
    agent.rs       # agent 连接注册、在线状态和 latest 状态协调
    connection.rs  # 在线连接 registry、连接替换、下行队列和断开清理
    dashboard.rs   # dashboard 聚合状态、摘要指标和前端展示数据
    command.rs     # server 下发命令、ack/error、超时和幂等处理
    event.rs       # dashboard 实时事件和内部状态变化事件
```

## 当前 crate 依赖配置

`smalux-server` 现在先只配置后续 server 会用到的相关 crate，模块内部暂时保持骨架。版本按当前已确认的 crates.io / `cargo info` 最新可用版本固定，避免未来自动跳版本带来不可控变化。

这次配置的目标是“server 基础设施尽量一次配全”：HTTP/WebSocket、Tower 中间件、限流、会话、数据库 ORM、迁移、协议复用、错误处理、时间、ID、校验和密钥包装都先放进 crate；认证方案、OpenAPI、gRPC、密码哈希、外部 API 客户端这些会绑定业务设计，暂时不硬塞进来。

- `tokio 1.52.3`: 异步运行时。
- `tokio-util 0.7.18`: stream、codec、IO 适配、超时工具。
- `axum 0.8.9`: HTTP / WebSocket 服务框架，已启用 `http2`、`ws`、`macros`、`multipart`。
- `axum-extra 0.12.6`: typed headers、cookie、扩展 extractor、typed routing、JSON Lines、文件流、multipart 等 axum 扩展能力。
- `tower 0.5.3`: 启用官方 `full` feature，覆盖 balance、buffer、filter、hedge、limit、load-shed、reconnect、retry、timeout、util 等通用 service middleware。
- `tower-http 0.6.11`: 启用官方 `full` feature，覆盖 trace、CORS、压缩/解压、request id、敏感 header、panic catch、request 校验、静态文件、metrics、redirect 等 HTTP middleware。
- `tower_governor 0.8.0`: Tower/axum 限流中间件，只启用 `axum` 和 `tracing`，不启用默认 `tonic`。
- `tower-sessions 0.15.0`: Tower/axum session 中间件，启用 signed/private cookie，后续可用于管理后台登录态；当前先只配置 crate。
- `clap 4.6.1`: 后续启动参数解析，启用 `derive`、`env`、`unicode`、`wrap_help`。
- `serde 1.0.228` / `serde_json 1.0.150`: 后续 JSON API 和协议辅助。
- `tracing 0.1.44`: server 运行日志。
- `sea-orm 2.0.0-rc.40`: server 数据库 ORM，当前按 SQLite / PostgreSQL / MySQL + Tokio/Rustls 配置；这是当前 crates.io 可搜索到的最新可用版本，但属于 RC，不是稳定线。
- `sea-orm-migration 2.0.0-rc.40`: SeaORM 迁移能力，关闭默认 CLI feature，只保留 Tokio/Rustls + SQLite / PostgreSQL / MySQL 迁移运行能力。
- `sea-query 1.0.1`: 后续手写动态 SQL、迁移辅助或复杂查询构建时使用，当前开启 SQLite / PostgreSQL / MySQL query backend。
- `anyhow 1.0.102` / `thiserror 2.0.18`: 启动错误和后续领域错误建模。
- `async-trait 0.1.89`: 后续存储 trait、服务 trait 需要 async 方法时使用。
- `futures-util 0.3.32`: WebSocket split/sink/stream 等异步组合工具。
- `form_urlencoded 1.2.2`: SQLite 连接地址 query 参数编码。
- `bytes 1.11.1`: WebSocket、HTTP body、二进制协议 buffer。
- `http 1.4.1` / `headers 0.4.1`: HTTP 类型和 typed header。
- `validator 0.20.0`: API 请求 DTO、管理后台表单和配置 patch 的结构化校验。
- `uuid 1.23.2`: connection id、command id、task id 等服务端生成 ID。
- `time 0.3.47`: server 收包时间、过期时间、日志/数据库时间字段。
- `url 2.5.8`: PostgreSQL/MySQL 连接 URL 构造和用户名、密码、query 编码。
- `secrecy 0.10.3`: token、secret、PSK 等敏感值包装，降低误打印风险。
- `smalux-core`: 复用日志初始化和通用工具。
- `smalux-protocol`: 后续 server 解包 `ClientFrame`、wire packet 和 `secure_psk` 时复用协议实现。

数据库依赖当前支持 SQLite、PostgreSQL 和 MySQL。CLI 不要求用户手写完整连接 URL，而是接收 driver、host、port、name、user、password 和额外 query 参数等关键属性；配置层会补齐默认值、校验跨字段约束，并生成 SeaORM/SQLx 需要的连接 URL。没有启用 `sqlx-all`，也没有引入 gRPC/Tonic。

暂时没有把 gRPC、系统采集类依赖、OpenAPI、密码哈希、外部 HTTP client 作为 server 直接依赖。原因是这些依赖会强绑定后续功能边界：gRPC 要看是否真的做独立传输协议，OpenAPI 要看 API 文档生成方式，密码哈希要等管理后台认证模型确定，外部 HTTP client 要等 server 是否主动调用第三方服务。后续需要时再加，比提前把业务方向锁死更稳。

`tower-sessions-sqlx-store` 和 `tower-sessions-seaorm-store` 也暂时没有加入：前者当前最新版本依赖的 `tower-sessions-core` 版本和 `tower-sessions 0.15.0` 不一致，后者版本较早且默认偏 Postgres。首版可以先用 `tower-sessions` 的内存 store 跑通管理后台登录态；如果要持久化 session，建议等 session 表结构确定后自己用 SeaORM 写 store 或等待生态版本对齐。

## CLI 参数设计

server CLI 只负责 server 进程启动时必须确定的静态运行环境，不负责 agent 凭据、agent 能力或管理端业务权限。agent token/key 在“添加 agent”流程中动态生成并写入数据库；remote task、remote shell、remote probe 是 agent 能力和管理端授权问题，不放到 server 启动参数里。

当前参数：

| 参数 | 短参数 | 环境变量 | 默认值 | 作用 |
| --- | --- | --- | --- | --- |
| `--bind-addr <ADDR>` | `-b` | `SMALUX_SERVER_BIND_ADDR` | `127.0.0.1` | HTTP 监听 IP 地址 |
| `--bind-port <PORT>` | `-p` | `SMALUX_SERVER_BIND_PORT` | `3000` | HTTP 监听端口 |
| `--database-driver <DRIVER>` | `-d` | `SMALUX_SERVER_DATABASE_DRIVER` | `sqlite` | 数据库类型，支持 `sqlite`、`postgres`、`mysql` |
| `--database-name <NAME>` | 无 | `SMALUX_SERVER_DATABASE_NAME` | 按 driver 决定 | 数据库目标；SQLite 默认 `smalux-server.db`，PostgreSQL/MySQL 默认 `smalux` |
| `--database-host <HOST>` | 无 | `SMALUX_SERVER_DATABASE_HOST` | PostgreSQL/MySQL 默认 `127.0.0.1` | PostgreSQL/MySQL 主机；SQLite 不允许传入 |
| `--database-port <PORT>` | 无 | `SMALUX_SERVER_DATABASE_PORT` | PostgreSQL 默认 `5432`，MySQL 默认 `3306` | PostgreSQL/MySQL 端口；SQLite 不允许传入 |
| `--database-user <USER>` | 无 | `SMALUX_SERVER_DATABASE_USER` | 无 | PostgreSQL/MySQL 用户名 |
| `--database-password <PASSWORD>` | 无 | `SMALUX_SERVER_DATABASE_PASSWORD` | 无 | PostgreSQL/MySQL 密码，Debug 输出会脱敏 |
| `--database-param <KEY=VALUE>` | 无 | 不支持 | 无 | 额外数据库连接 query 参数，可重复传入，例如 `mode=rwc`、`sslmode=require`、`charset=utf8mb4`、`options=--search_path=public` |
| `--serve-frontend [true|false]` | 无 | `SMALUX_SERVER_SERVE_FRONTEND` | `false` | 是否由 server 托管前端静态资源；只传 `--serve-frontend` 等价于 `true` |
| `--frontend-dir <PATH>` | 无 | `SMALUX_SERVER_FRONTEND_DIR` | `apps/smalux-web/dist` | 未编译内置前端资源时，server 托管的前端构建目录 |
| `--frontend-spa-fallback <true|false>` | 无 | `SMALUX_SERVER_FRONTEND_SPA_FALLBACK` | `true` | 是否为 React/Vite SPA 启用 `index.html` fallback |
| `--log-file <PATH>` | 无 | `SMALUX_SERVER_LOG_FILE` | `logs/smalux-server.log` | server 滚动日志文件路径 |
| `--log-retention-files <N>` | `-L` | `SMALUX_SERVER_LOG_RETENTION_FILES` | `14` | 保留最近 N 个滚动日志文件，必须大于 0 |
| `--log-max-size-mb <MB>` | 无 | `SMALUX_SERVER_LOG_MAX_SIZE_MB` | `64` | 单个滚动日志文件最大大小，单位 MB，必须大于 0 |

日志级别只使用 Rust 生态通用的 `RUST_LOG`，不再额外增加 server 专用日志级别参数：

```powershell
$env:RUST_LOG="smalux_server=debug,smalux_protocol=debug"
cargo run -p smalux-server -- --bind-addr 127.0.0.1 --bind-port 3000
```

常用启动示例：

```powershell
# 只启动 API 和 agent 接入，不托管前端。
cargo run -p smalux-server -- --bind-addr 127.0.0.1 --bind-port 3000

# 使用 SQLite，并托管 Vite/React 构建后的前端目录。
cargo run -p smalux-server -- -b 0.0.0.0 -p 3000 -d sqlite --database-name smalux-server.db --database-param mode=rwc --serve-frontend --frontend-dir apps/smalux-web/dist

# 使用 PostgreSQL。
cargo run -p smalux-server -- -d postgres --database-user user --database-password password --database-param sslmode=disable --database-param options=--search_path=public

# 使用 MySQL。
cargo run -p smalux-server -- -d mysql --database-user user --database-password password --database-param charset=utf8mb4
```

## 配置转换流程

server 当前启动时会把 CLI/env 原始输入转换成稳定 `ServerConfig`：

```text
ServerArgs::parse()
  -> ServerArgs::into_config()
  -> validate_server_config()
  -> DatabaseConfig::connection_url()
  -> DatabaseConfig::redacted_connection_url()
  -> 后续接日志、数据库连接和 HTTP server
```

配置层规则：

- `cli/args.rs` 只处理 clap 参数、环境变量和 CLI 专用类型，例如 `DatabaseDriverArg`。
- `config/model.rs` 保存稳定运行配置，不依赖 clap，后续配置文件或数据库下发配置也可以复用。
- HTTP 配置在模型中拆成 `bind_addr` 和 `bind_port`，真正绑定时通过 `HttpConfig::socket_addr()` 合成 `SocketAddr`。
- `config/validation.rs` 处理跨字段校验，例如 SQLite 不允许传 host/port/user/password，PostgreSQL/MySQL 必须有 user。
- `database-param` 在 CLI 可重复传入，但配置层会转换成稳定 map；重复 key 直接报错，避免覆盖行为不清晰。
- 数据库明文密码只参与真实连接 URL 构造；日志和 debug 摘要只使用 `redacted_connection_url()`，不会打印原始密码。

数据库连接 URL 生成规则：

| driver | 输入示例 | 生成结果 |
| --- | --- | --- |
| SQLite | `-d sqlite` | `sqlite://smalux-server.db` |
| SQLite memory | `-d sqlite --database-name :memory:` | `sqlite::memory:` |
| SQLite + query | `-d sqlite --database-param mode=rwc` | `sqlite://smalux-server.db?mode=rwc` |
| PostgreSQL | `-d postgres --database-user user --database-password password` | `postgres://user:***@127.0.0.1:5432/smalux`，日志脱敏；真实连接 URL 使用明文密码 |
| MySQL | `-d mysql --database-user user --database-param charset=utf8mb4` | `mysql://user@127.0.0.1:3306/smalux?charset=utf8mb4` |

暂时不要加入到 server CLI 的内容：

- `agent_token`、`agent_token_file`、`secure_key_file`：由添加 agent 流程动态生成并存库。
- `allow_remote_task`、`allow_remote_shell`、`allow_remote_probe`：这是 agent 启动能力和管理端权限，不是 server 启动配置。
- `max_agent_connections`、`max_request_body_bytes`、队列容量：等连接 registry、HTTP middleware 或 command queue 真正实现时再按行为加配置，避免 CLI 先承诺不存在的功能。
- `public_base_url`：server 当前不需要运行时拼外部访问地址；前端或部署层需要时再单独设计。

## 后续接入点

- `bootstrap.rs`: 串联启动流程，避免 `main.rs` 堆积启动细节。
- `state.rs`: 定义共享 `AppState`，集中持有配置、存储、连接 registry 和事件通道。
- `cli/`: 定义 server 启动参数、环境变量和命令行默认值解析。
- `config/`: 定义 server 运行配置模型、默认值和校验。
- `auth/`: 处理 agent 认证、secure key 查找、管理后台 session 和权限边界。
- `http/`: 构建 axum router，处理前端 REST API、agent 接入、前端实时通道、静态前端资源和 HTTP middleware。
- `ingest/`: 接收 `ClientFrame`，校验 agent 身份和 payload。
- `storage/`: 定义存储 trait，接入数据库。
- `service/`: 编排连接状态、控制命令、后台任务和业务服务。

建议职责边界：

- `http/` 只处理 axum/HTTP 框架细节：路由、upgrade、请求参数、响应码、连接超时、middleware 和静态前端资源。
- `auth/` 只处理身份和权限：token/key 查找、secure_psk 认证、session、控制权限判定。
- `ingest/` 只处理协议语义：decode `ClientFrame`、校验、snapshot/delta/heartbeat 分发、控制响应关联。
- `storage/` 只处理持久化：latest state、pending command、remote task/probe result，不直接依赖 axum。
- `cli/` 只处理 server 启动输入，例如监听地址、数据库连接地址和前端托管选项；agent token/key 由添加 agent 流程动态生成并存库，不从 CLI 传入。
- `config/` 只放运行配置模型、默认值和校验，不直接依赖 clap。
- `service/` 只做服务级编排，例如连接注册、命令投递、latest 读取、dashboard 聚合、后台清理和状态协调。

这样后续增加 REST 或 Web UI 时，不需要重写 agent 上报接入逻辑；都继续走 `http/`、`ingest/`、`storage/` 的边界。未来如果真的加 gRPC，再新增同级 `grpc/`，不要提前把当前 axum 入口泛化成不清晰的名字。

### 目录去重决策

server 同时服务 agent、前端 REST 和前端实时连接时，最容易混乱的是把所有 HTTP 入口都塞进 `api` 或 `ws`。当前约定是：`http/rest.rs` 只放前端 REST handler，`http/agent.rs` 只放 agent 主连接，`http/realtime.rs` 只放前端实时推送，`http/frontend.rs` 只放 React/Vite 静态资源服务，`http/router.rs` 负责把这些入口组合起来。

`query/` 模块已经删除。前端查询读模型不再单独占一个目录，agent 列表、latest report 和在线状态放在 `service/agent.rs`，dashboard 摘要和聚合数据放在 `service/dashboard.rs`，REST handler 通过 service 获取数据，不直接访问数据库细节。

空目录不提前保留。当前只保留已经有 `.rs` 模块入口或真实子模块的目录；例如 `auth/agent.rs` 暂时只有文件，没有继续保留空的 `auth/agent/`。后续当某个模块需要拆成多个文件时，再创建同名目录，例如 `auth/agent/token.rs`、`auth/agent/secure_psk.rs`，同时由 `auth/agent.rs` 引入它们。

当前目录职责总结：

| 目录 | 主要调用方 | 负责内容 | 不负责内容 |
| --- | --- | --- | --- |
| `http/router.rs` | main/bootstrap | 组合 REST、agent、realtime、frontend 和 middleware | 不写业务逻辑 |
| `http/rest.rs` | 前端 REST 请求 | axum handler、route 组织、请求/响应转换 | 不直接写 SQL，不解析 agent wire |
| `http/agent.rs` | agent | `/agent/v1/connect`、主连接、远程 shell stream | 不处理 dashboard 推送 |
| `http/realtime.rs` | 前端 | dashboard live update、事件订阅 | 不处理 agent ClientFrame |
| `http/frontend.rs` | 浏览器 | 静态前端资源、SPA fallback | 不处理 agent 上报 |
| `ingest/*` | agent 连接处理 | ClientFrame、snapshot、delta、heartbeat 语义 | 不处理前端 DTO |
| `service/agent.rs` | HTTP / ingest / storage | 连接状态、latest report、在线状态、agent 列表 | 不直接暴露 HTTP route |
| `service/dashboard.rs` | HTTP / storage / realtime | dashboard 聚合数据和摘要指标 | 不解析 wire frame |
| `service/command.rs` | HTTP / agent connection / storage | 命令调度、ack/error、远程任务结果关联 | 不直接暴露 HTTP route |
| `storage/*` | service | entity、migration、repository 和持久化 | 不依赖 axum handler |

### 推荐实现顺序和文件落点

server 还没正式实现时，优先把“能连接、能解包、能保存最新 snapshot”跑通。建议按下面顺序写：

1. `cli/`
   - 定义监听地址、数据库连接地址、前端托管和日志滚动相关的启动输入。
   - 只负责把命令行和环境变量解析成启动输入，不直接做数据库连接或 HTTP 启动。
   - 不放 agent token、secure key、wire mode 或 remote 能力开关；这些属于 agent 登记记录、连接认证和管理端业务配置。
2. `config/`
   - 定义运行配置模型、默认值和校验，把 CLI 启动输入转换成稳定配置。
   - 不在这里放 agent 上报 JSON 结构，JSON 结构来自 `smalux-protocol` 和 `smalux-core`。
3. `http/`
   - 建立 `/agent/v1/connect` WebSocket endpoint。
   - 建立 `/api/v1/*` 前端 REST endpoint。
   - 建立 `/live/v1/dashboard` 前端实时 endpoint。
   - 根据配置选择是否服务 React/Vite 静态资源。
   - 收到 agent binary/text 后只交给 frame 解包和 ingest，不直接操作 storage。
4. `auth/`
   - 定义 agent 认证方式、`key_id -> secret` 查找接口和后台 session 边界。
   - `secure_psk` 只在 Noise 握手成功后认为认证通过，不把 `key_id` 当成认证成功。
5. `ingest/`
   - 调用 `smalux_protocol::wire` / `secure` / `codec` 还原 `ClientFrame`。
   - 按 `ClientPayload` 分发到 `apply_snapshot()`、`apply_delta()`、`apply_heartbeat()`、`apply_control_response()`。
   - 负责生成下行 `ServerFrame`，但不直接决定 WebSocket 怎么加密发送。
6. `storage/`
   - 先实现 latest state：`agent_id -> latest_report + delta_base_sequence + last_seen_at`。
   - 再实现 pending command、remote task result、remote probe result。
   - 需要 SQLite 时在这里接入，避免 axum handler 直接写 SQL。
7. `service/`
   - 把 http、auth、ingest、storage 串起来。
   - 在 `service/agent.rs` 提供 agent 列表、latest report 和在线状态读取。
   - 在 `service/dashboard.rs` 提供 dashboard 聚合数据。
   - 后续远程任务、控制命令、连接清理和 desired config 下发都放在这里编排。

首版可以暂时只实现 `binary_plain + snapshot + heartbeat + snapshot_request`，确认 agent 能稳定在线后，再接 `delta`、`secure_psk`、remote task/probe/shell。这样每一步失败时都能明确定位在 http、wire、ingest 或 storage 哪一层。

### 前端服务模式

Rust server 不直接运行 React 源码。React/Vite 前端应先构建成静态资源，再由 server 按配置选择是否服务这些资源。这样可以同时支持开发期前后端独立运行和生产期整体部署。

前端是否内嵌由编译 feature 决定，运行时不需要 `embedded` 模式参数。运行时只需要决定是否启用 server 托管前端，以及没有内嵌资源时从哪个目录读取。

```text
frontend.enabled = false
  -> 不服务前端，只提供 `/api/v1/*`、`/agent/v1/connect` 和 `/live/v1/*`。

frontend.enabled = true + 编译了 frontend-embed
  -> 直接使用内置前端资源，`frontend.dir` 不参与选择。

frontend.enabled = true + 没有编译 frontend-embed
  -> 使用 tower-http ServeDir 从 `frontend.dir` 读取 React/Vite dist。
```

推荐默认：

```text
开发期:
  React/Vite dev server  -> http://127.0.0.1:5173
  smalux-server          -> http://127.0.0.1:3000
  Vite proxy             -> /api/v1、/live/v1 代理到 smalux-server

生产期:
  pnpm build
  smalux-server --serve-frontend --frontend-dir apps/smalux-web/dist

单文件发布:
  cargo build --release -p smalux-server --features frontend-embed
  smalux-server --serve-frontend
```

`http/frontend.rs` 只负责前端静态资源和 SPA fallback，不处理 REST handler，也不处理 agent 上报。`http/router.rs` 负责把 frontend route 放在 fallback 位置，避免静态资源路由抢走 `/api/v1/*`、`/agent/v1/connect` 或 `/live/v1/*`。

## Agent 上报接入设计

首版 server 先做“接收并保存最新快照”，不急着做历史时序库。这样可以先把 agent 到 server 的协议闭环跑通，再根据 UI 和查询需求决定是否落库、如何分表、是否保留明细历史。

完整上报 JSON 参数见 `crates/smalux-agent/README.md` 的 `ClientFrame` 和 `AgentReport` JSONC 示例，稳定 frame、wire packet 和 `secure_psk` 规则见 `crates/smalux-protocol/README.md`。server 侧只接收实际标准 JSON，不接收文档里的注释。

### 接入入口

建议首版使用 WebSocket：

```text
GET /agent/v1/connect
  -> WebSocket upgrade
  -> 按 wire mode 做连接级识别；binary_plain 可用 query/bearer，secure_psk 不使用明文 token
  -> 接收 smalux binary wire frame；开发兼容模式可接收 text frame
  -> binary_plain: WirePacket(PlainData).payload 得到 JSON bytes
  -> secure_psk: Hello + Noise 握手后，WirePacket(SecureData).payload 解密得到 JSON bytes
  -> wire/secure 直接复用 smalux_protocol::{wire, secure}
  -> smalux_protocol::decode_client_frame()
  -> ingest::validate_report()
  -> storage::save_latest_report()
```

后续如果加 gRPC，不改变 `ingest` 和 `storage` 的领域接口，只新增 transport adapter：

```text
WebSocket adapter ┐
HTTP adapter      ├─> ingest::handle_report(report)
gRPC adapter      ┘
```

### 交换流程

server 第一版按下面流程写，能覆盖 agent 当前自有协议闭环：

```text
agent connects /agent/v1/connect
  -> server 按 wire mode 选择连接识别方式
     -> binary_plain: 可按 query token / bearer token / none 识别
     -> secure_psk: 先只接收 Hello，Noise 握手成功后才算认证通过
  -> server 按 wire_mode 解包 JSON bytes
  -> server decode ClientFrame
  -> server 按 ClientFrame.type 分发
     -> snapshot: 保存完整最新状态
     -> delta: 校验 base_sequence 后按采样组覆盖
     -> heartbeat: 更新业务在线时间
     -> ack/error: 关联 server 下发的 ServerFrame.sequence
     -> remote_task_result: 更新任务结果
     -> job_result: 按 kind 分发；当前 kind=probe 时更新探测结果
  -> server 需要控制 agent 时，按当前 wire_mode 发送 ServerFrame
```

`secure_psk` 模式下，server 需要保存 `key_id -> secret`。收到 agent 的 `Hello` 后，直接使用 `smalux_protocol::secure` 里的共享实现解析 token、派生 32 字节 PSK，并以 `Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s` responder 身份回复第二条 handshake。握手成功后，所有业务 JSON 都必须先加密再放入 `SecureData`。

server 不应重复手写下面这些参数，优先调用 `smalux_protocol::secure`；如果后续用其它语言实现 server，也必须使用同样精确参数派生 PSK：

```text
input secret      = base64url_decode(secret_base64url)  # 兼容带 padding 和不带 padding
secret min length = 32 bytes
HKDF hash         = SHA-256
HKDF salt         = "smalux secure psk v1 salt"
HKDF info         = "smalux secure psk v1 " + key_id
output length     = 32 bytes
Noise psk slot    = psk(0, derived_psk)
Noise pattern     = Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s
Noise payload      = empty bytes during both handshake messages
```

### Agent 当前发送语义

server 第一版要按 agent 当前运行时语义处理数据，不要把所有消息理解成“固定间隔、严格递增、一次到达”的普通日志流。

- agent 采集是事件驱动：`collector_loop` 按 core/disk/network/processes/sockets 各自的 interval 调度；同一时间点到期的 group 会合并成一条 `TelemetryUpdate::Batch`。例如 disk=2s、network=3s 时，2s 发 disk，3s 发 network，4s 发 disk，6s 发 disk+network。
- `reporter_loop` 收到 telemetry update 后立即尝试生成 `snapshot`、`delta` 或 `heartbeat`；`report.interval` 只是兜底 tick，用于长时间没有采集变化时检查 snapshot/heartbeat 策略，不是唯一上报频率。
- `snapshot` 是完整最新状态；`delta` 只包含变化的顶层采集组；无变化且未到 heartbeat 间隔时，agent 会跳过发送。
- agent 的 report 是 latest-only 语义：export supervisor 只保留最新 report，慢连接或重连时可能跳过中间 report，因此 `ClientFrame.sequence` 允许跳号。
- 出站事件分普通队列和高优先级队列。`ack`、`error`、`remote_task_result`、`job_result` 会优先于普通 report 投递，所以 server 可能先收到 `sequence=11` 的控制响应，再收到 `sequence=10` 的 report。
- `ClientFrame.sequence` 是 agent 进程内全局出站序号，不是每种消息各自递增，也不是网络到达顺序保证。server 可以记录它用于排查跳号和乱序，但不能只因为小于等于最大已见序号就丢弃 frame。
- `delta.base_sequence` 才是合并 delta 的强约束。server 只用它判断当前 latest state 是否能应用该 delta；不匹配时请求 snapshot。
- `ack` / `error` 只表示 agent 已接收并调度带 `ServerFrame.sequence` 的控制命令，不代表 snapshot 已发送、远程 task 已完成或 remote job 已完成。
- `remote_task_result` 在发送成功前会被 agent pending，重连后可能重投；server 必须按 `task_id` 幂等。
- `job_result(kind=probe)` 在发送成功前会被 agent pending，重连后可能重投；server 必须按内部 `result.run_id` 幂等，并优先用 `point_id` 关联业务探测点，用 `request_id` 或 `job_id` 关联本次请求或持续任务。
- `remote_shell_open` 的 ack 比普通控制命令更严格：agent 会等独立 shell stream WebSocket 连接成功、PTY 启动、`opened` 事件发送成功后才回 ack。失败时会回 `remote_shell_open_failed`。
- `basic_info` 不是 Smalux 自有 `ClientFrame` 的核心类型；当前主要用于 Komari 兼容，由 reporter 低频生成后交给 Komari adapter 发送 HTTP `uploadBasicInfo`。
- Komari 兼容模式是 agent 侧 adapter：出站把内部 report/task/job_result(kind=probe) 映射为 Komari report/basic_info/task_result/ping_result；入站把 Komari terminal/exec/ping 转换为内部 remote shell/task/job_apply(kind=probe)。server 自有协议实现不要依赖 Komari 字段。

### Server 收包处理顺序

server 收到一条 agent 业务 frame 后，建议固定按下面顺序处理。这个顺序能同时处理 latest-only report、乱序控制响应和重连重投的一次性结果。

```text
1. transport 层
   -> WebSocket 已升级
   -> wire packet 已解包
   -> secure_psk 已完成 Noise 解密
   -> 得到 JSON bytes

2. frame 层
   -> decode_client_frame(json_bytes)
   -> 校验 protocol_version / agent_id / sequence / sent_at / type
   -> 记录 connection_id、agent_id、frame.sequence、frame.type

3. agent 连接态
   -> 更新 last_seen_at
   -> 更新 max_observed_client_sequence = max(old, frame.sequence)
   -> 如果 frame.sequence <= old max，只记录 possible_out_of_order=true，不直接丢弃

4. payload 分发
   -> snapshot: 覆盖 latest report，更新 delta_base_sequence = frame.sequence
   -> delta: 只在 delta.base_sequence == delta_base_sequence 时合并
   -> heartbeat: 只更新时间，不改 latest report 和 delta_base_sequence
   -> ack/error: 按 payload.sequence 关联 pending command
   -> remote_task_result: 按 result.task_id 幂等写入
   -> job_result(kind=probe): 按 result.result.run_id 幂等写入

5. 响应动作
   -> delta base 不匹配: 下发 snapshot_request
   -> pending command ack/error: 更新命令状态，不再修改业务结果
   -> task/probe result: 更新业务结果，可通知前端 live channel
```

不要把 `frame.sequence <= max_observed_client_sequence` 作为通用丢弃条件。agent 高优先级队列可能让控制响应先到，普通 report 后到；latest-only report 也可能跳过中间序号。server 的幂等判断应落到更具体的业务键上：snapshot/delta 看 `delta_base_sequence`，ack/error 看 `ServerFrame.sequence`，task 看 `task_id`，probe 看 `run_id`。

### 乱序和重复处理矩阵

| frame 类型 | 可乱序到达 | 幂等键或基准 | 低于最大已见 sequence 时如何处理 |
| --- | --- | --- | --- |
| `snapshot` | 可以 | `agent_id` + `frame.sequence` | 不默认丢弃；如果比当前 `delta_base_sequence` 旧很多，可以记录 stale snapshot 并按策略决定是否覆盖 |
| `delta` | 可以 | `delta.base_sequence` | 只要 `delta.base_sequence` 等于当前基准就处理；不匹配就请求 snapshot |
| `heartbeat` | 可以 | `agent_id` + `frame.sequence` | 只更新在线时间；旧 heartbeat 不应覆盖较新的 `last_seen_at` |
| `ack` | 可以 | `ack.sequence`，也就是 server 下发的 `ServerFrame.sequence` | 查 pending command；已处理过则当重复 ack 记录 |
| `error` | 可以 | `error.sequence`，也就是 server 下发的 `ServerFrame.sequence` | 查 pending command；已处理过则当重复 error 记录 |
| `remote_task_result` | 可以，会重投 | `result.task_id` | 幂等 upsert；同一 `task_id` 的最终状态不要重复创建 |
| `job_result(kind=probe)` | 可以，会重投 | `result.result.run_id` | 幂等 upsert；用内部 probe result 的 `point_id` 做业务探测点归属，用 `request_id/job_id` 做执行关联 |

首版建议不要为了“严格顺序”牺牲 agent 的高优先级响应能力。server 只要做到上面这些幂等规则，就能同时兼容实时 report、控制响应和重连重投。

HKDF 测试向量，server 第一版必须覆盖：

```text
key_id                = "agent-key"
secret bytes          = 32 bytes of 0x07
secret_base64url      = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc"
token                 = "smx1.agent-key.BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc"
derived_psk_hex       = "a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2"
derived_psk_base64url = "plsq_xK2fp0l-ucJSyQkgTOgQ6Hy8roWFXJ5gGstYqI"
```

server 测试时不要直接拿 token 里的 secret 当 Noise PSK。正确流程是：从 Hello 读取 `key_id`，查询 server 保存的 `secret_base64url`，base64url 解码出原始 secret bytes，再按上面 HKDF 参数派生 `derived_psk`，最后放入 Noise `psk(0)`。

`key_id` 只用于查 secret 和参与 HKDF info，不能当成认证已通过。只有 Noise 握手能用派生 PSK 成功完成时，server 才能把连接状态切到 ready。server 日志只能记录 `key_id`、wire kind、session id 和错误码，不要打印完整 token、secret、PSK、Authorization header 或带 token 的 URL。

### 连接状态机

server 侧可以把每条 agent 主连接按下面状态管理：

```text
accepted
  -> authenticating        # binary_plain 校验 query/bearer/none；secure_psk 校验是否允许该 wire mode
  -> wire_negotiating      # binary_plain 直接进入 ready；secure_psk 用 key_id 查 secret 并完成 Noise 握手
  -> ready                 # 可以收 ClientFrame，也可以下发控制消息
  -> closing               # 收到 close、读写失败或协议错误
  -> disconnected          # 清理内存连接态，latest snapshot 可保留
```

建议把“连接态”和“监控最新状态”分开保存。连接断开只清理 WebSocket sink、Noise transport、未完成的请求等待器，不删除 `latest_report`；这样 UI 还能显示最后一次上报和离线时间。

### Wire 解包伪代码

server 的 WebSocket binary 处理逻辑可以按这个顺序写：

```text
on_binary(bytes):
  packet = decode_wire_packet(bytes)
  assert packet.magic == "SMX1"
  assert packet.version == 1
  assert packet.payload_len == bytes.len - 36

  if mode == binary_plain:
    assert packet.kind == PlainData
    json_bytes = packet.payload

  if mode == secure_psk:
    if state == waiting_hello:
      assert packet.kind == Hello
      assert packet.sequence == 0
      hello = json_decode(packet.payload)
      assert hello.pattern == "Noise_NNpsk0_25519_ChaChaPoly_BLAKE2s"
      session_id = packet.session_id
      secret = lookup_secret(hello.key_id)
      psk = hkdf_sha256(secret, key_id=hello.key_id)
      start_noise_responder(psk)
      state = waiting_handshake

    else if state == waiting_handshake:
      assert packet.kind == Handshake
      assert packet.session_id == session_id
      assert packet.sequence == 1
      read_noise_msg1(packet.payload, handshake_payload=b"")
      msg2 = write_noise_msg2(handshake_payload=b"")
      send WirePacket(kind=Handshake, same session_id, payload=msg2)
      state = ready

    else if state == ready:
      assert packet.kind == SecureData
      assert packet.session_id == session_id
      json_bytes = noise_decrypt(packet.payload)

  text = utf8(json_bytes)
  route_json(text)
```

`binary_plain` 开发期可以允许 WebSocket text frame 直接进入 `route_json()`；`secure_psk` 不允许 text frame，因为 text 会绕过业务加密。

WirePacket 固定头和 agent 一致，所有整数都是 big-endian：

```text
magic       4 bytes   "SMX1"
version     1 byte    当前固定 1
kind        1 byte    1 PlainData, 2 Hello, 3 Handshake, 4 SecureData, 5 Close
flags       2 bytes   当前保留，写 0
session_id 16 bytes   当前连接或 stream 的随机 session id
sequence    8 bytes   业务消息序号；Hello=0，首个 Handshake=1
payload_len 4 bytes   payload 长度
payload     N bytes   明文 JSON、Noise 握手消息或密文
```

`payload_len` 最大为 `1 MiB`，超过应按连接级错误处理。`flags` 当前固定写 `0`；首版 server 可以拒绝非 0 flags，后续如果 wire 版本扩展再放宽。`session_id` 在同一条连接内必须一致：Hello 建立 session，Handshake、SecureData 和 server 回握手包都沿用同一个 `session_id`。

### 消息格式

agent 内部先生成 `OutboundReport::Snapshot`，当前默认 `smalux_json` 格式会把它编码成 `ClientFrame::Snapshot` JSON，snapshot payload 内包含完整 `AgentReport`。核心结构：

```jsonc
{
  "protocol_version": 1, // server 必须先校验通信协议版本
  "agent_id": "agent-1", // server 侧连接和最新快照主键
  "sequence": 1, // agent 侧递增消息序号
  "sent_at": 1710000000, // agent 发送 frame 的 Unix 秒
  "type": "snapshot",
  "report": {
    "meta": {
      "schema_version": 5, // server 必须校验上报模型版本
      "agent_version": "0.1.0",
      "report_at": 1710000000
    },
    "identity": {
      "agent_id": "agent-1",
      "hostname": "host-1",
      "public_ip": {
        "status": "ready" // ready | failed | stale | disabled | pending
      },
      "local_ips": []
    },
    "system": {}
  }
}
```

### Frame 分发语义

server 不应该把所有 payload 都当成完整指标：

- `snapshot`：完整状态，直接覆盖该 agent 的 latest state，并把当前 frame `sequence` 作为 delta 基准。
- `delta`：只包含变化的顶层采集组；server 只做顶层组替换，不做字段级 merge；如果 `base_sequence` 不匹配，应下发 `snapshot_request`。
- `heartbeat`：只说明 agent 业务上仍在线，不修改 CPU、磁盘、网络等指标。
- `ack` / `error`：只关联 server 之前下发的 `ServerFrame.sequence`，不代表远程任务或探测已经完成。
- `remote_task_result`：通过 `result.task_id` 关联非交互任务。
- `job_result(kind=probe)`：通过内部 `result.run_id` 幂等保存单次探测运行；`result.point_id` 是 server 下发的业务探测点 ID，用它关联 UI 里的固定探测点；`source=once` 时用 `result.request_id` 关联一次性请求，`source=job` 时用 `result.job_id` 关联持续探测任务。`status=rejected` 表示 agent 未发包，例如未启用或限频。

### Delta 合并伪代码

server 只需要保存一份 latest state 和一个 delta 基准序号：

```text
on_snapshot(frame):
  latest_report = frame.report
  base_sequence = frame.sequence
  last_seen_at = now()

on_delta(frame):
  if latest_report is None:
    send_snapshot_request("missing_snapshot")
    return

  if frame.delta.base_sequence != base_sequence:
    send_snapshot_request("delta_base_mismatch")
    return

  for group in [identity, core, disk, network, processes, sockets]:
    if group field is absent:
      keep existing group
    else if group field is null:
      clear existing group
    else:
      replace whole group with incoming group

  base_sequence = frame.sequence
  last_seen_at = now()
```

`identity` 是对象，不是 `Option<Option<...>>`；它出现时整体替换，不出现时保持旧值。`core/disk/network/processes/sockets` 出现 `null` 时表示该采集组被关闭。

### 校验规则

`ingest/` 首版只做轻量 fail-fast 校验：

- `protocol_version` 必须等于当前支持版本 `1`。
- `agent_id` 必须非空，并且 snapshot 中 `report.identity.agent_id` 应与 frame 顶层 `agent_id` 一致。
- `sequence` 必须大于 `0`；同一 agent 后续可用于记录跳号、乱序和排查发送路径，但不能作为唯一丢弃依据。
- `sent_at` 必须大于 `0`。
- `type=snapshot` 时必须包含 `report`。
- `report.meta.schema_version` 必须等于当前支持版本 `5`。
- `report.meta.agent_version` 不能为空。
- `report.meta.report_at` 必须大于 `0`。
- `report.identity.hostname` 允许为空但建议记录 warn，部分平台可能取不到主机名。
- `report.identity.public_ip.status=ready` 或 `stale` 时，`ip` 应存在。
- `report.identity.public_ip.status=failed` 时，`error` 应存在。
- `report.core`、`report.disk`、`report.network`、`report.processes`、`report.sockets` 都是可选分组；缺失表示 agent 侧禁用或尚未启用，不视为错误。
- `report.disk.value.disks=[]` 和 `report.network.value.networks=[]` 是合法状态，表示只上报汇总。
- `report.processes.value.level` / `report.sockets.value.level` 可能为 `count`、`light` 或 `details`；server 需要按字段是否存在处理 `light/details`，不要假设每次都有明细。

认证和授权后续单独设计。当前实现应明确分成三类：`none` 只用于本地开发或可信内网；query/bearer 只用于 `binary_plain` 这类兼容明文识别；`secure_psk` 通过 `key_id -> secret` 和 Noise 握手完成认证，不能再叠加 query/bearer token。

### 错误处理

server 收到异常数据时要区分“单条消息错误”和“连接级错误”，不要因为某个可忽略业务字段导致主连接频繁断开。

建议规则：

- 连接级错误：wire magic/version 错误、`secure_psk` 握手失败、token 或 key_id 不存在、密文解不开、payload 超过上限。这类错误应关闭连接。
- frame 级错误：JSON 语法错误、缺少 `protocol_version` / `agent_id` / `sequence` / `type`、`protocol_version` 不支持。这类错误记录后可以关闭连接，避免双方状态继续错位。
- 业务级错误：`delta.base_sequence` 不匹配、未知 `type`、未知可选字段、单个采集组字段不完整。这类错误优先记录日志和指标；`delta` 不匹配时发送 `snapshot_request`，未知字段默认忽略。
- 控制级错误：server 发出的 `ServerFrame` 收到 `error.sequence` 时，只把对应 pending command 标记为失败，不要关闭主连接。

建议给日志打上固定字段，方便后续排查：

```text
agent_id
connection_id
client_sequence
server_sequence
frame_type
wire_mode
error_code
error_message
```

REST API 建议统一错误响应：

```jsonc
{
  "error": {
    "code": "invalid_payload",
    "message": "field `command.type` is required",
    "request_id": "req_01hxyz...",
    "details": {
      "field": "command.type"
    }
  }
}
```

建议规则：

- `code` 用稳定 `snake_case`，供前端或 CLI 逻辑判断。
- `message` 面向人类排查，可以后续微调，但不要让调用方依赖整句文本。
- `request_id` 用于串联日志、trace 和用户报错。
- `details` 只放可公开的结构化字段，不放 token、header、secret 或原始 body。

推荐状态码：

| HTTP 状态码 | 场景 | `error.code` 建议 |
| --- | --- | --- |
| `400` | JSON 非法、字段缺失、字段类型错误 | `invalid_payload` |
| `401` | 未认证或 token/key 不匹配 | `unauthorized` |
| `403` | 已认证但权限不足 | `permission_denied` |
| `404` | agent/command/task/probe 不存在 | `not_found` |
| `409` | 并发状态冲突、重复创建、旧 generation | `conflict` |
| `422` | 结构合法但业务校验失败 | `validation_failed` |
| `500` | 内部错误 | `internal_error` |

### 控制消息

server 通过同一条 Smalux WebSocket 控制通道下发 `ServerFrame`。当前只保留这一种控制入口，避免不同命令有的回 ack、有的不回 ack，导致 server 状态难以维护。

当前稳定 `ServerFrame` 支持：

- `snapshot_request`
- `config_patch`
- `collect_processes_once`
- `collect_sockets_once`
- `remote_shell_open`
- `remote_task_run`
- `job_apply`

第一版 server 建议先实现：

- `ServerFrame(type=snapshot_request)`：当 server 没有完整状态、delta 基准不匹配或用户主动刷新时发送。
- `ServerFrame(type=config_patch)`：动态调整 `AgentConfig` 中的采集、上报和导出参数。
- `ack/error` 接收：只用于确认 agent 是否接收并调度了带 `sequence` 的控制命令。

后续再接：

- `collect_processes_once`：请求 agent 立即采样一次进程信息，结果进入下一次 snapshot/delta。
- `collect_sockets_once`：请求 agent 立即采样一次 socket 信息，结果进入下一次 snapshot/delta。
- `remote_shell_open`：打开远程交互式 shell，前提是 agent 启动时显式开启。
- `remote_task_run`：执行一次非交互命令，前提是 agent 启动时显式开启。
- `job_apply`：执行一次通用远程 job，或同步持续 job 表；当前稳定 `kind=probe`，用于 TCP/HTTP 探测。probe 默认关闭，但可以通过 `config_patch.remote_probe.enabled=true` 动态开启。

server 如果要远程打开 `processes.level=details` 或 `sockets.level=details`，agent 必须启动时带对应 CLI-only 授权：`--allow-process-level details` 或 `--allow-socket-level details`。一次性 details 采集同样受这个限制。

控制消息发送规则：

- 发送 `ServerFrame` 前先分配 server 侧递增 `sequence`，保存一条 pending command。
- 收到 `ack.sequence` 后，只能把该 command 标记为“已调度”；不能把远程 task/job 标记为完成。
- 收到 `error.sequence` 后，把该 command 标记为失败，并记录 `error.code` 和 `error.message`。
- 重连后不要盲目重发所有有副作用命令。`config_patch` 可以按当前 desired config 重发；`remote_task_run` 这类有副作用的命令必须靠 `task_id` 去重；`job_apply(operation=once, kind=probe)` 靠 `request_id` 关联一次性请求，`replace/patch` 靠 `generation` 防止旧任务表覆盖新任务表。

### 控制命令生命周期

server 下发控制命令时，建议把“命令是否被 agent 接收”和“业务动作是否完成”拆成两条状态线。

```text
create command
  -> allocate ServerFrame.sequence
  -> persist PendingCommand(status=created)
  -> send ServerFrame over current agent connection
  -> PendingCommand(status=sent)

agent returns ClientFrame(type=ack)
  -> mark PendingCommand(status=acked)
  -> only means agent accepted/scheduled the command

agent returns ClientFrame(type=error)
  -> mark PendingCommand(status=failed)
  -> save error.code/error.message

business result arrives later
  -> snapshot_request: later snapshot/delta arrives through report path
  -> collect_processes_once: later report contains processes group update
  -> collect_sockets_once: later report contains sockets group update
  -> remote_task_run: later remote_task_result arrives with task_id
  -> job_apply once kind=probe: later job_result arrives with point_id/request_id/run_id
  -> job_apply replace/patch kind=probe: later job_result arrives with point_id/job_id/run_id
 -> remote_shell_open: ack already means stream ready; session output goes through shell stream
```

建议把 command 状态收敛成固定集合：

| 状态 | 含义 | 是否终态 |
| --- | --- | --- |
| `queued` | REST 已创建，等待路由到连接 | 否 |
| `sent` | 已编码并写入 agent 连接发送队列 | 否 |
| `acked` | agent 已接收并调度 | 否 |
| `failed` | agent 返回 `error` 或 server 自己判定失败 | 是 |
| `finished` | 业务结果已到达，例如 task result 或 once probe result | 是 |
| `expired` | 超过有效期仍未 ack/result | 是 |

建议规则：

- `config_patch`、`snapshot_request`、`collect_*` 通常最多走到 `acked`，它们的业务完成更多由后续 report 体现。
- `remote_task_run` 在收到 `remote_task_result` 后进入 `finished`。
- `job_apply(operation=once, kind=probe)` 在收到对应 `job_result(kind=probe)` 后进入 `finished`。
- `job_apply(operation=replace/patch, kind=probe)` 更接近“配置同步命令”，通常在 `acked` 即视为命令链路完成，持续 job 的业务结果走独立结果流。

各命令的 server 状态建议：

| 命令 | ack 表示 | 最终业务结果来自 | 幂等键 | 是否建议重连后自动重发 |
| --- | --- | --- | --- | --- |
| `snapshot_request` | agent 已接受强制 snapshot 请求 | 后续 `snapshot` frame | `ServerFrame.sequence` | 可以限频重发 |
| `config_patch` | agent 已应用或接受配置变更 | 无独立业务结果；以后看 report 行为 | desired config version | 可以按 desired config 重发 |
| `collect_processes_once` | agent 已把一次性采集命令投递给 collector | 后续 report 的 `processes` 分组 | `ServerFrame.sequence` | 不建议自动重发，避免高成本扫描 |
| `collect_sockets_once` | agent 已把一次性采集命令投递给 collector | 后续 report 的 `sockets` 分组 | `ServerFrame.sequence` | 不建议自动重发，避免高成本扫描 |
| `remote_task_run` | agent 已接收或启动任务 | `remote_task_result.result.task_id` | `task_id` | 默认不自动重发，除非确认未执行 |
| `job_apply once kind=probe` | agent 已接收一次性探测请求 | `job_result(kind=probe).result.point_id/request_id/run_id` | `point_id` / `request_id` / `run_id` | 可按业务需求重发，但要限频 |
| `job_apply replace/patch kind=probe` | agent 已应用持续任务表变更 | 后续 `source=job` 的结果 | `generation` / `point_id` / `job_id` / `run_id` | 推荐重连后用新 `generation` replace 同步 |
| `remote_shell_open` | 独立 shell stream 已连接、PTY 已启动、opened 已发送 | shell stream 的 output/exit/error | `session_id` | 不自动重发，由用户重新打开 |

pending command 超时只说明没有收到 `ack/error`。对于 `remote_task_run`、`remote_shell_open` 这类有副作用命令，server 不能仅凭 pending 超时就假定 agent 没执行；需要结合业务结果、连接断开时间和人工操作决定。

### 控制消息示例

请求完整快照：

```jsonc
{
  "protocol_version": 1,
  "sequence": 201,
  "sent_at": 1710001000,
  "type": "snapshot_request",
  "request": { "reason": "manual_refresh" }
}
```

动态调整采样频率：

```jsonc
{
  "type": "config_patch",
  "patch": {
    "core": { "interval": "2s" },
    "network": { "interval": "10s" },
    "report": { "interval": "10s" },
    "outbound": {
      "realtime_report": { "send_on_start": true },
      "basic_info": { "refresh_interval": "5m" }
    }
  }
}
```

开启远程 probe 并请求 TCP 探测：

自有协议字段定义、稳定 `type` 列表和 wire/secure_psk 细节，统一以 [crates/smalux-protocol/README.md](/abs/path/F:/code/rust/smalux/crates/smalux-protocol/README.md) 为准；本节重点描述 server 侧如何使用这些 payload。

```jsonc
{ "type": "config_patch", "patch": { "remote_probe": { "enabled": true } } }
```

```jsonc
{
  "protocol_version": 1,
  "sequence": 202,
  "sent_at": 1710001001,
  "type": "job_apply",
  "request": {
    "operation": "once",
    "runs": [
      {
        "kind": "probe",
        "request_id": "probe-1",
        "point_id": "point-main-api",
        "probe_type": "tcp",
        "target": "example.com:443",
        "timeout": "5s"
      }
    ]
  }
}
```

### Server 内部状态建议

首版 server 不需要一开始就做复杂领域模型，但建议把下面几类状态分开：

```text
AgentConnectionState
  agent_id
  connection_id
  connected_at
  last_frame_at
  wire_mode
  secure_key_id
  websocket_sink
  noise_transport

AgentLatestState
  agent_id
  last_seen_at
  last_frame_sequence
  delta_base_sequence
  latest_report
  last_heartbeat_at
  connection_state

PendingCommand
  server_sequence
  agent_id
  command_type
  sent_at
  status          # queued | sent | acked | failed | timed_out
  error_code
  error_message

PendingRemoteTask
  task_id
  agent_id
  sent_at
  status          # sent | running | success | failed | timed_out | rejected
  result

PendingRemoteProbe
  run_id
  agent_id
  source          # once | job
  point_id        # server 业务探测点 ID
  request_id
  job_id
  probe_type
  target
  status          # sent | success | failed | rejected
  latency_ms
  result
```

这样拆分后，WebSocket 重连不会影响最新监控状态；server 下发命令的 ack/error 也不会和 remote task/job 的最终结果混在一起。

### 幂等和重连策略

server 需要把三类数据分开处理：

- 最新状态：`snapshot` / `delta` / `heartbeat`。只保存最新状态，旧 report 不排队，防止高频 agent 把 server 内存打满。
- 一次性结果：`ack` / `error` / `remote_task_result` / `job_result`。`ack/error` 用 `sequence` 关联 pending command，remote task 用 `task_id` 幂等，`job_result(kind=probe)` 用 `run_id` 幂等并优先用 `point_id` 做业务探测点关联。
- 控制命令：server 主动发送给 agent。`config_patch` 可以在重连后按 desired config 重新下发；`remote_task_run` 这类有副作用的命令不要自动重发，除非 server 能根据 `task_id` 确认 agent 没有执行过。持续探测任务建议保存 desired jobs，重连后用 `job_apply(operation=replace, kind=probe)` 带新 `generation` 同步。

建议规则：

- 同一 agent 的 `ClientFrame.sequence` 小于等于已见最大序号时，只记录为可能乱序或重复，不能默认忽略；是否处理要看 payload 类型、幂等键和 `delta.base_sequence`。
- 收到 `delta.base_sequence != delta_base_sequence` 时，不处理该 delta，立即发送 `snapshot_request`。
- 收到新的 `snapshot` 后，用它重建 latest state，并把 `delta_base_sequence` 设置为该 frame 的 `sequence`。
- 收到 `heartbeat` 时只更新 `last_heartbeat_at` 和 `last_seen_at`，不要覆盖指标。
- WebSocket 断开时，把连接态改成 disconnected，但保留 `latest_report` 和 pending 任务结果等待状态。
- pending command 超时只说明 agent 没回 ack/error，不代表命令一定没执行；对有副作用命令要靠业务结果或人工确认。

### 安全底线

即使首版只用于自用，也建议先固定下面的底线，避免后面补安全时推翻协议：

- 生产环境优先使用 `wss`；如果用 `ws`，至少限制在可信内网。
- `secure_psk` 模式下不要同时使用 query/bearer token，agent 当前也会拒绝这种组合，避免 token 明文出现在 URL 或 header。
- 如果 agent 当前 `export.secure_required=true`，server 不要下发关闭 `secure_required`、切到 `binary_plain` 或切到 `komari` 的 patch；agent 会拒绝这类降级。
- server 日志不要打印完整 token、secure secret、PSK、Authorization header、带 token 的 URL。
- `key_id` 只能用于查 secret，不是认证成功本身；认证成功发生在 Noise 握手能完成时。
- `remote_task_run` / `remote_shell_open` 默认不要在 UI 中暴露，必须确认 agent 启动时显式开启。
- server 下发 details 采集前，先确认 agent 启动时开启了 `--allow-process-level details` 或 `--allow-socket-level details`。
- 对单 agent 和单连接做基础频率限制，尤其是 `snapshot_request`、`job_apply(operation=once, kind=probe)` 和未来的 remote task。

### 测试清单

server 第一版建议至少覆盖这些测试：

| 类型 | 场景 | 期望 |
| --- | --- | --- |
| wire | `PlainData` 正常解包 | 得到 JSON bytes |
| wire | magic/version/payload_len 错误 | 拒绝 frame，不 panic |
| wire | payload 超过 `1 MiB` | 关闭连接或拒绝 frame |
| wire | flags 非 0 | 首版拒绝，避免未知语义 |
| secure | HKDF 测试向量 | 派生 PSK 等于 `a65b2aff12b67e9d25fae7094b24248133a043a1f2f2ba16157279806b2d62a2` |
| secure | Hello pattern 不支持 | 关闭连接，不能进入 ready |
| secure | Hello key_id 不存在 | 关闭连接或返回协议错误 |
| secure | Handshake / SecureData 的 session_id 不匹配 | 关闭连接 |
| secure | secret base64url 解码失败或不足 32 字节 | 拒绝配置或拒绝连接 |
| secure | PSK 不匹配 | Noise 握手失败，不能进入 ready |
| secure | `SecureData` AEAD 解密失败 | 关闭连接 |
| secure | secure_psk 收到 text frame | 拒绝并关闭连接 |
| secure | server 下发控制 JSON | 先 Noise encrypt，再封 `WirePacket(kind=SecureData)` |
| frame | `snapshot` 写入 | latest state 被完整覆盖 |
| frame | `delta.base_sequence` 匹配 | 顶层采集组整体替换 |
| frame | `delta.base_sequence` 不匹配 | 不修改 latest，发送 `snapshot_request` |
| frame | `heartbeat` | 只更新在线时间，不修改指标 |
| control | `snapshot_request` ack | pending command 标记为 acked |
| control | `snapshot_request` error | pending command 标记失败并保存错误 |
| task | 重复 `remote_task_result.task_id` | 幂等覆盖，不创建重复记录 |
| probe | 重复 `job_result(kind=probe).result.run_id` | 幂等覆盖，不创建重复记录 |
| reconnect | agent 断开重连后发 snapshot | connection state 更新，latest state 正常覆盖 |

### 存储策略

首版使用内存 latest-only 缓存：

```text
HashMap<agent_id, AgentRuntimeState>
```

建议状态结构：

```text
AgentRuntimeState
  agent_id
  last_seen_at        # server 收到上报的时间
  last_report_at      # agent payload 里的 meta.report_at
  agent_version
  hostname
  public_ip_status
  latest_report       # 完整 AgentReport
```

写入语义：

- 同一个 `agent_id` 的新 report 覆盖旧 report。
- 不做 report 队列，避免 server 因 agent 高频上报堆积。
- 如果后续需要历史曲线，再把 `core/disk/network/processes/sockets` 拆成时序写入，不影响 latest 缓存。

如果首版就接 SQLite，建议仍然先保持 latest-only 思路，把“在线最新状态”和“历史曲线”分开：

```text
agents
  agent_id TEXT PRIMARY KEY
  display_name TEXT NULL
  created_at INTEGER NOT NULL
  updated_at INTEGER NOT NULL

agent_connections
  connection_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  connected_at INTEGER NOT NULL
  disconnected_at INTEGER NULL
  remote_addr TEXT NULL
  wire_mode TEXT NOT NULL
  close_reason TEXT NULL

agent_latest_reports
  agent_id TEXT PRIMARY KEY
  last_seen_at INTEGER NOT NULL
  last_report_at INTEGER NOT NULL
  last_sequence INTEGER NOT NULL
  delta_base_sequence INTEGER NOT NULL
  schema_version INTEGER NOT NULL
  hostname TEXT NULL
  public_ip_status TEXT NOT NULL
  report_json TEXT NOT NULL

pending_commands
  command_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  server_sequence INTEGER NOT NULL
  command_type TEXT NOT NULL
  status TEXT NOT NULL        # sent | acked | failed | timeout
  request_json TEXT NOT NULL
  response_json TEXT NULL
  created_at INTEGER NOT NULL
  updated_at INTEGER NOT NULL

remote_task_results
  task_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  status TEXT NOT NULL
  exit_code INTEGER NULL
  stdout_truncated INTEGER NOT NULL
  stderr_truncated INTEGER NOT NULL
  result_json TEXT NOT NULL
  updated_at INTEGER NOT NULL

remote_job_results
  run_id TEXT PRIMARY KEY
  agent_id TEXT NOT NULL
  kind TEXT NOT NULL
  source TEXT NOT NULL
  point_id TEXT NULL
  request_id TEXT NULL
  job_id TEXT NULL
  probe_type TEXT NOT NULL
  target TEXT NOT NULL
  status TEXT NOT NULL
  latency_ms INTEGER NULL
  duration_ms INTEGER NOT NULL
  started_at INTEGER NOT NULL
  finished_at INTEGER NOT NULL
  error TEXT NULL
  updated_at INTEGER NOT NULL
```

落库事务建议：

- `snapshot`：一个事务内更新 `agents.updated_at`、覆盖 `agent_latest_reports.report_json`、更新 `last_sequence` 和 `delta_base_sequence`。
- `delta`：先读取当前 `delta_base_sequence`；匹配才合并 JSON 并写回，不匹配不写库，只发送 `snapshot_request`。
- `heartbeat`：只更新 `last_seen_at`，不改 `report_json` 和 `delta_base_sequence`。
- `ack/error`：只更新 `pending_commands`，不要修改 latest report。
- `remote_task_result`：按 `task_id` upsert，重复结果覆盖同一行，保证幂等。
- `job_result(kind=probe)`：按内部 `run_id` upsert，重复结果覆盖同一行；额外保存 `kind`、`source`、`point_id`、`request_id`、`job_id`，方便查询业务探测点、一次性请求和持续任务历史。

### 查询接口

首版前端 REST 查询可以先提供两个只读接口，统一放在 `/api/v1` 下：

```text
GET /api/v1/agents
  -> 返回 agent 列表和 last_seen_at、hostname、public_ip_status

GET /api/v1/agents/{agent_id}
  -> 返回该 agent 的 latest_report
```

建议从第一版开始就把响应 JSON 形状固定下来，避免前端或 CLI 接口先随手拼一版，后面再大面积改动。

`GET /api/v1/agents` 建议响应：

```jsonc
{
  "items": [
    {
      "agent_id": "agent-1",
      "hostname": "node-1",
      "online": true,
      "last_seen_at": 1710000100,
      "last_frame_sequence": 31,
      "public_ip_status": "ready",
      "public_ip": "1.2.3.4",
      "system": {
        "os": "linux",
        "platform": "ubuntu",
        "arch": "x86_64"
      }
    }
  ],
  "total": 1
}
```

字段建议：

- `agent_id`：稳定主键，直接对应 `ClientFrame.agent_id`。
- `online`：只表达当前连接是否在线，不代表最近一次任务一定成功。
- `last_seen_at`：server 收到任意 agent 上报的时间，Unix 秒。
- `last_frame_sequence`：最后一条已处理 `ClientFrame.sequence`。
- `public_ip_status`：直接透传 latest report 里的状态，前端不要自己猜。

`GET /api/v1/agents/{agent_id}` 建议响应：

```jsonc
{
  "agent_id": "agent-1",
  "online": true,
  "connected_at": 1710000000,
  "last_seen_at": 1710000100,
  "last_frame_sequence": 31,
  "delta_base_sequence": 30,
  "latest_report": {
    "meta": {
      "schema_version": 5,
      "agent_version": "0.1.0",
      "report_at": 1710000099
    },
    "identity": {},
    "system": {},
    "core": {},
    "disk": {},
    "network": {},
    "processes": {},
    "sockets": {}
  }
}
```

建议规则：

- 这个接口直接返回当前 latest state，不要额外做字段裁剪版和完整版两种形态。
- `latest_report` 不存在时返回 `null`，但整体接口仍可返回 200，用于区分“agent 已登记但尚未上报”和“agent 不存在”。
- `404` 只用于 `agent_id` 根本不存在。

### 控制命令接口 JSON

`POST /api/v1/agents/{agent_id}/commands` 建议请求体使用统一 envelope，而不是不同命令各开一个 REST 路径：

```jsonc
{
  "command": {
    "type": "snapshot_request",
    "request": {
      "reason": "manual_refresh"
    }
  }
}
```

```jsonc
{
  "command": {
    "type": "config_patch",
    "patch": {
      "core": { "interval": "2s" },
      "report": { "interval": "10s" }
    }
  }
}
```

```jsonc
{
  "command": {
    "type": "job_apply",
    "request": {
      "operation": "once",
      "runs": [
        {
          "kind": "probe",
          "request_id": "probe-1",
          "point_id": "point-main-api",
          "probe_type": "tcp",
          "target": "example.com:443",
          "timeout": "5s"
        }
      ]
    }
  }
}
```

建议响应：

```jsonc
{
  "command_id": "cmd_01hxyz...",
  "agent_id": "agent-1",
  "status": "queued", // queued | sent | acked | failed | finished | expired
  "server_sequence": 205,
  "command_type": "job_apply",
  "created_at": 1710000200
}
```

建议规则：

- `command_id` 是 server 侧主键；`server_sequence` 只是当前连接上的协议序号。
- `config_patch`、`snapshot_request`、`collect_*` 的最终完成更多依赖后续 report 行为，不要把 `acked` 误解成业务完成。
- `remote_task_run` 和 `job_apply(kind=probe)` 的业务结果分别通过 `task_id`、`request_id/job_id/run_id` 再继续关联。

`GET /api/v1/commands/{command_id}` 建议响应：

```jsonc
{
  "command_id": "cmd_01hxyz...",
  "agent_id": "agent-1",
  "command_type": "job_apply",
  "status": "acked",
  "server_sequence": 205,
  "request_json": {},
  "response_json": {
    "type": "ack",
    "sequence": 205
  },
  "created_at": 1710000200,
  "updated_at": 1710000201
}
```

### 结果查询接口 JSON

`GET /api/v1/agents/{agent_id}/tasks/{task_id}` 建议响应：

```jsonc
{
  "agent_id": "agent-1",
  "task_id": "task-1",
  "status": "success",
  "exit_code": 0,
  "stdout": "2026-05-29T12:00:00Z",
  "stderr": "",
  "started_at": 1710000099,
  "finished_at": 1710000100,
  "duration_ms": 1000,
  "timed_out": false,
  "stdout_truncated": false,
  "stderr_truncated": false,
  "error": null
}
```

`GET /api/v1/agents/{agent_id}/probes/{run_id}` 建议响应：

```jsonc
{
  "agent_id": "agent-1",
  "kind": "probe",
  "run_id": "4c21e6f8-8ef3-4ee5-9c91-8f630f650b92",
  "source": "once",
  "point_id": "point-main-api",
  "request_id": "probe-1",
  "job_id": null,
  "probe_type": "tcp",
  "target": "example.com:443",
  "status": "success",
  "latency_ms": 13,
  "started_at": 1710000100,
  "finished_at": 1710000101,
  "duration_ms": 13,
  "error": null
}
```

### 实时订阅 JSON

如果后面做 `/live/v1/dashboard`，建议事件统一用 `event + data` envelope：

```jsonc
{ "event": "agent.updated", "data": { "agent_id": "agent-1", "online": true, "last_seen_at": 1710000100 } }
{ "event": "command.updated", "data": { "command_id": "cmd_01hxyz...", "status": "acked" } }
{ "event": "task.updated", "data": { "agent_id": "agent-1", "task_id": "task-1", "status": "success" } }
{ "event": "probe.updated", "data": { "agent_id": "agent-1", "run_id": "4c21...", "status": "success" } }
```

建议规则：

- 实时事件只用于刷新 UI，不承担唯一真实来源；前端随时可以退回 REST 拉取 latest。
- `event` 名保持稳定，`data` 允许未来增加字段。

等 Web UI 需求明确后，再补：

- 按标签、主机名、公网 IP 状态筛选。
- 查询历史 CPU/内存/磁盘/网络曲线。
- 查询离线 agent 和最近错误状态。

### HTTP 端点规划

首版 server 可以按“写入入口少、查询入口清晰”的方式规划端点：

| 端点 | 方法 | 作用 | 首版是否需要 |
| --- | --- | --- | --- |
| `/agent/v1/connect` | `GET` upgrade | Smalux agent 主 WebSocket，接收 `ClientFrame` 和下发控制消息 | 必须 |
| `/api/v1/agents` | `GET` | 查询 agent 列表、在线状态和摘要字段 | 必须 |
| `/api/v1/agents/{agent_id}` | `GET` | 查询单个 agent 的 latest report | 必须 |
| `/api/v1/agents/{agent_id}/commands` | `POST` | 创建 server 控制命令，例如 `snapshot_request`、`job_apply` | 可后做 |
| `/api/v1/commands/{command_id}` | `GET` | 查询 pending command 的 ack/error 状态 | 可后做 |
| `/api/v1/agents/{agent_id}/tasks/{task_id}` | `GET` | 查询 remote task 结果 | 可后做 |
| `/api/v1/agents/{agent_id}/probes/{run_id}` | `GET` | 查询 remote probe 单次运行结果 | 可后做 |
| `/live/v1/dashboard` | `GET` upgrade 或 SSE | 前端 dashboard 实时推送、事件订阅和命令反馈 | 可后做 |

端点职责建议：

- `/agent/v1/connect` 不直接做复杂查询，只负责连接、解包、分发和发送控制消息。
- `/api/v1/*` 只给前端和管理端 REST 使用，不承载 agent 主连接。
- `/live/v1/*` 只给前端实时订阅使用，不承载 agent 上报。
- 查询端点只读 storage，不直接访问 WebSocket sink。
- 创建控制命令时先写 `pending_commands`，再投递到当前在线连接；如果 agent 离线，按命令类型决定是拒绝、排队还是只保存 desired config。
- `config_patch` 更像 desired config，不建议作为普通一次性命令长期排队；agent 重连后 server 可以比较 desired config 和当前 effective 状态后再下发。

### Ingest 分发伪代码

server 的 `ingest/` 可以把 transport 细节隔离掉，只接收已经解包出来的 JSON bytes：

```text
handle_client_json(connection, json_bytes):
  frame = decode ClientFrame(json_bytes)
  validate_common_fields(frame)

  if frame.agent_id != connection.agent_id:
    return protocol_error("agent_id_mismatch")

  match frame.type:
    snapshot:
      validate_report(frame.report)
      storage.apply_snapshot(frame.agent_id, frame.sequence, frame.report)
      connection.last_seen_at = now()

    delta:
      result = storage.apply_delta(frame.agent_id, frame.sequence, frame.delta)
      if result == DeltaBaseMismatch:
        send_server_frame(snapshot_request("delta_base_mismatch"))

    heartbeat:
      storage.touch_heartbeat(frame.agent_id, frame.sequence, frame.heartbeat)

    ack:
      storage.mark_command_acked(frame.agent_id, frame.ack.sequence)

    error:
      storage.mark_command_failed(frame.agent_id, frame.error.sequence, frame.error)

    remote_task_result:
      storage.upsert_remote_task_result(frame.agent_id, frame.result)

    job_result:
      if frame.result.kind == probe:
        storage.upsert_remote_job_result(frame.agent_id, frame.result)

    unknown:
      log and ignore
```

注意 `ClientFrame.sequence` 是 agent 的全局出站序号，不是每种消息各自递增。server 可以用它判断“该连接上是否见过更新的 frame”，但不要假设连续序号一定都到达；导出 job 可能因为只发送最新 report 而跳过中间 report。

### 实现顺序

建议按下面顺序写代码：

1. 在 `storage/` 定义 latest-only 存储 trait 和内存实现。
2. 在 `auth/` 定义 agent 认证接口和 secure key 查找接口。
3. 在 `ingest/` 实现 `validate_report()` 和 `handle_report()`。
4. 在 `ingest/` 实现 `apply_snapshot()` / `apply_delta()` / `apply_heartbeat()`。
5. 在 `http/agent.rs` 增加 `/agent/v1/connect` WebSocket handler。
6. 实现 `binary_plain` wire 解包和 `ClientFrame` 分发。
7. 增加本地测试：合法 report 写入成功、schema 不匹配失败、同 agent 覆盖旧快照、delta base 不匹配会请求 snapshot。
8. 再补 `GET /api/v1/agents` 和 `GET /api/v1/agents/{agent_id}` 查询接口。
9. 增加 `pending_commands` 和 desired config 状态，支持手动发送 `snapshot_request` 和 `config_patch`。
10. 最后接 `secure_psk`、remote task/probe/shell、历史指标落库和 Web UI。

## 常用命令

当前 `main.rs` 只是最小占位入口，server 业务逻辑还没实现。可以运行下面命令检查 crate：

```powershell
cargo check -p smalux-server
cargo test -p smalux-server
cargo run -p smalux-server
```
