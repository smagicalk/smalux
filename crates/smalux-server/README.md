# smalux-server

`smalux-server` 是中心服务端，负责接收 agent 上报、持久化最新状态、提供 Web/API 查询、下发控制命令，并可选托管前端资源。

当前 server 仍在开发中。已经有 CLI/config、日志、数据库初始化、migration、最小 axum router、health endpoint、agent WebSocket upgrade 和前端槽位骨架；业务闭环还需要继续实现。

## 当前能力

已接入：

- 启动参数解析：`clap` + env。
- 配置模型：HTTP、database、frontend、log。
- 日志初始化：复用 `smalux-core::log::init_tracing`，日志级别读 `RUST_LOG`。
- 数据库初始化：SeaORM，支持 SQLite / PostgreSQL / MySQL。
- Migration 入口：`storage/migration.rs`。
- HTTP router：agent transport、Web API、frontend slot 分开。
- 健康检查：`GET /api/v1/health`。
- agent 主连接入口：`GET /agent/v1/connect`，当前是最小 upgrade 和 ping/pong。
- 前端托管：site/admin 双槽位，支持 `embedded`、`directory`、`external`。

未完成：

- agent token/key 管理。
- `secure_psk` responder 和 wire/frame 收发循环。
- snapshot/delta/heartbeat 入库。
- latest state、agent 列表、dashboard 聚合。
- REST 命令下发、ack/result 回收。
- 管理端 session、权限和 realtime 推送。

## 目录

```text
src/
  main.rs              # 只调用 bootstrap::run()
  bootstrap.rs         # CLI -> config -> log -> database -> router -> listener
  state.rs             # axum handler 共享 AppState
  cli.rs
  cli/
    args.rs            # ServerArgs 和 CLI/env 转换
  config.rs
  config/
    defaults.rs
    validation.rs
    model.rs
    model/
      database.rs
      frontend.rs
      http.rs
      log.rs
      server.rs
  http.rs
  http/
    router.rs          # 总 router 组合
    middleware.rs      # 公共 middleware
    agent.rs           # agent transport 路由入口
    agent/
      agent_ws.rs      # /agent/v1/connect upgrade
    web.rs             # Web 面聚合入口
    web/
      api.rs
      api/
        health.rs
        realtime.rs
      frontend.rs      # site/admin frontend slots
  service.rs
  service/
    agent.rs
    agent/
      auth.rs
      command.rs
      connection.rs
      input.rs
      input/
        frame.rs
        report.rs
      state.rs
    web.rs
    web/
      auth.rs
      dashboard.rs
      slots.rs
    event.rs
  storage.rs
  storage/
    entity.rs
    memory.rs
    migration.rs
    migration/
      m20260614_000001_create_agents.rs
    repository.rs
```

## 路由

| 路由 | 作用 | 当前状态 |
| --- | --- | --- |
| `GET /agent/v1/connect` | agent 主 WebSocket | 已有 upgrade 骨架和 ping/pong |
| `GET /api/v1/health` | 健康检查 | 已有 |
| `GET /api/v1/realtime/*` | 前端实时通道 | 预留 |
| `GET /` | 站点前端 | 由 frontend slot 决定 |
| `GET /admin` | 管理后台 | 由 frontend slot 决定 |
| `GET /assets/site/*` | 站点静态资源 | 由 frontend slot 决定 |
| `GET /assets/admin/*` | 管理后台静态资源 | 由 frontend slot 决定 |

## CLI 参数

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `--bind-addr` | `SMALUX_SERVER_BIND_ADDR` | `127.0.0.1` | HTTP 监听地址 |
| `-p, --bind-port` | `SMALUX_SERVER_BIND_PORT` | `3000` | HTTP 监听端口 |
| `--database-driver` | `SMALUX_SERVER_DATABASE_DRIVER` | `sqlite` | `sqlite` / `postgres` / `mysql` |
| `--database-host` | `SMALUX_SERVER_DATABASE_HOST` | `127.0.0.1` | PostgreSQL/MySQL 主机 |
| `--database-port` | `SMALUX_SERVER_DATABASE_PORT` | 驱动默认端口 | PostgreSQL `5432`，MySQL `3306` |
| `--database-name` | `SMALUX_SERVER_DATABASE_NAME` | 驱动默认 | SQLite 默认 `smalux-server.db`，网络库默认 `smalux` |
| `--database-user` | `SMALUX_SERVER_DATABASE_USER` | 空 | PostgreSQL/MySQL 用户 |
| `--database-password` | `SMALUX_SERVER_DATABASE_PASSWORD` | 空 | PostgreSQL/MySQL 密码 |
| `--database-param KEY=VALUE` | 无 | 空 | 可重复传入数据库 URL query 参数 |
| `--serve-frontend[=BOOL]` | `SMALUX_SERVER_SERVE_FRONTEND` | `false` | 是否托管前端；只传 `--serve-frontend` 等价于 `true` |
| `--site-mode` | `SMALUX_SERVER_SITE_MODE` | `embedded` | site 槽位模式 |
| `--site-dir` | `SMALUX_SERVER_SITE_DIR` | `apps/smalux-web/dist` | site 目录模式路径 |
| `--site-external-url` | `SMALUX_SERVER_SITE_EXTERNAL_URL` | 空 | site 外部部署地址 |
| `--admin-mode` | `SMALUX_SERVER_ADMIN_MODE` | `embedded` | admin 槽位模式 |
| `--admin-dir` | `SMALUX_SERVER_ADMIN_DIR` | `apps/smalux-web/dist` | admin 目录模式路径 |
| `--admin-external-url` | `SMALUX_SERVER_ADMIN_EXTERNAL_URL` | 空 | admin 外部部署地址 |
| `--frontend-spa-fallback` | `SMALUX_SERVER_FRONTEND_SPA_FALLBACK` | `true` | 是否启用 SPA fallback |
| `--log-file` | `SMALUX_SERVER_LOG_FILE` | `logs/smalux-server.log` | 日志文件路径 |
| `-L, --log-retention-files` | `SMALUX_SERVER_LOG_RETENTION_FILES` | `14` | 滚动日志保留数量 |
| `--log-max-size-mb` | `SMALUX_SERVER_LOG_MAX_SIZE_MB` | `64` | 单个日志文件最大 MB |

日志级别只使用 `RUST_LOG`。

agent token/key 不属于 server 启动参数，后续由添加 agent 的业务流程生成并写入数据库。

配置边界：

- `cli/args.rs` 只解析启动输入。
- `config/model/*` 保存稳定运行配置，不依赖 clap。
- `storage.rs` 负责把数据库配置转换成连接 URL，并执行 migration。
- `bootstrap.rs` 负责串联 CLI、校验、日志、数据库和 HTTP server。

## 启动示例

只启动 API 和 agent 接入：

```powershell
$env:RUST_LOG="smalux_server=debug"
cargo run -p smalux-server -- --bind-addr 127.0.0.1 --bind-port 3000
```

SQLite + 本地前端目录：

```powershell
cargo run -p smalux-server -- --database-driver sqlite --database-name smalux-server.db --database-param mode=rwc --serve-frontend --site-mode directory --site-dir apps/smalux-web/dist --admin-mode directory --admin-dir apps/smalux-web/dist
```

PostgreSQL：

```powershell
cargo run -p smalux-server -- --database-driver postgres --database-host 127.0.0.1 --database-user smalux --database-password password --database-param sslmode=disable
```

MySQL：

```powershell
cargo run -p smalux-server -- --database-driver mysql --database-host 127.0.0.1 --database-user smalux --database-password password --database-param charset=utf8mb4
```

## 数据库 URL 规则

server 不要求用户直接传完整 `database_url`，而是从关键属性生成连接地址：

| driver | 生成形状 |
| --- | --- |
| SQLite 文件 | `sqlite://smalux-server.db` |
| SQLite memory | `sqlite::memory:` |
| PostgreSQL | `postgres://user:password@host:port/name?params` |
| MySQL | `mysql://user:password@host:port/name?params` |

日志里使用脱敏后的连接地址，不打印明文密码。

## 前端托管

server 有两个前端槽位：

- `site`: `/` 和 `/assets/site/*`
- `admin`: `/admin` 和 `/assets/admin/*`

槽位模式：

- `embedded`: 使用编译期内置资源，需要 `frontend-embed` feature。
- `directory`: 使用本地目录。
- `external`: 返回临时重定向到外部部署地址。

如果 `--serve-frontend=false`，server 不挂前端路由，只提供 API 和 agent transport。

## 协议对接

server 应复用 `smalux-protocol`：

- `smalux_protocol::wire::decode_wire_packet`
- `smalux_protocol::decode_client_frame_bytes`
- `smalux_protocol::encode_server_frame_bytes`
- `smalux_protocol::secure::parse_secure_token`
- `smalux_protocol::secure::build_noise_responder`
- `smalux_protocol::secure::decrypt_payload`
- `smalux_protocol::secure::encrypt_payload`

字段速查见 [plan.md](plan.md)。`README.md` 不再重复完整 JSON 字段，避免和协议模型漂移。

当前 `/agent/v1/connect` 只完成最小 WebSocket upgrade 和 ping/pong。真正的 `wire -> frame -> service` 读写循环还没接入。

## 推荐实现顺序

1. 完成 agent 认证：token/key 查找、`secure_psk` responder。
2. 完成 `/agent/v1/connect` 读写循环：wire decode -> frame decode -> service 分发。
3. 完成 snapshot/heartbeat latest state。
4. 再做 delta 合并和 snapshot request。
5. 完成 REST 查询：agent 列表、latest、命令状态。
6. 完成命令下发：remote task、job apply、remote shell open。
7. 最后补 dashboard realtime、session、权限和审计。

## 常用命令

```powershell
cargo fmt --all --check
cargo check -p smalux-server
cargo test -p smalux-server
cargo run -p smalux-server -- --help
```
