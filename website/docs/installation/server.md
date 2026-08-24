---
title: Server 安装边界
description: 当前 Server 骨架、数据库依赖和部署前必须补齐的能力。
---

# Server 安装边界

`smalux-server` 当前提供 Axum、Tower、SeaORM 和 Protocol 依赖构成的服务端骨架。它不是已经完成的
监控平台发行物，正式 Agent 注册表、Job 管理 API、指标存储和 Web 管理端仍需要继续实现。

:::warning 当前运行状态

正式 Server 默认监听 `127.0.0.1:8080`，启动时会读取数据库环境变量并执行 SeaORM migration，随后
装配前端健康检查和 Agent 路由。监听成功仍不等于 Agent 注册、授权和 Job 业务已经完成。

:::

## 构建

```powershell
cargo build -p smalux-server --release
```

## 下载 Release 产物

Release workflow 只允许手动触发，并创建 Draft Release。Agent 与 Server 使用相同版本号，
但每个程序分别归档。触发时输入的 Tag 必须带 `v` 前缀，并与 Agent、Server 两个
`Cargo.toml` 的版本完全一致：

```text
Cargo version: 0.1.0
Release tag:   v0.1.0
```

Server 产物命名为：

```text
smalux-server-v0.1.0-x86_64-pc-windows-msvc.zip
smalux-server-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
smalux-server-v0.1.0-x86_64-unknown-linux-musl.tar.gz
smalux-server-v0.1.0-x86_64-apple-darwin.tar.gz
smalux-server-v0.1.0-aarch64-apple-darwin.tar.gz
```

GNU Linux 产物用于 Ubuntu、Debian、Rocky Linux 等 glibc 系统；musl 产物用于 Alpine Linux。
macOS 产物分别用于 Intel 和 Apple Silicon。每个归档包含 Server 可执行文件、本 crate 的
README 和仓库 LICENSE。工作流还会用 Alpine 容器对 musl Agent/Server 执行 `--version` 启动
冒烟检查。

下载后先校验所有归档：

```bash
sha256sum -c SHA256SUMS --ignore-missing
```

Windows PowerShell 可使用 `Get-FileHash -Algorithm SHA256 <file>` 对照 `SHA256SUMS`。
当前 Release 不包含代码签名、systemd/Windows Service 文件或自动升级器；Draft Release 必须
人工检查后再公开，生产环境仍需自行配置服务管理、数据库备份和升级回滚。

Server manifest 已准备 SQLite、PostgreSQL 和 MySQL 的 SeaORM 驱动，并保留 `frontend-embed` feature：

```powershell
cargo build -p smalux-server --release --features frontend-embed
```

只有在前端静态资源构建和嵌入逻辑完成后，该 feature 才能形成完整的单文件部署体验。

## 运行当前骨架

```powershell
cargo run -p smalux-server
```

成功时会通过 tracing 输出类似：

```text
server listening listen_address=127.0.0.1:8080
```

端口已被占用时会返回操作系统 bind 错误。监听地址和端口目前仍由 `ServerConfig` 默认值提供，
CLI 覆盖入口尚未实现。

## 数据库配置

Server 支持 SQLite、PostgreSQL 和 MySQL。数据库连接配置由 `ServerConfig.database` 持有，
连接成功后运行态只保存连接池和脱敏的后端标签，不把原始数据库 URL 或密码放入路由状态。

常用环境变量：

```powershell
$env:SMALUX_DATABASE_URL = "postgres://127.0.0.1:5432/smalux"
$env:SMALUX_DATABASE_USERNAME = "smalux"
$env:SMALUX_DATABASE_PASSWORD = "change-me"
$env:SMALUX_DATABASE_MAX_CONNECTIONS = "20"
$env:SMALUX_DATABASE_MIN_CONNECTIONS = "2"
```

URL 不应包含用户名和密码；认证信息使用独立字段。未设置 `SMALUX_DATABASE_URL` 时，Server
使用应用数据目录下的 `server.db` SQLite 文件。连接池还支持连接超时、获取连接超时、空闲回收、
最大生命周期以及 SQLx 日志开关，详见 `DatabasePoolConfig`。

后端专属参数使用 `DatabaseConfig.options`，由对应 adapter 校验。例如 PostgreSQL 支持
`application_name`、`search_path` 和 `statement_timeout_seconds`，SQLite 支持 `mode`、`cache`
和 `immutable`，MySQL 支持 `charset` 和 `ssl-mode`。未知参数或参数类型错误会在启动时失败。

Agent gRPC 资源边界也可以通过环境变量调整：

```powershell
$env:SMALUX_AGENT_MAX_SESSIONS = "256"
$env:SMALUX_AGENT_MAX_REGISTRATION_SESSIONS = "32"
$env:SMALUX_AGENT_MAX_MESSAGE_BYTES = "1048576"
$env:SMALUX_SERVER_SHUTDOWN_GRACE_SECONDS = "15"
$env:SMALUX_LOG_COMPONENT = "server"
```

这些值必须是正整数。前两个限制并发会话和注册业务，消息大小限制单个 protobuf 消息；长期
双向流不使用普通 HTTP 总超时。`Ctrl+C` 会停止接收新连接、通知 Agent Session 和密钥环同步
任务退出，并最多等待 `SMALUX_SERVER_SHUTDOWN_GRACE_SECONDS` 秒。

日志默认写入公共数据目录的 `logs/<进程组件>/smalux.log`，按日期和 10 MiB 大小滚动；组件名
默认取可执行文件名，也可以用 `SMALUX_LOG_COMPONENT` 指定。控制台和文本文件同时使用
`RUST_LOG` 的过滤级别。

Server Noise 密钥只从数据库的 `server_keyrings` 表恢复；首次启动时在内存生成并写入数据库，
不再使用 `identity_path` 或旧的密钥文件目录。

停止前台进程使用 `Ctrl+C`。Server 会通过共享取消令牌通知 gRPC Session 和密钥环同步任务，
并在宽限期结束后停止等待。强制终止进程仍可能中断未完成的业务消息，因此 Agent 应依靠序号和
重连后的幂等上报恢复。

## 生产部署前必须确定

1. 数据库类型、连接池、迁移和备份恢复。
2. 注册 Token 的生成、TTL、单次消费、审计和限流。
3. Agent 公钥、吊销状态、租户和业务权限模型。
4. Server Noise 密钥的安全存储、轮换和多实例同步。
5. TaskReport 的幂等、确认水位、保留时间和批量写入。
6. REST、WebSocket 和 gRPC 的公开路径及反向代理规则。
7. 日志、指标、追踪、健康检查和优雅关闭。

## 不应直接复用 Example 的部分

Protocol Example 的固定 Token、目录注册表和控制台命令只用于展示交互顺序。它们缺少数据库事务、
并发控制、访问审计、密钥保护和管理 API，不能直接作为生产 Server 的认证模块。

可以复用的是协议调用顺序：`accept_incoming` 分类 XXpsk3/IK，业务层完成验证和落库后，再调用
`prepare`、`complete` 或 `authorize`。

## 从空 Router 到正式 Server

推荐按以下顺序增加能力：

1. 配置解析、日志和关闭信号；
2. `/health/live` 与 `/health/ready`，分别表达进程存活和依赖就绪；
3. 数据库连接、迁移和 repository；
4. Agent 注册 Token 与公钥授权；
5. Tonic `AgentTransport` 路由和 Session registry；
6. Job 管理 API、TaskReport ingest 和幂等存储；
7. WebSocket/REST 管理接口；
8. 身份认证、租户授权、限流、审计和管理端。

每一步应先有独立健康状态和测试，再对外开放路由。数据库已连接不等于迁移完成，HTTP 能监听也不等于
Agent 会话已经就绪。
