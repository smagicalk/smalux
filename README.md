<p align="center">
  <img src="assets/smalux.png" alt="Smalux" width="320" />
</p>

# Smalux

Smalux 是一个 Rust workspace，目标是实现轻量监控 agent、共享通信协议和中心 server。

当前开发重点在 `smalux-server`。`smalux-agent`、`smalux-core`、`smalux-protocol` 已经有较完整基础，server 还在搭接入、存储和 Web 管理面。

## Workspace

| crate | 作用 | 当前状态 |
| --- | --- | --- |
| `crates/smalux-agent` | 主机侧采集、上报、远程任务、远程 shell、Komari 兼容 | 主体功能已实现，后续随 server 协议继续收敛 |
| `crates/smalux-core` | 共享监控模型、日志初始化、脱敏工具、通用工具 | 可被 agent/server 复用 |
| `crates/smalux-protocol` | `ClientFrame` / `ServerFrame`、JSON codec、binary wire、`secure_psk` | agent/server 共享协议边界 |
| `crates/smalux-server` | agent 接入、REST API、实时通道、前端托管、数据库持久化 | CLI/config/bootstrap/DB/最小 axum 骨架已接入，业务仍在开发 |

## 当前交互面

| 方向 | 通道 | 主要内容 |
| --- | --- | --- |
| agent -> server | 主 WebSocket | `snapshot` / `delta` / `heartbeat` / `ack` / `error` / `remote_task_result` / `job_result` |
| server -> agent | 主 WebSocket | `snapshot_request` / `config_patch` / `collect_processes_once` / `collect_sockets_once` / `remote_task_run` / `job_apply` / `remote_shell_open` |
| shell stream | 临时 WebSocket | `input` / `resize` / `close` / `heartbeat` / `opened` / `output` / `exit` / `error` |
| agent <-> Komari | Komari 兼容 WebSocket/HTTP | report、basic info、terminal、exec、ping |

自有协议以 `smalux-protocol` 为准；第三方兼容放在 agent 的 adapter 中，不污染自有协议模型。

## 文档入口

建议按用途阅读：

1. 项目总览：当前文件。
2. agent 运行和扩展：[crates/smalux-agent/README.md](crates/smalux-agent/README.md)。
3. 共享模型和日志工具：[crates/smalux-core/README.md](crates/smalux-core/README.md)。
4. 协议 crate API：[crates/smalux-protocol/README.md](crates/smalux-protocol/README.md)。
5. server 结构和启动参数：[crates/smalux-server/README.md](crates/smalux-server/README.md)。
6. server 实现协议字段速查：[crates/smalux-server/plan.md](crates/smalux-server/plan.md)。
7. 会话恢复：[session.md](session.md)。

文档分工固定为：

- 根 README 只讲 workspace 总览和当前状态。
- crate README 只讲本 crate 的职责、入口、运行方式和扩展边界。
- `crates/smalux-server/plan.md` 只做 server 实现协议和参数速查。
- `session.md` 只保存跨电脑恢复上下文。

## 当前实现重点

agent 已经覆盖：

- CPU、内存、磁盘、网络、进程、socket、公网 IP 等采集。
- 独立采样频率和事件驱动上报。
- `snapshot`、可选 `delta`、业务级 `heartbeat`。
- WebSocket / HTTP 导出、Komari 兼容、自有 binary wire、`secure_psk`。
- `config_patch`、一次性诊断、remote task、remote job probe、remote shell。

server 当前已经覆盖：

- CLI/env 参数解析。
- `ServerConfig`、数据库配置、前端槽位配置、日志配置。
- SeaORM 数据库初始化和 migration 入口。
- 最小 axum router、`GET /api/v1/health`、`GET /agent/v1/connect` WebSocket upgrade。
- site/admin 双前端槽位：`embedded` / `directory` / `external`。

server 还需要继续实现：

- agent 认证和 `secure_psk` responder。
- 主连接 wire/frame 读写循环。
- snapshot/delta/heartbeat 入库和 latest state。
- REST 查询、命令下发、ack/result 回收。
- 前端 realtime 推送和管理端权限。

## 常用命令

```powershell
cargo fmt --all --check
cargo check --workspace
cargo test -p smalux-core
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo test -p smalux-server
```

启动 server 最小骨架：

```powershell
$env:RUST_LOG="smalux_server=debug"
cargo run -p smalux-server -- --bind-addr 127.0.0.1 --bind-port 3000
```

启动 Komari 兼容 agent 示例：

```powershell
cargo run -p smalux-agent -- -f komari -s https://monitor.smagical.de -t GAxOsu0Yrj6RBSPt7yRAdQ -S true -T true --remote-probe-enabled true
```
