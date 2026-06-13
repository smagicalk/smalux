<p align="center">
  <img src="assets/smalux.png" alt="Smalux" width="320" />
</p>

# Smalux

**Smalux** is a Rust workspace for a lightweight monitoring system built around a host-side agent, a shared protocol layer, and an in-progress central server.

当前项目更准确的状态是：

- `smalux-agent`、`smalux-core`、`smalux-protocol` 已经具备较完整实现。
- `smalux-server` 仍处于骨架和实现计划阶段，CLI/config 边界已搭好，但主体服务还在继续开发。

## Workspace

- `crates/smalux-agent`
  - 运行在目标主机上的采集与上报进程。
  - 已实现本机采集、动态配置、导出、控制消息处理、remote task、remote job、remote shell、Komari 兼容。
- `crates/smalux-core`
  - 共享模型、日志初始化、脱敏工具和公共辅助函数。
- `crates/smalux-protocol`
  - 共享 `ClientFrame` / `ServerFrame`、JSON codec、Smalux binary wire、`secure_psk` 安全通道。
- `crates/smalux-server`
  - 中心服务端，目标是接收 agent 上报、提供 REST/实时接口并下发控制命令。
  - 当前以目录结构、CLI/config 和详细实现计划为主。

## Current Status

### Agent

`smalux-agent` 当前已经实现：

- CPU、内存、磁盘、网络、进程、socket、公网 IP 采集。
- reporter/latest telemetry 聚合。
- `snapshot`、可选 `delta`、业务级 `heartbeat`。
- `smalux_json` 自有协议和 Komari 兼容导出。
- `binary_plain` 与 `secure_psk` WebSocket wire。
- `config_patch`、`snapshot_request`、一次性诊断、remote task、job_apply(kind=probe)、remote shell。

关键入口：

- [crates/smalux-agent/src/main.rs](crates/smalux-agent/src/main.rs)
- [crates/smalux-agent/src/service.rs](crates/smalux-agent/src/service.rs)
- [crates/smalux-agent/README.md](crates/smalux-agent/README.md)

### Protocol

`smalux-protocol` 当前承载：

- `ClientFrame` / `ServerFrame`
- JSON codec
- Smalux binary wire packet
- `secure_psk` token 解析、PSK 派生、Noise 握手和 payload 加解密

关键入口：

- [crates/smalux-protocol/src/lib.rs](crates/smalux-protocol/src/lib.rs)
- [crates/smalux-protocol/README.md](crates/smalux-protocol/README.md)

### Server

`smalux-server` 当前主要完成了：

- crate 依赖和目录骨架
- 启动参数模型
- 稳定配置模型和校验逻辑
- 详细实现计划与边界设计

它还没有完成真正的：

- HTTP server 启动
- `/agent/v1/connect` 接入
- storage/repository 闭环
- REST 查询与命令下发

关键入口：

- [crates/smalux-server/src/bootstrap.rs](crates/smalux-server/src/bootstrap.rs)
- [crates/smalux-server/README.md](crates/smalux-server/README.md)
- [crates/smalux-server/plan.md](crates/smalux-server/plan.md)

## Protocol Status

当前自有协议已经统一到通用远程 job 模型：

- server -> agent：`job_apply`
- agent -> server：`job_result`
- 当前稳定 job 类型：`kind=probe`

这意味着：

- 自有协议不再使用 `remote_probe_apply` / `remote_probe_result`。
- 远程网络探测是 `job_apply(kind=probe)` 的首个执行器。
- Komari 兼容层仍然保留，但它只在 adapter 内做转换：
  - Komari `ping` -> 内部 `job_apply(operation=once, kind=probe)`
  - 内部 `job_result(kind=probe)` -> Komari `ping_result`

相关入口：

- [crates/smalux-protocol/src/frame/remote/job.rs](crates/smalux-protocol/src/frame/remote/job.rs)
- [crates/smalux-agent/src/service/remote/job.rs](crates/smalux-agent/src/service/remote/job.rs)

## Interaction Matrix

当前主要交互面可以直接按下面理解：

| 方向 | 通道 | 协议/格式 | 主要消息 |
| --- | --- | --- | --- |
| agent -> server | 主 WebSocket | `smalux_json` + `binary_plain` / `secure_psk` | `snapshot` / `delta` / `heartbeat` / `ack` / `error` / `remote_task_result` / `job_result` |
| server -> agent | 主 WebSocket | `smalux_json` + `binary_plain` / `secure_psk` | `snapshot_request` / `config_patch` / `collect_processes_once` / `collect_sockets_once` / `remote_task_run` / `job_apply` / `remote_shell_open` |
| shell server -> agent | 临时 shell stream | shell stream JSON + wire | `input` / `resize` / `close` / `heartbeat` |
| shell agent -> server | 临时 shell stream | shell stream JSON + wire | `opened` / `output` / `exit` / `error` |
| agent -> Komari | report WebSocket / HTTP | Komari 文本 JSON | report / `uploadBasicInfo` / `task/result` / `ping_result` |
| Komari -> agent | WebSocket 文本 | Komari 文本 JSON | `terminal` / `exec` / `ping` |

建议阅读顺序：

- 协议字段定义：`crates/smalux-protocol/README.md`
- agent 实际交互流程：`crates/smalux-agent/README.md`
- server 对接和 REST/WS 形状：`crates/smalux-server/README.md`

## Validation

当前主线已验证：

```powershell
cargo fmt --all --check
cargo check --workspace
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo clippy --workspace --all-targets -- -D warnings
```

## Recommended Reading Order

如果要继续熟悉项目，建议按这个顺序：

1. `crates/smalux-agent/README.md`
2. `crates/smalux-protocol/README.md`
3. `crates/smalux-agent/src/main.rs`
4. `crates/smalux-agent/src/service.rs`
5. `crates/smalux-agent/src/service/reporter.rs`
6. `crates/smalux-agent/src/service/export.rs`
7. `crates/smalux-server/README.md`
8. `crates/smalux-server/plan.md`

## Next Steps

下一步建议继续推进 `smalux-server`：

1. 实现真正的 bootstrap 和最小 HTTP 启动。
2. 增加 `GET /api/v1/health`。
3. 接入 `MemoryRepository`。
4. 跑通 `/agent/v1/connect` + `snapshot/heartbeat`。
5. 再补 REST 查询和命令下发。
