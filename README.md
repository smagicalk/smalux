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
  - 已实现本机采集、动态配置、导出、控制消息处理、remote task、remote probe、remote shell、Komari 兼容。
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
- `config_patch`、`snapshot_request`、一次性诊断、remote task、remote probe、remote shell。

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

## Non-server Fixes In This Session

本轮已修复非 server 模块的几个明确问题：

- `smalux-core`
  - 修复文本脱敏在转义引号场景下可能泄漏敏感值尾部的问题。
- `smalux-protocol`
  - `RemoteProbeId` 从任意 JSON 收紧为“字符串或整数”，当前用于 `remote_probe_apply.request_id` 和 Komari ping task id。
  - `decode_secure_hello()` 现在会显式校验 Noise pattern，不再把错误拖到更晚的握手阶段。
- `smalux-agent`
  - 控制层 `ack/error` 不再用 `try_send`，避免队列满时静默丢失。
  - disabled/rate-limited 的 remote task/probe 即时拒绝结果改为可靠异步发送。
  - export 重连后的 pending 恢复失败现在会继续向上返回错误，避免 pending 事件卡住不再重试。
  - 进程和 socket 的 unsupported/stale 状态现在会保留调用方请求的采样级别，避免把 `light/details` 误报成默认 `count` 语义。
  - Komari exec 入站日志不再打印原始命令字符串，只记录 `task_id` 和长度，降低敏感参数落盘风险。
  - `remote_shell_open` 现在在 stream 建连、PTY 启动并成功发出 `opened` 事件后才视为 ready，避免过早成功确认。
  - 出站事件改为高低优先级双通道：control ack/error 与 remote task/probe result 优先于普通 report/basic info。
  - export 重连恢复现在按 latest state、控制响应、远程结果三层执行，减少整批恢复时的耦合。
  - 补充了协议适配边界测试，固定 Smalux 与 Komari handler 不能串线解析对方消息。
  - 收紧了 Smalux server error 入站日志，不再直接打印对端原始 `message`，只记录 `code` 和长度。
  - 收紧了 remote task/probe 运行日志，不再直接打印本地程序名或探测目标原文，改为长度/计数等结构化摘要。

## Validation

本轮已验证：

```powershell
cargo test -p smalux-core
cargo test -p smalux-protocol
cargo test -p smalux-agent
cargo fmt --all --check
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
