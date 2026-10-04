---
title: 命令速查
description: Rust workspace、Protocol Example 和文档站常用命令。
---

# 命令速查

所有 Rust 命令默认从仓库根目录运行。

## Workspace

```powershell
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

## 单个 crate

```powershell
cargo test -p smalux-agent
cargo test -p smalux-protocol --all-targets
cargo rustdoc -p smalux-protocol --lib -- -D warnings
cargo build -p smalux-server --release
```

## Agent 运行

推荐先完成一次 Debug 构建，再直接使用 `target/debug` 中的可执行文件。这样同时运行 Server 和 Agent 时不会让多个 `cargo run` 争用 workspace 的 build directory lock：

```powershell
target/debug/smalux-agent.exe run `
  --server-endpoint http://127.0.0.1:12345 `
  --token-file C:/smalux/edge-agent.token `
  --offline-job-timeout 30m
```

`--token-file` 从 UTF-8 文件读取一次性注册凭据，避免 Token 出现在命令历史或进程列表中。`--offline-job-timeout` 对应 `SMALUX_OFFLINE_JOB_TIMEOUT`，默认 `30m`。短暂断线期间远程 Job
继续执行；持续断线到期后 Agent 清空远程 Job，重连时由 Server 重新下发过滤后的权威目录。
## Server 运行与管理

无子命令和显式 `run` 都会启动 Server：

```powershell
cargo run -p smalux-server
cargo run -p smalux-server -- run --listen-address 127.0.0.1 --listen-port 12345
```

配置按 `CLI > 环境变量 > 默认值` 解析。数据库密码只接受
`SMALUX_DATABASE_PASSWORD` 或 `--database-password-file`，不提供会泄露到进程列表的
明文密码参数。

除 `run` 和 `config check` 外，管理命令都通过本地 IPC 访问正在运行的 Server。Windows
默认使用 `\\.\pipe\smalux-server`，Unix 默认使用
`<data_dir>/server/control.sock`；可用全局 `--control-endpoint` 覆盖。

脚本使用 `--output json` 时，管理终端应先设置 `$env:RUST_LOG = 'off'`，因为 CLI tracing 可能把日志写入 stdout
并污染 JSON；该设置只影响当前终端及其启动的短 CLI 进程。每次调用都先检查 `$LASTEXITCODE`，再把完整 stdout 用
`-join "`n"` 合并后交给 `ConvertFrom-Json`，不要过滤日志行来猜 JSON。

```powershell
# 校验配置、查看状态
cargo run -p smalux-server -- config check --database-url sqlite::memory:
cargo run -p smalux-server -- status --output json
cargo run -p smalux-server -- config show

# Token 默认 30m；支持 30m、24h、7d 或显式 --no-expiry
cargo run -p smalux-server -- registration-token create --agent-name node-a --expires-in 24h
cargo run -p smalux-server -- registration-token create --credential-file C:/smalux/node-a.token
cargo run -p smalux-server -- registration-token list --status active
cargo run -p smalux-server -- registration-token revoke <TOKEN_ID> --yes

# Agent 身份和当前 Session
cargo run -p smalux-server -- agent list --online
cargo run -p smalux-server -- agent show <AGENT_ID>
cargo run -p smalux-server -- agent rename <AGENT_ID> --name edge-node
cargo run -p smalux-server -- agent revoke <AGENT_ID> --yes
cargo run -p smalux-server -- session list --state authenticated
cargo run -p smalux-server -- session disconnect <SESSION_ID> --yes

# Agent 权威 Job 目录：替换、查询、清空
cargo run -p smalux-server -- job replace <AGENT_ID> --definition C:/smalux/cpu-job.pb
cargo run -p smalux-server -- job list <AGENT_ID>
cargo run -p smalux-server -- job clear <AGENT_ID> --yes

# Plus Worker runtime 快照
cargo run -p smalux-server -- plugin runtime-list <AGENT_ID>
cargo run -p smalux-server -- plugin runtime-replace <AGENT_ID> --file C:/smalux/plugin-runtime.json
cargo run -p smalux-server -- plugin runtime-clear <AGENT_ID> --yes

# Server 持久化的报告和事件
cargo run -p smalux-server -- report --agent-id <AGENT_ID>
cargo run -p smalux-server -- event --agent-id <AGENT_ID>

# 密钥环只读诊断和优雅关闭
cargo run -p smalux-server -- keyring status
cargo run -p smalux-server -- shutdown --yes
```

Token 完整凭据只在创建时返回一次；`list/show` 不返回 PSK。指定 `--credential-file` 后凭据
只写入新文件，不再打印。吊销 Agent 会立即断开其当前 Session，而单独断开 Session 不会
吊销 Agent。危险操作在非交互环境必须提供 `--yes`。

## Agent 本地管理

以下命令通过 Agent 本地 IPC 执行；策略修改只影响 Server 下发的远程 Job：

```powershell
target/debug/smalux-agent.exe status --output json
target/debug/smalux-agent.exe jobs list
target/debug/smalux-agent.exe jobs show <JOB_ID>
target/debug/smalux-agent.exe config show
target/debug/smalux-agent.exe plugins list
target/debug/smalux-agent.exe jobs policy show
target/debug/smalux-agent.exe jobs policy add-task smalux.collect.process.v1
target/debug/smalux-agent.exe jobs policy remove-task smalux.collect.process.v1
target/debug/smalux-agent.exe jobs policy deny-all
target/debug/smalux-agent.exe jobs policy allow-all
```

`tasks list/show` 与 `identity show` 可离线执行；前者列出二进制能力，后者只展示身份摘要：

```powershell
target/debug/smalux-agent.exe tasks list
target/debug/smalux-agent.exe tasks show smalux.collect.cpu.v1
target/debug/smalux-agent.exe identity show
```

策略文件损坏时必须先停止 Agent，再执行 `target/debug/smalux-agent.exe jobs policy repair --reset`。
该命令备份原文件并恢复空策略，会清除全局和逐项拒绝规则；重新启动前应评估放行风险，不能用它代替普通重连。
`allow-all` 只解除全局拒绝，保留逐项 Task 拒绝。
`server_sync=pending` 表示本地策略已生效但尚未收到 Server 确认；ACK 表示 Server 已收到快照，
不代表 Job 已重新下发或执行。以 `jobs list` 和 Server `report` 分别确认应用和执行结果。
完整 CPU 示例见 [生成、下发与清空](./agent-server-runtime.md#cpu-example)。

正式 `smalux-agent` 已装配 Client、Scheduler、动态 Job 策略、Plus Worker 管理和本地 IPC；正式
`smalux-server` 使用数据库 Provider 管理 Agent 权威 Job/runtime 目录，并将成功 TaskReport 和
JobEvent 按幂等键持久化。Server 会在每个会话中依据 Agent 的策略、能力和插件 inventory 过滤实际下发
目录；Agent 当前 Job、Scheduler 执行和报告/event outbox 仍是进程内存状态，不能把二进制连接成功理解为
跨进程不丢数据的完整监控保证。


## Protocol Example

Server：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server
# 在 Server 控制台输入：
# server> token generate [display-name]
```

Client：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_client
# Client 启动后在 paste registration token: 提示处粘贴 Server 生成的 token_id.psk
```

Driver 模式：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server -- --mode driver
cargo run -p smalux-protocol --example noise_shared_port_client -- --mode driver
```

Example 支持的环境变量：

| 变量 | 默认值/作用域 | 说明 |
| --- | --- | --- |
| `SMALUX_EXAMPLE_ADDR` | `127.0.0.1:8080`，Server | 监听地址。 |
| `SMALUX_EXAMPLE_ENDPOINT` | `http://127.0.0.1:8080`，Client | 公开 endpoint；经代理时设为 HTTPS 域名。 |
| `SMALUX_EXAMPLE_SERVER_DATA_DIR` | `target/smalux-noise-server` | Server identity 与注册状态目录。 |
| `SMALUX_EXAMPLE_AGENT_DATA_DIR` | `target/smalux-noise-agent` | Agent identity、Server key 与 pending 状态目录。 |
| `SMALUX_EXAMPLE_REVOKE_AGENT` | 未设置 | Server 启动时吊销指定示例 Agent。 |
| `SMALUX_EXAMPLE_TLS_CERT` | 未设置 | Server TLS 证书链 PEM；必须与私钥同时设置。 |
| `SMALUX_EXAMPLE_TLS_KEY` | 未设置 | Server TLS 私钥 PEM。 |

PowerShell 设置的 `$env:...` 只影响当前终端及其子进程。Token 不使用环境变量，而是在 Client
启动后的控制台提示中输入。切换数据目录可以并行模拟不同 Agent，也可以隔离错误 Token 测试，
避免覆盖已经完成注册的身份。

清理 Example 身份时应删除完整数据目录，不能只删除单个 key 文件：

```powershell
Remove-Item -Recurse -Force target/smalux-noise-agent
Remove-Item -Recurse -Force target/smalux-noise-server
```

这会删除示例长期 identity 和注册记录，下次运行会重新走 XXpsk3。只删除 Agent 或 Server 一侧会制造
身份状态不一致，适合故障演练，但不等同于正常注销流程。

## 文档站

```powershell
Set-Location website
pnpm install
pnpm start
pnpm typecheck
pnpm build
pnpm serve
```

## Git 检查

```powershell
git status --short
git diff --check
git diff --stat
```
