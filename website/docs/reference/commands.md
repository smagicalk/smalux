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

```powershell
cargo run -p smalux-agent -- run `
  --server-endpoint http://127.0.0.1:12345 `
  --token <TOKEN> `
  --offline-job-timeout 30m
```

`--offline-job-timeout` 对应 `SMALUX_OFFLINE_JOB_TIMEOUT`，默认 `30m`。短暂断线期间远程 Job
继续执行；持续断线到期后 Agent 清空远程 Job，重连时由 Server 重新下发权威目录。

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

```powershell
# 校验配置、查看状态
cargo run -p smalux-server -- config check --database-url sqlite::memory:
cargo run -p smalux-server -- status --output json
cargo run -p smalux-server -- config show

# Token 默认 30m；支持 30m、24h、7d 或显式 --no-expiry
cargo run -p smalux-server -- registration-token create --agent-name node-a --expires-in 24h
cargo run -p smalux-server -- registration-token create --credential-file token.txt
cargo run -p smalux-server -- registration-token list --status active
cargo run -p smalux-server -- registration-token revoke <TOKEN_ID> --yes

# Agent 身份和当前 Session
cargo run -p smalux-server -- agent list --online
cargo run -p smalux-server -- agent rename <AGENT_ID> --name edge-node
cargo run -p smalux-server -- agent revoke <AGENT_ID> --yes
cargo run -p smalux-server -- session list --state authenticated
cargo run -p smalux-server -- session disconnect <SESSION_ID> --yes

# 密钥环只读诊断和优雅关闭
cargo run -p smalux-server -- keyring status
cargo run -p smalux-server -- shutdown --yes
```

Token 完整凭据只在创建时返回一次；`list/show` 不返回 PSK。指定 `--credential-file` 后凭据
只写入新文件，不再打印。吊销 Agent 会立即断开其当前 Session，而单独断开 Session 不会
吊销 Agent。危险操作在非交互环境必须提供 `--yes`。

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

正式 `smalux-agent` 已装配 Client、Scheduler、动态 Job 策略和本地 IPC；正式 `smalux-server` 已装配
Axum、gRPC/Noise、注册中心和策略协商循环。Example 仍适合独立学习协议，但默认 Server Job Provider
不返回目录，监控结果也尚未持久化，不能把二进制连接成功理解为完整监控闭环。
