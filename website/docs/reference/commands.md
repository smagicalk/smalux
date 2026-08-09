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

## Protocol Example

Server：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server
# 在 Server 控制台输入：
# server> token generate
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

正式 `smalux-agent` 当前入口尚未装配运行循环，正式 `smalux-server` 当前固定监听
`127.0.0.1:8080` 且 Router 为空。验证完整 Protocol 流程应运行上面的两个独立 Example，不能把正式
二进制启动成功理解为 Agent/Server 业务已经接通。
