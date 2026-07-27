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
```

Client：

```powershell
$env:SMALUX_EXAMPLE_REGISTRATION_TOKEN = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
cargo run -p smalux-protocol --example noise_shared_port_client
```

Driver 模式：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server -- --mode driver
cargo run -p smalux-protocol --example noise_shared_port_client -- --mode driver
```

清理 Example 身份时应删除完整数据目录，不能只删除单个 key 文件：

```powershell
Remove-Item -Recurse -Force target/smalux-noise-agent
Remove-Item -Recurse -Force target/smalux-noise-server
```

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
