---
title: 源码构建
description: Smalux workspace 的平台依赖、构建产物与构建模式。
---

# 源码构建

## 前置条件

| 工具 | 用途 |
| --- | --- |
| Rust stable | 编译 Rust 2024 workspace。 |
| Cargo | 依赖解析、构建、测试和 Example。 |
| 平台 C/C++ 链接环境 | 编译部分系统与加密依赖。Windows 推荐 MSVC 工具链。 |
| Git | 获取源码和维护版本。 |

Protocol crate 使用 `protoc-bin-vendored`，构建脚本会选择当前平台对应的 `protoc`，避免要求开发机
预装全局 Proto 编译器。

## 获取源码

```powershell
git clone https://github.com/smagicalk/smalux.git
Set-Location smalux
```

开发工作目前主要位于 `dev` 分支，稳定入口以仓库默认分支和发布说明为准：

```powershell
git branch -a
```

## Debug 构建

```powershell
cargo build --workspace
```

产物写入 `target/debug/`。Debug 模式适合开发和测试，不适合作为最终性能基准。

## Release 构建

```powershell
cargo build --workspace --release
```

产物写入 `target/release/`。当前仓库尚未提供跨平台打包、签名、压缩归档或自动升级流程，发布时还需
自行处理配置文件、服务管理、目录权限和平台依赖。

## 单独构建 crate

```powershell
cargo build -p smalux-agent
cargo build -p smalux-server
cargo build -p smalux-protocol
```

使用 `-p` 只选择目标 package，Cargo 仍会自动构建其 workspace 依赖。

## 常见问题

### RustRover 找不到生成的 gRPC 模块

`agent_transport_client` 和 `agent_transport_server` 由 `build.rs` 生成到 Cargo `OUT_DIR`，源码目录
中不会出现对应 `.rs`。先执行：

```powershell
cargo check -p smalux-protocol
```

然后在 IDE 中 Reload Cargo Project。不要把 `target/**/out` 中的生成文件提交到仓库。

### 第一次构建很慢

Agent 和 Server 含 Tokio、Reqwest、Tonic、SeaORM 等依赖，首次构建需要建立完整依赖图。不要同时
启动多个首次 Cargo 构建，它们会争用 package cache 和 build directory 锁。
