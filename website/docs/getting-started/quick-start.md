---
title: 快速开始
description: 从源码检查 workspace，并运行 Protocol Client/Server 示例。
---

# 快速开始

当前项目没有发布二进制安装包，推荐从源码开始。以下命令在仓库根目录执行。

## 1. 检查工具链

需要支持 Rust 2024 edition 的稳定 Rust 工具链：

```powershell
rustc --version
cargo --version
```

Protocol 构建使用 vendored `protoc`，通常不需要单独安装系统 `protoc`。

## 2. 编译 workspace

```powershell
cargo check --workspace --all-targets
```

第一次构建会下载并编译依赖，耗时明显长于后续增量构建。

## 3. 运行测试

```powershell
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

测试覆盖 Agent Scheduler、采集配置、JobController、Proto round-trip、Noise 握手、注册恢复、
Driver 收发和错误路径。

## 4. 运行安全协议示例

先启动 Server：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server
```

Server 默认监听 `127.0.0.1:8080`，并在控制台打印固定示例 Token。保持 Server 运行，在另一个终端
设置 Token 后启动 Client：

```powershell
$env:SMALUX_EXAMPLE_REGISTRATION_TOKEN = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
cargo run -p smalux-protocol --example noise_shared_port_client
```

首次运行执行 XXpsk3 注册，后续使用已保存的双方静态身份执行 IK。示例同时演示：

- Axum REST、WebSocket 和 Tonic gRPC 共用端口；
- 注册 pending/commit/committed 时序；
- Client 不预置 Server Noise 公钥的首次信任；
- 加密业务消息、心跳和 rekey；
- 手动 Session 与 `SessionDriver` 两种驱动方式。

## 5. 切换 Driver 模式

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server -- --mode driver
cargo run -p smalux-protocol --example noise_shared_port_client -- --mode driver
```

Example 当前只接受 `--mode manual|driver`；未传参数时默认使用 `manual`。其他地址、Token、数据目录和
TLS 配置通过环境变量传入，完整列表见源码附近的 Example README。

## 下一步

- 想理解采集和调度：阅读 [Job 与 Task](../usage/job-task.md)。
- 想接入通信层：阅读 [Protocol 概览](../protocol/overview.md)。
- 想新增能力：阅读 [扩展 Task](../extensions/task.md)。
