---
title: 快速开始
description: 从源码检查 workspace，并运行 Protocol Client/Server 示例。
---

# 快速开始

当前项目没有发布二进制安装包，推荐从源码开始。以下命令在仓库根目录执行。

## 当前应该运行哪个入口

| 入口 | 当前行为 | 适合用途 |
| --- | --- | --- |
| `smalux-agent` | `main()` 只注册模块后立即退出，尚未启动 Scheduler 或网络连接。 | 编译和库级测试。 |
| `smalux-server` | 使用空 Axum Router 监听 `127.0.0.1:8080`，尚无正式业务路由。 | 验证 Server 启动骨架。 |
| `noise_shared_port_server` | 提供 REST、WebSocket、gRPC、注册表和控制台。 | 阅读并运行完整协议流程。 |
| `noise_shared_port_client` | 执行 XXpsk3 注册、IK 重连和加密消息。 | 与 Example Server 联调。 |

因此第一次体验完整交互时，应运行 Protocol 的两个 Example，而不是正式 Agent/Server 二进制。

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

只验证某个边界时可以缩小范围：

```powershell
cargo check -p smalux-agent --all-targets
cargo check -p smalux-protocol --all-targets
cargo check -p smalux-server --all-targets
```

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

默认持久化目录位于 `target/`：

```text
target/smalux-noise-server/   Server Noise 身份和示例注册表
target/smalux-noise-agent/    Agent Noise 身份、固定 Server 公钥和注册状态
```

第二次运行 Client 时，如果目录完整且带有 committed 标记，它不会再次读取 Token，而是直接使用 IK。
不要只删除目录中的一个密钥文件；身份材料不完整时 Example 会拒绝启动，避免混用新旧密钥。

## 5. 切换 Driver 模式

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server -- --mode driver
cargo run -p smalux-protocol --example noise_shared_port_client -- --mode driver
```

Example 当前只接受 `--mode manual|driver`；未传参数时默认使用 `manual`。其他地址、Token、数据目录和
TLS 配置通过环境变量传入，完整列表见源码附近的 Example README。

## 6. 判断运行是否正确

首次注册的关键日志顺序应包含：

```text
Server: Noise handshake completed mode=RegistrationXxPsk3
Server: pending agent=example-agent
Server: committed agent=example-agent
Client: learned Server key and registered agent=example-agent
```

后续运行应出现 IK authentication，而不是再次进入注册。错误 Token 会在 Noise 认证阶段失败；协议故意
不区分“PSK 输错”和“握手被篡改”，避免暴露认证细节。

## 下一步

- 想理解采集和调度：阅读 [Job 与 Task](../usage/job-task.md)。
- 想接入通信层：阅读 [Protocol 概览](../protocol/overview.md)。
- 想新增能力：阅读 [扩展 Task](../extensions/task.md)。
