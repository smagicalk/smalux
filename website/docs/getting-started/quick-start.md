---
title: 快速开始
description: 从源码检查 workspace，并运行 Protocol Client/Server 示例。
---

# 快速开始

当前仓库提供由 GitHub Actions 手动生成的跨平台 Draft Release 归档，但没有安装器；推荐先从源码构建。以下命令在仓库根目录执行。

## 当前应该运行哪个入口

| 入口 | 当前行为 | 适合用途 |
| --- | --- | --- |
| `smalux-agent` | 启动本地 IPC、Client、Scheduler 和远程 Job 控制循环，可执行最小连接流程。 | Agent/Server 联调。 |
| `smalux-server` | 启动数据库、Noise keyring、健康检查、Agent gRPC 路由和本地管理 IPC。 | Agent/Server 联调。 |
| `noise_shared_port_server` | 提供 REST、WebSocket、gRPC、注册表和控制台。 | 阅读并运行完整协议流程。 |
| `noise_shared_port_client` | 执行 XXpsk3 注册、IK 重连和加密消息。 | 与 Example Server 联调。 |

要验证 Agent 与 Server 的最小闭环，应优先运行正式的 `smalux-server` 和 `smalux-agent`；
Protocol 的两个 Example 仍适合单独阅读握手、SessionDriver 和协议错误路径。

## 1. 检查工具链

需要支持 Rust 2024 edition 的稳定 Rust 工具链：

```powershell
rustc --version
cargo --version
```

Protocol 构建使用 vendored `protoc`，通常不需要单独安装系统 `protoc`。

## 2. 构建 workspace

先构建正式联调需要的两个二进制：

```powershell
cargo build -p smalux-agent -p smalux-server
```

产物位于 `target/debug/smalux-agent.exe` 和 `target/debug/smalux-server.exe`（Unix 去掉 `.exe`）。
后续三终端直接运行这些已构建的文件，不要在服务运行期间重复调用 `cargo run`，避免多个构建争用
workspace 的 build directory lock。

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

测试覆盖 Agent Scheduler、采集配置、RemoteJobController、Proto round-trip、Noise 握手、注册恢复、
Driver 收发和错误路径。

## 4. 运行 Agent/Server 最小联调

使用三个终端，并复用上一节已生成的 `target/debug` 二进制。不要让每个终端都执行 `cargo run`。

终端一：启动 Server。

```powershell
$serverExe = 'target/debug/smalux-server.exe'
& $serverExe run
```

Server 默认监听 `http://127.0.0.1:12345`。

终端二：通过本地管理 IPC 创建只写入文件的一次性注册 Token。目标文件必须不存在：

```powershell
$serverExe = 'target/debug/smalux-server.exe'
New-Item -ItemType Directory -Force C:/smalux | Out-Null
& $serverExe registration-token create `
  --agent-name edge-agent `
  --credential-file C:/smalux/edge-agent.token
```

终端三：优先使用 `--token-file` 启动 Agent，不把完整 Token 放进命令历史或进程列表：

```powershell
$agentExe = 'target/debug/smalux-agent.exe'
& $agentExe run `
  --server-endpoint http://127.0.0.1:12345 `
  --token-file C:/smalux/edge-agent.token
```

首次运行执行 XXpsk3 注册，Agent 会上报能力、策略和运行实例摘要。回到终端二可查询连接：

```powershell
$agentExe = 'target/debug/smalux-agent.exe'
& $serverExe agent list --online
& $serverExe session list --state authenticated
& $agentExe status
& $agentExe jobs list
```

此阶段即使没有下发 Job，也能验证注册、加密长流、心跳和断线重连。Agent 的当前 Job、执行态和
outbox 只在内存中；进程重启后由新的实例摘要触发 Server 重新对账并按策略、能力和插件 inventory
下发权威目录。CPU Job 的完整生成、替换、查询、报告和清空流程见 [运行闭环文档](../reference/agent-server-runtime.md#cpu-example)。

## 5. 运行安全协议示例

先启动 Server：

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server
```

Server 默认监听 `127.0.0.1:8080`。保持 Server 运行，在 Server 控制台执行
`token generate [display-name]` 生成一次性 Token，并可由 Server 预先绑定展示名称：

```powershell
server> token generate edge-agent-01
```

然后在另一个终端启动 Client。Client 会提示 `paste registration token:`，把刚才生成的完整
`token_id.psk` 粘贴到控制台并回车：

```powershell
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
target/smalux-noise-agent/    Agent Noise 身份、Server 公钥和注册状态
```

第二次运行 Client 时，如果目录完整且带有 committed 标记，它不会再次读取 Token，而是直接使用 IK。
不要只删除目录中的一个密钥文件；身份材料不完整时 Example 会拒绝启动，避免混用新旧密钥。

## 6. 切换 Driver 模式

```powershell
cargo run -p smalux-protocol --example noise_shared_port_server -- --mode driver
cargo run -p smalux-protocol --example noise_shared_port_client -- --mode driver
```

Example 当前只接受 `--mode manual|driver`；未传参数时默认使用 `manual`。其他地址、数据目录和
TLS 配置通过环境变量传入；注册 Token 不通过环境变量传递，始终由 Client 控制台输入。

## 7. 判断运行是否正确

首次注册的关键日志顺序应包含：

```text
Server: Noise handshake completed mode=RegistrationXxPsk3
Server: pending agent=<server-assigned-agent-id>
Server: committed agent=<server-assigned-agent-id>
Client: learned Server key and registered agent=<server-assigned-agent-id>
```

后续运行应出现 IK authentication，而不是再次进入注册。错误 Token 会在 Noise 认证阶段失败；协议故意
不区分“PSK 输错”和“握手被篡改”，避免暴露认证细节。

## 下一步

- 想理解采集和调度：阅读 [Job 与 Task](../usage/job-task.md)。
- 想接入通信层：阅读 [Protocol 概览](../protocol/overview.md)。
- 想新增能力：阅读 [扩展 Task](../extensions/task.md)。
