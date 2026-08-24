---
title: 项目概览
description: 了解 Smalux 的模块边界和运行数据流。
---

# 项目概览

Smalux 是一个 Rust workspace。不同 crate 分别负责采集、调度、服务端、公共基础能力和
跨进程协议，避免网络、存储和具体采集逻辑互相渗透。

## Workspace 组成

| Crate | 当前职责 |
| --- | --- |
| `smalux-agent` | 系统信息采集、固定 Task、Scheduler、远程 Job 控制。 |
| `smalux-server` | Axum Server 骨架、配置、路由、服务和状态边界。 |
| `smalux-core` | Agent 与 Server 可复用的基础能力。 |
| `smalux-protocol` | Proto、Tonic、Noise、注册、会话维护和密钥轮换。 |
| `smalux-plus-rustic` | 为 Rustic 备份扩展预留的独立 crate，目前仅有模块骨架。 |

## Agent 内部分层

```text
collectors
    读取操作系统或网络原始数据
        |
        v
fixed tasks
    接收 Proto 配置，返回 Proto TaskResult
        |
        v
scheduler
    触发、超时、重试、并发、队列和生命周期
        |
        v
TaskReportSink
    本地保存、channel 或安全长流上报
```

Collector 不负责调度，也不决定结果发往哪里。Task 把配置和一次执行封装为稳定单元；Scheduler
只管理执行时序；Sink 决定输出。这种拆分让同一个 Task 可以用于周期 Job、Cron Job、手动执行或测试。

一次远程任务的职责分配是：Server 负责生成配置与版本，Protocol 负责可靠传递，RemoteJobController 负责
校验和安装，Scheduler 负责运行，Task 负责产生结果，Sink/连接层负责上报。每层只确认自己已经完成的
动作；例如 gRPC 发送成功不等于 Server 已持久化 TaskReport。

## Job 与 Task 的区别

- **Task** 描述一次具体操作，例如采集 CPU、统计 Socket、执行 TCP 探测。
- **Job** 描述 Task 如何长期运行，包括 ID、业务版本、触发器、超时、重试和并发策略。
- **Scheduler Job** 是 Agent 进程内部的运行实体，包含用于隔离旧执行的私有 generation。
- **JobDefinition.revision** 是 Server 维护的业务配置版本，不能与 Scheduler generation 混用。

远程 Job 通过 Proto `JobCommand` 安装到 Agent。连接短暂中断不会自动删除已安装 Job，Agent 可以
继续采集；但当前 Protocol 尚未定义跨连接的 TaskReport 持久化 ACK，因此需要可靠上报时应由 Agent
先写入本地有界队列。

## Protocol 分层

| 层 | 类型 | 作用 |
| --- | --- | --- |
| Wire | `ProtocolFrame`、`SecureMessage`、Job/Task Proto | 跨进程字段和兼容性。 |
| Noise | XXpsk3、IK、`SecureSession` | 身份认证、加密、rekey。 |
| Tonic | `AgentProtocolClient`、`ServerSessionAcceptor` | gRPC 双向流适配。 |
| Driver | `SessionDriver`、`SessionHandle` | 串行维护 nonce、心跳、收发和事件。 |

Protocol crate 不管理 Token 数据库、Agent 授权表、TLS 证书、HTTP Router 或业务存储。它提供可靠的
协议状态机，并把必须落库的状态返回给调用方。

## 适用边界

Smalux 当前适合继续开发和验证以下场景：

- 主机 CPU、内存、磁盘、网络、进程和 Socket 观测；
- ICMP、TCP Connect、UDP Request 和 HTTP 多节点探测；
- Server 下发固定类型 Job，Agent 持续执行；
- TLS/h2c 外层加 Noise 内层加密；
- Axum REST、WebSocket 和 gRPC 共用端口的集成验证。

在生产部署前仍需补齐正式持久化、权限管理、安装升级、上报队列、审计、限流和可观测性策略。

## 架构审查式总图

下面的图把一次请求从应用入口展开到具体实现。箭头表示调用方向；同一层的模块不应越过
自己的 seam 直接修改另一层的状态。

```text
Server 进程启动
  |
  +--> bootstrap::run_server
  |      +--> build_runtime
  |      |      +--> ServerDatabase::connect
  |      |      |      +--> DatabaseConfig::validate / resolve_connection_url
  |      |      |      +--> SeaORM connect
  |      |      |      +--> Migrator::up
  |      |      +--> AppState::build
  |      |      |      +--> ServerKeyRingManager::load_or_create
  |      |      |      +--> start_sync_task_with_shutdown
  |      |      |      +--> AgentRegistry::start_cleanup_task
  |      |      +--> route::build_app_router
  |      |             +--> controller::frontend::router
  |      |             +--> controller::agent::router
  |      |                    +--> AgentTransportService::new
  |      |                    +--> AgentTransportServer<AgentTransportService>
  |      |                    +--> Routes::into_axum_router
  |      +--> TcpListener::bind
  |      +--> serve_runtime
  |             +--> axum::serve
  |             +--> Ctrl+C / graceful shutdown

Agent gRPC OpenSession
  |
  +--> AgentTransportService::open_session
         +--> try_acquire_session
         +--> ServerSessionAcceptor::accept_incoming_with_psk_resolver
         |      +--> XXpsk3 / IK Noise handshake
         |      +--> AgentRegistry::resolve_registration_psk (XXpsk3)
         |      +--> IncomingSession::Registration / Authentication
         +--> AgentTransportService::run_session_worker
                +--> registration prepare/commit, or Agent authorization
                +--> TonicNoiseSession::receive
                +--> DiagnosticRequest echo
```

### 模块、接口和实现

| 模块 | 接口承诺 | 实现集中在哪里 | 不应负责什么 |
| --- | --- | --- | --- |
| `bootstrap` | 只在数据库、状态和路由都成功后监听端口 | `smalux-server/src/bootstrap.rs` | 不处理业务消息 |
| `AppState` | 持有进程级共享状态和取消令牌 | `smalux-server/src/state.rs` | 不解析 Proto |
| Agent Controller | 把 Agent 状态装配成 Axum Router | `controller/agent.rs` | 不实现注册策略 |
| Tonic Adapter | 接收 `OpenSession`、限流、启动 worker | `service/agent/transport.rs` | 不直接操作注册表规则 |
| Session Policy | 处理握手完成后的注册、授权和业务循环 | `transport/session.rs` | 不建立 Axum 路由 |
| `AgentRegistry` | Token、注册事务、Agent 授权和吊销 | `service/agent/agent_registry.rs` | 不发送 gRPC 帧 |
| Database Adapter | 事务、CAS、状态持久化和迁移 | `database/` | 不决定协议错误文本 |
| `RemoteJobController` | 命令幂等、目录 revision 和远程所有权 | `smalux-agent/src/remote_jobs.rs` | 不直接发送网络消息 |
| `compiler` | Proto 校验、Task 工厂和强类型转换 | `remote_jobs/compiler.rs` | 不修改 Scheduler |
| Scheduler | 时间、并发、队列、重试和 generation | `smalux-agent/src/scheduler/` | 不理解 CPU 或 gRPC |
| Task/Collector | 一次采集和结果构造 | `smalux-agent/src/tasks/collect/` | 不决定长期调度或输出位置 |
| Protocol Session | Noise 帧、心跳、rekey 和事件分类 | `smalux-protocol/src/tonic_transport/` | 不保存 Token 数据库 |

这里的模块是带接口的实现单元；接口不只有函数签名，还包括顺序、错误、持久化和取消规则。
例如 `AgentRegistry::authorize_agent` 返回的“已吊销”和“未知身份”都不会让会话进入业务流，
而 `RemoteJobController::apply_command` 必须在结果缓存和远程目录更新后才向调用方返回。

### 为什么这些模块是深模块

架构审查使用两个问题判断一个模块是否值得保留：

1. 删除这个模块后，复杂度是消失，还是会分散到所有调用方？
2. 调用方需要理解多少实现细节，才能安全调用它？

`RemoteJobController`、`ServerSessionAcceptor` 和 `ServerDatabase` 都隐藏了较多实现细节，调用方只需要
掌握较小的接口，因此具有较高 leverage。`controller/agent.rs` 过去只是转发函数，删除测试显示它
不会承载独立规则，所以现在直接负责 Router 装配。相反，Protocol 的 `TonicNoiseSession` 虽然文件较大，
但 `send`、`receive`、心跳和 rekey 共享 nonce 状态，继续拆分会破坏 locality，因此只把无状态的策略和
事件类型放到 `session/policy.rs`，把状态转换留在原模块。

## Server 启动调用流程

正式 Server 当前从 library 入口进入，配置、数据库、状态、路由和监听按顺序完成：

```rust
// smalux-server/src/lib.rs
run() -> config::ServerConfig::from_env()
      -> bootstrap::run_server(config)

// bootstrap::run_server
build_runtime(config)
    -> ServerDatabase::connect(config.database)
    -> DatabaseConfig::validate()
    -> Database::connect()
    -> ServerDatabase::migrate()
    -> AppState::build(runtime_config, database)
    -> route::build_app_router(app_state)
TcpListener::bind()
serve_runtime(runtime, listener)
    -> axum::serve(listener, router)
    -> Ctrl+C / graceful shutdown
```

`AppState::build` 还会启动两个可取消后台任务：Server Noise 密钥环的跨实例同步，以及 Agent
注册事务的过期清理。二者共享进程级 `CancellationToken`，关闭时由 `ShutdownOwner` 触发取消，
不会因为普通 `AppState` clone 提前停止。

路由装配时，Agent 路由会先调用 `ServerKeyRingManager::active_key_count` 做启动检查，再构造
`AgentTransportService` 和 `AgentTransportServer`。因此“Router 构造成功”表示当前至少有一个可用 Server
Noise 身份；数据库、密钥环或迁移失败时不会启动半可用监听器。

## 一次业务请求的完整路径

```text
AgentProtocolClient::connect / register_agent
  -> AgentTransportRpcClient::open_session
  -> ProtocolFrame(NoiseHandshake)
  -> AgentTransportService::open_session
  -> ServerSessionAcceptor
  -> TonicNoiseSession
  -> SessionDriver 或手动 receive_event
  -> RemoteJobController::apply_command / TaskReportSink
  -> SecureMessage(ciphertext)
  -> ProtocolFrame
  -> Server Session Policy
```

每一层只能确认自己的动作：gRPC stream 写入成功不代表业务已经持久化，Noise 解密成功不代表
Agent 已获得业务授权，Task 返回成功也不代表 `TaskReport` 已经送达 Server。需要可靠结果时，
应在连接层外增加本地有界队列、重放和 ACK，而不是把这些职责塞进 Collector 或 Scheduler。

## 当前边界如何影响开发

可以直接基于 crate 编写和测试新的 Collector、Task、Job 或 Protocol 行为，也可以运行 Example 验证
Axum/Tonic/Noise 调用链。但正式 Agent/Server 入口还不是完整产品进程，部署文档中的数据库、重连、
离线队列和授权要求属于接入正式应用时必须补齐的工程边界。
