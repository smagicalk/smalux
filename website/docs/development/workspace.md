---
title: Workspace 开发
description: Smalux 源码目录、模块边界和本地开发流程。
---

# Workspace 开发

## 目录

```text
smalux/
├── assets/                         # 项目品牌与文档资源
├── crates/
│   ├── smalux-agent/               # 采集、调度和 Job 控制
│   ├── smalux-server/              # Server 应用骨架
│   ├── smalux-core/                # 公共基础库
│   ├── smalux-protocol/            # Proto、Noise、Tonic
│   └── smalux-plus/
│       └── smalux-plus-rustic/     # 可选 Rustic 扩展
├── website/                        # 本文档站
├── Cargo.toml
└── Cargo.lock
```

## Agent 修改位置

| 需求 | 位置 |
| --- | --- |
| 新增原始系统读取 | `smalux-agent/src/tasks/collect/collectors/` |
| 新增固定采集 Task | `smalux-agent/src/tasks/collect/` |
| 修改触发、队列、并发 | `smalux-agent/src/scheduler/` |
| 修改远程 Job 应用行为 | `smalux-agent/src/remote_jobs.rs` |
| 修改 Proto Job 编译和固定 Task 工厂 | `smalux-agent/src/remote_jobs/compiler.rs` |
| 修改公开 Task 配置/结果 | `smalux-protocol/proto/.../task/` |

Collector 应尽量只返回领域数据或明确错误；Task 负责配置、采样状态和 Proto 结果；Scheduler 不理解 CPU、
Socket 或 Probe 业务；`RemoteJobController` 不直接执行具体 Task。

判断修改归属时可以按问题提问：

| 问题 | 应修改 |
| --- | --- |
| “操作系统原始数据怎么读？” | Collector。 |
| “配置如何筛选、排序并变成结果？” | Task。 |
| “什么时候执行、拥堵如何处理？” | Scheduler/JobDefinition。 |
| “Server 如何创建、更新或删除定义？” | RemoteJobController 与 Job Proto。 |
| “消息如何跨 Agent/Server 传输？” | Protocol。 |
| “结果如何保存、查询和展示？” | Server 应用与存储层。 |

同一需求跨越多层时，先稳定 Proto 契约，再分别实现两端；不要让数据库模型、生成的 Rust 类型或平台 API
结构直接渗透到所有层。

## Protocol 修改位置

| 需求 | 位置 |
| --- | --- |
| 修改 wire 消息 | `smalux-protocol/proto/smalux/agent/v1/` |
| 修改 Agent Noise 握手 | `src/noise/client/` |
| 修改 Server Noise 握手 | `src/noise/server/` |
| 修改加密 Session/rekey | `src/noise/session.rs` |
| 修改 Tonic Client/Server | `src/tonic_transport/client.rs`、`server.rs` |
| 修改 Session 策略、心跳统计和事件分类 | `src/tonic_transport/session/policy.rs` |
| 修改 Tonic Session 状态机 | `src/tonic_transport/session.rs` 及 `session/` 内部模块 |
| 修改后台 Driver | `src/tonic_transport/driver.rs` |

生成的 Rust 文件位于 Cargo `OUT_DIR`，不要手动修改或提交。

Protocol 的高层 API 与底层 Noise 状态机有意同时保留。普通应用修改应优先落在高层 Client/Acceptor/
Driver；只有增加传输适配或验证加密状态机时才直接操作底层 handshake/session，避免出现两个 owner 同时
推进 nonce。

## Server 的调用阅读顺序

从 Server 启动或 Agent RPC 开始阅读时，按下面的顺序可以保持较好的 locality，不需要在几十个小文件
之间来回跳转：

```text
lib.rs
  -> bootstrap.rs                 配置、数据库、监听和优雅关闭
  -> state.rs                     AppState、密钥环同步和清理任务
  -> route.rs                     顶层 Router 和请求 ID/Trace 中间件
  -> controller/agent.rs          Agent gRPC Router 装配
  -> service/agent/transport.rs
       Tonic trait、OpenSession、握手前错误和 worker 生命周期
  -> service/agent/transport/session.rs
       注册、授权、加密业务循环
  -> agent_registry.rs             Server 注册、授权与吊销接口
  -> database/agent_registration.rs
       Token、事务和 Agent 状态的持久化实现
```

数据库配置和运行连接也已经分开：`config/database.rs` 只解析 URL、环境变量、后端 options 和连接
池参数；`database/connection.rs` 只负责 `ServerDatabase::connect`、迁移和连接句柄。修改某个后端
配置时不需要阅读注册事务或 KeyRing 的实现。

## Agent Job 的调用阅读顺序

```text
SessionDriver / 连接层
  -> RemoteJobController::apply_command
       -> command_id 幂等缓存和 catalog_revision
       -> compiler::compile_job
            -> TaskFactory::build
            -> Trigger / JobOptions 强类型校验
       -> Scheduler::install/update/delete/enable/disable
       -> TaskReportSink
```

`remote_jobs.rs` 保留远程目录、命令状态和所有权；`remote_jobs/compiler.rs` 保留 Proto 编译和
Task 装配。不要为了添加一个采集器去修改控制器，也不要为了改变上报方式去修改 Collector。

## 用接口定位修改点

当一个需求跨越多个模块时，先写出调用者真正需要知道的接口，再判断实现属于哪一个 seam：

| 需求 | 首先阅读 | 不要直接修改 |
| --- | --- | --- |
| 增加 CPU/Socket 字段 | Proto task/result、Task 和 Collector | `RemoteJobController`、Tonic Session |
| 改 Job 版本或幂等 | `RemoteJobController::apply_command` 和 Job Proto | Scheduler 内部队列 |
| 改上报目标 | `TaskReportSink` adapter、SessionHandle | Collector 和 Task |
| 改握手超时/Token | `AgentProtocolClient`、`ServerSessionAcceptor` | Axum handler 业务逻辑 |
| 改 Agent 授权 | `AgentRegistry::authorize_agent`、数据库 adapter | Noise 静态密钥状态机 |
| 改数据库连接参数 | `config/database.rs` | 注册流程和 Router |

深模块的判断标准是：接口保持小而稳定，复杂性留在实现内部；adapter 是改变输出或传输方式的 seam，
不是把一层调用机械转发到另一层的包装。新增第二种 adapter 前，先确认两种实现共享的接口和错误语义。

## 推荐开发循环

```text
1. 明确行为和所属模块
2. 写最小失败测试
3. 实现最小修复或能力
4. 运行目标 crate 测试
5. 重构命名和边界
6. 运行 workspace 检查
7. 同步文档和 Example
```

目标 crate 的快速循环：

```powershell
cargo test -p smalux-agent
cargo test -p smalux-protocol
```

准备提交前再运行 workspace 全量检查，避免只验证局部 feature 或 example。

## 命名原则

- Collector 使用采集领域名称，例如 `SocketCollector`，不把调度职责写入名称。
- Task 表示一次可执行操作，例如 `CpuTask`，返回强类型 `TaskResult`。
- Job 表示包含触发器和策略的长期定义。
- `revision` 表示业务配置版本；`generation` 表示进程内部执行隔离版本。
- `handle` 表示向后台 owner 发送命令的轻量句柄；不要把它命名为完整 Session。
- `receive` 表示读取，`validate` 表示校验，`handle/apply` 表示会改变业务状态。

## 文档维护

源码附近的 README 和流程文档继续保留；网站内容单独写在 `website/docs/`。修改协议或 Task 时，应同时检查：

1. `.proto` 注释；
2. Rust 公开方法文档；
3. 测试和 Example；
4. 网站对应章节。

网站是面向使用者的整理层，不替代源码契约。

## 提交前自查

- 新公开类型是否有 Rustdoc，Proto 新字段是否有字段注释；
- 配置错误是否在创建阶段返回，而不是每次运行重复失败；
- 新增状态是否有恢复路径、超时和取消行为；
- 日志是否可能泄露 Token、私钥、命令行或业务载荷；
- Example 是否仍使用公开 API，而不是复制内部实现；
- 网站“已实现/示例/待完成”的描述是否同步更新。
