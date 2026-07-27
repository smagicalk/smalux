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
- ICMP、TCP Connect 和 HTTP 多节点探测；
- Server 下发固定类型 Job，Agent 持续执行；
- TLS/h2c 外层加 Noise 内层加密；
- Axum REST、WebSocket 和 gRPC 共用端口的集成验证。

在生产部署前仍需补齐正式持久化、权限管理、安装升级、上报队列、审计、限流和可观测性策略。
