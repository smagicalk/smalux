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
| 修改远程 Job 应用行为 | `smalux-agent/src/job_control.rs` |
| 修改公开 Task 配置/结果 | `smalux-protocol/proto/.../task/` |

Collector 应尽量只返回领域数据或明确错误；Task 负责配置、采样状态和 Proto 结果；Scheduler 不理解 CPU、
Socket 或 Probe 业务；`JobController` 不直接执行具体 Task。

## Protocol 修改位置

| 需求 | 位置 |
| --- | --- |
| 修改 wire 消息 | `smalux-protocol/proto/smalux/agent/v1/` |
| 修改 Agent Noise 握手 | `src/noise/client/` |
| 修改 Server Noise 握手 | `src/noise/server/` |
| 修改加密 Session/rekey | `src/noise/session.rs` |
| 修改 Tonic Client/Server | `src/tonic_transport/client.rs`、`server.rs` |
| 修改后台 Driver | `src/tonic_transport/driver.rs` |

生成的 Rust 文件位于 Cargo `OUT_DIR`，不要手动修改或提交。

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
