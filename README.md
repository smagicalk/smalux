<p align="center">
  <img src="assets/smalux.png" alt="Smalux" width="320" />
</p>

# Smalux

**Smalux** is a lightweight monitoring probe designed to illuminate system behavior with clarity and minimal intrusion.  
It focuses on continuous observation, providing reliable signals about system health and runtime state, while remaining quiet, stable, and easy to integrate into existing environments.

**Smalux** 是一个轻量级的探针监控系统，旨在以最小侵入的方式清晰地呈现系统运行状态。  
它专注于持续观测，在保持稳定、安静和易于集成的前提下，提供可靠的系统健康与运行时信号。

---

## Design Philosophy | 设计理念

Smalux is built around simplicity, stability, and long-term observability.  
Rather than pursuing aggressive data collection or exhaustive metrics, it prioritizes meaningful signals that help operators understand system behavior over time.

The project favors clarity over complexity and predictability over cleverness, aiming to remain useful and maintainable as systems evolve.

Smalux 的设计核心是简洁、稳定以及长期可观测性。  
相比激进的数据采集或指标堆砌，它更关注真正有价值、能够帮助理解系统行为的信号。

在设计取舍上，Smalux 更偏向清晰而非复杂、可预测而非炫技，以确保在系统不断演进的过程中依然可维护、可依赖。

---

## Architecture Overview | 架构概览

Smalux follows a modular architecture with clear boundaries between data collection, aggregation, storage, and visualization.

Each component is designed to operate independently, allowing the system to scale and evolve without introducing tight coupling or unnecessary coordination.

Smalux 采用模块化架构，明确划分数据采集、聚合、存储与展示等职责边界。  
各组件均可独立运行，使系统能够在不引入强耦合或额外复杂度的情况下进行扩展与演进。

---

## Project Structure | 项目结构

Smalux is organized as a Rust workspace.  
Executable components and shared libraries are separated into independent crates, allowing multiple binaries to be built in a single compilation while keeping responsibilities clearly isolated.

Smalux 采用 Rust workspace 组织项目结构，将可执行程序与共享库拆分为独立的 crate，  
在一次构建中生成多个运行程序，同时保持职责清晰、边界明确。

- **smalux-agent**  
  The monitoring probe deployed close to observed systems.  
  Responsible for data collection, preprocessing, buffering, and reporting.

- **smalux-server**
  The server-side control plane and data intake process.
  Handles Agent registration, Job catalogs, reconciliation, and report/event persistence.
  Management HTTP APIs, dashboards, and long-term metric aggregation remain future work.

- **smalux-core**  
  Shared core library containing common types, configuration models, error definitions, and utilities.

- **[smalux-protocol](crates/smalux-protocol/README.md)**
  Owns the versioned gRPC schema and generated Tonic types shared by Agent and Server.
  Job definitions, control commands, typed task results, authentication, and streaming RPCs share one wire model.

- **smalux-plus**
  Contains optional feature crates, such as the reserved Restic backup integration, so optional
  capabilities do not increase the coupling of the Agent core.

- **assets**  
  Static assets such as project icons, diagrams, and documentation resources.

当前实现中，Agent 的底层系统采集器位于 `tasks/collect/collectors/`，固定采集任务位于
`tasks/collect/`，触发、队列、并发、重试和生命周期位于 `scheduler/`。所有采集任务都
实现 `ReportingTask`，直接返回 Proto `TaskResult`；`TaskReportSink` 决定结果写入本地、
Channel 或连接层。`remote_jobs` 负责把 `JobDefinition` 校验并装配为调度任务，同时处理
`JobCommand` 的幂等、catalog revision 和远程 Job 所有权。原有 JSON Job 解析层已经移除。

---

## Core Components | 核心组件

Smalux consists of a small set of focused components, each responsible for a well-defined role within the monitoring pipeline:

- **Probe / Agent**  
  Runs close to the observed system and performs data collection, preprocessing, buffering, and reporting.

- **Collector / Server**  
  Receives and validates Agent messages, persists Job state and reports, and exposes the current
  local management interface. Query APIs and metric aggregation are planned separately.

- **Storage Layer**  
  Persists registration, Job/runtime state, reports, and lifecycle events using SeaORM backends.
  A time-series aggregation and retention model is not part of the current minimal loop.

- **Web Interface**  
  The current `website/` is a Docusaurus documentation site. A monitoring management interface
  has not been added yet.

- **gRPC Transport**
  Provides a built-in, strongly-typed bidirectional transport definition for Agent and Server communication.
  HTTP routing, listener setup, authorization, and connection lifecycle remain the responsibility of the calling application.

Smalux 由一组职责明确的核心组件构成：

- **探针 / Agent**  
  运行在被监控系统附近，负责数据采集、预处理、缓冲以及数据上报。

- **收集器 / 服务端**  
  接收并校验 Agent 消息，保存注册、Job、报告和事件状态，并提供本地管理 IPC；查询 API、
  聚合和告警能力留待后续实现。

- **存储层**  
  使用 SeaORM 后端持久化注册、Job/runtime、报告和生命周期事件；当前尚未定义完整的时序
  指标聚合与保留模型。

- **Web 界面**  
  当前 `website/` 是 Docusaurus 文档站，监控管理界面尚未实现。

- **gRPC Transport**
  提供 Agent 与 Server 通信所需的内置强类型双向流定义。
  HTTP 路由、监听地址、授权以及连接生命周期仍由调用应用负责。

---

## Data Collection | 数据采集

Smalux emphasizes continuous and unobtrusive observation.  
Data collection is guided by the principle that signals should be actionable, interpretable, and stable over time.

What to collect, how frequently to collect it, and how to transport it are treated as explicit design decisions rather than defaults.

Smalux 强调持续且低干扰的观测方式。  
数据采集遵循“可操作、可理解、长期稳定”的原则。

采集内容、采集频率以及数据传输方式都被视为明确的设计选择，而非默认行为。

当前采集 Task 默认返回全部磁盘、网卡和本地地址，也可按磁盘名称、挂载点或接口
完整名称配置 include/exclude；include 非空时优先于 exclude。公网 IP Task 可选择
同时查询 IPv4/IPv6，或只查询其中一个地址族。

---

## Observability & Signals | 可观测性与信号

Smalux focuses on exposing signals that help answer practical operational questions, such as system health, availability, and behavioral changes.

The goal is to improve understanding and confidence in system operation, not merely to increase the volume of metrics.

Smalux 专注于输出能够回答实际运维问题的观测信号，例如系统健康状况、可用性以及行为变化。

其目标是提升对系统运行状态的理解与信心，而不是单纯增加指标数量。

---

## Technology Stack | 技术选型

Smalux is built with a focus on reliability, performance, and long-term maintainability.  
The technology stack is intentionally kept minimal and composable, favoring mature ecosystems and clear operational characteristics.

Smalux 在技术选型上注重可靠性、性能以及长期可维护性。  
整体架构保持克制与可组合性，优先选择成熟生态与行为可预测的技术方案。

---

### Backend & Probe | 后端与探针

The backend and probe components of Smalux are implemented in **Rust**, chosen for its performance, memory safety, and suitability for long-running system-level services.

Rust is used consistently across probe agents and server-side components to ensure predictable behavior and low operational overhead.

---

### Frontend | 前端

The current `website/` is a **Docusaurus** documentation site built with React, TypeScript, and
pnpm. It is not yet the monitoring management UI.

---

### Data & Communication | 数据与通信

HTTP-based interfaces are used for health checks and future management integration. Agent/Server
business traffic uses the shared, versioned Protocol crate, which already contains the generated
Proto model, Noise authentication/encryption, Tonic streaming transport, Job/Task messages, and
reconciliation contracts.

---

## Deployment Model | 部署模型

Smalux components are designed to be deployed independently and operate reliably in long-running environments.

The deployment model favors simplicity and predictability, allowing Smalux to fit naturally into existing infrastructure setups.

---

## Extensibility | 可扩展性

The chosen architecture and technology stack allow Smalux to evolve gradually without forcing early complexity.

New capabilities can be introduced incrementally while preserving the core principles of simplicity and stability.

---

## Roadmap | 发展规划

Smalux is developed iteratively, with an emphasis on correctness, operational experience, and real-world feedback.

Future work includes management APIs and UI, metric querying/retention, production deployment,
and further operational hardening around the existing Agent/Server Protocol loop.

---

## License | 许可证

This project is licensed under the terms specified in the LICENSE file.


