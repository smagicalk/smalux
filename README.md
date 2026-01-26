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

## Core Components | 核心组件

Smalux consists of a small set of focused components, each responsible for a well-defined role within the monitoring pipeline:

- Probe / Agent: runs close to the observed system and performs data collection.
- Collector / Server: receives, aggregates, and exposes monitoring data.
- Storage Layer: persists metrics, events, and configuration data.
- Web Interface: provides visualization and operational access.

Smalux 由一组职责明确的核心组件构成：

- 探针 / Agent：运行在被监控系统附近，负责数据采集。
- 收集器 / 服务端：接收、聚合并对外暴露监控数据。
- 存储层：用于持久化指标、事件及配置信息。
- Web 界面：用于可视化展示与运维操作。

---

## Data Collection | 数据采集

Smalux emphasizes continuous and unobtrusive observation.  
Data collection is guided by the principle that signals should be actionable, interpretable, and stable over time.

What to collect, how frequently to collect it, and how to transport it are treated as explicit design decisions rather than defaults.

Smalux 强调持续且低干扰的观测方式。  
数据采集遵循“可操作、可理解、长期稳定”的原则。

采集内容、采集频率以及数据传输方式都被视为明确的设计选择，而非默认行为。

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

Smalux 的后端与探针组件基于 **Rust** 实现，  
Rust 在性能、内存安全以及长期运行的系统服务场景中具有明显优势。

统一使用 Rust 有助于保持行为一致性，并降低整体运行与维护成本。

---

### Frontend | 前端

The Smalux web interface is built with **React** and **TypeScript**, focusing on clarity, responsiveness, and ease of iteration.

The frontend is responsible for visualization, system overview, and operational interaction, while remaining decoupled from backend implementation details.

Smalux 的 Web 界面基于 **React** 与 **TypeScript** 构建，  
强调清晰的可视化、良好的交互体验以及快速迭代能力。

前端主要负责状态展示与操作入口，并与后端实现保持解耦。

---

### Data & Communication | 数据与通信

Smalux adopts straightforward and explicit communication patterns between components.  
Data exchange prioritizes readability, debuggability, and operational transparency.

Smalux 在组件间通信上采用清晰直接的模式，  
数据交互优先考虑可读性、可调试性以及运维透明度。

---

## Deployment Model | 部署模型

Smalux components are designed to be deployed independently and operate reliably in long-running environments.

The deployment model favors simplicity and predictability, allowing Smalux to fit naturally into existing infrastructure setups.

Smalux 的各个组件均可独立部署，并被设计为适合长期稳定运行。  
整体部署模型强调简单性与可预测性，便于融入现有基础设施环境。

---

## Extensibility | 可扩展性

The chosen architecture and technology stack allow Smalux to evolve gradually without forcing early complexity.

New capabilities can be introduced incrementally while preserving the core design principles of simplicity and stability.

当前架构与技术选型支持 Smalux 以渐进方式演进，  
在不引入过早复杂度的前提下，逐步扩展能力并保持简洁与稳定的核心原则。

---

## Roadmap | 发展规划

Smalux is developed iteratively, with an emphasis on correctness, operational experience, and real-world feedback.

Future work will focus on improving observability quality, operational ergonomics, and ecosystem integration.

Smalux 采用渐进式开发方式，优先关注正确性、运维体验以及真实使用场景中的反馈。

未来的工作将集中在提升可观测性质量、运维友好度以及与现有生态的集成能力上。

---

## License | 许可证

This project is licensed under the terms specified in the LICENSE file.
