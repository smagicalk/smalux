# Smalux

**Smalux** is a lightweight monitoring probe designed to illuminate system behavior with clarity and minimal intrusion.  
It focuses on continuous observation, providing reliable signals about system health and runtime state, while remaining quiet, stable, and easy to integrate into existing environments.

**Smalux** 是一个轻量级的探针监控系统，旨在以最小侵入的方式清晰地呈现系统运行状态。  
它专注于持续观测，在保持稳定、安静和易于集成的前提下，提供可靠的系统健康与运行时信号。

---

## Design Philosophy | 设计理念

Smalux is built around simplicity, stability, and long-term observability.  
Rather than pursuing aggressive data collection, it prioritizes meaningful signals, predictable behavior, and minimal operational overhead.

Smalux 的设计目标是简洁、稳定以及长期可观测性。  
它并不追求激进的数据采集，而是关注有价值的信号、可预测的行为以及尽可能低的运行负担。

---

## Architecture Overview | 架构概览

This section describes the high-level architecture of Smalux, including its core components, data flow, and interaction boundaries.  
The design emphasizes clear separation of responsibilities and ease of extension.

本节用于描述 Smalux 的整体架构，包括核心组件、数据流向以及模块之间的边界划分。  
整体设计强调职责清晰、结构简单，并便于后续扩展。

---

## Core Components | 核心组件

Smalux is composed of a small set of focused components, each responsible for a clearly defined role within the monitoring pipeline.

Smalux 由一组职责明确、功能单一的核心组件构成，每个组件在监控流程中承担清晰的角色。

---

## Data Collection | 数据采集

This section outlines how Smalux observes and collects runtime signals, as well as the principles guiding what should or should not be collected.

本节用于说明 Smalux 如何进行运行时观测与数据采集，以及在采集过程中所遵循的基本原则。

---

## Observability & Signals | 可观测性与信号

Smalux focuses on exposing signals that are actionable, interpretable, and stable over time.  
The goal is to improve system understanding rather than raw metric volume.

Smalux 专注于输出可操作、易理解且长期稳定的观测信号，  
目标是提升对系统状态的理解，而非单纯增加指标数量。

---

## Deployment Model | 部署方式

This section describes the intended deployment patterns for Smalux, including typical runtime environments and integration considerations.

本节用于说明 Smalux 的部署模型，包括常见的运行环境以及集成时需要考虑的因素。

---

## Operational Characteristics | 运行特性

Smalux is designed to run quietly and predictably over long periods of time.  
This section highlights expected resource usage, stability considerations, and operational behavior.

Smalux 被设计为可长期、稳定、低干扰地运行。  
本节描述其资源占用特性、稳定性考量以及运行时行为。

---

## Extensibility | 可扩展性

This section explains how Smalux can be extended or adapted to different use cases without compromising its core principles.

本节说明在不破坏 Smalux 核心设计原则的前提下，如何对其进行扩展或适配不同场景。

---

## Roadmap | 发展规划

This section outlines the general direction and long-term goals of the Smalux project.

本节用于描述 Smalux 项目的整体发展方向与长期目标。

---

## License | 许可证

License information for the Smalux project.
