---
slug: /
title: Smalux 文档
description: Smalux Agent、Server、任务调度与安全协议的开发和使用文档。
sidebar_position: 1
---

![Smalux](/smalux.png)

# Smalux 文档

Smalux 是一个使用 Rust 编写的轻量监控探针项目。它把系统信息采集、任务调度、远程 Job
控制和安全传输拆成边界清晰的模块，目标是在长期运行时保持低干扰、行为可预测且容易扩展。

## 当前可以阅读什么

| 目标 | 推荐入口 |
| --- | --- |
| 第一次了解项目 | [项目概览](getting-started/overview.md) |
| 在本机编译运行 | [快速开始](getting-started/quick-start.md) |
| 了解 Agent 采集能力 | [采集任务](usage/collectors.md) |
| 配置远程调度 | [Job 与 Task](usage/job-task.md) |
| 接入 gRPC + Noise | [Protocol 概览](protocol/overview.md) |
| 新增采集能力 | [扩展 Task](extensions/task.md) |
| 部署在反向代理后 | [反向代理](deployment/reverse-proxy.md) |

按角色阅读时，可以使用以下顺序：

- Agent 开发：项目概览 → Job 与 Task → 采集/探测 → 扩展 Task → 测试与质量；
- Server 开发：Protocol 概览 → 注册与会话 → Job 命令 → 部署拓扑 → 实现状态；
- 运维部署：源码构建 → Server/Agent 安装边界 → 安全 → 反向代理 → 命令速查；
- 协议开发：Protobuf 开发 → Session → 安全与密钥生命周期 → Protocol Example。

## 项目状态

项目尚未发布稳定版本。Agent 的采集器、Scheduler、Proto Job/Task 模型以及 Protocol 的
Noise 会话已经具有测试覆盖；Server 的生产存储、正式管理 API、Web 管理端和安装发行物仍在建设。

因此当前文档遵循两个规则：

1. 已实现能力给出实际模块、方法和验证命令。
2. 尚未实现的生产能力明确标为边界或后续工作，不提供看似可用的虚构配置。

## 核心数据流

```text
Server / 本地配置
        |
        v
   JobCommand -----> Agent JobController
                          |
                          v
                  Scheduler + ReportingTask
                          |
                          v
                     TaskReport
                          |
                          v
               本地缓冲或 Protocol Session
                          |
                          v
                        Server
```

Agent 与 Server 的长期连接使用 gRPC 双向流。首次通过 XXpsk3 完成注册并学习 Server Noise
公钥，后续通过 IK 快速重连；业务消息始终位于 Noise 密文内。外层 TLS 可以启用，也可以由
Cloudflare 或 Nginx 终止。

## 文档与源码的关系

本网站的页面单独维护在 `website/docs/`。仓库根 `README.md`、Protocol crate 的 README、
`PROTOCOL_FLOW.md` 和 Example README 都保留在原位置，便于在源码附近阅读。网站不会移动或
覆盖这些文件；协议字段的最终事实来源始终是 `.proto`，Rust 行为的最终事实来源始终是测试和源码。

遇到网站描述与代码不一致时，以当前分支的 Proto、公开 API 和测试为准，并同步修正文档。项目仍处于未
发布阶段，页面会明确区分“已实现 crate 能力”“可运行 Example”和“正式应用尚未接入”三种状态。
