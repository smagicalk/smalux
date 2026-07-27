---
title: Plus 模块
description: 将备份等可选、有副作用能力隔离为独立扩展。
---

# Plus 模块

`smalux-plus` 用于不应增加 Agent 核心耦合的可选能力。当前 workspace 包含
`smalux-plus-rustic` 骨架，用于探索 Rustic 备份集成，但尚未实现完整备份 Task。

## 推荐边界

```text
smalux-protocol
    配置、结果、错误和能力契约

smalux-plus-rustic
    Rustic 配置校验、执行逻辑、结果构造

smalux-agent TaskFactory
    把 Proto 分支映射到启用的 Plus 工厂

smalux-agent JobController / Scheduler
    不理解 Rustic 业务细节
```

Plus crate 不处理 gRPC Session，Protocol crate 不访问 Rustic 仓库，Scheduler 不保存仓库凭据。

## 使用资源引用

远程 Proto 只应携带稳定引用：

```proto
message RusticBackupTaskConfig {
  string repository_id = 1;
  string source_id = 2;
}
```

Agent 使用 `repository_id` 和 `source_id` 查询本地配置及安全存储。不要把仓库密码、云访问 Token、完整
shell 命令或任意路径直接放进 Job Proto。

## 有副作用任务

备份、更新、脚本和修复与只读采集不同，接入统一 Job 前必须回答：

- Server 是否允许远程创建和 `RunNow`；
- Agent 本地允许访问哪些目录、仓库和凭据；
- 重复命令是否安全，持久化幂等键是什么；
- Agent 重启后如何恢复 running/succeeded/failed 状态；
- 断联后继续多久，何时暂停；
- timeout 或 cancel 是否真的能终止底层进程；
- 日志、结果和审计保留多久；
- 如何防止任务耗尽磁盘、带宽和进程资源。

`JobController` 的 `command_id` 缓存是有限的进程内幂等窗口，不能替代备份操作的持久化幂等。Plus Task
必须保存业务 run ID 和最终状态，使 Agent 重启后仍能识别已开始或已完成的操作。

## Feature 与分发

可选 Plus 能力可使用 Cargo feature 或不同 Agent 发行物控制，但协议字段不能因 feature 改变编号。
Server 必须依据 Agent 上报的 capability 下发任务；未启用的 Agent 返回明确不支持错误。

当 Plus 模块成熟后，再决定它是静态链接进 Agent、独立子进程还是受控插件。当前阶段优先静态类型和明确
工厂映射，避免过早引入动态加载和任意代码执行风险。
