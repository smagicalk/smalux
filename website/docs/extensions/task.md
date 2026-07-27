---
title: 扩展 Task
description: 从 Proto 到 Collector、ReportingTask 和 TaskReport 的完整扩展步骤。
---

# 扩展 Task

新增 Task 应沿现有固定类型链路扩展，不能允许 Server 指定任意 Rust 类型或代码。

```text
Proto TaskConfig
    -> Agent TaskFactory
    -> 固定 ReportingTask
    -> 可选 Collector
    -> Proto TaskResult
    -> TaskReportSink
```

## 1. 定义协议

在 `smalux-protocol/proto/smalux/agent/v1/task/` 新建或修改领域 Proto：

1. 定义最小、稳定的配置消息；
2. 定义对应的强类型结果；
3. 把配置加入 `TaskDefinition.oneof task`；
4. 把结果加入 `TaskResult.oneof result`；
5. 为新增字段分配从未使用过的编号；
6. 补充字段级注释和 round-trip 测试。

不要把 interval、cron、timeout 和 retry 重复放入 TaskConfig，它们属于 Job/Scheduler。

## 2. 选择是否需要 Collector

Collector 适合封装可复用的平台读取：

```text
SocketCollector -> 原始 Socket 数据
SocketTask      -> 模式、筛选、截断、Proto 结果
```

如果操作只有一个 Task 使用且没有独立采样状态，直接放在 Task 内可能更清晰。不要为了目录对称创建没有
行为的抽象层。

## 3. 实现 ReportingTask

Task 应：

- 在创建时校验静态配置，尽早返回明确错误；
- 在 `run` 中完成一次有限操作；
- 响应取消和超时，不持有跨运行的无界资源；
- 返回对应 `TaskResult` 分支；
- 把可重试临时错误和永久配置错误分类清楚；
- 不直接发送网络消息或修改 Scheduler。

需要计算增量的 Task 可以在实例内保存前一样本，但必须定义首次运行和计数器回绕行为。

## 4. 注册工厂映射

顶层工厂只做稳定分发：

```text
Task::Cpu(config)       -> CpuTask
Task::Probe(config)     -> ProbeTask
Task::NewType(config)   -> NewTypeTask
```

未知、未编译或未启用的分支必须返回稳定的 `UNSUPPORTED_TASK` 类错误，不能静默忽略或降级成其他 Task。

一次完整扩展通常会触及：

| 修改点 | 内容 |
| --- | --- |
| Protocol task Proto | Config、Result 与 oneof 分支。 |
| Protocol 测试 | 编解码、未知/缺失字段和兼容性。 |
| Agent Task 模块 | 配置转换、一次执行和结果构造。 |
| Collector（可选） | 平台读取与可测试接口。 |
| TaskFactory | 固定 Proto 分支到 Task 的映射。 |
| Agent 测试 | 校验、取消、超时、平台错误和边界数据。 |
| 文档与 Example | 配置字段、结果语义和调用示例。 |

## 5. 测试

至少覆盖：

1. Proto 配置和结果 round-trip；
2. 空 oneof、非法范围和冲突选择；
3. 一次成功执行；
4. 平台权限不足或能力不可用；
5. 超时、取消和临时错误；
6. include/exclude、排序、截断或并发边界；
7. `TaskReport` 中 job revision、run ID 和 attempt 的关联。

## 6. 更新能力协商

不同 Agent 构建可能支持不同 Task。正式连接协议应上报 Agent 版本、协议版本、Task 类型和配置版本；Server
只向声明支持的 Agent 下发 Job。当前能力协商仍是待完善边界，新增 Plus Task 时尤其不能假设所有 Agent
已经包含它。

在能力协商完成前，Server 应把 `UNSUPPORTED_TASK` 视为明确的兼容结果并停止重复下发，而不是不断重试。
Task 配置本身若需要独立演进，可以增加配置版本或新的 oneof 分支；不要让同一字段随 Agent 版本静默改变
含义。
