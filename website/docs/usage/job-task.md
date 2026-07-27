---
title: Job 与 Task
description: 使用 Proto JobDefinition 配置 Agent 调度任务。
---

# Job 与 Task

Smalux 不再维护 JSON Job 模型。Server、本地存储和 Agent 之间共享的配置以 Proto
`JobDefinition` 为准，连接层收到 `JobCommand` 后可直接交给 `JobController::apply`。

## JobDefinition

一个完整 Job 包含：

| 字段 | 含义 |
| --- | --- |
| `job_id` | 16 字节 UUID，由业务层分配并长期保持稳定。 |
| `revision` | 单 Job 业务版本，从 1 开始，更新时必须严格增大。 |
| `enabled` | 是否生成新的计划执行；关闭不等于删除定义。 |
| `trigger` | once、interval 或 cron 之一，以及超时和 misfire 策略。 |
| `options` | 并发、Pending、优先级、合并、容量、重试和连续失败策略。 |
| `task` | 固定 Task 类型及其完整配置。 |

Proto 使用 `oneof` 限制一个 Job 只能选择一种时间规则和一种 Task。Agent 必须拒绝空 `oneof`，
不能根据缺失字段猜测 Server 意图。

## 触发器

### Once

在一个 UTC 时间执行一次。计划时间已经过去时，由 Scheduler 尽快执行。

### Interval

按固定时长重复执行。`every` 必须大于 Agent 允许的最小周期；`start_at` 缺失时从应用 Job 的时间开始。

### Cron

使用 Cron 表达式和 IANA 时区：

```text
expression = "0 */5 * * * *"
timezone   = "Asia/Shanghai"
```

必须保存 IANA 时区而不是固定 UTC offset，否则夏令时地区会在规则变化时产生错误执行时间。

## Misfire

当 Agent 重启、系统休眠或执行拥堵导致错过计划时间时，必须明确选择：

| 策略 | 行为 |
| --- | --- |
| `SKIP` | 丢弃过去周期，只等待下一次计划。 |
| `FIRE_ONCE` | 不管错过多少周期，只立即补一次。 |
| `CATCH_UP` | 按时间顺序补多次，但不超过 `max_runs`。 |

监控采样通常使用 `SKIP` 或 `FIRE_ONCE`。高频采样使用无上限 `CATCH_UP` 会在恢复后制造突发负载。

## 队列与失败策略

- `concurrency`：同一 Job 同时运行的 Task 数量。
- `max_pending`：等待执行的触发数量上限。
- `priority`：0 到 10，数值越大越优先。
- `coalescing`：保留全部触发，或只保留等待队列中的最新触发。
- `capacity`：反压、丢弃最新触发或用最新触发替换最旧 Pending。
- `retry`：不重试，或按有上限的指数退避重试临时错误、超时和 panic。
- `failure`：连续失败达到阈值后自动停用，可选择是否统计超时和 panic。

高频系统指标的常见组合是 `KEEP_LATEST + REPLACE_OLDEST_TRIGGER`，它优先保持数据新鲜度；不能丢失
每次执行的操作则应使用 `KEEP_ALL + BACKPRESSURE`，并严格限制 Pending 和上游提交速度。

## 远程控制命令

| 命令 | 用途 |
| --- | --- |
| `ReplaceAllJobs` | 首次连接或 revision 断档后，用完整远程目录重新对账。 |
| `UpsertJob` | 创建或完整替换一个远程 Job。 |
| `DeleteJob` | 使用 `expected_revision` 删除一个远程 Job。 |
| `RunJobNow` | 不改变正常时间相位，额外立即触发一次。 |

每条 `JobCommand` 必须带 16 字节 `command_id`。Agent 缓存首次处理结果，相同 ID 重发时返回原结果，
避免网络重试重复执行 `RunJobNow`。

```rust
let result = job_controller.apply(command).await;
session_handle.send_job_command_result(result).await?;
```

`catalog_revision` 表示整个远程 Job 集合版本。增量命令必须恰好等于 Agent 当前版本加一，否则 Agent
返回 `RESYNC_REQUIRED`，Server 应发送新的 `ReplaceAllJobs`，不能继续盲目追加增量。

## 本地 Job 与远程 Job

本地 Job 由 Agent 本地配置或内置策略创建，远程 Job 由 `JobController` 管理。两者可以共用 Scheduler，
但必须保持所有权：`ReplaceAllJobs` 和 `clear()` 只操作远程 Job，不得删除本地 Job。

连接断开后远程 Job 会继续运行。若希望只离线执行一段时间，应由 Agent 连接管理层记录断开时间，超过
策略窗口后显式暂停远程 Job；这不是 Scheduler 根据 socket 状态自行判断的职责。
