---
title: Job 与 Task
description: 使用 Proto JobDefinition 配置 Agent 调度任务。
---

# Job 与 Task

Smalux 不再维护 JSON Job 模型。Server、本地存储和 Agent 之间共享的配置以 Proto
`JobDefinition` 为准，连接层收到 `JobCommand` 后可直接交给 `JobController::apply`。

完整执行链如下：

```text
Server 构造 JobCommand
  -> Protocol 会话发送命令
  -> Agent 的 JobController 校验 revision 和幂等键
  -> JobFactory 把 Proto Task 配置转换为可执行 Task
  -> Scheduler 根据 Trigger 产生执行实例
  -> Task 返回 TaskResult
  -> Agent 包装 TaskReport 并通过会话上报
```

`JobController` 负责远程目录和命令语义，`Scheduler` 负责时间、队列与并发，Task 只负责一次业务执行。
不要让 Task 自己发送网络消息，否则 Task 会同时依赖采集、连接状态和重试策略，难以测试和复用。

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

### Scheduler 默认保护值

当前实现提供以下默认上限。Server 生成 Job 时仍应显式设置关键参数，不应把实现默认值当作永久协议：

| 项目 | 当前默认值 | 作用 |
| --- | --- | --- |
| 全局运行并发 | 逻辑 CPU 数 × 4 | 限制整个 Scheduler 同时执行的 Task。 |
| 单 Job 并发 | 1 | 避免同一采集任务默认重叠执行。 |
| 全局 Pending | 8192 | 限制全部等待触发的总量。 |
| 单 Job Pending | 1024 | 防止单个 Job 占满队列。 |
| 最小 Interval | 100 ms | 拒绝异常高频的周期配置。 |
| 最大 Job 数 | 1024 | 限制单 Agent 目录规模。 |
| 关闭等待时间 | 30 s | 为正在执行的 Task 留出退出窗口。 |
| 最大连续补跑数 | 1000 | 限制 `CATCH_UP` 恢复风暴。 |

### 常见策略组合

| 目标 | 推荐组合 | 原因 |
| --- | --- | --- |
| CPU、内存等最新状态 | `concurrency=1`、`KEEP_LATEST`、替换最旧 Pending | 旧采样价值低，优先保留最新状态。 |
| 一次性诊断命令 | `KEEP_ALL`、`BACKPRESSURE`、有限重试 | 每次请求都有独立业务意义。 |
| 外部网络探测 | 有限 Pending、指数退避、统计超时 | 防止故障期间持续放大流量。 |
| 进程或 Socket 详情 | `concurrency=1`、低频、较小 Pending | 平台扫描成本和结果体积较高。 |

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

一次典型对账过程是：Server 先读取 Agent 报告的目录版本；版本一致时继续发送下一条增量命令，版本
缺失或断档时发送完整 `ReplaceAllJobs`。Agent 只有在完整目录校验并应用成功后才能提交新的
`catalog_revision`，不能先更新版本再逐项写入，否则中途失败会留下无法解释的半更新状态。

## 本地 Job 与远程 Job

本地 Job 由 Agent 本地配置或内置策略创建，远程 Job 由 `JobController` 管理。两者可以共用 Scheduler，
但必须保持所有权：`ReplaceAllJobs` 和 `clear()` 只操作远程 Job，不得删除本地 Job。

连接断开后远程 Job 会继续运行。若希望只离线执行一段时间，应由 Agent 连接管理层记录断开时间，超过
策略窗口后显式暂停远程 Job；这不是 Scheduler 根据 socket 状态自行判断的职责。

## TaskReport

Task 的直接返回值由运行层转换为 `TaskReport`。Server 消费结果时至少应区分：

| 信息 | 用途 |
| --- | --- |
| Job ID 与 revision | 判断结果属于哪一版配置。 |
| 计划时间与实际开始时间 | 计算排队和 misfire 延迟。 |
| 完成时间与耗时 | 判断超时、性能退化和采集开销。 |
| 执行结果或错误 | 解码具体 `TaskResult`，或记录结构化失败。 |
| 尝试次数 | 区分首次成功与重试后成功。 |

配置 revision 更新后，旧执行实例可能仍在完成。Server 不应只按 `job_id` 覆盖结果，还要保留或检查
revision，防止旧配置的迟到结果污染新配置序列。
