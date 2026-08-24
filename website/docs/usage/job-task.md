---
title: Job 与 Task
description: 使用 Proto JobDefinition 配置 Agent 调度任务。
---

# Job 与 Task

Smalux 不再维护 JSON Job 模型。Server、本地存储和 Agent 之间共享的配置以 Proto
`JobDefinition` 为准，连接层收到 `JobCommand` 后可直接交给 `RemoteJobController::apply_command`。

完整执行链如下：

```text
Server 构造 JobCommand
  -> Protocol 会话发送命令
  -> Agent 的 RemoteJobController 校验 revision 和幂等键
  -> JobFactory 把 Proto Task 配置转换为可执行 Task
  -> Scheduler 根据 Trigger 产生执行实例
  -> Task 返回 TaskResult
  -> Agent 包装 TaskReport 并通过会话上报
```

`RemoteJobController` 负责远程目录和命令语义，`Scheduler` 负责时间、队列与并发，Task 只负责一次业务执行。
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
let result = remote_jobs.apply_command(command).await;
session_handle.send_job_command_result(result).await?;
```

`catalog_revision` 表示整个远程 Job 集合版本。增量命令必须恰好等于 Agent 当前版本加一，否则 Agent
返回 `RESYNC_REQUIRED`，Server 应发送新的 `ReplaceAllJobs`，不能继续盲目追加增量。`ReplaceAllJobs`
携带 Server 的权威快照版本，可以跨过缺失的增量；相同版本可以重放，低于 Agent 当前版本的快照
必须拒绝。

一次典型对账过程是：Server 先读取 Agent 报告的目录版本；版本一致时继续发送下一条增量命令，版本
缺失或断档时发送完整 `ReplaceAllJobs`。Agent 只有在完整目录校验并应用成功后才能提交新的
`catalog_revision`，不能先更新版本再逐项写入，否则中途失败会留下无法解释的半更新状态。

## 本地 Job 与远程 Job

本地 Job 由 Agent 本地配置或内置策略创建，远程 Job 由 `RemoteJobController` 管理。两者可以共用 Scheduler，
但必须保持所有权：`ReplaceAllJobs` 和 `clear()` 只操作远程 Job，不得删除本地 Job。

连接断开后远程 Job 会继续运行。若希望只离线执行一段时间，应由 Agent 连接管理层记录断开时间，超过
策略窗口后显式暂停远程 Job；这不是 Scheduler 根据 socket 状态自行判断的职责。

## 动态远程 Job 黑名单

黑名单是 Agent 本地策略，不由 Server 数据库拥有。使用本机 IPC 修改：

```powershell
smalux-agent jobs policy show
smalux-agent jobs policy add-task smalux.collect.process.v1
smalux-agent jobs policy remove-task smalux.collect.process.v1
smalux-agent jobs policy deny-all
smalux-agent jobs policy allow-all
```

`add-task` 只接受 `smalux-agent tasks list` 显示的完整稳定 kind。加入后，匹配的远程 Job
立即变为 Disabled，定时器、Pending 和重试被清理，正在运行的实例收到取消信号；本地 Job
不受影响。规则只匹配外层 Task kind，CPU 不会连带匹配 System。

`remove-task` 和 `allow-all` 不直接启用旧定义。Agent 上报新策略后，Server 应重新读取权威
目录并发送 `ReplaceAllJobs`。`allow-all` 只解除 `deny_all`，不会清空逐项黑名单。

策略保存在 `<data_dir>/agent/job-policy.json`。离线修改会显示 `server_sync=pending`，重连后
自动上报最新完整快照。文件损坏时 Agent 拒绝非交互启动；使用
`smalux-agent jobs policy repair --reset` 备份损坏文件并恢复空策略。

完整 Wire 顺序和 revision 幂等规则见
[协议会话](../protocol/session.md#agent-job-策略同步)。

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

## JobCommand 的实际调用链

Agent 收到 `JobCommand` 后，唯一推荐入口是 `RemoteJobController::apply_command`。连接层不应直接调用
`Scheduler::install`，因为目录版本、命令幂等和远程所有权都必须在同一个控制器锁内处理：

```text
SessionDriver::recv
  -> SessionEvent::JobCommand(command)
  -> RemoteJobController::apply_command(command)
       -> parse_uuid(command_id)
       -> cached_results[command_id] 命中？返回首次结果
       -> state.lock()
       -> apply_locked
          -> UpsertJob / DeleteJob / RunJobNow / ReplaceAllJobs
       -> cache_result(command_id, result)
  -> SessionHandle::send_job_command_result(result)
```

`command_id` 是命令级幂等键，`catalog_revision` 是远程 Job 集合版本，`JobDefinition.revision`
是单个 Job 版本，Scheduler `JobSnapshot.version` 是进程内 generation。它们的使用位置如下：

| 版本 | 由谁产生 | 校验位置 | 用来解决什么问题 |
| --- | --- | --- | --- |
| `command_id` | Server 命令构造器 | `RemoteJobController::apply_command` | 网络重试不重复执行命令。 |
| `catalog_revision` | Server 远程目录 | `require_next_catalog` / `require_replace_all_catalog` | 检测增量丢失、乱序和旧快照。 |
| `JobDefinition.revision` | Job 配置拥有者 | `compile_job`、`upsert_compiled` | 防止旧配置覆盖新配置。 |
| `JobSnapshot.version` | Agent Scheduler | `update/delete/enable/disable` | 防止并发修改覆盖。 |

### UpsertJob

```text
UpsertJob
  -> require_next_catalog(catalog_revision)
  -> compile_job(definition, TaskFactory)
       -> parse_uuid(job_id)
       -> validate revision/trigger/options
       -> TaskFactory::build(TaskDefinition)
          -> CpuTask / MemoryTask / ProcessTask / ProbeTask ...
          -> TaskBinding::reporting(task, job_revision, sink)
  -> existing remote Job?
       -> Scheduler::update(id, generation, full_patch)
       -> set_enabled(snapshot, enabled)
     new Job?
       -> Scheduler::install(id, generation=1, ...)
       -> Scheduler::get(id)
  -> remote_jobs[id] = { revision, generation }
  -> catalog_revision = command.catalog_revision
  -> JobCommandResult::Applied
```

编译阶段不会修改 Scheduler。这样非法的 UUID、空 `oneof`、无效时间、错误重试策略或不支持的
Task 配置会在安装前被拒绝，不会留下半安装 Job。`compiler.rs` 只负责 Proto 到强类型的转换，
不持有网络连接，也不推进目录状态。

### DeleteJob 和 RunJobNow

```text
DeleteJob
  -> require_next_catalog
  -> parse_uuid(job_id)
  -> remote_jobs 中查找并校验 expected_revision
  -> Scheduler::delete(id, generation)
  -> 删除远程所有权索引
  -> 推进 catalog_revision

RunJobNow
  -> parse_uuid(job_id)
  -> 校验 expected_revision
  -> JobPatch { reschedule: RunNow }
  -> Scheduler::update(id, generation, patch)
  -> 只更新 generation，不改变 JobDefinition.revision
```

`RunJobNow` 是额外触发，不会把 interval 或 cron 的原始相位改成“从现在开始”。删除只接受
`remote_jobs` 中的 Job，因此不会误删 Agent 本地创建的 Job。

### ReplaceAllJobs

全量快照分成两个阶段：

```text
阶段一：纯校验
  遍历全部 JobDefinition
  -> 检查 catalog_revision
  -> 检查重复 job_id
  -> compile_job 全部成功

阶段二：应用
  -> 逐个 upsert_compiled
  -> 删除快照中不存在的旧远程 Job
  -> 最后写入新的 catalog_revision
```

相同版本可以重放，用来修复 Agent 与 Server 的运行状态差异；低于当前版本的快照拒绝；高于
当前版本的快照可以跨过丢失的增量。单个 Scheduler 写入是原子的，但多个 Job 不是跨 Job 事务，
所以 Server 仍应在收到失败结果后重新发送权威快照，而不是假设所有项都已成功。

## TaskFactory、Scheduler 和结果出口

Task 的职责是“一次执行并返回结果”，不是向 gRPC 发送消息。固定 Task 的装配发生在
`remote_jobs/compiler.rs`：

```text
TaskDefinition.oneof
  -> TaskFactory::build
  -> 固定 Task::with_config / try_with_config
  -> ReportingTask trait object
  -> TaskBinding::reporting(task, revision, sink)
  -> Scheduler::install/update
```

Scheduler 只读取 `Trigger`、`JobOptions` 和 `TaskBinding`，负责：

- 根据 once/interval/cron 产生触发；
- 执行全局和单 Job 并发限制；
- 处理 Pending、coalescing、capacity 和 misfire；
- 对临时错误、超时和 panic 应用重试策略；
- 在取消、删除或连续失败时结束执行；
- 为每次更新维护内部 generation。

Task 通过 `TaskReportSink` 把执行结果交给调用方。Sink 可以是内存 channel、本地持久化队列或
Protocol Session 适配器，RemoteJobController 不假设它是哪一种：

```rust
let controller = RemoteJobController::new(scheduler, report_sink);
let result = controller.apply_command(command).await;
session_handle.send_job_command_result(result).await?;

// Scheduler 执行时，ReportingTask 将业务 revision 写入 TaskReport。
// 连接层再决定是否立即发送、落盘等待重连，或批量上报。
```

这种接口是一个真正的 seam：现在至少有 Scheduler 内部结果出口和 Protocol/测试出口两类 adapter，
更换输出方式不会修改 Collector、Task 或调度规则。

## 断线、恢复和旧结果

断线不会自动调用 `RemoteJobController::clear`，已安装的远程 Job 可以继续运行一段时间。重新连接后，
Server 应根据 Agent 上报的 `catalog_revision` 选择增量同步或 `ReplaceAllJobs`。如果应用要求“断线
超过 N 分钟自动停采”，应由连接管理器记录断线时间并显式暂停 Job，不能让 Scheduler 通过 socket
状态隐式改变业务状态。

执行结果必须同时携带 `job_id` 和业务 `revision`。配置从 revision 7 更新到 revision 8 后，旧的
revision 7 执行可能仍在完成；Server 只能把它记录为旧版本结果或按策略丢弃，不能仅按 `job_id`
覆盖 revision 8 的状态。Protocol 当前没有跨 Session 的 TaskReport ACK，可靠上报需要在 Sink 外部
实现本地有界队列、重放、去重和确认。
