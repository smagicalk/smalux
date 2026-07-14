//! Scheduler 实时事件模型。

use super::{JobId, RunId};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Scheduler 实时事件。
#[derive(Debug, Clone, Serialize)]
pub struct SchedulerEvent {
    /// Actor 内单调递增的事件序号；溢出后按 u64 回绕。
    pub sequence: u64,
    /// 事件产生时的 UTC 时间。
    pub emitted_at: DateTime<Utc>,
    /// 具体生命周期事件及其调度元数据。
    pub kind: SchedulerEventKind,
}

/// 事件只携带调度元数据，不携带 Task 业务结果。
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchedulerEventKind {
    /// Scheduler Actor 已开始运行。
    SchedulerStarted,
    /// Scheduler 动态配置已更新。
    SchedulerConfigUpdated {
        /// 更新后的配置 revision。
        revision: u64,
    },
    /// Scheduler 开始优雅关闭。
    SchedulerStopping,
    /// Scheduler 已停止且运行实例已退出或被中止。
    SchedulerStopped,
    /// Scheduler 因不可恢复错误退出。
    SchedulerFailed {
        /// 失败原因摘要。
        message: String,
    },
    /// 新 Job 已注册并安排首次 Trigger。
    JobAdded {
        /// Job 标识。
        job_id: JobId,
        /// 新 Job 初始版本，当前固定为 0。
        version: u64,
    },
    /// Job 配置或 Task 已更新。
    JobUpdated {
        /// Job 标识。
        job_id: JobId,
        /// 更新后的 Job 版本。
        version: u64,
    },
    /// Job 已从 Disabled 或 Completed 恢复执行。
    JobEnabled {
        /// Job 标识。
        job_id: JobId,
        /// 启用后的 Job 版本。
        version: u64,
    },
    /// Job 已手动或自动停用。
    JobDisabled {
        /// Job 标识。
        job_id: JobId,
        /// 停用后的 Job 版本。
        version: u64,
        /// 停用原因。
        reason: String,
    },
    /// Job 已删除，不再接受 Trigger。
    JobDeleted {
        /// 被删除的 Job 标识。
        job_id: JobId,
        /// 删除前校验通过的版本。
        version: u64,
    },
    /// 正常 Trigger 已写入 DelayQueue。
    TriggerScheduled {
        /// Job 标识。
        job_id: JobId,
        /// Trigger 所属 Job 版本。
        version: u64,
        /// 计划触发时间。
        run_at: DateTime<Utc>,
    },
    /// Trigger 因 misfire 或容量策略被跳过。
    TriggerSkipped {
        /// Job 标识。
        job_id: JobId,
        /// Trigger 所属 Job 版本。
        version: u64,
        /// 原计划触发时间。
        scheduled_at: DateTime<Utc>,
        /// 跳过原因。
        reason: String,
    },
    /// 执行实例已进入 ReadyQueue。
    ExecutionQueued {
        /// Job 标识。
        job_id: JobId,
        /// 执行实例所属 Job 版本。
        version: u64,
        /// 单次逻辑执行标识。
        run_id: RunId,
        /// 入队后该 Job 的 ReadyQueue Pending 数量。
        pending_count: usize,
    },
    /// 执行实例已获得并发槽位并启动。
    ExecutionStarted {
        /// Job 标识。
        job_id: JobId,
        /// 执行实例所属 Job 版本。
        version: u64,
        /// 单次逻辑执行标识。
        run_id: RunId,
        /// 当前尝试次数，从 1 开始。
        attempt: u32,
        /// Trigger 原计划时间。
        scheduled_at: DateTime<Utc>,
    },
    /// Task 以及输出适配器成功完成。
    ExecutionSucceeded {
        /// Job 标识。
        job_id: JobId,
        /// 执行实例所属 Job 版本。
        version: u64,
        /// 单次逻辑执行标识。
        run_id: RunId,
        /// 成功的尝试次数。
        attempt: u32,
        /// 本次尝试耗时，单位毫秒。
        duration_ms: u64,
    },
    /// Task 执行失败。
    ExecutionFailed {
        /// Job 标识。
        job_id: JobId,
        /// 执行实例所属 Job 版本。
        version: u64,
        /// 单次逻辑执行标识。
        run_id: RunId,
        /// 失败的尝试次数。
        attempt: u32,
        /// 错误摘要。
        error: String,
        /// 是否已安排下一次 Retry。
        will_retry: bool,
    },
    /// Task 超过 Trigger timeout。
    ExecutionTimedOut {
        /// Job 标识。
        job_id: JobId,
        /// 执行实例所属 Job 版本。
        version: u64,
        /// 单次逻辑执行标识。
        run_id: RunId,
        /// 超时的尝试次数。
        attempt: u32,
    },
    /// Task Future 发生 panic，panic 已被 Scheduler 捕获。
    ExecutionPanicked {
        /// Job 标识。
        job_id: JobId,
        /// 执行实例所属 Job 版本。
        version: u64,
        /// 单次逻辑执行标识。
        run_id: RunId,
        /// panic 的尝试次数。
        attempt: u32,
        /// panic payload 转换后的文本。
        message: String,
    },
    /// 执行实例因删除、停用、超时或关闭而取消。
    ExecutionCancelled {
        /// Job 标识。
        job_id: JobId,
        /// 被取消实例所属 Job 版本。
        version: u64,
        /// 单次逻辑执行标识。
        run_id: RunId,
    },
    /// 临时失败已安排 Retry 计时项。
    RetryScheduled {
        /// Job 标识。
        job_id: JobId,
        /// Retry 所属 Job 版本。
        version: u64,
        /// 保持不变的逻辑执行标识。
        run_id: RunId,
        /// 即将运行的尝试次数。
        attempt: u32,
        /// Retry 计划时间。
        run_at: DateTime<Utc>,
    },
    /// ValueTask 输出已成功送到 Channel 或 Callback。
    OutputDelivered {
        /// Job 标识。
        job_id: JobId,
        /// 输出所属 Job 版本。
        version: u64,
        /// 产生输出的逻辑执行标识。
        run_id: RunId,
    },
    /// Callback 未能消费 ValueTask 输出。
    CallbackFailed {
        /// Job 标识。
        job_id: JobId,
        /// 输出所属 Job 版本。
        version: u64,
        /// 产生输出的逻辑执行标识。
        run_id: RunId,
        /// Callback 错误摘要。
        error: String,
    },
    /// ValueTask 输出 Channel 的接收端已关闭，Job 将自动删除。
    OutputChannelClosed {
        /// Job 标识。
        job_id: JobId,
        /// 输出所属 Job 版本。
        version: u64,
        /// 产生输出的逻辑执行标识。
        run_id: RunId,
    },
    /// KeepLatest 或 ReplaceOldestTrigger 替换了 Pending 实例。
    PendingReplaced {
        /// Job 标识。
        job_id: JobId,
        /// 被移除的旧逻辑执行标识。
        removed_run_id: RunId,
        /// 替换它的新逻辑执行标识。
        replacement_run_id: RunId,
    },
    /// Pending 达到容量上限，当前 Trigger 被暂存并暂停后续周期。
    BackpressureApplied {
        /// Job 标识。
        job_id: JobId,
        /// 触发 Backpressure 时的 Job 版本。
        version: u64,
    },
}
