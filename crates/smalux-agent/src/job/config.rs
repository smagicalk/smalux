//! Scheduler 运行配置、动态配置 Patch、生命周期状态和管理错误。

use std::{num::NonZeroUsize, time::Duration};

use serde::Serialize;

use super::model::JobId;

/// Scheduler 运行时配置。
#[derive(Debug, Clone, Serialize)]
pub struct SchedulerConfig {
    /// 所有 Job 合计允许同时运行的最大实例数。
    pub global_concurrency: NonZeroUsize,
    /// 未单独设置时每个 Job 的默认并发数。
    pub default_job_concurrency: NonZeroUsize,
    /// ReadyQueue 允许保存的全局 Pending 上限。
    pub global_max_pending: usize,
    /// 未单独设置时每个 Job 的默认 Pending 上限。
    pub default_job_max_pending: usize,
    /// 单次 DelayQueue 唤醒最多处理的到期计时项数量。
    pub due_batch_size: NonZeroUsize,
    /// Scheduler 内允许注册的最大 Job 数。
    pub max_jobs: usize,
    /// 广播生命周期事件的缓冲容量；慢订阅者可能收到 lag 错误。
    pub event_channel_capacity: usize,
    /// Scheduler 句柄向 Actor 提交命令的通道容量。
    pub command_channel_capacity: usize,
    /// 优雅关闭等待运行中 Task 退出的最长时间。
    pub shutdown_timeout: Duration,
    /// Interval Trigger 允许设置的最小周期。
    pub minimum_interval: Duration,
    /// 指数重试允许设置的最小初始等待时间。
    pub minimum_retry_delay: Duration,
    /// 任一 CatchUp Trigger 允许配置的最大补执行次数。
    pub maximum_catch_up: u32,
    /// 任一重试策略允许配置的最大总尝试次数。
    pub maximum_retry_attempts: u32,
}

impl Default for SchedulerConfig {
    /// 创建面向轻量 Agent 的安全默认配置。
    ///
    /// 全局并发默认为逻辑 CPU 数的 4 倍，其他容量和安全限制见各字段说明。
    fn default() -> Self {
        let cpu_count = std::thread::available_parallelism()
            .map(NonZeroUsize::get)
            .unwrap_or(1);
        Self {
            global_concurrency: NonZeroUsize::new(cpu_count.saturating_mul(4)).unwrap(),
            default_job_concurrency: NonZeroUsize::MIN,
            global_max_pending: 8_192,
            default_job_max_pending: 1_024,
            due_batch_size: NonZeroUsize::new(256).unwrap(),
            max_jobs: 1_024,
            event_channel_capacity: 1_024,
            command_channel_capacity: 256,
            shutdown_timeout: Duration::from_secs(30),
            minimum_interval: Duration::from_millis(100),
            minimum_retry_delay: Duration::from_millis(100),
            maximum_catch_up: 1_000,
            maximum_retry_attempts: 1_000,
        }
    }
}

/// Scheduler 配置快照。
#[derive(Debug, Clone, Serialize)]
pub struct SchedulerConfigSnapshot {
    /// 配置版本，用于 [`crate::job::Scheduler::update_config`] 乐观锁。
    pub revision: u64,
    /// 当前完整 Scheduler 配置。
    pub config: SchedulerConfig,
}

/// 可动态修改的 Scheduler 配置。
#[derive(Debug, Clone, Default)]
pub struct SchedulerConfigPatch {
    /// 修改全局并发数。
    pub global_concurrency: Option<NonZeroUsize>,
    /// 修改默认单 Job 并发数。
    pub default_job_concurrency: Option<NonZeroUsize>,
    /// 修改全局 Pending 上限。
    pub global_max_pending: Option<usize>,
    /// 修改默认单 Job Pending 上限。
    pub default_job_max_pending: Option<usize>,
    /// 修改每次处理的到期计时项数量。
    pub due_batch_size: Option<NonZeroUsize>,
    /// 修改最大 Job 数；不会删除已存在 Job，只限制后续新增。
    pub max_jobs: Option<usize>,
    /// 修改优雅关闭超时。
    pub shutdown_timeout: Option<Duration>,
    /// 修改 Interval 最小周期；违反新限制的 Job 会自动停用。
    pub minimum_interval: Option<Duration>,
    /// 修改最小重试等待；违反新限制的 Job 会自动停用。
    pub minimum_retry_delay: Option<Duration>,
    /// 修改 CatchUp 安全上限；违反新限制的 Job 会自动停用。
    pub maximum_catch_up: Option<u32>,
    /// 修改重试总尝试次数上限；违反新限制的 Job 会自动停用。
    pub maximum_retry_attempts: Option<u32>,
}

/// Scheduler 生命周期状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SchedulerStatus {
    /// 正常接受命令并执行 Job。
    Running,
    /// 已停止接受新工作，正在等待运行实例退出。
    Stopping,
    /// Actor 已完全退出。
    Stopped,
    /// Actor 因不可恢复错误退出。
    Failed {
        /// 失败原因摘要。
        message: String,
    },
}

/// 调度管理错误。
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// Actor 或命令通道已经关闭。
    #[error("scheduler is closed")]
    Closed,
    /// Scheduler 正在关闭，不能接受当前操作。
    #[error("scheduler is stopping")]
    Stopping,
    /// 指定 JobId 不存在或已被删除。
    #[error("job `{0}` was not found")]
    JobNotFound(JobId),
    /// Job 的期望版本与当前版本不一致。
    #[error("job `{job_id}` version conflict: expected {expected}, actual {actual}")]
    VersionConflict {
        /// 发生冲突的 Job。
        job_id: JobId,
        /// 调用方读取到并提交的版本。
        expected: u64,
        /// Actor 内当前权威版本。
        actual: u64,
    },
    /// Scheduler 配置的期望 revision 与当前 revision 不一致。
    #[error("scheduler config revision conflict: expected {expected}, actual {actual}")]
    ConfigRevisionConflict {
        /// 调用方提交的配置 revision。
        expected: u64,
        /// Actor 内当前配置 revision。
        actual: u64,
    },
    /// 当前 Job 数已达到配置上限。
    #[error("maximum job count {0} has been reached")]
    MaximumJobsReached(usize),
    /// Trigger 或队列策略组合不合法。
    #[error("invalid trigger: {0}")]
    InvalidTrigger(String),
    /// 重试次数或等待时间违反 Scheduler 安全限制。
    #[error("invalid retry policy: {0}")]
    InvalidRetry(String),
    /// 优先级不在 `0..=10`。
    #[error("priority {0} is outside 0..=10")]
    InvalidPriority(u8),
    /// Job 版本达到 `u64::MAX`，无法继续修改。
    #[error("job `{0}` version overflow")]
    VersionOverflow(JobId),
    /// Scheduler 配置 revision 达到 `u64::MAX`。
    #[error("scheduler config revision overflow")]
    ConfigRevisionOverflow,
    /// Actor 初始化或内部状态错误。
    #[error("scheduler actor failed: {0}")]
    Actor(String),
    /// 等待 Actor Tokio 任务时发生 JoinError。
    #[error("scheduler actor join failed: {0}")]
    Join(String),
}
