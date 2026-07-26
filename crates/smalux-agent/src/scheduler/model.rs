//! Job 触发规则、执行策略、状态快照和部分更新模型。

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;
use uuid::Uuid;

use super::{config::SchedulerError, task::TaskBinding};

/// Job 唯一标识。
pub type JobId = Uuid;

/// 单次逻辑执行标识；重试时保持不变。
pub type RunId = Uuid;

/// Trigger 的具体时间规则。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Schedule {
    /// 在指定 UTC 时间只触发一次；过去时间会在注册后尽快执行。
    Once {
        /// 期望触发的 UTC 墙上时间。
        at: DateTime<Utc>,
    },
    /// 按固定时长重复触发，并保持以起始时间为基准的周期相位。
    Interval {
        /// 两次计划触发之间的固定间隔。
        #[serde(with = "humantime_serde")]
        every: Duration,
        /// 首次计划时间；为 `None` 时从 Job 创建时间开始。
        start_at: Option<DateTime<Utc>>,
    },
    /// 按 Cron 表达式和 IANA 时区计算触发时间。
    Cron {
        /// Cron 表达式，例如 `0 */5 * * * *`。
        expression: String,
        /// 解释 Cron 表达式使用的时区，例如 `Asia/Shanghai`。
        timezone: Tz,
    },
}

/// 错过一个或多个完整周期后的处理方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MisfirePolicy {
    /// 跳过已经错过的周期，只保留下一次未来触发。
    Skip,
    /// 将一批错过的周期合并为一次立即触发。
    FireOnce,
    /// 依次补执行有限数量的错过周期。
    CatchUp {
        /// 单次恢复最多生成的执行次数，同时受 Scheduler 全局上限约束。
        max_runs: NonZeroU32,
    },
}

/// 调度触发器。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trigger {
    /// 具体时间规则。
    pub schedule: Schedule,
    /// Scheduler 晚于计划时间唤醒时的补偿策略。
    pub misfire: MisfirePolicy,
    /// 单次 Task 执行上限；`None` 表示不由 Scheduler 设置超时。
    #[serde(with = "humantime_serde::option")]
    pub timeout: Option<Duration>,
}

impl Trigger {
    /// 创建一次性 Trigger，默认超时 30 秒。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// let trigger = Trigger::once(Utc::now() + TimeDelta::seconds(10));
    /// ```
    pub fn once(at: DateTime<Utc>) -> Self {
        Self {
            schedule: Schedule::Once { at },
            misfire: MisfirePolicy::FireOnce,
            timeout: Some(Duration::from_secs(30)),
        }
    }

    /// 创建固定周期 Trigger，默认使用 [`MisfirePolicy::Skip`] 和 30 秒超时。
    ///
    /// `every` 仍会在 Job 注册时按
    /// [`SchedulerConfig::minimum_interval`](crate::scheduler::SchedulerConfig::minimum_interval) 校验。
    pub fn interval(every: Duration) -> Self {
        Self {
            schedule: Schedule::Interval {
                every,
                start_at: None,
            },
            misfire: MisfirePolicy::Skip,
            timeout: Some(Duration::from_secs(30)),
        }
    }

    /// 创建 UTC Cron Trigger，默认跳过错过周期并设置 30 秒超时。
    ///
    /// 可继续调用 [`Trigger::with_timezone`] 修改 Cron 时区。
    pub fn cron(expression: impl Into<String>) -> Self {
        Self {
            schedule: Schedule::Cron {
                expression: expression.into(),
                timezone: chrono_tz::UTC,
            },
            misfire: MisfirePolicy::Skip,
            timeout: Some(Duration::from_secs(30)),
        }
    }

    /// 覆盖单次执行超时；传入 `None` 可关闭 Scheduler 超时。
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// 修改 Cron Trigger 的时区。
    ///
    /// 对 Once 或 Interval 调用时保持原值不变，便于链式构造。
    pub fn with_timezone(mut self, timezone: Tz) -> Self {
        if let Schedule::Cron {
            timezone: current, ..
        } = &mut self.schedule
        {
            *current = timezone;
        }
        self
    }
}

/// Job 优先级，数值越大越优先。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct JobPriority(u8);

impl JobPriority {
    /// 最低优先级。
    pub const LOWEST: Self = Self(0);
    /// 低优先级。
    pub const LOW: Self = Self(3);
    /// 默认优先级。
    pub const NORMAL: Self = Self(5);
    /// 高优先级。
    pub const HIGH: Self = Self(7);
    /// 最高优先级。
    pub const CRITICAL: Self = Self(10);

    /// 从 `0..=10` 创建优先级，超出范围返回 [`SchedulerError::InvalidPriority`]。
    pub fn new(value: u8) -> Result<Self, SchedulerError> {
        (value <= 10)
            .then_some(Self(value))
            .ok_or(SchedulerError::InvalidPriority(value))
    }

    /// 返回用于排序和序列化的原始优先级数值。
    pub const fn get(self) -> u8 {
        self.0
    }
}

impl Default for JobPriority {
    /// 默认使用 NORMAL，兼顾普通采集任务的公平性。
    fn default() -> Self {
        Self::NORMAL
    }
}

impl TryFrom<u8> for JobPriority {
    type Error = SchedulerError;

    /// 校验原始数值并转换为 JobPriority。
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<JobPriority> for u8 {
    /// 提取用于存储或协议传输的原始数值。
    fn from(value: JobPriority) -> Self {
        value.0
    }
}

/// 正常周期 Trigger 在执行队列中的合并规则。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerCoalescing {
    /// 保留每个正常 Trigger，适合不能丢失采样或 CatchUp 的任务。
    KeepAll,
    /// 尚未执行时只保留最新正常 Trigger，降低慢任务的积压。
    KeepLatest,
}

/// Pending 达到容量上限后的处理规则。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityPolicy {
    /// 暂停生成后续周期，等待执行队列释放容量。
    Backpressure,
    /// 容量不足时丢弃本次最新 Trigger。
    SkipNewest,
    /// 用本次 Trigger 替换同 Job 最旧的可替换正常 Trigger。
    ReplaceOldestTrigger,
}

/// 可重试的执行结果类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryCondition {
    /// 是否重试 [`TaskError::Transient`](crate::scheduler::TaskError::Transient)。
    pub transient_error: bool,
    /// 是否重试执行超时。
    pub timeout: bool,
    /// 是否重试 Task panic；默认关闭，避免重复执行存在程序缺陷的代码。
    pub panic: bool,
}

impl Default for RetryCondition {
    /// 默认重试临时错误和超时，不自动重试 panic。
    fn default() -> Self {
        Self {
            transient_error: true,
            timeout: true,
            panic: false,
        }
    }
}

/// Task 执行阶段的重试策略。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionRetryPolicy {
    /// 不在同一逻辑执行内重试。
    #[default]
    None,
    /// 使用有上限的指数退避重试。
    Exponential {
        /// 包含首次执行在内的最大尝试次数。
        max_attempts: NonZeroU32,
        /// 第一次重试前的等待时间。
        #[serde(with = "humantime_serde")]
        initial_delay: Duration,
        /// 指数增长后的最大等待时间。
        #[serde(with = "humantime_serde")]
        max_delay: Duration,
        /// 哪些失败类型允许进入重试。
        retry_on: RetryCondition,
    },
}

/// 连续失败保护策略。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailurePolicy {
    /// 连续最终失败达到该次数后自动停用；`None` 表示永不按次数停用。
    pub disable_after_consecutive_failures: Option<NonZeroU32>,
    /// 超时在耗尽重试后是否计入连续失败。
    pub count_timeout: bool,
    /// panic 在耗尽重试后是否计入连续失败。
    pub count_panic: bool,
}

impl Default for FailurePolicy {
    /// 默认连续 5 次最终失败后停用，并统计超时与 panic。
    fn default() -> Self {
        Self {
            disable_after_consecutive_failures: NonZeroU32::new(5),
            count_timeout: true,
            count_panic: true,
        }
    }
}

/// Job 创建时的执行参数。
#[derive(Debug, Clone)]
pub struct JobOptions {
    /// 单 Job 最大并发；`None` 继承 Scheduler 默认值。
    pub concurrency: Option<NonZeroUsize>,
    /// 单 Job 最大待执行数量；`None` 继承 Scheduler 默认值。
    pub max_pending: Option<usize>,
    /// 同一到期批次中的执行优先级。
    pub priority: JobPriority,
    /// 正常 Trigger 合并策略；`None` 根据 Trigger 类型选择安全默认值。
    pub coalescing: Option<TriggerCoalescing>,
    /// 队列容量不足时的策略；`None` 根据 Trigger 类型选择安全默认值。
    pub capacity: Option<CapacityPolicy>,
    /// Task 执行失败后的重试策略。
    pub retry: ExecutionRetryPolicy,
    /// 最终失败累计和自动停用策略。
    pub failure: FailurePolicy,
}

impl Default for JobOptions {
    /// 默认继承 Scheduler 限制，使用普通优先级且不执行 Retry。
    fn default() -> Self {
        Self {
            concurrency: None,
            max_pending: None,
            priority: JobPriority::NORMAL,
            coalescing: None,
            capacity: None,
            retry: ExecutionRetryPolicy::None,
            failure: FailurePolicy::default(),
        }
    }
}

/// 可继承配置字段的 Patch 语义。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum PatchValue<T> {
    /// 保持 Job 当前值。
    #[default]
    Keep,
    /// 设置 Job 专属值，覆盖 Scheduler 默认值。
    Set(T),
    /// 清除 Job 专属值，恢复继承 Scheduler 默认值。
    Inherit,
}

/// 修改后如何处理正常 Trigger 的下次时间。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RescheduleMode {
    /// 尽量保留当前下一次计划时间；修改 Trigger 时自动转为 Recalculate。
    #[default]
    Preserve,
    /// 从当前时间和新 Trigger 重新计算下一次计划时间。
    Recalculate,
    /// 更新成功后立即触发一次，再按 Trigger 计算后续周期。
    RunNow,
}

/// Job 部分更新。
#[derive(Default)]
pub struct JobPatch {
    /// 替换 Task 注册；`None` 保持原 Task。
    pub task: Option<TaskBinding>,
    /// 替换 Trigger；`None` 保持原 Trigger。
    pub trigger: Option<Trigger>,
    /// 修改或恢复继承单 Job 并发限制。
    pub concurrency: PatchValue<NonZeroUsize>,
    /// 修改或恢复继承单 Job Pending 限制。
    pub max_pending: PatchValue<usize>,
    /// 替换优先级；`None` 保持原值。
    pub priority: Option<JobPriority>,
    /// 替换 Trigger 合并策略；`None` 保持原值。
    pub coalescing: Option<TriggerCoalescing>,
    /// 替换容量策略；`None` 保持原值。
    pub capacity: Option<CapacityPolicy>,
    /// 替换执行重试策略；`None` 保持原值。
    pub retry: Option<ExecutionRetryPolicy>,
    /// 替换连续失败策略；`None` 保持原值。
    pub failure: Option<FailurePolicy>,
    /// 本次更新如何处理下一次正常 Trigger。
    pub reschedule: RescheduleMode,
}

impl JobPatch {
    /// 创建所有字段均为“保持当前值”的空 Patch。
    ///
    /// # 示例
    ///
    /// ```ignore
    /// let mut patch = JobPatch::new();
    /// patch.priority = Some(JobPriority::HIGH);
    /// patch.concurrency = PatchValue::Set(NonZeroUsize::new(2).unwrap());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }
}

/// Job 当前生命周期状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JobState {
    /// 可继续接收 Trigger 并执行。
    Enabled,
    /// 一次性 Job 已完成，不再自动执行；可通过 enable 再次运行。
    Completed,
    /// 被用户或失败保护停用。
    Disabled {
        /// 便于运维和事件追踪的停用原因。
        reason: String,
    },
}

/// 对外提供的 Job 一致性快照。
#[derive(Debug, Clone, Serialize)]
pub struct JobSnapshot {
    /// Job 唯一标识。
    pub id: JobId,
    /// Job 当前版本，用于后续 update/enable/disable/delete 的乐观锁。
    pub version: u64,
    /// Task 提供的稳定类型名称，便于诊断当前注册内容。
    pub task_kind: String,
    /// 当前生命周期状态。
    pub state: JobState,
    /// 当前 Trigger 配置。
    pub trigger: Trigger,
    /// 当前有效优先级。
    pub priority: JobPriority,
    /// 展开继承后实际生效的单 Job 并发数。
    pub concurrency: usize,
    /// 展开继承后实际生效的单 Job Pending 上限。
    pub max_pending: usize,
    /// 当前正在执行的实例数，包括等待旧版本结束的实例。
    pub running_count: usize,
    /// ReadyQueue 与 Backpressure 暂存区中的待执行实例总数。
    pub pending_count: usize,
    /// 当前连续最终失败次数；成功后重置为零。
    pub consecutive_failures: u32,
    /// 下一次正常 Trigger 时间；停用、完成或阻塞时可能为 `None`。
    pub next_run_at: Option<DateTime<Utc>>,
    /// 最近一次开始执行的时间。
    pub last_started_at: Option<DateTime<Utc>>,
    /// 最近一次结束执行的时间。
    pub last_finished_at: Option<DateTime<Utc>>,
    /// 最近一次最终结果摘要，成功时为 `success`，失败时为错误文本。
    pub last_outcome: Option<String>,
    /// Job 创建时间。
    pub created_at: DateTime<Utc>,
    /// Job 配置或状态最近更新时间。
    pub updated_at: DateTime<Utc>,
}
