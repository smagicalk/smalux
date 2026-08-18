//! Proto Job 定义的校验、类型转换与固定 Task 装配。
//!
//! 本模块只把不可信的协议数据编译为 Scheduler 可接受的强类型对象，
//! 不修改 Scheduler，也不维护远程命令版本和所有权状态。

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use smalux_protocol::agent::v1 as proto;
use uuid::Uuid;

use crate::{
    scheduler::{
        CapacityPolicy, ExecutionRetryPolicy, FailurePolicy, JobId, JobOptions, JobPriority,
        MisfirePolicy, ReportingTask, RetryCondition, Schedule, TaskBinding, TaskReportSink,
        Trigger, TriggerCoalescing,
    },
    tasks::collect::{
        CpuTask, DiskIoTask, HostTask, LoadTask, LocalIpTask, MemoryTask, NetworkIoTask, ProbeTask,
        ProcessTask, PublicIpTask, SocketTask, SystemTask,
    },
};

/// 已通过协议边界校验、可以安装到 Scheduler 的完整 Job。
///
/// “编译”只表示单个定义已经完成解析和类型转换，不会修改 Scheduler。
pub(super) struct CompiledJob {
    /// Server 分配的稳定 Job UUID，同时也是 Scheduler 的主键。
    pub(super) id: JobId,
    /// Server 维护的单 Job 业务版本，随 [`TaskReport`](proto::TaskReport) 上报。
    pub(super) revision: u64,
    /// 安装后是否立即产生新的计划执行。
    pub(super) enabled: bool,
    /// 已解析为 Scheduler 强类型的时间规则和超时策略。
    pub(super) trigger: Trigger,
    /// 已合并默认值并完成范围校验的运行策略。
    pub(super) options: JobOptions,
    /// 已绑定采集实现、业务 revision 和统一结果出口的类型擦除任务。
    pub(super) task: TaskBinding,
}

/// 根据固定 Proto Task 类型创建实现代码，并绑定统一结果出口。
pub(super) struct TaskFactory {
    /// 所有固定采集 Task 共用的报告出口；具体实现可写 Channel、磁盘或网络。
    sink: Arc<dyn TaskReportSink>,
}

impl TaskFactory {
    /// 创建固定 Task 工厂；工厂自身不持有 Scheduler 或连接。
    pub(super) fn new(sink: Arc<dyn TaskReportSink>) -> Self {
        Self { sink }
    }

    /// 将 Proto `oneof task` 映射到唯一的本地实现并完成配置校验。
    ///
    /// 返回的 [`TaskBinding`] 已经擦除具体类型，可以直接存入 Scheduler。
    pub(super) fn build(
        &self,
        definition: &proto::TaskDefinition,
        job_revision: u64,
    ) -> anyhow::Result<TaskBinding> {
        use proto::task_definition::Task;

        // `oneof` 在解码后仍可能为空，因此必须在协议边界显式拒绝。
        let task = definition
            .task
            .clone()
            .ok_or_else(|| anyhow::anyhow!("task definition is required"))?;
        // 每个 Proto 分支只对应一个内置实现，Server 不能指定任意 Rust 类型。
        match task {
            Task::System(config) => Ok(self.bind(SystemTask::with_config(config), job_revision)),
            Task::Cpu(_) => Ok(self.bind(CpuTask::new(), job_revision)),
            Task::Memory(_) => Ok(self.bind(MemoryTask::new(), job_revision)),
            Task::Load(_) => Ok(self.bind(LoadTask::new(), job_revision)),
            Task::Host(_) => Ok(self.bind(HostTask::new(), job_revision)),
            Task::DiskIo(config) => Ok(self.bind(DiskIoTask::with_config(config), job_revision)),
            Task::NetworkIo(config) => {
                Ok(self.bind(NetworkIoTask::with_config(config), job_revision))
            }
            Task::LocalIp(config) => Ok(self.bind(LocalIpTask::with_config(config), job_revision)),
            Task::PublicIp(config) => {
                Ok(self.bind(PublicIpTask::with_config(config)?, job_revision))
            }
            Task::Process(config) => {
                Ok(self.bind(ProcessTask::try_with_config(config)?, job_revision))
            }
            Task::Socket(config) => Ok(self.bind(SocketTask::with_config(config)?, job_revision)),
            Task::Probe(config) => Ok(self.bind(ProbeTask::try_with_config(config)?, job_revision)),
        }
    }

    /// 把具体采集器、业务版本和结果出口封装成 Scheduler 可执行对象。
    fn bind<T: ReportingTask>(&self, task: T, job_revision: u64) -> TaskBinding {
        tracing::debug!(
            task_kind = task.kind(),
            job_revision,
            "agent task binding created"
        );
        TaskBinding::reporting(Arc::new(task), job_revision, self.sink.clone())
    }
}

/// 校验并编译一个完整的远程 Job 定义。
pub(super) fn compile_job(
    definition: &proto::JobDefinition,
    factory: &TaskFactory,
) -> anyhow::Result<CompiledJob> {
    // UUID 与业务 revision 是远程 Job 的身份边界，必须在任何装配前校验。
    let id = parse_uuid(&definition.job_id, "job_id")?;
    anyhow::ensure!(
        definition.revision > 0,
        "job revision must be greater than zero"
    );
    // 必填子消息在 proto3 解码后可能为 None，所以逐层显式检查。
    let trigger = compile_trigger(
        definition
            .trigger
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("job trigger is required"))?,
    )?;
    let options = compile_options(definition.options.as_ref())?;
    let task = factory.build(
        definition
            .task
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("job task is required"))?,
        definition.revision,
    )?;
    // 到这里所有协议值都已变成 Scheduler 可接受的强类型。
    Ok(CompiledJob {
        id,
        revision: definition.revision,
        enabled: definition.enabled,
        trigger,
        options,
        task,
    })
}

/// 解析协议中的固定 16 字节 UUID，并在错误中保留字段名。
pub(super) fn parse_uuid(bytes: &[u8], field: &str) -> anyhow::Result<Uuid> {
    Uuid::from_slice(bytes).map_err(|_| anyhow::anyhow!("{field} must contain a 16-byte UUID"))
}

/// 编译一次、固定间隔或 Cron 触发规则，并校验 Misfire 与超时。
fn compile_trigger(value: &proto::JobTrigger) -> anyhow::Result<Trigger> {
    use proto::job_trigger::Schedule as ProtoSchedule;

    // oneof 保证最多一个分支，但不保证一定存在。
    let schedule = match value.schedule.as_ref() {
        Some(ProtoSchedule::Once(once)) => Schedule::Once {
            at: timestamp(once.at.as_ref(), "once.at")?,
        },
        Some(ProtoSchedule::Interval(interval)) => Schedule::Interval {
            every: duration(interval.every.as_ref(), "interval.every", false)?,
            start_at: interval
                .start_at
                .as_ref()
                .map(|value| timestamp(Some(value), "interval.start_at"))
                .transpose()?,
        },
        Some(ProtoSchedule::Cron(cron)) => Schedule::Cron {
            expression: cron.expression.clone(),
            // 时区使用 chrono-tz 解析；无效 IANA 名称在安装前失败。
            timezone: cron.timezone.parse()?,
        },
        None => anyhow::bail!("job schedule is required"),
    };
    // Misfire 不提供隐式默认值，避免 Agent 猜测 Server 的补执行意图。
    let misfire = value
        .misfire
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("misfire policy is required"))?;
    let behavior = proto::MisfireBehavior::try_from(misfire.behavior)
        .map_err(|_| anyhow::anyhow!("misfire behavior is unknown"))?;
    let misfire = match behavior {
        proto::MisfireBehavior::Unspecified => anyhow::bail!("misfire behavior is unspecified"),
        proto::MisfireBehavior::Skip => MisfirePolicy::Skip,
        proto::MisfireBehavior::FireOnce => MisfirePolicy::FireOnce,
        proto::MisfireBehavior::CatchUp => MisfirePolicy::CatchUp {
            max_runs: NonZeroU32::new(misfire.max_runs)
                .ok_or_else(|| anyhow::anyhow!("catch-up max_runs must be greater than zero"))?,
        },
    };
    Ok(Trigger {
        schedule,
        misfire,
        timeout: value
            .timeout
            .as_ref()
            .map(|value| duration(Some(value), "trigger.timeout", false))
            .transpose()?,
    })
}

/// 编译可选 JobOptions；整段缺失时使用 Scheduler 安全默认值。
fn compile_options(value: Option<&proto::JobOptions>) -> anyhow::Result<JobOptions> {
    let Some(value) = value else {
        return Ok(JobOptions::default());
    };
    // 先复制默认值，再只覆盖协议明确提供的字段。
    let defaults = JobOptions::default();
    let mut options = JobOptions {
        concurrency: value
            .concurrency
            .map(|value| {
                NonZeroUsize::new(value as usize)
                    .ok_or_else(|| anyhow::anyhow!("job concurrency must be greater than zero"))
            })
            .transpose()?,
        max_pending: value.max_pending.map(|value| value as usize),
        ..defaults
    };
    // priority 有明确范围，先完成 u32 -> u8 的无损转换再由领域类型校验。
    if let Some(priority) = value.priority {
        options.priority = JobPriority::new(u8::try_from(priority)?)?;
    }
    // Proto 枚举可能携带未知整数，try_from 会拒绝未来或损坏的值。
    options.coalescing = match proto::TriggerCoalescing::try_from(value.coalescing)? {
        proto::TriggerCoalescing::Unspecified => None,
        proto::TriggerCoalescing::KeepAll => Some(TriggerCoalescing::KeepAll),
        proto::TriggerCoalescing::KeepLatest => Some(TriggerCoalescing::KeepLatest),
    };
    options.capacity = match proto::CapacityPolicy::try_from(value.capacity)? {
        proto::CapacityPolicy::Unspecified => None,
        proto::CapacityPolicy::Backpressure => Some(CapacityPolicy::Backpressure),
        proto::CapacityPolicy::SkipNewest => Some(CapacityPolicy::SkipNewest),
        proto::CapacityPolicy::ReplaceOldestTrigger => Some(CapacityPolicy::ReplaceOldestTrigger),
    };
    if let Some(retry) = value.retry.as_ref() {
        options.retry = compile_retry(retry)?;
    }
    if let Some(failure) = value.failure.as_ref() {
        options.failure = FailurePolicy {
            disable_after_consecutive_failures: failure
                .disable_after_consecutive_failures
                .map(|value| {
                    NonZeroU32::new(value).ok_or_else(|| {
                        anyhow::anyhow!("failure threshold must be greater than zero")
                    })
                })
                .transpose()?,
            count_timeout: failure.count_timeout,
            count_panic: failure.count_panic,
        };
    }
    Ok(options)
}

/// 将 Proto 重试 oneof 转换为 Scheduler 的无重试或指数退避策略。
fn compile_retry(value: &proto::RetryPolicy) -> anyhow::Result<ExecutionRetryPolicy> {
    use proto::retry_policy::Policy;

    match value.policy.as_ref() {
        Some(Policy::NoRetry(_)) => Ok(ExecutionRetryPolicy::None),
        Some(Policy::Exponential(value)) => Ok(ExecutionRetryPolicy::Exponential {
            max_attempts: NonZeroU32::new(value.max_attempts)
                .ok_or_else(|| anyhow::anyhow!("retry max_attempts must be greater than zero"))?,
            initial_delay: duration(value.initial_delay.as_ref(), "retry.initial_delay", true)?,
            max_delay: duration(value.max_delay.as_ref(), "retry.max_delay", false)?,
            retry_on: value
                .retry_on
                .as_ref()
                .map(|value| RetryCondition {
                    transient_error: value.transient_error,
                    timeout: value.timeout,
                    panic: value.panic,
                })
                .unwrap_or_default(),
        }),
        None => anyhow::bail!("retry policy is required when retry is present"),
    }
}

/// 安全转换 Prost Duration，并按调用场景决定是否允许零时长。
fn duration(
    value: Option<&prost_types::Duration>,
    field: &str,
    allow_zero: bool,
) -> anyhow::Result<Duration> {
    let value = value.ok_or_else(|| anyhow::anyhow!("{field} is required"))?;
    // Prost Duration 允许负值，但 Scheduler 的间隔、超时和退避均不允许。
    anyhow::ensure!(
        value.seconds >= 0 && (0..1_000_000_000).contains(&value.nanos),
        "{field} is invalid"
    );
    let value = Duration::new(value.seconds as u64, value.nanos as u32);
    anyhow::ensure!(
        allow_zero || !value.is_zero(),
        "{field} must be greater than zero"
    );
    Ok(value)
}

/// 将 Prost Timestamp 转为 UTC 时间，并拒绝 chrono 无法表示的范围。
fn timestamp(value: Option<&prost_types::Timestamp>, field: &str) -> anyhow::Result<DateTime<Utc>> {
    let value = value.ok_or_else(|| anyhow::anyhow!("{field} is required"))?;
    DateTime::from_timestamp(value.seconds, value.nanos as u32)
        .ok_or_else(|| anyhow::anyhow!("{field} is outside the supported timestamp range"))
}
