//! Proto Job 定义到 Agent 调度运行时的校验、装配与命令执行。
//!
//! 该模块位于“网络协议”和“本地 Scheduler”之间，负责四件事：
//!
//! 1. 把 Server 下发的 [`proto::JobCommand`] 作为幂等命令处理；
//! 2. 把不可信的 Proto 字段校验并转换为 Scheduler 的强类型配置；
//! 3. 只维护远程 Job 的所有权索引，不接管调用方创建的本地 Job；
//! 4. 把采集结果交给调用方提供的 [`TaskReportSink`]，本模块不决定网络或持久化方式。
//!
//! # 版本模型
//!
//! - `catalog_revision` 是 Server 侧远程 Job 集合的顺序号，变更命令必须严格 `+1`；
//! - `JobDefinition.revision` 是单个 Job 的业务配置版本，更新时必须递增；
//! - `JobSnapshot.version` 是 Scheduler 内部的并发控制 generation，每次修改都会变化。
//!
//! 三者不能互换。上报使用业务 `revision`，Scheduler 更新使用内部 `generation`。

use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use smalux_protocol::agent::v1 as proto;
use uuid::Uuid;

use crate::{
    scheduler::{
        CapacityPolicy, ExecutionRetryPolicy, FailurePolicy, JobId, JobOptions, JobPatch,
        JobPriority, JobSnapshot, JobState, MisfirePolicy, PatchValue, ReportingTask,
        RescheduleMode, RetryCondition, Schedule, Scheduler, TaskBinding, TaskReportSink, Trigger,
        TriggerCoalescing,
    },
    tasks::collect::{
        CpuTask, DiskIoTask, HostTask, LoadTask, LocalIpTask, MemoryTask, NetworkIoTask, ProbeTask,
        ProcessTask, PublicIpTask, SocketTask, SystemTask,
    },
};

/// 已通过协议边界校验、可以安装到 Scheduler 的完整 Job。
///
/// “编译”只表示单个定义已经完成解析和类型转换，不会修改 Scheduler。
pub(crate) struct CompiledJob {
    /// Server 分配的稳定 Job UUID，同时也是 Scheduler 的主键。
    pub(crate) id: JobId,
    /// Server 维护的单 Job 业务版本，随 [`TaskReport`](proto::TaskReport) 上报。
    pub(crate) revision: u64,
    /// 安装后是否立即产生新的计划执行。
    pub(crate) enabled: bool,
    /// 已解析为 Scheduler 强类型的时间规则和超时策略。
    pub(crate) trigger: Trigger,
    /// 已合并默认值并完成范围校验的运行策略。
    pub(crate) options: JobOptions,
    /// 已绑定采集实现、业务 revision 和统一结果出口的类型擦除任务。
    pub(crate) task: TaskBinding,
}

/// 根据固定 Proto Task 类型创建实现代码，并绑定统一结果出口。
pub(crate) struct TaskFactory {
    /// 所有固定采集 Task 共用的报告出口；具体实现可写 Channel、磁盘或网络。
    sink: Arc<dyn TaskReportSink>,
}

impl TaskFactory {
    /// 创建固定 Task 工厂；工厂自身不持有 Scheduler 或连接。
    pub(crate) fn new(sink: Arc<dyn TaskReportSink>) -> Self {
        Self { sink }
    }

    /// 将 Proto `oneof task` 映射到唯一的本地实现并完成配置校验。
    ///
    /// 返回的 [`TaskBinding`] 已经擦除具体类型，可以直接存入 Scheduler。
    pub(crate) fn build(
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

#[derive(Debug, Clone, Copy)]
struct ManagedJob {
    /// 最近成功应用的 Server 业务版本。
    revision: u64,
    /// Scheduler 当前 generation，用于乐观并发更新和删除。
    generation: u64,
}

/// 控制器在内存中维护的远程所有权和命令幂等状态。
#[derive(Default)]
struct ControllerState {
    /// 最近成功应用的远程 Job 集合版本。
    catalog_revision: u64,
    /// 仅包含本控制器安装的远程 Job，因此 [`JobController::clear`] 不会误删本地 Job。
    remote_jobs: HashMap<JobId, ManagedJob>,
    /// 按 `command_id` 保存首次执行结果，用于处理重发和断线重连。
    cached_results: HashMap<Uuid, proto::JobCommandResult>,
    /// 记录缓存插入顺序，以便在容量固定时淘汰最旧结果。
    cache_order: VecDeque<Uuid>,
}

/// 应用 Server 下发的 Proto Job 命令；连接、重连和本地持久化由调用方负责。
pub struct JobController {
    /// 本地调度器句柄；命令最终都通过该公开接口生效。
    scheduler: Scheduler,
    /// 把 Proto Task 配置转换为固定采集实现。
    factory: TaskFactory,
    /// 串行化远程命令，保证 revision 检查和状态更新之间不会交错。
    state: tokio::sync::Mutex<ControllerState>,
}

impl JobController {
    /// 创建只管理“远程所有权”Job 的控制器。
    pub fn new(scheduler: Scheduler, sink: Arc<dyn TaskReportSink>) -> Self {
        Self {
            scheduler,
            factory: TaskFactory::new(sink),
            state: tokio::sync::Mutex::new(ControllerState::default()),
        }
    }

    /// 幂等应用一条命令；相同 command_id 返回首次结果，不重复 RunNow。
    pub async fn apply(&self, command: proto::JobCommand) -> proto::JobCommandResult {
        // command_id 是幂等键；无效 UUID 无法可靠去重，所以直接拒绝且不缓存。
        let command_id = match parse_uuid(&command.command_id, "command_id") {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "agent rejected job command with invalid command id"
                );
                return command_error(
                    &command.command_id,
                    0,
                    proto::JobCommandErrorCode::InvalidCommand,
                    error,
                );
            }
        };
        // 锁覆盖一次完整命令的校验、Scheduler 调用和本地索引更新。
        let mut state = self.state.lock().await;
        let action = command_action_name(command.action.as_ref());
        // Server 重发相同 command_id 时返回首次结果，尤其避免 RunNow 被执行两次。
        if let Some(result) = state.cached_results.get(&command_id) {
            tracing::debug!(
                command_id = %command_id,
                action,
                "agent reused cached job command result"
            );
            return result.clone();
        }
        tracing::debug!(
            command_id = %command_id,
            action,
            catalog_revision = state.catalog_revision,
            "agent applying job command"
        );
        // 首次命令在锁内执行，完成后无论成功或失败都缓存结构化结果。
        let result = self.apply_locked(&mut state, &command).await;
        cache_result(&mut state, command_id, result.clone());
        if result.status == proto::JobCommandStatus::Applied as i32 {
            tracing::info!(
                command_id = %command_id,
                action,
                catalog_revision = result.catalog_revision,
                "agent job command applied"
            );
        } else {
            tracing::warn!(
                command_id = %command_id,
                action,
                catalog_revision = result.catalog_revision,
                error_code = result.error.as_ref().map(|error| error.code),
                error_message = ?result.error.as_ref().map(|error| error.message.as_str()),
                "agent job command rejected"
            );
        }
        result
    }

    /// 删除全部远程所有权 Job；本地 Job 不在控制器索引中，因此不会受影响。
    pub async fn clear(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().await;
        tracing::info!(
            remote_jobs = state.remote_jobs.len(),
            "clearing agent remote jobs"
        );
        // 克隆小型所有权索引，避免遍历时直接修改原 HashMap。
        let jobs = state.remote_jobs.clone();
        for (job_id, managed) in jobs {
            match self.scheduler.delete(job_id, managed.generation).await {
                Ok(()) => {
                    tracing::debug!(
                        job_id = %job_id,
                        generation = managed.generation,
                        "agent cleared remote job"
                    );
                    state.remote_jobs.remove(&job_id);
                }
                Err(error) => {
                    tracing::error!(
                        job_id = %job_id,
                        generation = managed.generation,
                        error = %error,
                        "agent failed to clear remote job"
                    );
                    return Err(error.into());
                }
            }
        }
        // clear 表示丢弃当前远程目录；下一次同步应从 catalog revision 1 开始。
        state.catalog_revision = 0;
        tracing::info!("agent remote jobs cleared");
        Ok(())
    }

    async fn apply_locked(
        &self,
        state: &mut ControllerState,
        command: &proto::JobCommand,
    ) -> proto::JobCommandResult {
        use proto::job_command::Action;
        // oneof 可能为空；每个具体处理函数只返回领域结果，不重复构造协议响应。
        let result = match command.action.as_ref() {
            Some(Action::Upsert(value)) => self.upsert(state, value).await,
            Some(Action::Delete(value)) => self.delete(state, value).await,
            Some(Action::RunNow(value)) => self.run_now(state, value).await,
            Some(Action::ReplaceAll(value)) => self.replace_all(state, value).await,
            None => Err((
                proto::JobCommandErrorCode::InvalidCommand,
                anyhow::anyhow!("job command action is required"),
            )),
        };
        // 将所有分支统一转换为可直接发回 Server 的 JobCommandResult。
        match result {
            Ok(status) => proto::JobCommandResult {
                command_id: command.command_id.clone(),
                status: proto::JobCommandStatus::Applied as i32,
                catalog_revision: state.catalog_revision,
                job: status,
                error: None,
            },
            Err((code, error)) => {
                command_error(&command.command_id, state.catalog_revision, code, error)
            }
        }
    }

    async fn upsert(
        &self,
        state: &mut ControllerState,
        command: &proto::UpsertJob,
    ) -> ControlResult<Option<proto::JobStatus>> {
        // 先检查集合顺序，防止漏命令或乱序命令覆盖较新的本地状态。
        require_next_catalog(state, command.catalog_revision)?;
        let definition = command
            .job
            .as_ref()
            .ok_or_else(|| invalid_job("upsert job is required"))?;
        // 编译阶段不修改 Scheduler，因此无效定义不会留下半安装 Job。
        let compiled = match compile_job(definition, &self.factory) {
            Ok(compiled) => compiled,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    catalog_revision = command.catalog_revision,
                    "agent rejected invalid job definition"
                );
                return Err((proto::JobCommandErrorCode::InvalidJob, error));
            }
        };
        let status = self.upsert_compiled(state, compiled, true).await?;
        // 只有 Scheduler 已成功更新后才推进目录版本。
        state.catalog_revision = command.catalog_revision;
        Ok(Some(status))
    }

    async fn upsert_compiled(
        &self,
        state: &mut ControllerState,
        compiled: CompiledJob,
        require_increase: bool,
    ) -> ControlResult<proto::JobStatus> {
        // 是否存在于 remote_jobs 同时决定更新路径和所有权；本地同 ID Job 不会被接管。
        let snapshot = if let Some(current) = state.remote_jobs.get(&compiled.id).copied() {
            if compiled.revision < current.revision
                || (require_increase && compiled.revision == current.revision)
            {
                return Err((
                    proto::JobCommandErrorCode::RevisionConflict,
                    anyhow::anyhow!("job revision must increase"),
                ));
            }
            // 使用完整 Patch 替换远程配置，并用 generation 防止并发覆盖。
            let mut snapshot = self
                .scheduler
                .update(
                    compiled.id,
                    current.generation,
                    full_patch(compiled.trigger, compiled.task, compiled.options),
                )
                .await
                .map_err(scheduler_error)?;
            snapshot = set_enabled(&self.scheduler, snapshot, compiled.enabled)
                .await
                .map_err(scheduler_error)?;
            snapshot
        } else {
            // 新 Job 从 generation 1 安装；若 ID 已被本地 Job 占用，Scheduler 返回所有权冲突。
            self.scheduler
                .install(
                    compiled.id,
                    1,
                    compiled.enabled,
                    compiled.trigger,
                    compiled.task,
                    compiled.options,
                )
                .await
                .map_err(scheduler_error)?;
            self.scheduler
                .get(compiled.id)
                .await
                .map_err(scheduler_error)?
                .ok_or_else(|| {
                    (
                        proto::JobCommandErrorCode::SchedulerUnavailable,
                        anyhow::anyhow!("installed job is missing"),
                    )
                })?
        };
        // Scheduler 成功后才更新业务 revision 与最新 generation 的映射。
        state.remote_jobs.insert(
            compiled.id,
            ManagedJob {
                revision: compiled.revision,
                generation: snapshot.version,
            },
        );
        Ok(job_status(&snapshot, compiled.revision))
    }

    async fn delete(
        &self,
        state: &mut ControllerState,
        command: &proto::DeleteJob,
    ) -> ControlResult<Option<proto::JobStatus>> {
        require_next_catalog(state, command.catalog_revision)?;
        let id = parse_uuid(&command.job_id, "job_id")
            .map_err(|error| (proto::JobCommandErrorCode::InvalidCommand, error))?;
        // 只允许删除远程所有权索引中的 Job，避免误删本地任务。
        let current = state.remote_jobs.get(&id).copied().ok_or_else(|| {
            (
                proto::JobCommandErrorCode::NotFound,
                anyhow::anyhow!("remote job was not found"),
            )
        })?;
        // expected_revision 是业务层乐观锁，不是 Scheduler generation。
        if command.expected_revision != current.revision {
            return Err((
                proto::JobCommandErrorCode::RevisionConflict,
                anyhow::anyhow!("expected revision does not match"),
            ));
        }
        self.scheduler
            .delete(id, current.generation)
            .await
            .map_err(scheduler_error)?;
        state.remote_jobs.remove(&id);
        // 删除成功后再推进目录版本；失败时 Server 可重试同一命令。
        state.catalog_revision = command.catalog_revision;
        Ok(None)
    }

    async fn run_now(
        &self,
        state: &mut ControllerState,
        command: &proto::RunJobNow,
    ) -> ControlResult<Option<proto::JobStatus>> {
        let id = parse_uuid(&command.job_id, "job_id")
            .map_err(|error| (proto::JobCommandErrorCode::InvalidCommand, error))?;
        let current = state.remote_jobs.get(&id).copied().ok_or_else(|| {
            (
                proto::JobCommandErrorCode::NotFound,
                anyhow::anyhow!("remote job was not found"),
            )
        })?;
        if command.expected_revision != current.revision {
            return Err((
                proto::JobCommandErrorCode::RevisionConflict,
                anyhow::anyhow!("expected revision does not match"),
            ));
        }
        // RunNow 通过 Scheduler Patch 注入一次立即触发，不修改原周期相位。
        let mut patch = JobPatch::new();
        patch.reschedule = RescheduleMode::RunNow;
        let snapshot = self
            .scheduler
            .update(id, current.generation, patch)
            .await
            .map_err(scheduler_error)?;
        // RunNow 不改变业务 revision，但 Scheduler generation 会递增。
        state.remote_jobs.insert(
            id,
            ManagedJob {
                revision: current.revision,
                generation: snapshot.version,
            },
        );
        Ok(Some(job_status(&snapshot, current.revision)))
    }

    async fn replace_all(
        &self,
        state: &mut ControllerState,
        command: &proto::ReplaceAllJobs,
    ) -> ControlResult<Option<proto::JobStatus>> {
        require_next_catalog(state, command.catalog_revision)?;
        // 第一阶段只解析和校验全部定义，避免普通配置错误导致部分应用。
        let mut ids = HashSet::new();
        let mut compiled_jobs = Vec::with_capacity(command.jobs.len());
        for job in &command.jobs {
            let id = parse_uuid(&job.job_id, "job_id")
                .map_err(|error| (proto::JobCommandErrorCode::InvalidJob, error))?;
            if !ids.insert(id) {
                return Err((
                    proto::JobCommandErrorCode::InvalidJob,
                    anyhow::anyhow!("replace_all contains duplicate job_id"),
                ));
            }
            compiled_jobs.push(match compile_job(job, &self.factory) {
                Ok(compiled) => compiled,
                Err(error) => {
                    tracing::warn!(error = %error, "agent rejected invalid job in replace-all");
                    return Err((proto::JobCommandErrorCode::InvalidJob, error));
                }
            });
        }
        // 第二阶段逐个写入 Scheduler；单个写入原子，但多个 Job 不是跨 Job 事务。
        for compiled in compiled_jobs {
            self.upsert_compiled(state, compiled, false).await?;
        }
        // 完整集合中不存在的远程 Job 属于陈旧项，需要在成功 upsert 后删除。
        let stale = state
            .remote_jobs
            .keys()
            .filter(|id| !ids.contains(id))
            .copied()
            .collect::<Vec<_>>();
        for id in stale {
            let managed = state.remote_jobs[&id];
            self.scheduler
                .delete(id, managed.generation)
                .await
                .map_err(scheduler_error)?;
            state.remote_jobs.remove(&id);
        }
        // 所有新增、更新和删除都成功后，ReplaceAll 才算应用完成。
        state.catalog_revision = command.catalog_revision;
        Ok(None)
    }
}

type ControlResult<T> = Result<T, (proto::JobCommandErrorCode, anyhow::Error)>;

/// 返回 Job 命令的稳定动作名称，日志不记录完整 Proto 内容。
fn command_action_name(action: Option<&proto::job_command::Action>) -> &'static str {
    match action {
        Some(proto::job_command::Action::Upsert(_)) => "upsert",
        Some(proto::job_command::Action::Delete(_)) => "delete",
        Some(proto::job_command::Action::RunNow(_)) => "run_now",
        Some(proto::job_command::Action::ReplaceAll(_)) => "replace_all",
        None => "missing",
    }
}

/// 要求集合变更命令连续到达，发现丢包或乱序时让 Server 重新同步。
fn require_next_catalog(state: &ControllerState, revision: u64) -> ControlResult<()> {
    if revision != state.catalog_revision + 1 {
        return Err((
            proto::JobCommandErrorCode::RevisionConflict,
            anyhow::anyhow!("catalog revision must be exactly current + 1"),
        ));
    }
    Ok(())
}

/// 将完整 [`JobOptions`] 展开为 Scheduler 的全量替换 Patch。
///
/// `None` 表示继承 Scheduler 默认值，因此要用 [`PatchValue::Inherit`] 明确清除旧覆盖值。
fn full_patch(trigger: Trigger, task: TaskBinding, options: JobOptions) -> JobPatch {
    JobPatch {
        task: Some(task),
        trigger: Some(trigger),
        concurrency: options
            .concurrency
            .map(PatchValue::Set)
            .unwrap_or(PatchValue::Inherit),
        max_pending: options
            .max_pending
            .map(PatchValue::Set)
            .unwrap_or(PatchValue::Inherit),
        priority: Some(options.priority),
        coalescing: options.coalescing,
        capacity: options.capacity,
        retry: Some(options.retry),
        failure: Some(options.failure),
        reschedule: RescheduleMode::Recalculate,
    }
}

/// 将 Job 调整到定义声明的启用状态，并返回变更后的最新 generation。
async fn set_enabled(
    scheduler: &Scheduler,
    snapshot: JobSnapshot,
    enabled: bool,
) -> Result<JobSnapshot, crate::scheduler::SchedulerError> {
    // 状态已经满足要求时不发送无意义更新，避免 generation 平白递增。
    match (enabled, &snapshot.state) {
        (true, JobState::Enabled) | (false, JobState::Disabled { .. }) => Ok(snapshot),
        (true, _) => scheduler.enable(snapshot.id, snapshot.version).await,
        (false, _) => {
            scheduler
                .disable(snapshot.id, snapshot.version, "disabled by job definition")
                .await
        }
    }
}

/// 保存命令首次结果，并限制缓存大小，防止长时间运行后无限占用内存。
fn cache_result(state: &mut ControllerState, id: Uuid, result: proto::JobCommandResult) {
    // 该值只限制进程内幂等窗口；跨重启幂等需要调用方把结果持久化。
    const CACHE_CAPACITY: usize = 1_024;
    state.cached_results.insert(id, result);
    state.cache_order.push_back(id);
    while state.cache_order.len() > CACHE_CAPACITY {
        if let Some(expired) = state.cache_order.pop_front() {
            state.cached_results.remove(&expired);
        }
    }
}

/// 把 Scheduler 内部错误归类为稳定的协议错误码，同时保留原始错误链。
fn scheduler_error(
    error: crate::scheduler::SchedulerError,
) -> (proto::JobCommandErrorCode, anyhow::Error) {
    let code = match error {
        crate::scheduler::SchedulerError::JobNotFound(_) => proto::JobCommandErrorCode::NotFound,
        crate::scheduler::SchedulerError::JobAlreadyExists(_) => {
            proto::JobCommandErrorCode::OwnershipConflict
        }
        crate::scheduler::SchedulerError::VersionConflict { .. } => {
            proto::JobCommandErrorCode::RevisionConflict
        }
        crate::scheduler::SchedulerError::MaximumJobsReached(_) => {
            proto::JobCommandErrorCode::CapacityExceeded
        }
        _ => proto::JobCommandErrorCode::SchedulerUnavailable,
    };
    (code, anyhow::Error::new(error))
}

/// 构造“Job 定义无效”的控制器内部错误，减少调用点的重复元组代码。
fn invalid_job(message: &str) -> (proto::JobCommandErrorCode, anyhow::Error) {
    (
        proto::JobCommandErrorCode::InvalidJob,
        anyhow::anyhow!(message.to_owned()),
    )
}

/// 将控制器错误包装成 Server 可关联到原命令的拒绝响应。
fn command_error(
    command_id: &[u8],
    catalog_revision: u64,
    code: proto::JobCommandErrorCode,
    error: anyhow::Error,
) -> proto::JobCommandResult {
    proto::JobCommandResult {
        command_id: command_id.to_vec(),
        status: proto::JobCommandStatus::Rejected as i32,
        catalog_revision,
        job: None,
        error: Some(proto::JobCommandError {
            code: code as i32,
            message: format!("{error:#}"),
        }),
    }
}

/// 把 Scheduler 快照转换成不暴露内部 Task 实例的协议状态。
fn job_status(snapshot: &JobSnapshot, revision: u64) -> proto::JobStatus {
    // Proto 将 Disabled 原因拆成独立字符串，其他状态使用空字符串。
    let (state, disabled_reason) = match &snapshot.state {
        JobState::Enabled => (proto::JobRuntimeState::Enabled, String::new()),
        JobState::Completed => (proto::JobRuntimeState::Completed, String::new()),
        JobState::Disabled { reason } => (proto::JobRuntimeState::Disabled, reason.clone()),
    };
    proto::JobStatus {
        job_id: snapshot.id.as_bytes().to_vec(),
        revision,
        task_kind: snapshot.task_kind.clone(),
        state: state as i32,
        disabled_reason,
        // Scheduler 使用 usize，协议使用 u32；极端平台值按协议最大值饱和上报。
        concurrency: snapshot.concurrency.try_into().unwrap_or(u32::MAX),
        max_pending: snapshot.max_pending.try_into().unwrap_or(u32::MAX),
        running_count: snapshot.running_count.try_into().unwrap_or(u32::MAX),
        pending_count: snapshot.pending_count.try_into().unwrap_or(u32::MAX),
        consecutive_failures: snapshot.consecutive_failures,
        next_run_at: snapshot.next_run_at.map(timestamp_proto),
        last_started_at: snapshot.last_started_at.map(timestamp_proto),
        last_finished_at: snapshot.last_finished_at.map(timestamp_proto),
        last_outcome: snapshot.last_outcome.clone().unwrap_or_default(),
        created_at: Some(timestamp_proto(snapshot.created_at)),
        updated_at: Some(timestamp_proto(snapshot.updated_at)),
    }
}

/// 将 chrono UTC 时间无损转换为 Prost Timestamp。
fn timestamp_proto(value: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)] // 测试与控制器相邻，后续私有 Proto 编译函数仍由该测试覆盖。
mod tests {
    use super::*;
    use crate::scheduler::{CallbackError, SchedulerConfig, SchedulerRuntime};

    fn definition(id: Uuid, revision: u64) -> proto::JobDefinition {
        proto::JobDefinition {
            job_id: id.as_bytes().to_vec(),
            revision,
            enabled: true,
            trigger: Some(proto::JobTrigger {
                timeout: None,
                misfire: Some(proto::MisfirePolicy {
                    behavior: proto::MisfireBehavior::Skip as i32,
                    max_runs: 0,
                }),
                schedule: Some(proto::job_trigger::Schedule::Interval(
                    proto::IntervalSchedule {
                        every: Some(prost_types::Duration {
                            seconds: 60,
                            nanos: 0,
                        }),
                        start_at: None,
                    },
                )),
            }),
            options: None,
            task: Some(proto::TaskDefinition {
                task: Some(proto::task_definition::Task::Cpu(proto::CpuTaskConfig {})),
            }),
        }
    }

    fn command(
        command_id: Uuid,
        catalog_revision: u64,
        job: proto::JobDefinition,
    ) -> proto::JobCommand {
        proto::JobCommand {
            command_id: command_id.as_bytes().to_vec(),
            action: Some(proto::job_command::Action::Upsert(Box::new(
                proto::UpsertJob {
                    catalog_revision,
                    job: Some(job),
                },
            ))),
        }
    }

    fn sink() -> Arc<dyn TaskReportSink> {
        Arc::new(|_| async { Ok::<(), CallbackError>(()) })
    }

    #[test]
    fn compile_job_rejects_invalid_identity_and_zero_revision() {
        let factory = TaskFactory::new(sink());
        let mut invalid_id = definition(Uuid::new_v4(), 1);
        invalid_id.job_id = vec![1, 2, 3];
        assert!(compile_job(&invalid_id, &factory).is_err());

        let zero_revision = definition(Uuid::new_v4(), 0);
        assert!(compile_job(&zero_revision, &factory).is_err());
    }

    #[tokio::test]
    async fn controller_upsert_is_idempotent_and_preserves_server_job_id() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let controller = JobController::new(scheduler.clone(), sink());
        let job_id = Uuid::new_v4();
        let command_id = Uuid::new_v4();
        let command = command(command_id, 1, definition(job_id, 1));

        let first = controller.apply(command.clone()).await;
        let repeated = controller.apply(command).await;

        assert_eq!(first.status, proto::JobCommandStatus::Applied as i32);
        assert_eq!(first, repeated);
        assert!(scheduler.get(job_id).await.unwrap().is_some());

        controller.clear().await.unwrap();
        assert!(scheduler.get(job_id).await.unwrap().is_none());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn controller_rejects_non_increasing_job_revision() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let controller = JobController::new(runtime.scheduler(), sink());
        let job_id = Uuid::new_v4();

        let first = controller
            .apply(command(Uuid::new_v4(), 1, definition(job_id, 3)))
            .await;
        let conflict = controller
            .apply(command(Uuid::new_v4(), 2, definition(job_id, 3)))
            .await;

        assert_eq!(first.status, proto::JobCommandStatus::Applied as i32);
        assert_eq!(conflict.status, proto::JobCommandStatus::Rejected as i32);
        assert_eq!(
            conflict.error.expect("structured error").code,
            proto::JobCommandErrorCode::RevisionConflict as i32
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reporting_adapter_uses_business_revision_in_task_report() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let sink: Arc<dyn TaskReportSink> = Arc::new(move |report| {
            let sender = sender.clone();
            async move {
                sender
                    .send(report)
                    .await
                    .map_err(|_| CallbackError::Permanent(anyhow::anyhow!("test receiver closed")))
            }
        });
        let controller = JobController::new(runtime.scheduler(), sink);
        let job_id = Uuid::new_v4();
        let mut job = definition(job_id, 7);
        job.trigger.as_mut().unwrap().schedule =
            Some(proto::job_trigger::Schedule::Once(proto::OnceSchedule {
                at: Some(timestamp_proto(Utc::now())),
            }));

        let applied = controller.apply(command(Uuid::new_v4(), 1, job)).await;
        assert_eq!(applied.status, proto::JobCommandStatus::Applied as i32);

        let report = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .expect("task report");
        assert_eq!(report.job_id, job_id.as_bytes());
        assert_eq!(report.job_revision, 7);
        assert!(report.result.is_some());
        runtime.shutdown().await.unwrap();
    }
}

pub(crate) fn compile_job(
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
pub(crate) fn parse_uuid(bytes: &[u8], field: &str) -> anyhow::Result<Uuid> {
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
