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
//! - 增量 `catalog_revision` 是 Server 侧远程 Job 集合的顺序号，必须严格 `+1`；
//! - `ReplaceAllJobs.catalog_revision` 是 Server 的权威快照版本，可以跨过丢失的增量，
//!   但不能回滚到 Agent 当前版本之前；
//! - `JobDefinition.revision` 是单个 Job 的业务配置版本，更新时必须递增；
//! - `JobSnapshot.version` 是 Scheduler 内部的并发控制 generation，每次修改都会变化。
//!
//! 三者不能互换。上报使用业务 `revision`，Scheduler 更新使用内部 `generation`。

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use smalux_protocol::agent::v1 as proto;
use uuid::Uuid;

use crate::plugins::PluginManager;
use crate::scheduler::{
    JobId, JobOptions, JobPatch, JobSnapshot, JobState, PatchValue, RescheduleMode, Scheduler,
    TaskBinding, TaskReportSink, Trigger,
};

mod compiler;
mod policy;

use compiler::{CompiledJob, TaskFactory, compile_job, parse_uuid};
pub use policy::{
    PolicyDenial, RemoteJobPolicy, RemoteJobPolicyChange, RemoteJobPolicyManager,
    RemoteJobPolicySnapshot,
};

/// 动态策略修改在本地 Scheduler 中生效后的摘要。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteJobPolicyApplication {
    pub policy: RemoteJobPolicySnapshot,
    pub affected_jobs: usize,
}

#[derive(Debug, Clone)]
struct ManagedJob {
    /// 最近成功应用的 Server 业务版本。
    revision: u64,
    /// Scheduler 当前 generation，用于乐观并发更新和删除。
    generation: u64,
    /// 稳定 Task 标识，用于 RunNow 策略检查。
    task_kind: String,
    /// Plus Task 的插件身份；内置 Task 为 `None`。
    plugin: Option<(String, String)>,
}

/// 控制器在内存中维护的远程所有权和命令幂等状态。
#[derive(Default)]
struct ControllerState {
    /// 最近成功应用的远程 Job 集合版本。
    catalog_revision: u64,
    /// 仅包含本控制器安装的远程 Job，因此 [`RemoteJobController::clear`] 不会误删本地 Job。
    remote_jobs: HashMap<JobId, ManagedJob>,
    /// 按 `command_id` 保存首次执行结果，用于处理重发和断线重连。
    cached_results: HashMap<Uuid, proto::JobCommandResult>,
    /// 记录缓存插入顺序，以便在容量固定时淘汰最旧结果。
    cache_order: VecDeque<Uuid>,
    /// 已处理过暂停动作的插件，避免每次通知重发都重复递增 Job generation。
    paused_plugins: HashSet<(String, String)>,
}

/// 应用 Server 下发的 Proto Job 命令。
///
/// 控制器只负责把协议命令转换为 Scheduler 操作，并维护“远程 Job 目录”的版本和
/// 所有权索引。网络连接、断线重连，以及把目录版本持久化到磁盘或数据库，仍由调用方负责。
///
/// # 目录版本规则
///
/// - `UpsertJob` 与 `DeleteJob` 是增量变更，版本必须恰好为当前版本加一；
/// - 增量出现断档时，返回 `RESYNC_REQUIRED`，调用方应请求 Server 下发 `ReplaceAllJobs`；
/// - `ReplaceAllJobs` 是权威快照，版本可高于当前版本以覆盖断档；相同版本可安全重放；
/// - 低于当前版本的快照会被拒绝，防止过期 Server 状态覆盖较新的 Agent 目录。
pub struct RemoteJobController {
    /// 本地调度器句柄；命令最终都通过该公开接口生效。
    scheduler: Scheduler,
    /// 把 Proto Task 配置转换为固定采集实现。
    factory: TaskFactory,
    /// 串行化远程命令，保证 revision 检查和状态更新之间不会交错。
    state: tokio::sync::Mutex<ControllerState>,
    /// 在构造远程 Task 和修改 Scheduler 前执行的本地策略。
    policy: Arc<RemoteJobPolicyManager>,
}

impl RemoteJobController {
    /// 创建只管理“远程所有权”Job 的控制器。
    pub fn new(scheduler: Scheduler, sink: Arc<dyn TaskReportSink>) -> Self {
        Self::with_policy(scheduler, sink, RemoteJobPolicy::default())
    }

    /// 创建采用指定本地安全策略的远程 Job 控制器。
    pub fn with_policy(
        scheduler: Scheduler,
        sink: Arc<dyn TaskReportSink>,
        policy: RemoteJobPolicy,
    ) -> Self {
        Self::with_policy_manager(
            scheduler,
            sink,
            Arc::new(RemoteJobPolicyManager::in_memory(policy)),
        )
    }

    /// 创建共享持久化策略 Manager 的远程 Job 控制器。
    pub fn with_policy_manager(
        scheduler: Scheduler,
        sink: Arc<dyn TaskReportSink>,
        policy: Arc<RemoteJobPolicyManager>,
    ) -> Self {
        Self::with_policy_manager_and_plugins(
            scheduler,
            sink,
            policy,
            Arc::new(PluginManager::empty()),
        )
    }

    /// 创建共享策略和当前会话 Plus Worker Manager 的远程 Job 控制器。
    pub fn with_policy_manager_and_plugins(
        scheduler: Scheduler,
        sink: Arc<dyn TaskReportSink>,
        policy: Arc<RemoteJobPolicyManager>,
        plugins: Arc<PluginManager>,
    ) -> Self {
        Self {
            scheduler,
            factory: TaskFactory::with_plugins(sink, plugins),
            state: tokio::sync::Mutex::new(ControllerState::default()),
            policy,
        }
    }

    /// 返回当前启动实例采用的脱敏策略快照。
    pub async fn policy(&self) -> RemoteJobPolicySnapshot {
        self.policy.snapshot().await
    }

    /// 持久化本地策略变化，并立即停用新策略禁止的现有远程 Job。
    ///
    /// 本方法与 [`apply_command`](Self::apply_command) 使用同一把状态锁，因此 Server
    /// 无法在策略更新和 Scheduler 停用之间插入一条通过旧策略检查的命令。
    pub async fn update_policy(
        &self,
        change: RemoteJobPolicyChange,
    ) -> anyhow::Result<RemoteJobPolicyApplication> {
        let mut state = self.state.lock().await;
        let update = self.policy.apply(change).await?;
        if !update.changed {
            return Ok(RemoteJobPolicyApplication {
                policy: update.snapshot,
                affected_jobs: 0,
            });
        }

        let mut affected_jobs = 0;
        for (id, managed) in &mut state.remote_jobs {
            if !update.snapshot.denies(&managed.task_kind) {
                continue;
            }
            let snapshot = self
                .scheduler
                .disable(
                    *id,
                    managed.generation,
                    "disabled by local Agent Job policy",
                )
                .await?;
            managed.generation = snapshot.version;
            affected_jobs += 1;
        }
        Ok(RemoteJobPolicyApplication {
            policy: update.snapshot,
            affected_jobs,
        })
    }

    /// 返回当前控制器拥有的全部远程 Job 状态，按 Job ID 稳定排序。
    pub async fn list_jobs(
        &self,
    ) -> Result<Vec<proto::JobStatus>, crate::scheduler::SchedulerError> {
        // 先复制小型所有权索引，避免等待 Scheduler Actor 时阻塞新 JobCommand。
        let managed_jobs = self.state.lock().await.remote_jobs.clone();
        let mut jobs = self
            .scheduler
            .list()
            .await?
            .into_iter()
            .filter_map(|snapshot| {
                managed_jobs
                    .get(&snapshot.id)
                    .map(|managed| job_status(&snapshot, managed.revision))
            })
            .collect::<Vec<_>>();
        jobs.sort_unstable_by(|left, right| left.job_id.cmp(&right.job_id));
        Ok(jobs)
    }

    /// 幂等应用一条命令；相同 command_id 返回首次结果，不重复 RunNow。
    pub async fn apply_command(&self, command: proto::JobCommand) -> proto::JobCommandResult {
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
                    proto::JobCommandStatus::Rejected,
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
        // 首次命令在锁内执行。可恢复的 Scheduler/容量错误不缓存，允许同一命令在
        // 运行时恢复后重试；确定性的协议和版本错误仍保持幂等结果。
        let result = self.apply_locked(&mut state, &command).await;
        if should_cache_result(&result) {
            cache_result(&mut state, command_id, result.clone());
        }
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

    /// 停止当前控制器拥有的指定 Plus 插件 Job，但保留定义用于诊断和后续恢复。
    pub async fn pause_plugin(
        &self,
        plugin_id: &str,
        plugin_version: &str,
    ) -> anyhow::Result<usize> {
        let mut state = self.state.lock().await;
        let plugin_key = (plugin_id.to_owned(), plugin_version.to_owned());
        if state.paused_plugins.contains(&plugin_key) {
            return Ok(0);
        }
        let matching = state
            .remote_jobs
            .iter()
            .filter(|(_, job)| {
                job.plugin
                    .as_ref()
                    .is_some_and(|(id, version)| id == plugin_id && version == plugin_version)
            })
            .map(|(id, job)| (*id, job.clone()))
            .collect::<Vec<_>>();
        let mut affected = 0;
        for (job_id, managed) in matching {
            let snapshot = self
                .scheduler
                .disable(
                    job_id,
                    managed.generation,
                    "Plus Worker paused after repeated failures",
                )
                .await?;
            if let Some(current) = state.remote_jobs.get_mut(&job_id) {
                current.generation = snapshot.version;
            }
            affected += 1;
        }
        if affected > 0 {
            tracing::warn!(%plugin_id, %plugin_version, affected_jobs = affected, "paused existing Plus Jobs after Worker failure");
        }
        state.paused_plugins.insert(plugin_key);
        Ok(affected)
    }

    /// 新运行时快照到达后清除本地暂停标记，允许 Server 的 ReplaceAll 恢复 Job。
    pub async fn resume_plugin(&self, plugin_id: &str, plugin_version: &str) {
        self.state
            .lock()
            .await
            .paused_plugins
            .remove(&(plugin_id.to_owned(), plugin_version.to_owned()));
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
            None => Err(ControlError::rejected(
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
            Err(error) => command_error(
                &command.command_id,
                state.catalog_revision,
                error.status,
                error.code,
                error.error,
            ),
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
        self.policy
            .check_definition(definition)
            .await
            .map_err(local_policy_error)?;
        // 编译阶段不修改 Scheduler，因此无效定义不会留下半安装 Job。
        let compiled = match compile_job(definition, &self.factory) {
            Ok(compiled) => compiled,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    catalog_revision = command.catalog_revision,
                    "agent rejected invalid job definition"
                );
                return Err(ControlError::rejected(
                    proto::JobCommandErrorCode::InvalidJob,
                    error,
                ));
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
        let snapshot = if let Some(current) = state.remote_jobs.get(&compiled.id).cloned() {
            if compiled.revision < current.revision
                || (require_increase && compiled.revision == current.revision)
            {
                return Err(ControlError::rejected(
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
                    ControlError::rejected(
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
                task_kind: compiled.task_kind,
                plugin: compiled.plugin,
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
        let id = parse_uuid(&command.job_id, "job_id").map_err(|error| {
            ControlError::rejected(proto::JobCommandErrorCode::InvalidCommand, error)
        })?;
        // 只允许删除远程所有权索引中的 Job，避免误删本地任务。
        let current = state.remote_jobs.get(&id).cloned().ok_or_else(|| {
            ControlError::rejected(
                proto::JobCommandErrorCode::NotFound,
                anyhow::anyhow!("remote job was not found"),
            )
        })?;
        // expected_revision 是业务层乐观锁，不是 Scheduler generation。
        if command.expected_revision != current.revision {
            return Err(ControlError::rejected(
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
        let id = parse_uuid(&command.job_id, "job_id").map_err(|error| {
            ControlError::rejected(proto::JobCommandErrorCode::InvalidCommand, error)
        })?;
        let current = state.remote_jobs.get(&id).cloned().ok_or_else(|| {
            ControlError::rejected(
                proto::JobCommandErrorCode::NotFound,
                anyhow::anyhow!("remote job was not found"),
            )
        })?;
        if command.expected_revision != current.revision {
            return Err(ControlError::rejected(
                proto::JobCommandErrorCode::RevisionConflict,
                anyhow::anyhow!("expected revision does not match"),
            ));
        }
        self.policy
            .check_installed(&current.task_kind)
            .await
            .map_err(local_policy_error)?;
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
                task_kind: current.task_kind,
                plugin: current.plugin,
            },
        );
        Ok(Some(job_status(&snapshot, current.revision)))
    }

    async fn replace_all(
        &self,
        state: &mut ControllerState,
        command: &proto::ReplaceAllJobs,
    ) -> ControlResult<Option<proto::JobStatus>> {
        require_replace_all_catalog(state, command.catalog_revision)?;
        // 第一阶段只解析和校验全部定义，避免普通配置错误导致部分应用。
        let mut ids = HashSet::new();
        let mut compiled_jobs = Vec::with_capacity(command.jobs.len());
        for job in &command.jobs {
            let id = parse_uuid(&job.job_id, "job_id").map_err(|error| {
                ControlError::rejected(proto::JobCommandErrorCode::InvalidJob, error)
            })?;
            if !ids.insert(id) {
                return Err(ControlError::rejected(
                    proto::JobCommandErrorCode::InvalidJob,
                    anyhow::anyhow!("replace_all contains duplicate job_id"),
                ));
            }
            self.policy
                .check_definition(job)
                .await
                .map_err(local_policy_error)?;
            compiled_jobs.push(match compile_job(job, &self.factory) {
                Ok(compiled) => compiled,
                Err(error) => {
                    tracing::warn!(error = %error, "agent rejected invalid job in replace-all");
                    return Err(ControlError::rejected(
                        proto::JobCommandErrorCode::InvalidJob,
                        error,
                    ));
                }
            });
        }
        // 第二阶段在修改前一次性读取 Scheduler，提前发现所有权、generation 和最终容量问题。
        // 这样正常的可预见错误不会在多个 Job 之间留下半应用状态。
        let snapshots = self.scheduler.list().await.map_err(scheduler_error)?;
        let snapshot_versions = snapshots
            .iter()
            .map(|snapshot| (snapshot.id, snapshot.version))
            .collect::<HashMap<_, _>>();
        for (id, managed) in &state.remote_jobs {
            match snapshot_versions.get(id) {
                Some(version) if *version == managed.generation => {}
                Some(version) => {
                    return Err(ControlError::rejected(
                        proto::JobCommandErrorCode::RevisionConflict,
                        anyhow::anyhow!(
                            "Scheduler generation for {id} changed from {} to {version}",
                            managed.generation
                        ),
                    ));
                }
                None => {
                    return Err(ControlError::rejected(
                        proto::JobCommandErrorCode::SchedulerUnavailable,
                        anyhow::anyhow!("remote Job {id} is missing from Scheduler"),
                    ));
                }
            }
        }
        for id in &ids {
            if !state.remote_jobs.contains_key(id) && snapshot_versions.contains_key(id) {
                return Err(ControlError::rejected(
                    proto::JobCommandErrorCode::OwnershipConflict,
                    anyhow::anyhow!("Job {id} is owned by local Scheduler configuration"),
                ));
            }
        }
        let scheduler_config = self.scheduler.get_config().await.map_err(scheduler_error)?;
        let retained_local_jobs = snapshots.len().saturating_sub(state.remote_jobs.len());
        let final_job_count = retained_local_jobs.saturating_add(ids.len());
        if final_job_count > scheduler_config.config.max_jobs {
            return Err(ControlError::rejected(
                proto::JobCommandErrorCode::CapacityExceeded,
                anyhow::anyhow!(
                    "replace-all would create {final_job_count} Jobs, exceeding the Scheduler limit {}",
                    scheduler_config.config.max_jobs
                ),
            ));
        }

        let (existing_jobs, new_jobs): (Vec<_>, Vec<_>) = compiled_jobs
            .into_iter()
            .partition(|compiled| state.remote_jobs.contains_key(&compiled.id));
        // 已有 Job 的原子更新不改变总容量，先完成它们。
        for compiled in existing_jobs {
            self.upsert_compiled(state, compiled, false).await?;
        }
        // 删除快照中不存在的旧远程 Job，为后续新增项释放容量。
        let stale = state
            .remote_jobs
            .keys()
            .filter(|id| !ids.contains(id))
            .copied()
            .collect::<Vec<_>>();
        for id in stale {
            let managed = state.remote_jobs[&id].clone();
            self.scheduler
                .delete(id, managed.generation)
                .await
                .map_err(scheduler_error)?;
            state.remote_jobs.remove(&id);
        }
        // 所有配置和最终容量都已预检，此阶段只安装快照中的新增 Job。
        for compiled in new_jobs {
            self.upsert_compiled(state, compiled, false).await?;
        }
        // 所有新增、更新和删除都成功后，ReplaceAll 才算应用完成。
        state.catalog_revision = command.catalog_revision;
        Ok(None)
    }
}

type ControlResult<T> = Result<T, ControlError>;

/// 控制器命令执行失败时返回的内部错误。
///
/// `status` 与 `code` 分开保存：目录断档需要返回 `RESYNC_REQUIRED`，普通的
/// Job revision 冲突仍然只是 `REJECTED`。两者都可能使用 `REVISION_CONFLICT` 错误码，
/// 因此 Server 必须根据 `status` 决定下一步：仅 `RESYNC_REQUIRED` 时请求完整快照。
struct ControlError {
    status: proto::JobCommandStatus,
    code: proto::JobCommandErrorCode,
    error: anyhow::Error,
}

impl ControlError {
    fn rejected(code: proto::JobCommandErrorCode, error: anyhow::Error) -> Self {
        Self {
            status: proto::JobCommandStatus::Rejected,
            code,
            error,
        }
    }

    fn resync(error: anyhow::Error) -> Self {
        Self {
            status: proto::JobCommandStatus::ResyncRequired,
            code: proto::JobCommandErrorCode::RevisionConflict,
            error,
        }
    }
}

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

/// 要求增量集合变更命令连续到达，发现丢包或乱序时让 Server 重新同步。
///
/// 例如 Agent 当前目录为版本 4 而收到版本 6 的 `UpsertJob` 时，不能猜测版本 5 的内容；
/// 这里返回 `RESYNC_REQUIRED`，保留当前目录版本 4，等待 Server 发送权威快照。
fn require_next_catalog(state: &ControllerState, revision: u64) -> ControlResult<()> {
    if revision != state.catalog_revision + 1 {
        return Err(ControlError::resync(anyhow::anyhow!(
            "catalog revision must be exactly current + 1"
        )));
    }
    Ok(())
}

/// 校验全量目录版本。
///
/// 全量目录是 Server 的权威快照，可以跨过丢失的增量版本；相同版本允许重放，
/// 旧版本则拒绝，避免过期快照回滚 Agent 当前目录。版本 0 没有可排序的业务含义，
/// 也一律拒绝。
fn require_replace_all_catalog(state: &ControllerState, revision: u64) -> ControlResult<()> {
    if revision == 0 || revision < state.catalog_revision {
        return Err(ControlError::rejected(
            proto::JobCommandErrorCode::RevisionConflict,
            anyhow::anyhow!(
                "replace-all catalog revision must be positive and not older than current"
            ),
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

/// 临时 Scheduler 故障和容量限制可能在不改变命令内容时恢复，因此不固定其失败结果。
fn should_cache_result(result: &proto::JobCommandResult) -> bool {
    let Some(error) = &result.error else {
        return true;
    };
    !matches!(
        proto::JobCommandErrorCode::try_from(error.code),
        Ok(proto::JobCommandErrorCode::SchedulerUnavailable)
            | Ok(proto::JobCommandErrorCode::CapacityExceeded)
    )
}

/// 把 Scheduler 内部错误归类为稳定的协议错误码，同时保留原始错误链。
fn scheduler_error(error: crate::scheduler::SchedulerError) -> ControlError {
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
    ControlError::rejected(code, anyhow::Error::new(error))
}

/// 构造“Job 定义无效”的控制器内部错误，减少调用点的重复元组代码。
fn invalid_job(message: &str) -> ControlError {
    ControlError::rejected(
        proto::JobCommandErrorCode::InvalidJob,
        anyhow::anyhow!(message.to_owned()),
    )
}

/// 将本地策略拒绝转换成 Server 可机器识别的稳定结果。
fn local_policy_error(denial: PolicyDenial) -> ControlError {
    ControlError::rejected(
        proto::JobCommandErrorCode::LocalPolicyDenied,
        anyhow::anyhow!(denial),
    )
}

/// 将控制器错误包装成 Server 可关联到原命令的结构化响应。
fn command_error(
    command_id: &[u8],
    catalog_revision: u64,
    status: proto::JobCommandStatus,
    code: proto::JobCommandErrorCode,
    error: anyhow::Error,
) -> proto::JobCommandResult {
    proto::JobCommandResult {
        command_id: command_id.to_vec(),
        status: status as i32,
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
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::scheduler::{CallbackError, SchedulerConfig, SchedulerRuntime};
    use crate::tasks::collect::CpuTask;

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
        let controller = RemoteJobController::new(scheduler.clone(), sink());
        let job_id = Uuid::new_v4();
        let command_id = Uuid::new_v4();
        let command = command(command_id, 1, definition(job_id, 1));

        let first = controller.apply_command(command.clone()).await;
        let repeated = controller.apply_command(command).await;

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
        let controller = RemoteJobController::new(runtime.scheduler(), sink());
        let job_id = Uuid::new_v4();

        let first = controller
            .apply_command(command(Uuid::new_v4(), 1, definition(job_id, 3)))
            .await;
        let conflict = controller
            .apply_command(command(Uuid::new_v4(), 2, definition(job_id, 3)))
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
    async fn local_policy_rejects_denied_task_before_scheduler_mutation() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let policy = RemoteJobPolicy::new(false, [CpuTask::KIND.to_owned()]);
        let controller = RemoteJobController::with_policy(scheduler.clone(), sink(), policy);
        let job_id = Uuid::new_v4();

        let result = controller
            .apply_command(command(Uuid::new_v4(), 1, definition(job_id, 1)))
            .await;

        assert_eq!(result.status, proto::JobCommandStatus::Rejected as i32);
        assert_eq!(result.catalog_revision, 0);
        assert_eq!(
            result.error.expect("policy error").code,
            proto::JobCommandErrorCode::LocalPolicyDenied as i32
        );
        assert!(scheduler.get(job_id).await.unwrap().is_none());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn adding_a_task_policy_disables_an_installed_remote_job() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let controller = RemoteJobController::new(scheduler.clone(), sink());
        let job_id = Uuid::new_v4();
        let installed = controller
            .apply_command(command(Uuid::new_v4(), 1, definition(job_id, 1)))
            .await;
        assert_eq!(installed.status, proto::JobCommandStatus::Applied as i32);

        let update = controller
            .update_policy(RemoteJobPolicyChange::AddTask(CpuTask::KIND.to_owned()))
            .await
            .unwrap();

        assert_eq!(update.affected_jobs, 1);
        assert_eq!(update.policy.revision, 1);
        let snapshot = scheduler.get(job_id).await.unwrap().unwrap();
        assert!(matches!(snapshot.state, JobState::Disabled { .. }));
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replace_all_policy_rejection_is_atomic_and_does_not_advance_catalog() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let allowed = Uuid::new_v4();
        let denied = Uuid::new_v4();
        let policy = RemoteJobPolicy::new(false, [CpuTask::KIND.to_owned()]);
        let controller = RemoteJobController::with_policy(scheduler.clone(), sink(), policy);
        let mut allowed_definition = definition(allowed, 1);
        allowed_definition.task = Some(proto::TaskDefinition {
            task: Some(proto::task_definition::Task::Host(proto::HostTaskConfig {})),
        });
        let command = proto::JobCommand {
            command_id: Uuid::new_v4().as_bytes().to_vec(),
            action: Some(proto::job_command::Action::ReplaceAll(
                proto::ReplaceAllJobs {
                    catalog_revision: 8,
                    jobs: vec![allowed_definition, definition(denied, 1)],
                },
            )),
        };

        let result = controller.apply_command(command).await;

        assert_eq!(result.status, proto::JobCommandStatus::Rejected as i32);
        assert_eq!(result.catalog_revision, 0);
        assert!(scheduler.get(allowed).await.unwrap().is_none());
        assert!(scheduler.get(denied).await.unwrap().is_none());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn deny_all_policy_allows_empty_authoritative_snapshot() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let controller = RemoteJobController::with_policy(
            runtime.scheduler(),
            sink(),
            RemoteJobPolicy::new(true, []),
        );
        let result = controller
            .apply_command(proto::JobCommand {
                command_id: Uuid::new_v4().as_bytes().to_vec(),
                action: Some(proto::job_command::Action::ReplaceAll(
                    proto::ReplaceAllJobs {
                        catalog_revision: 1,
                        jobs: Vec::new(),
                    },
                )),
            })
            .await;

        assert_eq!(result.status, proto::JobCommandStatus::Applied as i32);
        assert_eq!(result.catalog_revision, 1);
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replace_all_reconciles_authoritative_snapshot_after_incremental_gap() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let controller = RemoteJobController::new(scheduler.clone(), sink());
        let retained_job_id = Uuid::new_v4();
        let replacement_job_id = Uuid::new_v4();

        // Arrange: Agent 已应用目录版本 1，随后版本 2 到 6 在传输中丢失。
        let first_increment = controller
            .apply_command(command(Uuid::new_v4(), 1, definition(retained_job_id, 1)))
            .await;
        assert_eq!(
            first_increment.status,
            proto::JobCommandStatus::Applied as i32
        );
        assert!(scheduler.get(retained_job_id).await.unwrap().is_some());

        // Act: Server 不再发送缺失的增量，而是用版本 7 的权威完整快照对账。
        let authoritative_snapshot = proto::JobCommand {
            command_id: Uuid::new_v4().as_bytes().to_vec(),
            action: Some(proto::job_command::Action::ReplaceAll(
                proto::ReplaceAllJobs {
                    catalog_revision: 7,
                    jobs: vec![definition(replacement_job_id, 1)],
                },
            )),
        };

        let result = controller.apply_command(authoritative_snapshot).await;

        // Assert: 快照成为目录版本 7，并精确替换旧的远程 Job 集合。
        assert_eq!(result.status, proto::JobCommandStatus::Applied as i32);
        assert_eq!(result.catalog_revision, 7);
        assert!(scheduler.get(retained_job_id).await.unwrap().is_none());
        assert!(scheduler.get(replacement_job_id).await.unwrap().is_some());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn incremental_catalog_gap_returns_resync_required_without_mutating_state() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let controller = RemoteJobController::new(scheduler.clone(), sink());
        let job_id = Uuid::new_v4();

        // Arrange: 当前远程目录版本为 1，且 Scheduler 已安装对应 Job。
        let first = controller
            .apply_command(command(Uuid::new_v4(), 1, definition(job_id, 1)))
            .await;
        assert_eq!(first.status, proto::JobCommandStatus::Applied as i32);
        // `JobSnapshot.version` 是 Scheduler 的乐观锁 generation；任意更新都会使它变化。
        let scheduler_version_before_gap = scheduler.get(job_id).await.unwrap().unwrap().version;

        // Act: 版本 2 缺失，Agent 直接收到了版本 3 的增量更新。
        let gap = controller
            .apply_command(command(Uuid::new_v4(), 3, definition(job_id, 2)))
            .await;

        // Assert: 这是恢复信号而非普通拒绝；目录版本与 Scheduler generation 都保持不变。
        assert_eq!(gap.status, proto::JobCommandStatus::ResyncRequired as i32);
        assert_eq!(gap.catalog_revision, 1);
        assert_eq!(
            gap.error.expect("structured error").code,
            proto::JobCommandErrorCode::RevisionConflict as i32
        );
        assert_eq!(
            scheduler.get(job_id).await.unwrap().unwrap().version,
            scheduler_version_before_gap
        );
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replace_all_rejects_stale_snapshot_without_mutating_current_directory() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let controller = RemoteJobController::new(scheduler.clone(), sink());
        let first_job_id = Uuid::new_v4();
        let stale_job_id = Uuid::new_v4();

        // Arrange: Agent 已从版本 5 的权威快照建立当前远程 Job 目录。
        let first = controller
            .apply_command(proto::JobCommand {
                command_id: Uuid::new_v4().as_bytes().to_vec(),
                action: Some(proto::job_command::Action::ReplaceAll(
                    proto::ReplaceAllJobs {
                        catalog_revision: 5,
                        jobs: vec![definition(first_job_id, 1)],
                    },
                )),
            })
            .await;
        assert_eq!(first.status, proto::JobCommandStatus::Applied as i32);
        assert!(scheduler.get(first_job_id).await.unwrap().is_some());

        // Act: 延迟到达的版本 4 快照试图用不同 Job 覆盖当前目录。
        let stale = controller
            .apply_command(proto::JobCommand {
                command_id: Uuid::new_v4().as_bytes().to_vec(),
                action: Some(proto::job_command::Action::ReplaceAll(
                    proto::ReplaceAllJobs {
                        catalog_revision: 4,
                        jobs: vec![definition(stale_job_id, 1)],
                    },
                )),
            })
            .await;

        // Assert: 只拒绝旧快照，不删除版本 5 的 Job，也不安装旧快照中的 Job。
        assert_eq!(stale.status, proto::JobCommandStatus::Rejected as i32);
        assert_eq!(stale.catalog_revision, 5);
        assert_eq!(
            stale.error.expect("structured error").code,
            proto::JobCommandErrorCode::RevisionConflict as i32
        );
        assert!(scheduler.get(first_job_id).await.unwrap().is_some());
        assert!(scheduler.get(stale_job_id).await.unwrap().is_none());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replace_all_replays_equal_revision_to_reconcile_scheduler_state() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let controller = RemoteJobController::new(scheduler.clone(), sink());
        let job_id = Uuid::new_v4();

        // Arrange: Agent 已成功应用版本 5 的完整权威快照。
        let first = controller
            .apply_command(proto::JobCommand {
                command_id: Uuid::new_v4().as_bytes().to_vec(),
                action: Some(proto::job_command::Action::ReplaceAll(
                    proto::ReplaceAllJobs {
                        catalog_revision: 5,
                        jobs: vec![definition(job_id, 1)],
                    },
                )),
            })
            .await;
        assert_eq!(first.status, proto::JobCommandStatus::Applied as i32);

        // Act: 网络重试使用新的 command_id 重发完全相同的权威快照。
        let replay = controller
            .apply_command(proto::JobCommand {
                command_id: Uuid::new_v4().as_bytes().to_vec(),
                action: Some(proto::job_command::Action::ReplaceAll(
                    proto::ReplaceAllJobs {
                        catalog_revision: 5,
                        jobs: vec![definition(job_id, 1)],
                    },
                )),
            })
            .await;

        // Assert: 目录版本不倒退也不递增，Scheduler 中的 Job 仍与该快照一致。
        assert_eq!(replay.status, proto::JobCommandStatus::Applied as i32);
        assert_eq!(replay.catalog_revision, 5);
        assert!(scheduler.get(job_id).await.unwrap().is_some());
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn replace_all_can_replace_a_job_when_scheduler_is_at_capacity() {
        let runtime = SchedulerRuntime::start(SchedulerConfig {
            max_jobs: 1,
            ..SchedulerConfig::default()
        })
        .unwrap();
        let scheduler = runtime.scheduler();
        let controller = RemoteJobController::new(scheduler.clone(), sink());
        let old_id = Uuid::new_v4();
        let new_id = Uuid::new_v4();

        let installed = controller
            .apply_command(command(Uuid::new_v4(), 1, definition(old_id, 1)))
            .await;
        assert_eq!(installed.status, proto::JobCommandStatus::Applied as i32);

        let replaced = controller
            .apply_command(proto::JobCommand {
                command_id: Uuid::new_v4().as_bytes().to_vec(),
                action: Some(proto::job_command::Action::ReplaceAll(
                    proto::ReplaceAllJobs {
                        catalog_revision: 2,
                        jobs: vec![definition(new_id, 1)],
                    },
                )),
            })
            .await;

        assert_eq!(replaced.status, proto::JobCommandStatus::Applied as i32);
        assert!(scheduler.get(old_id).await.unwrap().is_none());
        assert!(scheduler.get(new_id).await.unwrap().is_some());
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
        let controller = RemoteJobController::new(runtime.scheduler(), sink);
        let job_id = Uuid::new_v4();
        let mut job = definition(job_id, 7);
        job.trigger.as_mut().unwrap().schedule =
            Some(proto::job_trigger::Schedule::Once(proto::OnceSchedule {
                at: Some(timestamp_proto(Utc::now())),
            }));

        let applied = controller
            .apply_command(command(Uuid::new_v4(), 1, job))
            .await;
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
