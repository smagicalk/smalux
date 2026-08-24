use smalux_protocol::agent::v1::JobStatus;
use std::{path::PathBuf, sync::Arc, time::Instant};
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    client::{AgentStateStore, AuthenticationMode, ClientConnectionStatus, SmaluxClientHandle},
    plugins::PluginManager,
    remote_jobs::{RemoteJobController, RemoteJobPolicyManager},
    scheduler::{Scheduler, SchedulerStatus},
};

use super::protocol::*;
use super::serve;

/// 在启动流程各阶段逐步填充的共享只读状态。
pub struct ManagementState {
    started_at: Instant,
    state_store: Arc<dyn AgentStateStore>,
    connection_status: watch::Receiver<ClientConnectionStatus>,
    authentication_mode: RwLock<Option<AuthenticationMode>>,
    last_disconnect_reason: RwLock<Option<String>>,
    runtime: RwLock<Option<RuntimeInspection>>,
    config: EffectiveConfigSnapshot,
    policy: Arc<RemoteJobPolicyManager>,
    plugins: Arc<PluginManager>,
    job_result_stats: Arc<JobResultBufferStats>,
    acknowledged_policy_revision: RwLock<Option<u64>>,
}

struct RuntimeInspection {
    scheduler: Scheduler,
    jobs: Arc<RemoteJobController>,
    client: SmaluxClientHandle,
}

impl ManagementState {
    pub fn new(
        state_store: Arc<dyn AgentStateStore>,
        connection_status: watch::Receiver<ClientConnectionStatus>,
        config: EffectiveConfigSnapshot,
        policy: Arc<RemoteJobPolicyManager>,
        plugins: Arc<PluginManager>,
        job_result_stats: Arc<JobResultBufferStats>,
    ) -> Self {
        Self {
            started_at: Instant::now(),
            state_store,
            connection_status,
            authentication_mode: RwLock::new(None),
            last_disconnect_reason: RwLock::new(None),
            runtime: RwLock::new(None),
            config,
            policy,
            plugins,
            job_result_stats,
            acknowledged_policy_revision: RwLock::new(None),
        }
    }

    /// 连接成功并启动 Scheduler 后，公开其只读句柄。
    pub async fn attach_runtime(
        &self,
        scheduler: Scheduler,
        jobs: Arc<RemoteJobController>,
        client: SmaluxClientHandle,
    ) {
        *self.runtime.write().await = Some(RuntimeInspection {
            scheduler,
            jobs,
            client,
        });
    }

    pub async fn record_connected(&self, mode: AuthenticationMode) {
        *self.authentication_mode.write().await = Some(mode);
        *self.last_disconnect_reason.write().await = None;
        *self.acknowledged_policy_revision.write().await = None;
    }

    pub async fn record_disconnected(&self, reason: String) {
        *self.last_disconnect_reason.write().await = Some(reason);
        *self.acknowledged_policy_revision.write().await = None;
    }

    /// 只接受当前 revision 的确认；延迟到达的旧 ACK 不得覆盖新策略状态。
    pub async fn record_policy_acknowledgement(&self, revision: u64) {
        if self.policy.snapshot().await.revision == revision {
            *self.acknowledged_policy_revision.write().await = Some(revision);
        }
    }

    pub(crate) async fn handle(&self, request: ControlRequest) -> ControlResponse {
        match request {
            ControlRequest::Status => match self.status().await {
                Ok(value) => ControlResponse::Status(value),
                Err(error) => internal_error(error),
            },
            ControlRequest::ListJobs => match self.jobs().await {
                Ok(value) => ControlResponse::Jobs(value),
                Err(error) => internal_error(error),
            },
            ControlRequest::GetJob { job_id } => match self.jobs().await {
                Ok(value) => {
                    ControlResponse::Job(value.into_iter().find(|job| job.job_id == job_id))
                }
                Err(error) => internal_error(error),
            },
            ControlRequest::JobPolicy => ControlResponse::JobPolicy(self.policy_view().await),
            ControlRequest::UpdateJobPolicy { change } => match self.update_policy(change).await {
                Ok((policy, affected_jobs)) => ControlResponse::JobPolicyUpdated {
                    policy,
                    affected_jobs,
                },
                Err(error) => internal_error(error),
            },
            ControlRequest::ListPlugins => ControlResponse::Plugins(self.plugins().await),
            ControlRequest::GetPlugin { plugin_id } => ControlResponse::Plugin(
                self.plugins()
                    .await
                    .into_iter()
                    .find(|plugin| plugin.plugin_id == plugin_id),
            ),
            ControlRequest::EffectiveConfig => {
                ControlResponse::EffectiveConfig(self.config.clone())
            }
        }
    }

    async fn policy_view(&self) -> JobPolicyView {
        let snapshot = self.policy.snapshot().await;
        let acknowledged = *self.acknowledged_policy_revision.read().await;
        JobPolicyView {
            revision: snapshot.revision,
            deny_all: snapshot.deny_all,
            denied_task_kinds: snapshot.denied_task_kinds,
            server_sync: if acknowledged == Some(snapshot.revision) {
                "acknowledged"
            } else {
                "pending"
            }
            .to_owned(),
        }
    }

    async fn update_policy(
        &self,
        change: JobPolicyMutation,
    ) -> anyhow::Result<(JobPolicyView, usize)> {
        let runtime = self.runtime.read().await;
        let (snapshot, affected_jobs) = if let Some(runtime) = runtime.as_ref() {
            let application = runtime.jobs.update_policy(change.into()).await?;
            (application.policy, application.affected_jobs)
        } else {
            let update = self.policy.apply(change.into()).await?;
            (update.snapshot, 0)
        };
        *self.acknowledged_policy_revision.write().await = None;
        if let Some(runtime) = runtime.as_ref()
            && let Err(error) = runtime
                .client
                .send_agent_job_policy(snapshot.to_protocol_message())
                .await
        {
            tracing::debug!(%error, "Agent Job policy was saved locally; Server sync remains pending");
        }
        drop(runtime);
        Ok((self.policy_view().await, affected_jobs))
    }

    async fn jobs(&self) -> anyhow::Result<Vec<JobSnapshot>> {
        let runtime = self.runtime.read().await;
        let Some(runtime) = runtime.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(runtime
            .jobs
            .list_jobs()
            .await?
            .into_iter()
            .filter_map(job_snapshot)
            .collect())
    }

    async fn plugins(&self) -> Vec<PluginSnapshot> {
        self.plugins
            .status()
            .await
            .into_iter()
            .map(|plugin| PluginSnapshot {
                plugin_id: plugin.plugin_id,
                version: plugin.version,
                installed: plugin.installed,
                active: plugin.active,
                worker_pid: plugin.worker_pid,
                config_revision: plugin.config_revision,
                concurrency: plugin.concurrency,
                state: plugin.state,
                failure_count: plugin.failure_count,
                failure_window_started_at_ms: plugin.failure_window_started_at_ms,
                paused_at_ms: plugin.paused_at_ms,
                last_exit_reason: plugin.last_exit_reason,
                last_error: plugin.last_error,
                server_pause_acknowledged: plugin.server_pause_acknowledged,
            })
            .collect()
    }

    async fn status(&self) -> anyhow::Result<StatusSnapshot> {
        // watch::Ref 不是 Send，必须在任何 await 前复制值并立即释放 guard。
        let current_connection_status = {
            let status = self.connection_status.borrow();
            *status
        };
        let persisted = self.state_store.load().await?;
        let (agent_id, registration_stage) = persisted
            .as_ref()
            .map(|state| {
                (
                    state.agent_id().map(str::to_owned),
                    Some(registration_stage(state.stage()).to_owned()),
                )
            })
            .unwrap_or((None, None));
        let runtime = self.runtime.read().await;
        let plugins = self.plugins().await;
        let (scheduler_status, jobs, heartbeat) = if let Some(runtime) = runtime.as_ref() {
            let scheduler_status = {
                let receiver = runtime.scheduler.subscribe_status();
                let status = receiver.borrow();
                scheduler_status(&status)
            };
            let jobs = runtime.jobs.list_jobs().await?;
            let heartbeat = runtime.client.heartbeat_stats().await.ok();
            (scheduler_status, jobs, heartbeat)
        } else {
            ("starting".to_owned(), Vec::new(), None)
        };
        let running_jobs = jobs.iter().filter(|job| job.running_count > 0).count();
        let pending_runs = jobs.iter().map(|job| job.pending_count as usize).sum();
        let (pending_job_results, dropped_job_results) = self.job_result_stats.snapshot();
        Ok(StatusSnapshot {
            uptime_ms: millis(self.started_at.elapsed()),
            connection_status: connection_status(current_connection_status),
            authentication_mode: self
                .authentication_mode
                .read()
                .await
                .map(authentication_mode),
            agent_id,
            registration_stage,
            scheduler_status,
            job_count: jobs.len(),
            running_jobs,
            pending_runs,
            pending_job_results,
            dropped_job_results,
            heartbeat_sent: heartbeat.as_ref().map(|stats| stats.sent_count),
            heartbeat_received: heartbeat.as_ref().map(|stats| stats.received_count),
            heartbeat_rtt_ms: heartbeat
                .and_then(|stats| stats.last_sample)
                .map(|sample| millis(sample.rtt)),
            last_disconnect_reason: self.last_disconnect_reason.read().await.clone(),
            plugin_subsystem: if plugins.iter().any(|plugin| plugin.active) {
                "active"
            } else if plugins.is_empty() {
                "empty"
            } else {
                "installed"
            }
            .to_owned(),
            active_plugin_workers: plugins.iter().filter(|plugin| plugin.active).count(),
        })
    }
}

fn internal_error(error: anyhow::Error) -> ControlResponse {
    ControlResponse::Error {
        code: "internal_error".to_owned(),
        message: format!("{error:#}"),
    }
}

fn job_snapshot(job: JobStatus) -> Option<JobSnapshot> {
    let job_id = Uuid::from_slice(&job.job_id).ok()?;
    Some(JobSnapshot {
        job_id,
        source: "remote".to_owned(),
        revision: job.revision,
        task_kind: job.task_kind,
        state: job_runtime_state(
            smalux_protocol::agent::v1::JobRuntimeState::try_from(job.state).ok()?,
        )
        .to_owned(),
        disabled_reason: (!job.disabled_reason.is_empty()).then_some(job.disabled_reason),
        running_count: job.running_count,
        pending_count: job.pending_count,
        consecutive_failures: job.consecutive_failures,
        next_run_at: job.next_run_at.map(timestamp_string),
        last_outcome: (!job.last_outcome.is_empty()).then_some(job.last_outcome),
    })
}

fn timestamp_string(value: prost_types::Timestamp) -> String {
    format!("{}.{:09}Z", value.seconds, value.nanos)
}

fn connection_status(value: ClientConnectionStatus) -> String {
    match value {
        ClientConnectionStatus::Disconnected => "disconnected",
        ClientConnectionStatus::Connecting => "connecting",
        ClientConnectionStatus::Connected => "connected",
        ClientConnectionStatus::Reconnecting => "reconnecting",
        ClientConnectionStatus::Fatal => "fatal",
    }
    .to_owned()
}

fn authentication_mode(value: AuthenticationMode) -> String {
    match value {
        AuthenticationMode::RegistrationXxPsk3 => "registration_xx_psk3",
        AuthenticationMode::ReconnectIk => "reconnect_ik",
    }
    .to_owned()
}

fn registration_stage(value: crate::client::RegistrationStage) -> &'static str {
    match value {
        crate::client::RegistrationStage::IdentityPrepared => "identity_prepared",
        crate::client::RegistrationStage::RegistrationPending => "registration_pending",
        crate::client::RegistrationStage::Registered => "registered",
    }
}

fn job_runtime_state(value: smalux_protocol::agent::v1::JobRuntimeState) -> &'static str {
    use smalux_protocol::agent::v1::JobRuntimeState;
    match value {
        JobRuntimeState::Unspecified => "unspecified",
        JobRuntimeState::Enabled => "enabled",
        JobRuntimeState::Completed => "completed",
        JobRuntimeState::Disabled => "disabled",
    }
}

fn scheduler_status(value: &SchedulerStatus) -> String {
    match value {
        SchedulerStatus::Running => "running",
        SchedulerStatus::Stopping => "stopping",
        SchedulerStatus::Stopped => "stopped",
        SchedulerStatus::Failed { .. } => "failed",
    }
    .to_owned()
}

fn millis(value: std::time::Duration) -> u64 {
    value.as_millis().try_into().unwrap_or(u64::MAX)
}

/// 启动本地管理端点并运行到取消令牌触发。
pub async fn run_server(
    endpoint: PathBuf,
    state: Arc<ManagementState>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    serve(endpoint, state, shutdown).await
}
