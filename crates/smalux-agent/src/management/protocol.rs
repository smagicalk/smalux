//! Agent 本地管理 IPC 协议 DTO。

use crate::remote_jobs::RemoteJobPolicyChange;
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};
use uuid::Uuid;

/// Agent 结果队列的轻量运行统计；只保存计数，不保存结果内容。
#[derive(Default)]
pub struct JobResultBufferStats {
    pending: AtomicUsize,
    dropped: AtomicU64,
}

impl JobResultBufferStats {
    pub fn update(&self, pending: usize, dropped: u64) {
        self.pending.store(pending, Ordering::Relaxed);
        self.dropped.store(dropped, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> (usize, u64) {
        (
            self.pending.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
        )
    }
}
/// IPC 协议版本；不兼容改动必须递增该值。
pub const CONTROL_PROTOCOL_VERSION: u32 = 2;

/// 单帧最大字节数，防止本地客户端造成无界分配。
pub const MAX_CONTROL_FRAME_BYTES: usize = 1024 * 1024;

/// CLI 发给常驻 Agent 的只读请求。
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Status,
    ListJobs,
    GetJob { job_id: Uuid },
    JobPolicy,
    UpdateJobPolicy { change: JobPolicyMutation },
    ListPlugins,
    GetPlugin { plugin_id: String },
    EffectiveConfig,
}

/// 带显式版本的请求封装。
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestEnvelope {
    pub protocol_version: u32,
    pub request: ControlRequest,
}

/// IPC 返回的只读数据。
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ControlResponse {
    Status(StatusSnapshot),
    Jobs(Vec<JobSnapshot>),
    Job(Option<JobSnapshot>),
    JobPolicy(JobPolicyView),
    JobPolicyUpdated {
        policy: JobPolicyView,
        affected_jobs: usize,
    },
    PluginsUnavailable {
        message: String,
    },
    Plugins(Vec<PluginSnapshot>),
    Plugin(Option<PluginSnapshot>),
    EffectiveConfig(EffectiveConfigSnapshot),
    Error {
        code: String,
        message: String,
    },
}

/// 带显式版本的响应封装。
#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseEnvelope {
    pub protocol_version: u32,
    pub response: ControlResponse,
}

/// 不包含 Token 的最终连接配置。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EffectiveConfigSnapshot {
    pub server_endpoint: String,
    pub grpc_prefix: Option<String>,
    pub registration_token: String,
    pub state_file: PathBuf,
    pub control_endpoint: PathBuf,
    pub policy_file: PathBuf,
    pub handshake_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub reconnect_initial_delay_ms: u64,
    pub reconnect_max_delay_ms: u64,
    pub task_report_buffer_capacity: usize,
    pub job_result_buffer_capacity: usize,
    pub shutdown_drain_timeout_ms: u64,
    pub offline_job_timeout_ms: u64,
    pub scheduler_global_concurrency: usize,
    pub scheduler_global_max_pending: usize,
    pub scheduler_default_job_concurrency: usize,
    pub scheduler_default_job_max_pending: usize,
    pub scheduler_max_jobs: usize,
    pub scheduler_shutdown_timeout_ms: u64,
    pub plugin_max_workers: usize,
    pub plugin_max_concurrency: usize,
    pub plugin_task_timeout_ms: u64,
    pub plugin_shutdown_timeout_ms: u64,
    pub plugin_startup_timeout_ms: u64,
    pub plugin_restart_max_attempts: usize,
    pub plugin_restart_window_ms: u64,
}

/// IPC 可表达的幂等策略修改。
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", content = "value", rename_all = "snake_case")]
pub enum JobPolicyMutation {
    AddTask(String),
    RemoveTask(String),
    DenyAll,
    AllowAll,
}

impl From<JobPolicyMutation> for RemoteJobPolicyChange {
    fn from(value: JobPolicyMutation) -> Self {
        match value {
            JobPolicyMutation::AddTask(kind) => Self::AddTask(kind),
            JobPolicyMutation::RemoveTask(kind) => Self::RemoveTask(kind),
            JobPolicyMutation::DenyAll => Self::DenyAll,
            JobPolicyMutation::AllowAll => Self::AllowAll,
        }
    }
}

/// 当前策略及本次连接是否已收到 Server ACK。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobPolicyView {
    pub revision: u64,
    pub deny_all: bool,
    pub denied_task_kinds: Vec<String>,
    pub server_sync: String,
}

/// 常驻进程整体状态快照。
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub uptime_ms: u64,
    pub connection_status: String,
    pub authentication_mode: Option<String>,
    pub agent_id: Option<String>,
    pub registration_stage: Option<String>,
    pub scheduler_status: String,
    pub job_count: usize,
    pub running_jobs: usize,
    pub pending_runs: usize,
    pub pending_job_results: usize,
    pub dropped_job_results: u64,
    pub heartbeat_sent: Option<u64>,
    pub heartbeat_received: Option<u64>,
    pub heartbeat_rtt_ms: Option<u64>,
    pub last_disconnect_reason: Option<String>,
    pub plugin_subsystem: String,
    pub active_plugin_workers: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PluginSnapshot {
    pub plugin_id: String,
    pub version: String,
    pub installed: bool,
    pub active: bool,
    pub worker_pid: Option<u32>,
    pub config_revision: Option<u64>,
    pub concurrency: Option<u32>,
    pub state: String,
    pub failure_count: usize,
    pub failure_window_started_at_ms: Option<u64>,
    pub paused_at_ms: Option<u64>,
    pub last_exit_reason: Option<String>,
    pub last_error: Option<String>,
    pub server_pause_acknowledged: bool,
}

/// IPC 使用的 Job 诊断 DTO；不暴露本地 Task 实例。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub job_id: Uuid,
    pub source: String,
    pub revision: u64,
    pub task_kind: String,
    pub state: String,
    pub disabled_reason: Option<String>,
    pub running_count: u32,
    pub pending_count: u32,
    pub consecutive_failures: u32,
    pub next_run_at: Option<String>,
    pub last_outcome: Option<String>,
}
