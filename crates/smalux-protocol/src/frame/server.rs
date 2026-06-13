//! server 发往 agent 的 frame。

use super::{
    Ack, MetricCollectionRequest, ProtocolError, RemoteJobApplyRequest, RemoteShellOpenRequest,
    RemoteTaskRequest, SMALUX_PROTOCOL_VERSION, SnapshotRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// server 发往 agent 的 frame。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerFrame {
    /// 协议版本，独立于业务 payload 版本。
    pub protocol_version: u16,
    /// server 侧递增的消息序号。
    pub sequence: u64,
    /// frame 发送时间，Unix 时间戳，单位秒。
    pub sent_at: u64,
    /// 目标 agent ID；为空时表示当前连接上的 agent。
    ///
    /// 该字段只做路由保护，不做认证。agent 收到不匹配的目标 ID 时会直接丢弃。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_agent_id: Option<String>,
    /// 具体业务 payload。
    #[serde(flatten)]
    pub payload: ServerPayload,
}

impl ServerFrame {
    /// 用指定 payload 构造 server frame。
    pub fn new(sequence: u64, sent_at: u64, payload: ServerPayload) -> Self {
        Self {
            protocol_version: SMALUX_PROTOCOL_VERSION,
            sequence,
            sent_at,
            target_agent_id: None,
            payload,
        }
    }

    /// 给 server frame 设置目标 agent ID。
    pub fn with_target_agent_id(mut self, target_agent_id: impl Into<String>) -> Self {
        self.target_agent_id = Some(target_agent_id.into());
        self
    }

    /// 构造请求完整快照的 frame。
    pub fn snapshot_request(sequence: u64, sent_at: u64, request: SnapshotRequest) -> Self {
        Self::new(
            sequence,
            sent_at,
            ServerPayload::SnapshotRequest { request },
        )
    }

    /// 构造配置 patch frame。
    pub fn config_patch(sequence: u64, sent_at: u64, patch: Value) -> Self {
        Self::new(sequence, sent_at, ServerPayload::ConfigPatch { patch })
    }

    /// 构造一次性进程采集请求 frame。
    pub fn collect_processes_once(
        sequence: u64,
        sent_at: u64,
        request: MetricCollectionRequest,
    ) -> Self {
        Self::new(
            sequence,
            sent_at,
            ServerPayload::CollectProcessesOnce { request },
        )
    }

    /// 构造一次性 Socket 采集请求 frame。
    pub fn collect_sockets_once(
        sequence: u64,
        sent_at: u64,
        request: MetricCollectionRequest,
    ) -> Self {
        Self::new(
            sequence,
            sent_at,
            ServerPayload::CollectSocketsOnce { request },
        )
    }

    /// 构造通用远程 job 应用请求 frame。
    pub fn job_apply(sequence: u64, sent_at: u64, request: RemoteJobApplyRequest) -> Self {
        Self::new(sequence, sent_at, ServerPayload::JobApply { request })
    }

    /// 构造远程 shell 打开请求 frame。
    pub fn remote_shell_open(sequence: u64, sent_at: u64, request: RemoteShellOpenRequest) -> Self {
        Self::new(
            sequence,
            sent_at,
            ServerPayload::RemoteShellOpen { request },
        )
    }

    /// 构造远程非交互任务请求 frame。
    pub fn remote_task_run(sequence: u64, sent_at: u64, request: RemoteTaskRequest) -> Self {
        Self::new(sequence, sent_at, ServerPayload::RemoteTaskRun { request })
    }
}

/// server 发往 agent 的 payload。
///
/// 当前稳定 `ServerFrame` 只放需要协议级 `sequence` 和 agent `ack/error` 的命令。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerPayload {
    /// 请求 agent 发送完整快照。
    ///
    /// server 在缺少完整状态、delta 基准不匹配或手动刷新时使用。agent 会受
    /// `report.force_snapshot_min_interval` 限制，过快的重复请求可能被合并。
    SnapshotRequest {
        /// 请求原因。
        request: SnapshotRequest,
    },
    /// server 对 agent 消息的确认。
    ///
    /// 当前 agent 只记录日志，不依赖 server ack 驱动重发；server 可以先不实现下发 ack。
    Ack {
        /// 确认信息。
        ack: Ack,
    },
    /// server 返回协议级错误。
    ///
    /// 当前 agent 只记录日志，不会因为单条 server error 自动断开连接。
    Error {
        /// 错误信息。
        error: ProtocolError,
    },
    /// 下发动态配置 patch。
    ///
    /// 这里使用通用 JSON 值承载 patch，避免协议 crate 直接依赖 agent 内部配置类型。
    /// agent 会在 handler 层把它转换为当前版本的 `AgentConfigPatch`。
    ConfigPatch {
        /// 配置 patch JSON。
        patch: Value,
    },
    /// 请求 agent 立即采集一次进程信息。
    CollectProcessesOnce {
        /// 采集请求。
        request: MetricCollectionRequest,
    },
    /// 请求 agent 立即采集一次 Socket 信息。
    CollectSocketsOnce {
        /// 采集请求。
        request: MetricCollectionRequest,
    },
    /// 请求 agent 执行一次远程非交互任务。
    RemoteTaskRun {
        /// 远程任务请求。
        request: RemoteTaskRequest,
    },
    /// 请求 agent 应用通用远程 job。
    ///
    /// `once` 会在真实执行完成后回 `job_result`；`replace/patch` 只更新本地 job 调度状态。
    JobApply {
        /// job 应用请求。
        request: RemoteJobApplyRequest,
    },
    /// 请求 agent 打开一个交互式远程 shell stream。
    ///
    /// 该命令有协议级 sequence，因此 agent 接收并通过静态权限/动态配置校验后会先回
    /// `ack`；实际终端 IO 走 `request.stream_url` 指向的独立临时 WebSocket。
    RemoteShellOpen {
        /// shell 打开请求。
        request: RemoteShellOpenRequest,
    },
}
