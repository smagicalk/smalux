//! agent 发往 server 的 frame。

use super::{
    Ack, DeltaReport, Heartbeat, ProtocolError, RemoteJobResult, RemoteTaskResult,
    SMALUX_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use smalux_core::model::info::AgentReport;

/// agent 发往 server 的 frame。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientFrame {
    /// 协议版本，独立于 `AgentReport.meta.schema_version`。
    pub protocol_version: u16,
    /// agent 实例 ID，便于 server 在不解析 snapshot 时也能定位连接。
    pub agent_id: String,
    /// agent 本连接或本进程内递增的消息序号。
    pub sequence: u64,
    /// frame 发送时间，Unix 时间戳，单位秒。
    pub sent_at: u64,
    /// 具体业务 payload。
    #[serde(flatten)]
    pub payload: ClientPayload,
}

impl ClientFrame {
    /// 用指定 payload 构造 smalux JSON frame。
    pub fn new(
        agent_id: impl Into<String>,
        sequence: u64,
        sent_at: u64,
        payload: ClientPayload,
    ) -> Self {
        Self {
            protocol_version: SMALUX_PROTOCOL_VERSION,
            agent_id: agent_id.into(),
            sequence,
            sent_at,
            payload,
        }
    }
}

/// agent 发往 server 的 payload。
///
/// server 侧应先根据顶层 `ClientFrame.type` 分发，再处理对应 payload。`snapshot` 是
/// 完整状态，`delta` 是采样组级替换语义，`heartbeat` 只更新在线状态；控制结果类
/// payload 用来关联 server 之前下发的命令或任务。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientPayload {
    /// 完整监控快照。
    Snapshot {
        /// 完整 `AgentReport`。
        report: Box<AgentReport>,
    },
    /// 低成本在线心跳。
    Heartbeat {
        /// 心跳附加状态。
        heartbeat: Heartbeat,
    },
    /// 增量监控上报。
    Delta {
        /// 增量上报内容。
        delta: Box<DeltaReport>,
    },
    /// agent 对 server 消息的确认。
    Ack {
        /// 确认信息。
        ack: Ack,
    },
    /// agent 返回协议级错误。
    Error {
        /// 错误信息。
        error: ProtocolError,
    },
    /// 远程非交互任务执行结果。
    RemoteTaskResult {
        /// 任务执行结果。
        result: RemoteTaskResult,
    },
    /// 通用远程 job 执行结果。
    JobResult {
        /// job 执行结果。
        result: RemoteJobResult,
    },
}
