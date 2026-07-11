//! agent 内部待导出的上报语义。

use super::{Ack, DeltaReport, Heartbeat};
use smalux_core::model::info::AgentReport;

/// agent 内部待导出的 client 事件。
///
/// 该结构不是最终 wire JSON；不同导出格式可以把它编码成不同外层。
#[derive(Debug, Clone)]
pub struct ClientEvent {
    /// agent 实例 ID。
    pub agent_id: String,
    /// agent 本连接或本进程内递增的消息序号。
    pub sequence: u64,
    /// 上报创建时间，Unix 时间戳，单位秒。
    pub created_at: u64,
    /// client 事件语义。
    pub kind: ClientEventKind,
}

impl ClientEvent {
    /// 构造完整快照上报。
    pub fn snapshot(sequence: u64, created_at: u64, report: AgentReport) -> Self {
        Self {
            agent_id: report.identity.agent_id.clone(),
            sequence,
            created_at,
            kind: ClientEventKind::Snapshot {
                report: Box::new(report),
            },
        }
    }

    /// 构造心跳上报。
    pub fn heartbeat(
        agent_id: impl Into<String>,
        sequence: u64,
        created_at: u64,
        heartbeat: Heartbeat,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            sequence,
            created_at,
            kind: ClientEventKind::Heartbeat { heartbeat },
        }
    }

    /// 构造增量上报。
    pub fn delta(
        agent_id: impl Into<String>,
        sequence: u64,
        created_at: u64,
        delta: DeltaReport,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            sequence,
            created_at,
            kind: ClientEventKind::Delta {
                delta: Box::new(delta),
            },
        }
    }
}

/// agent 内部待导出的 client 事件类型。
#[derive(Debug, Clone)]
pub enum ClientEventKind {
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
    /// 相对上一份完整快照或 delta 的增量变更。
    Delta {
        /// 增量上报内容。
        delta: Box<DeltaReport>,
    },
    /// agent 对 server 消息的确认。
    Ack {
        /// 确认信息。
        ack: Ack,
    },
}
