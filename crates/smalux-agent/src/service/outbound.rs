//! Service 内部出站事件队列。
//!
//! 采样上报、远程任务结果等业务事件先进入统一队列，再由 export supervisor
//! 根据当前导出格式编码和投递到具体 transport。

use crate::collect::unix_timestamp_secs;
use smalux_protocol::{Ack, OutboundReport, ProtocolError, RemoteProbeResult, RemoteTaskResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

/// 出站事件队列默认容量。
const OUTBOUND_EVENT_QUEUE_CAPACITY: usize = 256;

/// 出站事件发送端。
pub(crate) type OutboundSender = mpsc::Sender<OutboundEvent>;
/// 出站事件接收端。
pub(crate) type OutboundReceiver = mpsc::Receiver<OutboundEvent>;

/// 创建 service 出站事件队列。
pub(crate) fn outbound_channel() -> (OutboundSender, OutboundReceiver) {
    mpsc::channel(OUTBOUND_EVENT_QUEUE_CAPACITY)
}

/// 全局出站 frame 序号分配器。
#[derive(Debug, Clone, Default)]
pub(crate) struct OutboundSequence {
    /// 最近一次已经分配的序号。
    current: Arc<AtomicU64>,
}

impl OutboundSequence {
    /// 分配下一条出站 frame 序号。
    pub(crate) fn next(&self) -> u64 {
        self.current
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }
}

/// 待导出的监控上报。
#[derive(Debug, Clone)]
pub(crate) struct ReportEnvelope {
    /// 单调递增序号，用于导出端排序和 server 侧 delta 基准检查。
    pub(crate) sequence: u64,
    /// report 构建时间，Unix 时间戳，单位秒。
    pub(crate) created_at: u64,
    /// 待导出的内部上报语义。
    pub(crate) outbound: OutboundReport,
}

impl ReportEnvelope {
    /// 从协议上报构造 report envelope。
    pub(crate) fn from_outbound(outbound: OutboundReport) -> Self {
        Self {
            sequence: outbound.sequence,
            created_at: outbound.created_at,
            outbound,
        }
    }
}

/// 待导出的远程任务结果。
#[derive(Debug, Clone)]
pub(crate) struct RemoteTaskResultEnvelope {
    /// agent 实例 ID。
    pub(crate) agent_id: String,
    /// 出站 frame 序号。
    pub(crate) sequence: u64,
    /// 结果生成时间，Unix 时间戳，单位秒。
    pub(crate) created_at: u64,
    /// 任务执行结果。
    pub(crate) result: RemoteTaskResult,
}

impl RemoteTaskResultEnvelope {
    /// 创建远程任务结果 envelope。
    pub(crate) fn new(agent_id: String, sequence: u64, result: RemoteTaskResult) -> Self {
        Self {
            agent_id,
            sequence,
            created_at: unix_timestamp_secs(),
            result,
        }
    }
}

/// 待导出的远程探测结果。
#[derive(Debug, Clone)]
pub(crate) struct RemoteProbeResultEnvelope {
    /// agent 实例 ID。
    pub(crate) agent_id: String,
    /// 出站 frame 序号。
    pub(crate) sequence: u64,
    /// 结果生成时间，Unix 时间戳，单位秒。
    pub(crate) created_at: u64,
    /// 探测执行结果。
    pub(crate) result: RemoteProbeResult,
}

impl RemoteProbeResultEnvelope {
    /// 创建远程探测结果 envelope。
    pub(crate) fn new(agent_id: String, sequence: u64, result: RemoteProbeResult) -> Self {
        Self {
            agent_id,
            sequence,
            created_at: unix_timestamp_secs(),
            result,
        }
    }
}

/// 待导出的控制命令确认。
#[derive(Debug, Clone)]
pub(crate) struct ControlAckEnvelope {
    /// agent 实例 ID。
    pub(crate) agent_id: String,
    /// 出站 frame 序号。
    pub(crate) sequence: u64,
    /// 结果生成时间，Unix 时间戳，单位秒。
    pub(crate) created_at: u64,
    /// 确认信息。
    pub(crate) ack: Ack,
}

impl ControlAckEnvelope {
    /// 创建控制命令确认 envelope。
    pub(crate) fn new(agent_id: String, sequence: u64, ack: Ack) -> Self {
        Self {
            agent_id,
            sequence,
            created_at: unix_timestamp_secs(),
            ack,
        }
    }
}

/// 待导出的控制命令错误。
#[derive(Debug, Clone)]
pub(crate) struct ControlErrorEnvelope {
    /// agent 实例 ID。
    pub(crate) agent_id: String,
    /// 出站 frame 序号。
    pub(crate) sequence: u64,
    /// 结果生成时间，Unix 时间戳，单位秒。
    pub(crate) created_at: u64,
    /// 协议错误信息。
    pub(crate) error: ProtocolError,
}

impl ControlErrorEnvelope {
    /// 创建控制命令错误 envelope。
    pub(crate) fn new(agent_id: String, sequence: u64, error: ProtocolError) -> Self {
        Self {
            agent_id,
            sequence,
            created_at: unix_timestamp_secs(),
            error,
        }
    }
}

/// 出站业务事件。
#[derive(Debug, Clone)]
pub(crate) enum OutboundEvent {
    /// 监控上报事件。
    Report(ReportEnvelope),
    /// 控制命令确认。
    ControlAck(ControlAckEnvelope),
    /// 控制命令错误。
    ControlError(ControlErrorEnvelope),
    /// 远程任务执行结果。
    RemoteTaskResult(RemoteTaskResultEnvelope),
    /// 远程网络探测结果。
    RemoteProbeResult(RemoteProbeResultEnvelope),
}

impl OutboundEvent {
    /// 返回事件序号。
    pub(crate) fn sequence(&self) -> u64 {
        match self {
            Self::Report(report) => report.sequence,
            Self::ControlAck(ack) => ack.sequence,
            Self::ControlError(error) => error.sequence,
            Self::RemoteTaskResult(result) => result.sequence,
            Self::RemoteProbeResult(result) => result.sequence,
        }
    }

    /// 返回事件生成时间。
    pub(crate) fn created_at(&self) -> u64 {
        match self {
            Self::Report(report) => report.created_at,
            Self::ControlAck(ack) => ack.created_at,
            Self::ControlError(error) => error.created_at,
            Self::RemoteTaskResult(result) => result.created_at,
            Self::RemoteProbeResult(result) => result.created_at,
        }
    }

    /// 返回稳定事件类型名称。
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Report(_report) => "report",
            Self::ControlAck(_ack) => "control_ack",
            Self::ControlError(_error) => "control_error",
            Self::RemoteTaskResult(_result) => "remote_task_result",
            Self::RemoteProbeResult(_result) => "remote_probe_result",
        }
    }
}

#[cfg(test)]
mod tests {
    //! 出站事件队列测试。

    use super::*;

    /// 验证出站队列是有界队列。
    #[test]
    fn outbound_channel_is_bounded() {
        let (tx, _rx) = outbound_channel();

        assert_eq!(tx.max_capacity(), OUTBOUND_EVENT_QUEUE_CAPACITY);
    }

    /// 验证出站序号在克隆后仍然全局递增。
    #[test]
    fn outbound_sequence_is_shared_between_clones() {
        let sequence = OutboundSequence::default();
        let cloned = sequence.clone();

        assert_eq!(sequence.next(), 1);
        assert_eq!(cloned.next(), 2);
    }
}
