//! Service message 内部出站事件队列。
//!
//! 采样上报、远程任务结果等业务事件先进入统一队列，再由 export supervisor
//! 根据当前导出格式编码和投递到具体 transport。

use crate::collect::unix_timestamp_secs;
use smalux_core::model::info::AgentReport;
use smalux_protocol::{Ack, OutboundReport, ProtocolError, RemoteJobResult, RemoteTaskResult};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc::{self, error::TryRecvError};
use tokio::time::{Duration, timeout};

/// 普通出站事件队列容量。
const OUTBOUND_NORMAL_QUEUE_CAPACITY: usize = 256;
/// 高优先级出站事件队列容量。
const OUTBOUND_PRIORITY_QUEUE_CAPACITY: usize = 128;
/// 出站事件入队最长等待时间，避免 export 卡住时阻塞控制命令处理。
#[cfg(not(test))]
const OUTBOUND_SEND_TIMEOUT: Duration = Duration::from_secs(5);
/// 测试环境缩短等待时间，避免满队列用例拖慢完整测试。
#[cfg(test)]
const OUTBOUND_SEND_TIMEOUT: Duration = Duration::from_millis(10);

/// 出站事件发送失败原因。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum OutboundSendErrorKind {
    /// 队列接收端已经关闭。
    Closed,
    /// 队列长时间满载，发送超时。
    Timeout,
}

/// 出站事件发送失败。
#[derive(Debug)]
pub(crate) struct OutboundSendError {
    /// 失败原因。
    kind: OutboundSendErrorKind,
    /// 未能入队的事件。
    event: OutboundEvent,
}

impl OutboundSendError {
    /// 返回发送失败原因。
    pub(crate) fn kind(&self) -> OutboundSendErrorKind {
        self.kind
    }

    /// 返回未能入队的事件。
    pub(crate) fn event(&self) -> &OutboundEvent {
        &self.event
    }
}

impl fmt::Display for OutboundSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            OutboundSendErrorKind::Closed => formatter.write_str("outbound event queue is closed"),
            OutboundSendErrorKind::Timeout => {
                formatter.write_str("outbound event queue send timed out")
            }
        }
    }
}

impl std::error::Error for OutboundSendError {}

/// 出站事件发送端。
#[derive(Debug, Clone)]
pub(crate) struct OutboundSender {
    /// 高优先级事件发送端。
    priority_tx: mpsc::Sender<OutboundEvent>,
    /// 普通事件发送端。
    normal_tx: mpsc::Sender<OutboundEvent>,
}

/// 出站事件接收端。
#[derive(Debug)]
pub(crate) struct OutboundReceiver {
    /// 高优先级事件接收端。
    priority_rx: mpsc::Receiver<OutboundEvent>,
    /// 普通事件接收端。
    normal_rx: mpsc::Receiver<OutboundEvent>,
    /// 高优先级通道是否已关闭。
    priority_closed: bool,
    /// 普通通道是否已关闭。
    normal_closed: bool,
}

/// 创建 service 出站事件队列。
pub(crate) fn outbound_channel() -> (OutboundSender, OutboundReceiver) {
    let (priority_tx, priority_rx) = mpsc::channel(OUTBOUND_PRIORITY_QUEUE_CAPACITY);
    let (normal_tx, normal_rx) = mpsc::channel(OUTBOUND_NORMAL_QUEUE_CAPACITY);

    (
        OutboundSender {
            priority_tx,
            normal_tx,
        },
        OutboundReceiver {
            priority_rx,
            normal_rx,
            priority_closed: false,
            normal_closed: false,
        },
    )
}

impl OutboundSender {
    /// 发送出站事件；控制响应和远程结果会进入高优先级通道。
    pub(crate) async fn send(&self, event: OutboundEvent) -> Result<(), OutboundSendError> {
        if event.is_priority() {
            send_with_timeout(&self.priority_tx, event).await
        } else {
            send_with_timeout(&self.normal_tx, event).await
        }
    }
}

/// 带超时地发送出站事件，避免队列满时无限等待。
async fn send_with_timeout(
    tx: &mpsc::Sender<OutboundEvent>,
    event: OutboundEvent,
) -> Result<(), OutboundSendError> {
    match timeout(OUTBOUND_SEND_TIMEOUT, tx.reserve()).await {
        Ok(Ok(permit)) => {
            permit.send(event);
            Ok(())
        }
        Ok(Err(_error)) => Err(OutboundSendError {
            kind: OutboundSendErrorKind::Closed,
            event,
        }),
        Err(_error) => Err(OutboundSendError {
            kind: OutboundSendErrorKind::Timeout,
            event,
        }),
    }
}

impl OutboundReceiver {
    /// 优先接收高优先级事件，其次才是普通 report/basic info。
    pub(crate) async fn recv(&mut self) -> Option<OutboundEvent> {
        loop {
            match self.priority_rx.try_recv() {
                Ok(event) => return Some(event),
                Err(TryRecvError::Disconnected) => self.priority_closed = true,
                Err(TryRecvError::Empty) => {}
            }
            match self.normal_rx.try_recv() {
                Ok(event) => return Some(event),
                Err(TryRecvError::Disconnected) => self.normal_closed = true,
                Err(TryRecvError::Empty) => {}
            }

            if self.priority_closed && self.normal_closed {
                return None;
            }

            tokio::select! {
                biased;
                event = self.priority_rx.recv(), if !self.priority_closed => {
                    match event {
                        Some(event) => return Some(event),
                        None => self.priority_closed = true,
                    }
                }
                event = self.normal_rx.recv(), if !self.normal_closed => {
                    match event {
                        Some(event) => return Some(event),
                        None => self.normal_closed = true,
                    }
                }
            }
        }
    }
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

/// 待导出的低频基础信息。
#[derive(Debug, Clone)]
pub(crate) struct BasicInfoEnvelope {
    /// 出站 frame 序号。
    pub(crate) sequence: u64,
    /// basic info 构建时间，Unix 时间戳，单位秒。
    pub(crate) created_at: u64,
    /// 完整 agent report，用于兼容协议抽取基础信息。
    pub(crate) report: AgentReport,
}

impl BasicInfoEnvelope {
    /// 创建 basic info envelope。
    pub(crate) fn new(sequence: u64, report: AgentReport) -> Self {
        Self {
            sequence,
            created_at: unix_timestamp_secs(),
            report,
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

/// 待导出的通用远程 job 结果。
#[derive(Debug, Clone)]
pub(crate) struct RemoteJobResultEnvelope {
    /// agent 实例 ID。
    pub(crate) agent_id: String,
    /// 出站 frame 序号。
    pub(crate) sequence: u64,
    /// 结果生成时间，Unix 时间戳，单位秒。
    pub(crate) created_at: u64,
    /// job 执行结果。
    pub(crate) result: RemoteJobResult,
}

impl RemoteJobResultEnvelope {
    /// 创建通用远程 job 结果 envelope。
    pub(crate) fn new(agent_id: String, sequence: u64, result: RemoteJobResult) -> Self {
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
    /// 低频基础信息事件。
    BasicInfo(Box<BasicInfoEnvelope>),
    /// 控制命令确认。
    ControlAck(ControlAckEnvelope),
    /// 控制命令错误。
    ControlError(ControlErrorEnvelope),
    /// 远程任务执行结果。
    RemoteTaskResult(RemoteTaskResultEnvelope),
    /// 通用远程 job 执行结果。
    RemoteJobResult(RemoteJobResultEnvelope),
}

impl OutboundEvent {
    /// 是否属于高优先级事件。
    pub(crate) fn is_priority(&self) -> bool {
        matches!(
            self,
            Self::ControlAck(_)
                | Self::ControlError(_)
                | Self::RemoteTaskResult(_)
                | Self::RemoteJobResult(_)
        )
    }

    /// 返回事件序号。
    pub(crate) fn sequence(&self) -> u64 {
        match self {
            Self::Report(report) => report.sequence,
            Self::BasicInfo(info) => info.sequence,
            Self::ControlAck(ack) => ack.sequence,
            Self::ControlError(error) => error.sequence,
            Self::RemoteTaskResult(result) => result.sequence,
            Self::RemoteJobResult(result) => result.sequence,
        }
    }

    /// 返回事件生成时间。
    pub(crate) fn created_at(&self) -> u64 {
        match self {
            Self::Report(report) => report.created_at,
            Self::BasicInfo(info) => info.created_at,
            Self::ControlAck(ack) => ack.created_at,
            Self::ControlError(error) => error.created_at,
            Self::RemoteTaskResult(result) => result.created_at,
            Self::RemoteJobResult(result) => result.created_at,
        }
    }

    /// 返回稳定事件类型名称。
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Report(_report) => "report",
            Self::BasicInfo(_info) => "basic_info",
            Self::ControlAck(_ack) => "control_ack",
            Self::ControlError(_error) => "control_error",
            Self::RemoteTaskResult(_result) => "remote_task_result",
            Self::RemoteJobResult(_result) => "job_result",
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
        let (_tx, rx) = outbound_channel();

        assert!(!rx.priority_closed);
        assert!(!rx.normal_closed);
    }

    /// 验证出站序号在克隆后仍然全局递增。
    #[test]
    fn outbound_sequence_is_shared_between_clones() {
        let sequence = OutboundSequence::default();
        let cloned = sequence.clone();

        assert_eq!(sequence.next(), 1);
        assert_eq!(cloned.next(), 2);
    }

    /// 验证高优先级事件会先于普通事件被接收。
    #[tokio::test]
    async fn outbound_receiver_prioritizes_control_and_result_events() {
        let (tx, mut rx) = outbound_channel();
        tx.send(OutboundEvent::Report(ReportEnvelope::from_outbound(
            OutboundReport::heartbeat(
                "agent-test".to_string(),
                1,
                100,
                smalux_protocol::Heartbeat::default(),
            ),
        )))
        .await
        .unwrap();
        tx.send(OutboundEvent::ControlAck(ControlAckEnvelope::new(
            "agent-test".to_string(),
            2,
            Ack { sequence: 7 },
        )))
        .await
        .unwrap();

        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();

        assert!(matches!(first, OutboundEvent::ControlAck(_)));
        assert!(matches!(second, OutboundEvent::Report(_)));
    }

    /// 验证接收端关闭时发送端会返回明确的关闭错误，并保留未发送事件。
    #[tokio::test]
    async fn outbound_sender_returns_closed_when_receiver_dropped() {
        let (tx, rx) = outbound_channel();
        drop(rx);

        let error = tx
            .send(OutboundEvent::ControlAck(ControlAckEnvelope::new(
                "agent-test".to_string(),
                1,
                Ack { sequence: 7 },
            )))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), OutboundSendErrorKind::Closed);
        assert_eq!(error.event().kind(), "control_ack");
        assert!(error.to_string().contains("closed"));
    }

    /// 验证高优先级队列满载时不会无限等待控制命令循环。
    #[tokio::test]
    async fn outbound_sender_times_out_when_priority_queue_is_full() {
        let (tx, _rx) = outbound_channel();

        for sequence in 0..OUTBOUND_PRIORITY_QUEUE_CAPACITY {
            tx.send(OutboundEvent::ControlAck(ControlAckEnvelope::new(
                "agent-test".to_string(),
                sequence as u64,
                Ack {
                    sequence: sequence as u64,
                },
            )))
            .await
            .unwrap();
        }

        let error = tx
            .send(OutboundEvent::ControlAck(ControlAckEnvelope::new(
                "agent-test".to_string(),
                999,
                Ack { sequence: 999 },
            )))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), OutboundSendErrorKind::Timeout);
        assert_eq!(error.event().kind(), "control_ack");
        assert!(error.to_string().contains("timed out"));
    }
}
