//! 传输无关的协议 frame。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use smalux_core::model::info::{
    AgentReport, CoreInfo, DiskInfo, IdentityInfo, NetworkInfo, ProcessInfo, SocketInfo, Stamped,
};

/// Smalux wire protocol 当前版本。
pub const SMALUX_PROTOCOL_VERSION: u16 = 1;

/// agent 内部待导出的上报语义。
///
/// 该结构不是最终 wire JSON；不同导出格式可以把它编码成不同外层。
#[derive(Debug, Clone)]
pub struct OutboundReport {
    /// agent 实例 ID。
    pub agent_id: String,
    /// agent 本连接或本进程内递增的消息序号。
    pub sequence: u64,
    /// 上报创建时间，Unix 时间戳，单位秒。
    pub created_at: u64,
    /// 上报语义。
    pub kind: OutboundReportKind,
}

impl OutboundReport {
    /// 构造完整快照上报。
    pub fn snapshot(sequence: u64, created_at: u64, report: AgentReport) -> Self {
        Self {
            agent_id: report.identity.agent_id.clone(),
            sequence,
            created_at,
            kind: OutboundReportKind::Snapshot { report },
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
            kind: OutboundReportKind::Heartbeat { heartbeat },
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
            kind: OutboundReportKind::Delta { delta },
        }
    }
}

/// agent 内部待导出的上报类型。
#[derive(Debug, Clone)]
pub enum OutboundReportKind {
    /// 完整监控快照。
    Snapshot {
        /// 完整 `AgentReport`。
        report: AgentReport,
    },
    /// 低成本在线心跳。
    Heartbeat {
        /// 心跳附加状态。
        heartbeat: Heartbeat,
    },
    /// 相对上一份完整快照或 delta 的增量变更。
    Delta {
        /// 增量上报内容。
        delta: DeltaReport,
    },
    /// agent 对 server 消息的确认。
    Ack {
        /// 确认信息。
        ack: Ack,
    },
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientPayload {
    /// 完整监控快照。
    Snapshot {
        /// 完整 `AgentReport`。
        report: AgentReport,
    },
    /// 低成本在线心跳。
    Heartbeat {
        /// 心跳附加状态。
        heartbeat: Heartbeat,
    },
    /// 增量监控上报。
    Delta {
        /// 增量上报内容。
        delta: DeltaReport,
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
    /// 远程网络探测结果。
    RemoteProbeResult {
        /// 探测执行结果。
        result: RemoteProbeResult,
    },
}

/// server 发往 agent 的 frame。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerFrame {
    /// 协议版本，独立于业务 payload 版本。
    pub protocol_version: u16,
    /// server 侧递增的消息序号。
    pub sequence: u64,
    /// frame 发送时间，Unix 时间戳，单位秒。
    pub sent_at: u64,
    /// 具体业务 payload。
    #[serde(flatten)]
    pub payload: ServerPayload,
}

impl ServerFrame {
    /// 构造请求完整快照的 frame。
    pub fn snapshot_request(sequence: u64, sent_at: u64, request: SnapshotRequest) -> Self {
        Self {
            protocol_version: SMALUX_PROTOCOL_VERSION,
            sequence,
            sent_at,
            payload: ServerPayload::SnapshotRequest { request },
        }
    }

    /// 构造远程网络探测请求 frame。
    pub fn remote_probe_run(sequence: u64, sent_at: u64, request: RemoteProbeRequest) -> Self {
        Self {
            protocol_version: SMALUX_PROTOCOL_VERSION,
            sequence,
            sent_at,
            payload: ServerPayload::RemoteProbeRun { request },
        }
    }
}

/// server 发往 agent 的 payload。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerPayload {
    /// 请求 agent 发送完整快照。
    SnapshotRequest {
        /// 请求原因。
        request: SnapshotRequest,
    },
    /// server 对 agent 消息的确认。
    Ack {
        /// 确认信息。
        ack: Ack,
    },
    /// server 返回协议级错误。
    Error {
        /// 错误信息。
        error: ProtocolError,
    },
    /// 请求 agent 执行一次远程网络探测。
    RemoteProbeRun {
        /// 探测请求。
        request: RemoteProbeRequest,
    },
}

/// 心跳附加状态。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Heartbeat {
    /// agent 最近一次完整 report 时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_report_at: Option<u64>,
    /// agent 最近一次完整 report 序号。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_report_sequence: Option<u64>,
}

/// 增量上报内容。
///
/// 字段缺省表示该字段相对上一份状态没有变化；可选采集组字段出现 `null` 表示该组被关闭。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaReport {
    /// 增量基于的上一条消息序号。
    pub base_sequence: u64,
    /// 本次增量生成时间，Unix 时间戳，单位秒。
    pub report_at: u64,
    /// 身份信息变化。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityInfo>,
    /// 核心指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core: Option<Option<Stamped<CoreInfo>>>,
    /// 磁盘指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk: Option<Option<Stamped<DiskInfo>>>,
    /// 网络指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<Option<Stamped<NetworkInfo>>>,
    /// 进程汇总指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processes: Option<Option<Stamped<ProcessInfo>>>,
    /// Socket 汇总指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sockets: Option<Option<Stamped<SocketInfo>>>,
}

/// 请求完整快照。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// 请求原因，便于 agent 日志定位。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// 协议确认信息。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Ack {
    /// 被确认的消息序号。
    pub sequence: u64,
}

/// 远程非交互任务执行状态。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteTaskStatus {
    /// 命令执行成功，退出码为 0。
    Success,
    /// 命令执行完成，但退出码非 0 或启动失败。
    Failed,
    /// 命令超过 agent 配置的最大运行时间。
    TimedOut,
    /// agent 拒绝执行，例如能力未开启或并发已满。
    Rejected,
}

/// 远程非交互任务执行结果。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteTaskResult {
    /// server 下发的任务 ID。
    pub task_id: String,
    /// 执行状态。
    pub status: RemoteTaskStatus,
    /// 进程退出码；启动失败、超时或拒绝执行时为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// 标准输出，超过 agent 配置上限会被截断。
    pub stdout: String,
    /// 标准错误，超过 agent 配置上限会被截断。
    pub stderr: String,
    /// 开始执行时间，Unix 时间戳，单位秒。
    pub started_at: u64,
    /// 完成时间，Unix 时间戳，单位秒。
    pub finished_at: u64,
    /// 执行耗时，单位毫秒。
    pub duration_ms: u64,
    /// 是否因为超时结束。
    pub timed_out: bool,
    /// 标准输出是否被截断。
    pub stdout_truncated: bool,
    /// 标准错误是否被截断。
    pub stderr_truncated: bool,
    /// 面向日志和调试的错误说明。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 远程网络探测类型。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteProbeType {
    /// TCP 连接耗时探测。
    Tcp,
    /// HTTP/HTTPS 请求耗时探测。
    Http,
    /// ICMP 探测。
    Icmp,
}

impl RemoteProbeType {
    /// 返回稳定字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Http => "http",
            Self::Icmp => "icmp",
        }
    }
}

/// 远程网络探测请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeRequest {
    /// server 侧生成的探测任务 ID；兼容协议可能使用数字或字符串。
    pub task_id: Value,
    /// 探测类型。
    pub probe_type: RemoteProbeType,
    /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
    pub target: String,
}

/// 远程网络探测结果。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeResult {
    /// server 侧生成的探测任务 ID。
    pub task_id: Value,
    /// 探测类型。
    pub probe_type: RemoteProbeType,
    /// 探测目标。
    pub target: String,
    /// 成功时为耗时毫秒，失败、禁用或限频时为 -1。
    pub value: i64,
    /// 开始时间，Unix 时间戳，单位秒。
    pub started_at: u64,
    /// 完成时间，Unix 时间戳，单位秒。
    pub finished_at: u64,
    /// 执行耗时，单位毫秒。
    pub duration_ms: u64,
    /// 失败、禁用或限频原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// 协议级错误。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtocolError {
    /// 被错误关联的对端消息序号。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// 稳定错误码。
    pub code: String,
    /// 面向日志和调试的错误说明。
    pub message: String,
}

#[cfg(test)]
mod tests {
    //! frame 构造测试。

    use super::*;

    /// 验证 snapshot 上报会从 report 中复制 agent_id。
    #[test]
    fn outbound_snapshot_uses_report_agent_id() {
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();

        let outbound = OutboundReport::snapshot(7, 100, report);

        assert_eq!(outbound.agent_id, "agent-1");
        assert_eq!(outbound.sequence, 7);
        assert_eq!(outbound.created_at, 100);
    }

    /// 验证 server snapshot request frame 会带协议版本。
    #[test]
    fn server_snapshot_request_uses_protocol_version() {
        let frame = ServerFrame::snapshot_request(
            3,
            100,
            SnapshotRequest {
                reason: Some("delta base missing".to_string()),
            },
        );

        assert_eq!(frame.protocol_version, SMALUX_PROTOCOL_VERSION);
        assert_eq!(frame.sequence, 3);
    }

    /// 验证 server remote probe request frame 会带协议版本。
    #[test]
    fn server_remote_probe_run_uses_protocol_version() {
        let frame = ServerFrame::remote_probe_run(
            4,
            100,
            RemoteProbeRequest {
                task_id: Value::from(7),
                probe_type: RemoteProbeType::Tcp,
                target: "example.com:443".to_string(),
            },
        );

        assert_eq!(frame.protocol_version, SMALUX_PROTOCOL_VERSION);
        assert_eq!(frame.sequence, 4);
    }

    /// 验证 delta 上报会保留 agent_id 和基准序号。
    #[test]
    fn outbound_delta_uses_given_agent_id_and_base_sequence() {
        let delta = DeltaReport {
            base_sequence: 7,
            report_at: 100,
            ..DeltaReport::default()
        };

        let outbound = OutboundReport::delta("agent-1", 8, 101, delta);

        assert_eq!(outbound.agent_id, "agent-1");
        assert_eq!(outbound.sequence, 8);
        match outbound.kind {
            OutboundReportKind::Delta { delta } => {
                assert_eq!(delta.base_sequence, 7);
            }
            _ => panic!("expected delta report"),
        }
    }
}
