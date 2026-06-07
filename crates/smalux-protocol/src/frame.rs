//! 传输无关的协议 frame。
//!
//! 本文件只描述 agent 和 server 都需要稳定理解的 JSON frame，不关心这些 frame 最后
//! 是通过 WebSocket binary、HTTP body 还是后续 gRPC message 发送。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use smalux_core::model::info::{
    AgentReport, CoreInfo, DiskInfo, IdentityInfo, MetricLevel, NetworkInfo, ProcessInfo,
    SocketInfo, Stamped,
};
use std::time::Duration;

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
            kind: OutboundReportKind::Snapshot {
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
            kind: OutboundReportKind::Delta {
                delta: Box::new(delta),
            },
        }
    }
}

/// agent 内部待导出的上报类型。
#[derive(Debug, Clone)]
pub enum OutboundReportKind {
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

    /// 构造远程网络探测请求 frame。
    pub fn remote_probe_run(sequence: u64, sent_at: u64, request: RemoteProbeRequest) -> Self {
        Self::new(sequence, sent_at, ServerPayload::RemoteProbeRun { request })
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
    /// 请求 agent 执行一次远程网络探测。
    ///
    /// 该命令有协议级 sequence，因此 agent 调度成功会先回 `ack`，真实探测完成后再回
    /// `remote_probe_result`；如果探测被禁用或限频，也会返回一个失败结果。
    RemoteProbeRun {
        /// 探测请求。
        request: RemoteProbeRequest,
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

/// 一次性诊断采集请求。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetricCollectionRequest {
    /// 本次采集级别；缺省使用 agent 当前配置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<MetricLevel>,
    /// 本次返回条数上限；缺省使用 agent 当前配置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
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

/// 远程 shell 打开请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteShellOpenRequest {
    /// 本次 shell 会话 ID，由 server 生成并在 stream 消息中回显。
    pub session_id: String,
    /// 本次 shell 会话使用的临时 WebSocket stream 地址。
    pub stream_url: String,
    /// 初始终端列数；缺省由 agent 使用平台默认值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cols: Option<u16>,
    /// 初始终端行数；缺省由 agent 使用平台默认值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u16>,
}

/// 远程 shell stream 数据编码。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteShellDataEncoding {
    /// UTF-8 文本。
    Utf8,
    /// base64 编码的原始字节。
    Base64,
}

/// shell stream 上 server 发给 agent 的消息。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteShellStreamCommand {
    /// 写入 PTY 输入。
    Input {
        /// 输入数据。
        data: String,
        /// 输入数据编码，缺省按 UTF-8 文本处理。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        encoding: Option<RemoteShellDataEncoding>,
    },
    /// 调整 PTY 终端尺寸。
    Resize {
        /// 终端列数。
        cols: u16,
        /// 终端行数。
        rows: u16,
    },
    /// 请求关闭 shell 会话。
    Close,
    /// stream 保活消息，不产生 PTY 输入。
    Heartbeat,
}

/// shell stream 上 agent 发给 server 的消息。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteShellStreamEvent {
    /// shell 会话已经启动。
    Opened {
        /// shell 会话 ID。
        session_id: String,
    },
    /// PTY 输出。
    Output {
        /// shell 会话 ID。
        session_id: String,
        /// 输出数据，当前使用 base64 保留原始字节。
        data: String,
        /// 输出数据编码。
        encoding: RemoteShellDataEncoding,
    },
    /// shell 进程退出。
    Exit {
        /// shell 会话 ID。
        session_id: String,
        /// 退出码；被系统信号或强制关闭时可能为空。
        code: Option<i32>,
    },
    /// shell 会话错误。
    Error {
        /// shell 会话 ID。
        session_id: String,
        /// 错误信息。
        message: String,
    },
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

/// 远程非交互任务请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteTaskRequest {
    /// server 侧生成的任务 ID。
    pub task_id: String,
    /// 要执行的程序路径或程序名。
    pub program: String,
    /// 直接传给程序的参数；agent 不做 shell 拼接。
    #[serde(default)]
    pub args: Vec<String>,
    /// 本次任务期望超时；agent 会限制在当前配置允许范围内。
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout: Option<Duration>,
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

    /// 验证 server remote shell open frame 会带协议版本。
    #[test]
    fn server_remote_shell_open_uses_protocol_version() {
        let frame = ServerFrame::remote_shell_open(
            5,
            100,
            RemoteShellOpenRequest {
                session_id: "shell-1".to_string(),
                stream_url: "wss://example.com/shell/shell-1".to_string(),
                cols: Some(120),
                rows: Some(30),
            },
        );

        assert_eq!(frame.protocol_version, SMALUX_PROTOCOL_VERSION);
        assert_eq!(frame.sequence, 5);
        match frame.payload {
            ServerPayload::RemoteShellOpen { request } => {
                assert_eq!(request.session_id, "shell-1");
                assert_eq!(request.cols, Some(120));
            }
            _ => panic!("expected remote shell open payload"),
        }
    }

    /// 验证 shell stream command 使用稳定 snake_case JSON。
    #[test]
    fn remote_shell_stream_command_roundtrips_json() {
        let command = RemoteShellStreamCommand::Input {
            data: "echo ok\n".to_string(),
            encoding: Some(RemoteShellDataEncoding::Utf8),
        };

        let json = serde_json::to_string(&command).unwrap();
        let decoded: RemoteShellStreamCommand = serde_json::from_str(&json).unwrap();

        assert!(json.contains(r#""type":"input""#));
        assert_eq!(decoded, command);
    }

    /// 验证 shell stream event 会保留 base64 输出编码字段。
    #[test]
    fn remote_shell_stream_event_roundtrips_json() {
        let event = RemoteShellStreamEvent::Output {
            session_id: "shell-1".to_string(),
            data: "aGVsbG8=".to_string(),
            encoding: RemoteShellDataEncoding::Base64,
        };

        let json = serde_json::to_string(&event).unwrap();
        let decoded: RemoteShellStreamEvent = serde_json::from_str(&json).unwrap();

        assert!(json.contains(r#""encoding":"base64""#));
        assert_eq!(decoded, event);
    }

    /// 验证 shell exit 事件会显式保留 null 退出码。
    #[test]
    fn remote_shell_exit_event_keeps_null_code() {
        let event = RemoteShellStreamEvent::Exit {
            session_id: "shell-1".to_string(),
            code: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        let decoded: RemoteShellStreamEvent = serde_json::from_str(&json).unwrap();

        assert!(json.contains(r#""code":null"#));
        assert_eq!(decoded, event);
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
