//! 传输无关的协议 frame。
//!
//! 本模块只描述 agent 和 server 都需要稳定理解的 JSON frame，不关心这些 frame 最后
//! 是通过 WebSocket binary、HTTP body 还是后续 gRPC message 发送。

/// agent 发往 server 的 frame。
mod client;
/// 协议确认和错误模型。
mod control;
/// agent 内部待导出的上报语义。
mod outbound;
/// 远程任务、网络探测和交互 shell 协议模型。
mod remote;
/// 监控上报和采集控制模型。
mod report;
/// server 发往 agent 的 frame。
mod server;
/// 协议版本常量。
mod version;

pub use self::client::{ClientFrame, ClientPayload};
pub use self::control::{Ack, ProtocolError};
pub use self::outbound::{OutboundReport, OutboundReportKind};
pub use self::remote::{
    RemoteProbeRequest, RemoteProbeResult, RemoteProbeType, RemoteShellDataEncoding,
    RemoteShellOpenRequest, RemoteShellStreamCommand, RemoteShellStreamEvent, RemoteTaskRequest,
    RemoteTaskResult, RemoteTaskStatus,
};
pub use self::report::{DeltaReport, Heartbeat, MetricCollectionRequest, SnapshotRequest};
pub use self::server::{ServerFrame, ServerPayload};
pub use self::version::SMALUX_PROTOCOL_VERSION;

#[cfg(test)]
mod tests {
    //! frame 构造测试。

    use super::*;
    use serde_json::Value;
    use smalux_core::model::info::AgentReport;

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
