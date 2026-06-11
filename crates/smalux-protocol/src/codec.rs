//! 协议 frame 的 JSON 编解码。
//!
//! codec 只负责 `ClientFrame` / `ServerFrame` 和 JSON 字节之间的转换，不处理
//! WebSocket binary wire、Noise 加密、HTTP 状态码或重连。这样 server 端可以复用
//! 同一套 decode 逻辑接入不同 transport。

use crate::frame::{
    Ack, ClientFrame, ClientPayload, OutboundReport, OutboundReportKind, ProtocolError,
    RemoteProbeResult, RemoteShellStreamCommand, RemoteShellStreamEvent, RemoteTaskResult,
    ServerFrame,
};

/// 将内部上报语义编码为 smalux 默认 JSON frame。
pub fn encode_outbound_report_as_smalux_json(
    outbound: &OutboundReport,
) -> serde_json::Result<String> {
    let frame = outbound_report_to_client_frame(outbound);
    encode_client_frame(&frame)
}

/// 将内部上报语义编码为 smalux 默认 JSON bytes。
///
/// WebSocket binary wire 使用这个 bytes 版本：transport 会把返回值继续封装成
/// `PlainData` 或 `SecureData`，而不是再做一次字符串转换。
pub fn encode_outbound_report_as_smalux_json_bytes(
    outbound: &OutboundReport,
) -> serde_json::Result<Vec<u8>> {
    let frame = outbound_report_to_client_frame(outbound);
    serde_json::to_vec(&frame)
}

/// 将远程任务结果编码为 smalux 默认 JSON bytes。
pub fn encode_remote_task_result_as_smalux_json_bytes(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    result: &RemoteTaskResult,
) -> serde_json::Result<Vec<u8>> {
    let frame = ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::RemoteTaskResult {
            result: result.clone(),
        },
    );
    serde_json::to_vec(&frame)
}

/// 将远程探测结果编码为 smalux 默认 JSON bytes。
pub fn encode_remote_probe_result_as_smalux_json_bytes(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    result: &RemoteProbeResult,
) -> serde_json::Result<Vec<u8>> {
    let frame = ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::RemoteProbeResult {
            result: result.clone(),
        },
    );
    serde_json::to_vec(&frame)
}

/// 将控制命令确认编码为 smalux 默认 JSON bytes。
pub fn encode_ack_as_smalux_json_bytes(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    ack: &Ack,
) -> serde_json::Result<Vec<u8>> {
    let frame = ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::Ack { ack: ack.clone() },
    );
    serde_json::to_vec(&frame)
}

/// 将协议错误编码为 smalux 默认 JSON bytes。
pub fn encode_protocol_error_as_smalux_json_bytes(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    error: &ProtocolError,
) -> serde_json::Result<Vec<u8>> {
    let frame = ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::Error {
            error: error.clone(),
        },
    );
    serde_json::to_vec(&frame)
}

/// 将内部上报语义转换为 client frame。
fn outbound_report_to_client_frame(outbound: &OutboundReport) -> ClientFrame {
    ClientFrame::new(
        outbound.agent_id.clone(),
        outbound.sequence,
        outbound.created_at,
        match outbound.kind.clone() {
            OutboundReportKind::Snapshot { report } => ClientPayload::Snapshot { report },
            OutboundReportKind::Heartbeat { heartbeat } => ClientPayload::Heartbeat { heartbeat },
            OutboundReportKind::Delta { delta } => ClientPayload::Delta { delta },
            OutboundReportKind::Ack { ack } => ClientPayload::Ack { ack },
        },
    )
}

/// 编码 agent 发往 server 的 frame。
pub fn encode_client_frame(frame: &ClientFrame) -> serde_json::Result<String> {
    serde_json::to_string(frame)
}

/// 解码 agent 发往 server 的 frame。
pub fn decode_client_frame(input: &str) -> serde_json::Result<ClientFrame> {
    serde_json::from_str(input)
}

/// 编码 server 发往 agent 的 frame。
pub fn encode_server_frame(frame: &ServerFrame) -> serde_json::Result<String> {
    serde_json::to_string(frame)
}

/// 解码 server 发往 agent 的 frame。
///
/// 自有协议下行只识别稳定 `ServerFrame`。第三方兼容消息应由各自 adapter 转换。
pub fn decode_server_frame(input: &str) -> serde_json::Result<ServerFrame> {
    serde_json::from_str(input)
}

/// 编码远程 shell stream 事件。
///
/// 这里仅生成业务 JSON；WebSocket transport 会继续按当前 wire mode 封装或加密。
pub fn encode_remote_shell_stream_event(
    event: &RemoteShellStreamEvent,
) -> serde_json::Result<String> {
    serde_json::to_string(event)
}

/// 解码远程 shell stream 命令。
///
/// server 发给 agent 的临时 shell stream payload 应先按 wire mode 解包，再交给本函数解析。
pub fn decode_remote_shell_stream_command(
    input: &str,
) -> serde_json::Result<RemoteShellStreamCommand> {
    serde_json::from_str(input)
}

#[cfg(test)]
mod tests {
    //! JSON codec 测试。

    use super::*;
    use crate::frame::{
        Ack, ClientPayload, DeltaReport, Heartbeat, MetricCollectionRequest, OutboundReport,
        ProtocolError, RemoteProbeId, RemoteProbeResult, RemoteProbeResultSource,
        RemoteProbeResultStatus, RemoteProbeType, RemoteShellDataEncoding, RemoteShellOpenRequest,
        RemoteShellStreamCommand, RemoteShellStreamEvent, RemoteTaskRequest, RemoteTaskResult,
        RemoteTaskStatus, ServerFrame, ServerPayload, SnapshotRequest,
    };
    use smalux_core::model::info::MetricLevel;
    use std::time::Duration;

    /// 验证 client heartbeat frame 可以往返 JSON。
    #[test]
    fn client_heartbeat_frame_roundtrips_json() {
        let outbound = OutboundReport::heartbeat(
            "agent-1",
            1,
            100,
            Heartbeat {
                last_report_at: Some(90),
                last_report_sequence: Some(7),
            },
        );

        let json = encode_outbound_report_as_smalux_json(&outbound).unwrap();
        let decoded = decode_client_frame(&json).unwrap();

        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 1);
        match decoded.payload {
            ClientPayload::Heartbeat { heartbeat } => {
                assert_eq!(heartbeat.last_report_at, Some(90));
                assert_eq!(heartbeat.last_report_sequence, Some(7));
            }
            _ => panic!("expected heartbeat payload"),
        }
    }

    /// 验证 client delta frame 可以往返 JSON。
    #[test]
    fn client_delta_frame_roundtrips_json() {
        let outbound = OutboundReport::delta(
            "agent-1",
            2,
            110,
            DeltaReport {
                base_sequence: 1,
                report_at: 109,
                ..DeltaReport::default()
            },
        );

        let json = encode_outbound_report_as_smalux_json(&outbound).unwrap();
        let decoded = decode_client_frame(&json).unwrap();

        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 2);
        match decoded.payload {
            ClientPayload::Delta { delta } => {
                assert_eq!(delta.base_sequence, 1);
                assert_eq!(delta.report_at, 109);
            }
            _ => panic!("expected delta payload"),
        }
    }

    /// 验证 server snapshot request frame 可以往返 JSON。
    #[test]
    fn server_snapshot_request_roundtrips_json() {
        let frame = ServerFrame::snapshot_request(
            2,
            100,
            SnapshotRequest {
                reason: Some("manual".to_string()),
            },
        );

        let json = encode_server_frame(&frame).unwrap();
        let decoded = decode_server_frame(&json).unwrap();

        assert_eq!(decoded.sequence, 2);
        match decoded.payload {
            ServerPayload::SnapshotRequest { request } => {
                assert_eq!(request.reason.as_deref(), Some("manual"));
            }
            _ => panic!("expected snapshot request payload"),
        }
    }

    /// 验证 server config_patch frame 可以往返 JSON，并保留目标 agent ID。
    #[test]
    fn server_config_patch_roundtrips_json() {
        let frame =
            ServerFrame::config_patch(3, 100, serde_json::json!({ "core": { "interval": "2s" } }))
                .with_target_agent_id("agent-1");

        let json = encode_server_frame(&frame).unwrap();
        let decoded = decode_server_frame(&json).unwrap();

        assert_eq!(decoded.sequence, 3);
        assert_eq!(decoded.target_agent_id.as_deref(), Some("agent-1"));
        match decoded.payload {
            ServerPayload::ConfigPatch { patch } => {
                assert_eq!(patch["core"]["interval"], "2s");
            }
            _ => panic!("expected config patch payload"),
        }
    }

    /// 验证一次性进程采集 frame 可以往返 JSON。
    #[test]
    fn server_collect_processes_once_roundtrips_json() {
        let frame = ServerFrame::collect_processes_once(
            4,
            100,
            MetricCollectionRequest {
                level: Some(MetricLevel::Details),
                limit: Some(5),
            },
        );

        let json = encode_server_frame(&frame).unwrap();
        let decoded = decode_server_frame(&json).unwrap();

        assert_eq!(decoded.sequence, 4);
        match decoded.payload {
            ServerPayload::CollectProcessesOnce { request } => {
                assert_eq!(request.level, Some(MetricLevel::Details));
                assert_eq!(request.limit, Some(5));
            }
            _ => panic!("expected collect processes once payload"),
        }
    }

    /// 验证一次性 Socket 采集 frame 可以往返 JSON。
    #[test]
    fn server_collect_sockets_once_roundtrips_json() {
        let frame = ServerFrame::collect_sockets_once(
            5,
            100,
            MetricCollectionRequest {
                level: Some(MetricLevel::Light),
                limit: Some(7),
            },
        );

        let json = encode_server_frame(&frame).unwrap();
        let decoded = decode_server_frame(&json).unwrap();

        assert_eq!(decoded.sequence, 5);
        match decoded.payload {
            ServerPayload::CollectSocketsOnce { request } => {
                assert_eq!(request.level, Some(MetricLevel::Light));
                assert_eq!(request.limit, Some(7));
            }
            _ => panic!("expected collect sockets once payload"),
        }
    }

    /// 验证远程非交互任务 frame 可以往返 JSON。
    #[test]
    fn server_remote_task_run_roundtrips_json() {
        let frame = ServerFrame::remote_task_run(
            6,
            100,
            RemoteTaskRequest {
                task_id: "task-1".to_string(),
                program: "echo".to_string(),
                args: vec!["ok".to_string()],
                timeout: Some(Duration::from_secs(3)),
            },
        );

        let json = encode_server_frame(&frame).unwrap();
        let decoded = decode_server_frame(&json).unwrap();

        assert_eq!(decoded.sequence, 6);
        match decoded.payload {
            ServerPayload::RemoteTaskRun { request } => {
                assert_eq!(request.task_id, "task-1");
                assert_eq!(request.program, "echo");
                assert_eq!(request.args, vec!["ok"]);
                assert_eq!(request.timeout, Some(Duration::from_secs(3)));
            }
            _ => panic!("expected remote task run payload"),
        }
    }

    /// 验证 server remote shell open frame 可以往返 JSON。
    #[test]
    fn server_remote_shell_open_roundtrips_json() {
        let frame = ServerFrame::remote_shell_open(
            3,
            100,
            RemoteShellOpenRequest {
                session_id: "shell-1".to_string(),
                stream_url: "wss://example.com/shell/shell-1".to_string(),
                cols: None,
                rows: None,
            },
        );

        let json = encode_server_frame(&frame).unwrap();
        let decoded = decode_server_frame(&json).unwrap();

        assert_eq!(decoded.sequence, 3);
        match decoded.payload {
            ServerPayload::RemoteShellOpen { request } => {
                assert_eq!(request.session_id, "shell-1");
                assert_eq!(request.stream_url, "wss://example.com/shell/shell-1");
            }
            _ => panic!("expected remote shell open payload"),
        }
    }

    /// 验证 shell stream command 可以通过 protocol codec 解码。
    #[test]
    fn remote_shell_stream_command_decodes_json() {
        let decoded = decode_remote_shell_stream_command(
            r#"{ "type": "input", "data": "echo ok\n", "encoding": "utf8" }"#,
        )
        .unwrap();

        assert_eq!(
            decoded,
            RemoteShellStreamCommand::Input {
                data: "echo ok\n".to_string(),
                encoding: Some(RemoteShellDataEncoding::Utf8),
            }
        );
    }

    /// 验证 shell stream event 可以通过 protocol codec 编码。
    #[test]
    fn remote_shell_stream_event_encodes_json() {
        let encoded = encode_remote_shell_stream_event(&RemoteShellStreamEvent::Exit {
            session_id: "shell-1".to_string(),
            code: None,
        })
        .unwrap();

        assert!(encoded.contains(r#""type":"exit""#));
        assert!(encoded.contains(r#""code":null"#));
    }

    /// 验证远程任务结果可以编码为 client frame。
    #[test]
    fn remote_task_result_encodes_as_client_frame() {
        let result = RemoteTaskResult {
            task_id: "task-1".to_string(),
            status: RemoteTaskStatus::Success,
            exit_code: Some(0),
            stdout: "ok".to_string(),
            stderr: String::new(),
            started_at: 100,
            finished_at: 101,
            duration_ms: 1000,
            timed_out: false,
            stdout_truncated: false,
            stderr_truncated: false,
            error: None,
        };

        let json =
            encode_remote_task_result_as_smalux_json_bytes("agent-1", 7, 101, &result).unwrap();
        let decoded = decode_client_frame(std::str::from_utf8(&json).unwrap()).unwrap();

        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 7);
        match decoded.payload {
            ClientPayload::RemoteTaskResult { result } => {
                assert_eq!(result.task_id, "task-1");
                assert_eq!(result.status, RemoteTaskStatus::Success);
            }
            _ => panic!("expected remote task result payload"),
        }
    }

    /// 验证远程探测结果可以编码为 client frame。
    #[test]
    fn remote_probe_result_encodes_as_client_frame() {
        let result = RemoteProbeResult {
            run_id: "probe-run-1".to_string(),
            source: RemoteProbeResultSource::Once,
            point_id: Some(RemoteProbeId::from("point-7")),
            request_id: Some(RemoteProbeId::from(7)),
            job_id: None,
            probe_type: RemoteProbeType::Tcp,
            target: "example.com:443".to_string(),
            status: RemoteProbeResultStatus::Success,
            latency_ms: Some(12),
            started_at: 100,
            finished_at: 101,
            duration_ms: 12,
            error: None,
        };

        let json =
            encode_remote_probe_result_as_smalux_json_bytes("agent-1", 8, 101, &result).unwrap();
        let decoded = decode_client_frame(std::str::from_utf8(&json).unwrap()).unwrap();

        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 8);
        match decoded.payload {
            ClientPayload::RemoteProbeResult { result } => {
                assert_eq!(result.run_id, "probe-run-1");
                assert_eq!(result.source, RemoteProbeResultSource::Once);
                assert_eq!(result.point_id, Some(RemoteProbeId::from("point-7")));
                assert_eq!(result.request_id, Some(RemoteProbeId::from(7)));
                assert_eq!(result.job_id, None);
                assert_eq!(result.probe_type, RemoteProbeType::Tcp);
                assert_eq!(result.status, RemoteProbeResultStatus::Success);
                assert_eq!(result.latency_ms, Some(12));
            }
            _ => panic!("expected remote probe result payload"),
        }
    }

    /// 验证控制命令确认可以编码为 client frame。
    #[test]
    fn ack_encodes_as_client_frame() {
        let json =
            encode_ack_as_smalux_json_bytes("agent-1", 8, 102, &Ack { sequence: 7 }).unwrap();
        let decoded = decode_client_frame(std::str::from_utf8(&json).unwrap()).unwrap();

        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 8);
        match decoded.payload {
            ClientPayload::Ack { ack } => assert_eq!(ack.sequence, 7),
            _ => panic!("expected ack payload"),
        }
    }

    /// 验证协议错误可以关联到对端消息序号。
    #[test]
    fn protocol_error_encodes_as_client_frame() {
        let error = ProtocolError {
            sequence: Some(7),
            code: "snapshot_not_ready".to_string(),
            message: "telemetry state is not ready".to_string(),
        };

        let json = encode_protocol_error_as_smalux_json_bytes("agent-1", 9, 103, &error).unwrap();
        let decoded = decode_client_frame(std::str::from_utf8(&json).unwrap()).unwrap();

        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 9);
        match decoded.payload {
            ClientPayload::Error { error } => {
                assert_eq!(error.sequence, Some(7));
                assert_eq!(error.code, "snapshot_not_ready");
            }
            _ => panic!("expected error payload"),
        }
    }
}
