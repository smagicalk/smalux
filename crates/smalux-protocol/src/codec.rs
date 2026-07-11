//! 协议 frame 的 JSON 编解码。
//!
//! codec 只负责 `ClientFrame` / `ServerFrame` 和 JSON 字节之间的转换，不处理
//! WebSocket binary wire、Noise 加密、HTTP 状态码或重连。这样 server 端可以复用
//! 同一套 decode 逻辑接入不同 transport。

use crate::frame::{
    Ack, ClientFrame, ClientPayload, ProtocolError, RemoteJobResult, RemoteShellStreamCommand,
    RemoteShellStreamEvent, RemoteTaskResult, ServerFrame,
};

/// 从远程任务结果构造 client frame。
pub fn build_client_frame_from_remote_task_result(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    result: &RemoteTaskResult,
) -> ClientFrame {
    ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::RemoteTaskResult {
            result: result.clone(),
        },
    )
}

/// 从通用远程 job 结果构造 client frame。
pub fn build_client_frame_from_remote_job_result(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    result: &RemoteJobResult,
) -> ClientFrame {
    ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::JobResult {
            result: result.clone(),
        },
    )
}

/// 从控制确认构造 client frame。
pub fn build_client_frame_from_ack(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    ack: &Ack,
) -> ClientFrame {
    ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::Ack { ack: ack.clone() },
    )
}

/// 从协议错误构造 client frame。
pub fn build_client_frame_from_protocol_error(
    agent_id: &str,
    sequence: u64,
    sent_at: u64,
    error: &ProtocolError,
) -> ClientFrame {
    ClientFrame::new(
        agent_id.to_string(),
        sequence,
        sent_at,
        ClientPayload::Error {
            error: error.clone(),
        },
    )
}

/// 编码 agent 发往 server 的 frame。
pub fn encode_client_frame(frame: &ClientFrame) -> serde_json::Result<String> {
    serde_json::to_string(frame)
}

/// 编码 agent 发往 server 的 frame bytes。
pub fn encode_client_frame_bytes(frame: &ClientFrame) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(frame)
}

/// 解码 agent 发往 server 的 frame。
pub fn decode_client_frame(input: &str) -> serde_json::Result<ClientFrame> {
    serde_json::from_str(input)
}

/// 解码 agent 发往 server 的 frame bytes。
pub fn decode_client_frame_bytes(input: &[u8]) -> serde_json::Result<ClientFrame> {
    serde_json::from_slice(input)
}

/// 编码 server 发往 agent 的 frame。
pub fn encode_server_frame(frame: &ServerFrame) -> serde_json::Result<String> {
    serde_json::to_string(frame)
}

/// 编码 server 发往 agent 的 frame bytes。
pub fn encode_server_frame_bytes(frame: &ServerFrame) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(frame)
}

/// 解码 server 发往 agent 的 frame。
///
/// 自有协议下行只识别稳定 `ServerFrame`。第三方兼容消息应由各自 adapter 转换。
pub fn decode_server_frame(input: &str) -> serde_json::Result<ServerFrame> {
    serde_json::from_str(input)
}

/// 解码 server 发往 agent 的 frame bytes。
pub fn decode_server_frame_bytes(input: &[u8]) -> serde_json::Result<ServerFrame> {
    serde_json::from_slice(input)
}

/// 编码远程 shell stream 事件。
///
/// 这里仅生成业务 JSON；WebSocket transport 会继续按当前 wire mode 封装或加密。
pub fn encode_shell_stream_event(event: &RemoteShellStreamEvent) -> serde_json::Result<String> {
    serde_json::to_string(event)
}

/// 解码远程 shell stream 命令。
///
/// server 发给 agent 的临时 shell stream payload 应先按 wire mode 解包，再交给本函数解析。
pub fn decode_shell_stream_command(input: &str) -> serde_json::Result<RemoteShellStreamCommand> {
    serde_json::from_str(input)
}

#[cfg(test)]
mod tests {
    //! JSON codec 测试。

    use super::*;
    use crate::frame::{
        Ack, ClientEvent, ClientPayload, DeltaReport, Heartbeat, MetricCollectionRequest,
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
        let event = ClientEvent::heartbeat(
            "agent-1",
            1,
            100,
            Heartbeat {
                last_report_at: Some(90),
                last_report_sequence: Some(7),
            },
        );

        let frame = ClientFrame::from_event(&event);
        let json = encode_client_frame(&frame).unwrap();
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
        let event = ClientEvent::delta(
            "agent-1",
            2,
            110,
            DeltaReport {
                base_sequence: 1,
                report_at: 109,
                ..DeltaReport::default()
            },
        );

        let frame = ClientFrame::from_event(&event);
        let json = encode_client_frame(&frame).unwrap();
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

    /// 验证自有 job_apply 只接受 request_id，不接受第三方 task_id alias。
    #[test]
    fn server_job_apply_rejects_probe_task_id_alias() {
        let json = r#"{
            "protocol_version": 1,
            "sequence": 9,
            "sent_at": 100,
            "type": "job_apply",
            "request": {
                "operation": "once",
                "runs": [
                    {
                        "kind": "probe",
                        "task_id": "probe-1",
                        "probe_type": "tcp",
                        "target": "example.com:443"
                    }
                ]
            }
        }"#;

        let error = decode_server_frame(json).unwrap_err();
        assert!(error.to_string().contains("request_id"));
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
        let decoded = decode_shell_stream_command(
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
        let encoded = encode_shell_stream_event(&RemoteShellStreamEvent::Exit {
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

        let frame = build_client_frame_from_remote_task_result("agent-1", 7, 101, &result);
        let json = encode_client_frame_bytes(&frame).unwrap();
        let decoded = decode_client_frame_bytes(&json).unwrap();

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

    /// 验证通用远程 job 结果可以编码为 client frame。
    #[test]
    fn remote_job_result_encodes_as_client_frame() {
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

        let frame = build_client_frame_from_remote_job_result(
            "agent-1",
            8,
            101,
            &RemoteJobResult::probe(result),
        );
        let json = encode_client_frame_bytes(&frame).unwrap();
        let decoded = decode_client_frame_bytes(&json).unwrap();

        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 8);
        match decoded.payload {
            ClientPayload::JobResult { result } => {
                let Some(result) = result.as_probe() else {
                    panic!("expected probe job result");
                };
                assert_eq!(result.run_id, "probe-run-1");
                assert_eq!(result.source, RemoteProbeResultSource::Once);
                assert_eq!(result.point_id, Some(RemoteProbeId::from("point-7")));
                assert_eq!(result.request_id, Some(RemoteProbeId::from(7)));
                assert_eq!(result.job_id, None);
                assert_eq!(result.probe_type, RemoteProbeType::Tcp);
                assert_eq!(result.status, RemoteProbeResultStatus::Success);
                assert_eq!(result.latency_ms, Some(12));
            }
            _ => panic!("expected remote job result payload"),
        }
    }

    /// 验证控制命令确认可以编码为 client frame。
    #[test]
    fn ack_encodes_as_client_frame() {
        let frame = build_client_frame_from_ack("agent-1", 8, 102, &Ack { sequence: 7 });
        let json = encode_client_frame_bytes(&frame).unwrap();
        let decoded = decode_client_frame_bytes(&json).unwrap();

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

        let frame = build_client_frame_from_protocol_error("agent-1", 9, 103, &error);
        let json = encode_client_frame_bytes(&frame).unwrap();
        let decoded = decode_client_frame_bytes(&json).unwrap();

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
