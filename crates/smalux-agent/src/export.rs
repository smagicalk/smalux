//! Agent 数据导出抽象。
//!
//! 这里定义发送端和消息监听器的通用 trait，具体协议实现放在子模块。service 层只关心
//! “要发送什么语义”，adapter 决定“编码成什么格式”，transport 决定“怎么发出去”。

/// 导出格式 adapter。
mod adapter;
/// HTTP 导出实现。
mod http;
/// 导出 transport hub。
mod hub;
/// Komari 兼容导出实现。
mod komari;
/// 导出通用模型和 trait。
mod model;
/// 导出 plan、job 和 request。
mod plan;
/// 导出路由。
mod router;
/// rustls 相关 TLS 适配。
mod rustls;
/// Smalux 自有协议安全通道。
pub(crate) mod security;
/// Smalux 自有二进制 wire packet。
pub(crate) mod wire;
/// Transport 后台发送 worker。
mod worker;
/// WebSocket 导出实现。
pub(crate) mod ws;

pub(crate) use adapter::{ExportAdapter, build_export_adapter, build_komari_message_listener};
pub(crate) use hub::{ExportTransportClient, TransportHub};
pub(crate) use model::{
    EncodedExportMessage, ExportInboundMessage, ExportMessageListener, ExportProtocol,
    ExportTransport, inbound_message_into_string,
};
pub(crate) use plan::{
    ExportJobFailurePolicy, ExportJobId, ExportJobSpec, ExportJobTrigger, TransportId,
    TransportPlan, TransportRequest, TransportSpec,
};
pub(crate) use router::ExportRouter;
pub(crate) use worker::{TransportEvent, TransportEventReceiver, transport_event_channel};

#[cfg(test)]
mod tests {
    //! 导出协议选择测试。

    use super::*;
    use crate::config::model::{ExportConfig, ExportFormat};
    use crate::service::outbound::{
        ControlAckEnvelope, ControlErrorEnvelope, RemoteProbeResultEnvelope,
        RemoteTaskResultEnvelope,
    };
    use base64::Engine;
    use smalux_core::model::info::AgentReport;
    use smalux_protocol::{ClientPayload, OutboundReportKind, decode_client_frame};

    /// 验证 ws 地址会生成 WebSocket transport plan。
    #[test]
    fn smalux_json_adapter_plans_websocket_for_ws_url() {
        let config = ExportConfig::default();
        let mut adapter = build_export_adapter(config.format);
        let plan = adapter.transport_plan(&config).unwrap();

        assert_eq!(plan.transports.len(), 1);
        assert!(matches!(
            plan.transports[0],
            TransportSpec::WebSocket { .. }
        ));
    }

    /// 验证 job 配置会把 adapter 声明的 job 转成可配置 interval 调度。
    #[test]
    fn transport_plan_applies_job_config() {
        let config = crate::config::model::JobsConfig::default();
        let mut plan = TransportPlan::new(vec![]);

        plan.apply_job_config(&config);

        assert_eq!(plan.jobs.len(), 1);
        assert_eq!(
            plan.jobs[0].trigger,
            ExportJobTrigger::Interval(config.realtime_report.interval)
        );
        assert!(plan.jobs[0].run_on_start);
    }

    /// 验证禁用的 job 不会进入运行时调度。
    #[test]
    fn transport_plan_removes_disabled_job() {
        let mut config = crate::config::model::JobsConfig::default();
        config.realtime_report.enabled = false;
        let mut plan = TransportPlan::new(vec![]);

        plan.apply_job_config(&config);

        assert!(plan.jobs.is_empty());
    }

    /// 验证 wss 地址会选择 WebSocket transport。
    #[test]
    fn export_protocol_accepts_wss_url() {
        let protocol = ExportProtocol::from_server_url("wss://example.com/ws").unwrap();

        assert_eq!(protocol, ExportProtocol::WebSocket);
    }

    /// 验证暂未实现的协议会快速失败。
    #[test]
    fn export_protocol_rejects_unsupported_scheme() {
        let error = ExportProtocol::from_server_url("grpc://127.0.0.1:9000").unwrap_err();

        assert!(error.to_string().contains("unsupported export protocol"));
    }

    /// 验证默认 smalux_json adapter 会输出 WebSocket binary wire 请求。
    #[test]
    fn smalux_json_adapter_outputs_websocket_binary_request() {
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();
        let outbound = smalux_protocol::OutboundReport::snapshot(1, 100, report);
        let mut adapter = build_export_adapter(ExportFormat::SmaluxJson);
        let config = ExportConfig::default();
        adapter.transport_plan(&config).unwrap();

        let requests = adapter
            .encode_report(ExportJobId::RealtimeReport, &outbound)
            .unwrap();
        let [TransportRequest::WebSocketBinary { sequence, body, .. }] = requests.as_slice() else {
            panic!("expected smalux_json websocket binary request");
        };
        let json = String::from_utf8(body.clone()).unwrap();
        let decoded = decode_client_frame(&json).unwrap();

        assert_eq!(*sequence, 1);
        assert_eq!(decoded.agent_id, "agent-1");
        assert_eq!(decoded.sequence, 1);
    }

    /// 验证 secure_psk wire 模式不会影响 adapter 输出，安全处理交给 transport。
    #[test]
    fn smalux_json_secure_psk_keeps_adapter_transport_neutral() {
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();
        let outbound = smalux_protocol::OutboundReport::snapshot(1, 100, report);
        let mut adapter = build_export_adapter(ExportFormat::SmaluxJson);
        let token = format!(
            "smx1.agent-key.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([1u8; 32])
        );
        let config = ExportConfig {
            wire_mode: crate::config::model::ExportWireMode::SecurePsk,
            secure_required: true,
            token: Some(token),
            ..ExportConfig::default()
        };
        adapter.transport_plan(&config).unwrap();

        let requests = adapter
            .encode_report(ExportJobId::RealtimeReport, &outbound)
            .unwrap();

        assert!(matches!(
            requests.as_slice(),
            [TransportRequest::WebSocketBinary { sequence: 1, .. }]
        ));
    }

    /// 验证 smalux_json adapter 目前不会跳过已支持的上报事件。
    #[test]
    fn smalux_json_adapter_keeps_supported_reports() {
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();
        let outbound = smalux_protocol::OutboundReport::snapshot(1, 100, report);
        let mut adapter = build_export_adapter(ExportFormat::SmaluxJson);

        assert!(matches!(outbound.kind, OutboundReportKind::Snapshot { .. }));
        assert!(
            !adapter
                .encode_report(ExportJobId::RealtimeReport, &outbound)
                .unwrap()
                .is_empty()
        );
    }

    /// 验证 smalux_json adapter 会输出远程任务结果。
    #[test]
    fn smalux_json_adapter_outputs_remote_task_result() {
        let mut adapter = build_export_adapter(ExportFormat::SmaluxJson);
        let result = RemoteTaskResultEnvelope {
            agent_id: "agent-1".to_string(),
            sequence: 9,
            created_at: 100,
            result: smalux_protocol::RemoteTaskResult {
                task_id: "task-1".to_string(),
                status: smalux_protocol::RemoteTaskStatus::Success,
                exit_code: Some(0),
                stdout: "ok".to_string(),
                stderr: String::new(),
                started_at: 99,
                finished_at: 100,
                duration_ms: 1000,
                timed_out: false,
                stdout_truncated: false,
                stderr_truncated: false,
                error: None,
            },
        };

        let requests = adapter.encode_remote_task_result(&result).unwrap();

        assert!(matches!(
            requests.as_slice(),
            [TransportRequest::WebSocketBinary { sequence: 9, .. }]
        ));
    }

    /// 验证 smalux_json adapter 会输出远程探测结果。
    #[test]
    fn smalux_json_adapter_outputs_remote_probe_result() {
        let mut adapter = build_export_adapter(ExportFormat::SmaluxJson);
        let result = RemoteProbeResultEnvelope {
            agent_id: "agent-1".to_string(),
            sequence: 12,
            created_at: 100,
            result: smalux_protocol::RemoteProbeResult {
                task_id: serde_json::Value::from(7),
                probe_type: smalux_protocol::RemoteProbeType::Tcp,
                target: "example.com:443".to_string(),
                value: 13,
                started_at: 99,
                finished_at: 100,
                duration_ms: 13,
                error: None,
            },
        };

        let requests = adapter.encode_remote_probe_result(&result).unwrap();

        let [TransportRequest::WebSocketBinary { sequence, body, .. }] = requests.as_slice() else {
            panic!("expected smalux_json websocket binary request");
        };
        let decoded = decode_client_frame(std::str::from_utf8(body).unwrap()).unwrap();

        assert_eq!(*sequence, 12);
        match decoded.payload {
            ClientPayload::RemoteProbeResult { result } => {
                assert_eq!(result.task_id, serde_json::Value::from(7));
                assert_eq!(result.value, 13);
            }
            _ => panic!("expected remote probe result payload"),
        }
    }

    /// 验证 smalux_json adapter 会输出控制命令确认。
    #[test]
    fn smalux_json_adapter_outputs_control_ack() {
        let mut adapter = build_export_adapter(ExportFormat::SmaluxJson);
        let ack = ControlAckEnvelope {
            agent_id: "agent-1".to_string(),
            sequence: 10,
            created_at: 100,
            ack: smalux_protocol::Ack { sequence: 7 },
        };

        let requests = adapter.encode_control_ack(&ack).unwrap();

        assert!(matches!(
            requests.as_slice(),
            [TransportRequest::WebSocketBinary { sequence: 10, .. }]
        ));
    }

    /// 验证 smalux_json adapter 会输出控制命令错误。
    #[test]
    fn smalux_json_adapter_outputs_control_error() {
        let mut adapter = build_export_adapter(ExportFormat::SmaluxJson);
        let error = ControlErrorEnvelope {
            agent_id: "agent-1".to_string(),
            sequence: 11,
            created_at: 100,
            error: smalux_protocol::ProtocolError {
                sequence: Some(7),
                code: "snapshot_request_failed".to_string(),
                message: "reporting is disabled".to_string(),
            },
        };

        let requests = adapter.encode_control_error(&error).unwrap();

        assert!(matches!(
            requests.as_slice(),
            [TransportRequest::WebSocketBinary { sequence: 11, .. }]
        ));
    }
}
