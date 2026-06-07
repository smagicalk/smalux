//! Komari 兼容导出 adapter。
//!
//! Komari 标准 agent 协议使用 WebSocket 发送实时 report，使用 HTTP 低频发送 basic info。
//!
//! 已确认的 Komari 请求约定：
//!
//! - WebSocket report：`wss://host/api/clients/report?token=TOKEN`
//! - HTTP basic info：`POST https://host/api/clients/uploadBasicInfo?token=TOKEN`
//! - HTTP task result：`POST https://host/api/clients/task/result?token=TOKEN`
//!
//! 配置 `export.base_url=https://host` 时会自动派生为 WebSocket report endpoint。
//!
//! token 必须作为 query 参数发送，可以放在 `export.query` 中，也可以使用
//! `auth_mode=query` + `export.token` 由 adapter 追加。Komari 不支持
//! `Authorization: Bearer ...`，因此 `auth_mode=bearer` 会在配置校验阶段被拒绝。
//!
//! report 请求体是直接 JSON 对象，不包 `type` / `data` 外壳。当前映射字段包括
//! `cpu.usage`、`ram.total/used`、`swap.total/used`、`load.load1/load5/load15`、
//! `disk.total/used`、`network.up/down/totalUp/totalDown`、`connections.tcp/udp`、
//! `uptime`、`process` 和 `message`。
//!
//! basic info 请求体字段包括 `arch`、`cpu_cores`、`cpu_name`、`disk_total`、
//! `gpu_name`、`ipv4`、`ipv6`、`mem_total`、`os`、`kernel_version`、
//! `swap_total`、`version` 和 `virtualization`。
//!
//! 兼容策略：
//!
//! - 只消费完整 snapshot；delta 和业务级 heartbeat 不发送。
//! - WebSocket 模式要求 `report.interval <= 10s`，避免第三方服务认为连接空闲。
//! - basic info 由 reporter 生成 `OutboundEvent::BasicInfo`，默认 5 分钟刷新一次。
//! - Komari terminal 消息会转给 remote shell manager。
//! - Komari exec 消息会转给 remote task manager，结果按 task/result HTTP 接口回传。
//! - 其它 Komari server 消息安全忽略。

/// Komari server 消息监听。
mod message;
/// Komari 请求和响应模型。
mod model;
/// Komari terminal 消息解析。
mod terminal;
/// Komari URL 构造和 query 规则。
mod url;

use super::{
    ExportDeliveryFailurePolicy, ExportDeliveryId, ExportDeliverySpec, InboundProtocolHandler,
    ProtocolAdapter, TransportId, TransportPlan, TransportRequest, TransportSpec, http, ws,
};
use crate::config::ConfigManager;
use crate::config::model::ExportConfig;
use crate::service::InboundCommandSender;
use crate::service::outbound::{
    BasicInfoEnvelope, RemoteProbeResultEnvelope, RemoteTaskResultEnvelope,
};
use model::{BasicInfo, PingResult, Report, TaskResult};
use smalux_protocol::{OutboundReport, OutboundReportKind};
use url::{
    komari_basic_info_url, komari_report_websocket_url, komari_task_result_url, redact_komari_url,
};

/// Komari 兼容协议 adapter。
#[derive(Debug)]
pub(crate) struct KomariProtocolAdapter {
    /// HTTP basic info URL。
    basic_info_url: Option<String>,
    /// HTTP task result URL。
    task_result_url: Option<String>,
}

impl Default for KomariProtocolAdapter {
    /// 默认使用 WebSocket report，basic info 每 5 分钟刷新一次。
    fn default() -> Self {
        Self {
            basic_info_url: None,
            task_result_url: None,
        }
    }
}

impl KomariProtocolAdapter {
    /// Komari 需要 reporter 生成独立 basic info 事件。
    pub(super) const fn needs_basic_info() -> bool {
        true
    }
}

impl ProtocolAdapter for KomariProtocolAdapter {
    /// Komari 需要独立的 HTTP basic info 上报。
    fn needs_basic_info_events(&self) -> bool {
        Self::needs_basic_info()
    }

    /// 根据 base_url 生成 Komari transport plan。
    fn transport_plan(&mut self, config: &ExportConfig) -> anyhow::Result<TransportPlan> {
        self.basic_info_url = Some(komari_basic_info_url(config)?);
        self.task_result_url = Some(komari_task_result_url(config)?);
        let websocket_config = komari_websocket_config(config)?;

        Ok(TransportPlan::with_deliveries(
            vec![
                TransportSpec::WebSocket {
                    id: TransportId::RealtimeReport,
                    config: websocket_config,
                    connect_on_start: false,
                },
                TransportSpec::Http {
                    id: TransportId::AuxiliaryHttp,
                    config: http::HttpConfig::default().with_unsafe_cert(config.unsafe_cert),
                },
            ],
            vec![
                ExportDeliverySpec::event_driven(
                    ExportDeliveryId::BasicInfo,
                    ExportDeliveryFailurePolicy::LogAndContinue,
                ),
                ExportDeliverySpec::on_latest_report(ExportDeliveryId::RealtimeReport),
            ],
        ))
    }

    /// 按 delivery 将完整 snapshot 映射成 Komari 请求。
    fn encode_report(
        &mut self,
        delivery_id: ExportDeliveryId,
        outbound: &OutboundReport,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let OutboundReportKind::Snapshot { report } = &outbound.kind else {
            return Ok(vec![]);
        };

        match delivery_id {
            ExportDeliveryId::RealtimeReport => {
                let report_body = serde_json::to_string(&Report::from_agent_report(report))?;
                tracing::debug!(
                    created_at = outbound.created_at,
                    body = %report_body,
                    "komari websocket report request encoded"
                );
                Ok(vec![TransportRequest::WebSocketText {
                    transport: TransportId::RealtimeReport,
                    body: report_body,
                }])
            }
            ExportDeliveryId::BasicInfo => Ok(vec![]),
            ExportDeliveryId::RemoteTaskResult
            | ExportDeliveryId::RemoteProbeResult
            | ExportDeliveryId::ControlAck
            | ExportDeliveryId::ControlError => Ok(vec![]),
        }
    }

    /// 将内部 basic info 事件映射为 Komari uploadBasicInfo HTTP POST。
    fn encode_basic_info(
        &mut self,
        info: &BasicInfoEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let url = self
            .basic_info_url
            .clone()
            .ok_or_else(|| anyhow::anyhow!("komari basic info url is not initialized"))?;
        let redacted_url = redact_komari_url(&url);
        let body = serde_json::to_value(BasicInfo::from_agent_report(&info.report))?;
        tracing::info!(
            sequence = info.sequence,
            created_at = info.created_at,
            url = %redacted_url,
            body = %body,
            "komari basic info request encoded"
        );

        Ok(vec![TransportRequest::HttpJson {
            transport: TransportId::AuxiliaryHttp,
            method: http::HttpMethod::Post,
            url,
            body,
        }])
    }

    /// 将内部 remote task result 映射为 Komari task/result HTTP POST。
    fn encode_remote_task_result(
        &mut self,
        result: &RemoteTaskResultEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let url = self
            .task_result_url
            .clone()
            .ok_or_else(|| anyhow::anyhow!("komari task result url is not initialized"))?;
        let redacted_url = redact_komari_url(&url);
        let body = serde_json::to_value(TaskResult::from_remote_task_result(&result.result))?;
        tracing::info!(
            sequence = result.sequence,
            task_id = %result.result.task_id,
            url = %redacted_url,
            body = %body,
            "komari task result request encoded"
        );

        Ok(vec![TransportRequest::HttpJson {
            transport: TransportId::AuxiliaryHttp,
            method: http::HttpMethod::Post,
            url,
            body,
        }])
    }

    /// 将内部 remote probe result 映射为 Komari WebSocket ping_result。
    fn encode_remote_probe_result(
        &mut self,
        result: &RemoteProbeResultEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let body = serde_json::to_string(&PingResult::from_remote_probe_result(&result.result))?;
        tracing::info!(
            sequence = result.sequence,
            task_id = %crate::service::display_probe_task_id(&result.result.task_id),
            probe_type = result.result.probe_type.as_str(),
            target = %result.result.target,
            value = result.result.value,
            body = %body,
            "komari ping result encoded"
        );

        Ok(vec![TransportRequest::WebSocketText {
            transport: TransportId::RealtimeReport,
            body,
        }])
    }
}

/// 构造 Komari WebSocket 配置；base_url 会先派生为 report WebSocket URL。
fn komari_websocket_config(config: &ExportConfig) -> anyhow::Result<ws::WebSocketConfig> {
    let endpoint = komari_report_websocket_url(config)?;
    ws::WebSocketConfig::from_export_endpoint(config, endpoint)
}

/// 构造 Komari server 消息入站处理器。
pub(crate) fn inbound_handler(
    config_manager: ConfigManager,
    commands: InboundCommandSender,
) -> Box<dyn InboundProtocolHandler> {
    message::inbound_handler(config_manager, commands)
}

#[cfg(test)]
mod tests {
    //! Komari adapter 测试。

    use super::*;
    use crate::config::model::ExportAuthMode;
    use crate::export::ExportDeliveryTrigger;
    use crate::service::outbound::{RemoteProbeResultEnvelope, RemoteTaskResultEnvelope};
    use smalux_core::model::info::AgentReport;
    use smalux_protocol::{RemoteProbeResult, RemoteProbeType, RemoteTaskResult, RemoteTaskStatus};

    /// 构造 Komari 测试配置。
    fn komari_config(base_url: &str) -> ExportConfig {
        ExportConfig {
            base_url: base_url.to_string(),
            auth_mode: ExportAuthMode::Query,
            token: Some("secret-token".to_string()),
            ..ExportConfig::default()
        }
    }

    /// 验证 WebSocket 模式会同时声明 report WS 和 basic info HTTP。
    #[test]
    fn komari_plan_uses_websocket_report_and_http_basic_info() {
        let mut adapter = KomariProtocolAdapter::default();
        let plan = adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();

        assert_eq!(plan.transports.len(), 2);
        assert!(matches!(
            plan.transports[0],
            TransportSpec::WebSocket { .. }
        ));
        assert!(matches!(plan.transports[1], TransportSpec::Http { .. }));
    }

    /// 验证 base_url 根地址会自动使用 WebSocket report。
    #[test]
    fn komari_plan_uses_websocket_report_for_base_url() {
        let mut adapter = KomariProtocolAdapter::default();
        let plan = adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();

        assert_eq!(plan.transports.len(), 2);
        assert!(matches!(
            plan.transports[0],
            TransportSpec::WebSocket { .. }
        ));
        assert!(matches!(plan.transports[1], TransportSpec::Http { .. }));
    }

    /// 验证带 path 的 URL 会被拒绝，避免配置混入 endpoint。
    #[test]
    fn komari_plan_rejects_base_url_with_path() {
        let mut adapter = KomariProtocolAdapter::default();
        let error = match adapter
            .transport_plan(&komari_config("https://example.com/api/clients/report"))
        {
            Ok(_) => panic!("expected base_url with path to be rejected"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("path"));
    }

    /// 验证 HTTP transport 会继承 unsafe_cert 配置。
    #[test]
    fn komari_http_transport_uses_unsafe_cert_config() {
        let mut config = komari_config("https://example.com");
        config.unsafe_cert = true;
        let mut adapter = KomariProtocolAdapter::default();
        let plan = adapter.transport_plan(&config).unwrap();

        let TransportSpec::Http { config, .. } = &plan.transports[1] else {
            panic!("expected http transport");
        };

        assert!(config.unsafe_cert);
    }

    /// 验证 Komari plan 显式声明 report 和 basic info 两个 delivery。
    #[test]
    fn komari_plan_declares_report_and_basic_info_deliveries() {
        let mut adapter = KomariProtocolAdapter::default();
        let plan = adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();

        assert_eq!(plan.deliveries.len(), 2);
        assert_eq!(plan.deliveries[0].id, ExportDeliveryId::BasicInfo);
        assert_eq!(
            plan.deliveries[0].trigger,
            ExportDeliveryTrigger::EventDriven
        );
        assert_eq!(
            plan.deliveries[0].failure_policy,
            ExportDeliveryFailurePolicy::LogAndContinue
        );
        assert_eq!(plan.deliveries[1].id, ExportDeliveryId::RealtimeReport);
        assert_eq!(
            plan.deliveries[1].trigger,
            ExportDeliveryTrigger::OnLatestReport
        );
    }

    /// 验证 basic info 事件会生成 HTTP POST。
    #[test]
    fn komari_adapter_encodes_basic_info_event() {
        let mut adapter = KomariProtocolAdapter::default();
        adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();

        let requests = adapter
            .encode_basic_info(&BasicInfoEnvelope {
                sequence: 1,
                created_at: 100,
                report,
            })
            .unwrap();

        assert_eq!(requests.len(), 1);
        assert!(matches!(requests[0], TransportRequest::HttpJson { .. }));
    }

    /// 验证 report delivery 会生成 WebSocket text。
    #[test]
    fn komari_adapter_encodes_websocket_report_delivery() {
        let mut adapter = KomariProtocolAdapter::default();
        adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();
        let outbound = OutboundReport::snapshot(1, 100, report);

        let requests = adapter
            .encode_report(ExportDeliveryId::RealtimeReport, &outbound)
            .unwrap();

        assert_eq!(requests.len(), 1);
        assert!(matches!(
            requests[0],
            TransportRequest::WebSocketText { .. }
        ));
    }

    /// 验证非 snapshot 事件会被 Komari adapter 跳过。
    #[test]
    fn komari_adapter_skips_non_snapshot_report() {
        let mut adapter = KomariProtocolAdapter::default();
        adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();
        let outbound =
            OutboundReport::heartbeat("agent-1", 1, 100, smalux_protocol::Heartbeat::default());

        let basic_info = adapter
            .encode_report(ExportDeliveryId::BasicInfo, &outbound)
            .unwrap();
        let realtime_report = adapter
            .encode_report(ExportDeliveryId::RealtimeReport, &outbound)
            .unwrap();

        assert!(basic_info.is_empty());
        assert!(realtime_report.is_empty());
    }

    /// 验证 Komari 会把 remote task result 编码为 HTTP task/result 请求。
    #[test]
    fn komari_adapter_encodes_remote_task_result() {
        let mut adapter = KomariProtocolAdapter::default();
        adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();
        let result = RemoteTaskResultEnvelope {
            agent_id: "agent-1".to_string(),
            sequence: 10,
            created_at: 100,
            result: RemoteTaskResult {
                task_id: "task-1".to_string(),
                status: RemoteTaskStatus::Success,
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

        assert_eq!(requests.len(), 1);
        let TransportRequest::HttpJson { url, body, .. } = &requests[0] else {
            panic!("expected http task result request");
        };
        assert_eq!(
            url,
            "https://example.com/api/clients/task/result?token=secret-token"
        );
        assert_eq!(body["task_id"], "task-1");
        assert_eq!(body["result"], "ok");
        assert_eq!(body["exit_code"], 0);
    }

    /// 验证 Komari 会把 remote probe result 编码为 WebSocket ping_result。
    #[test]
    fn komari_adapter_encodes_remote_probe_result() {
        let mut adapter = KomariProtocolAdapter::default();
        adapter
            .transport_plan(&komari_config("https://example.com"))
            .unwrap();
        let result = RemoteProbeResultEnvelope {
            agent_id: "agent-1".to_string(),
            sequence: 11,
            created_at: 100,
            result: RemoteProbeResult {
                task_id: serde_json::Value::from(123),
                probe_type: RemoteProbeType::Tcp,
                target: "example.com:443".to_string(),
                value: 13,
                started_at: 99,
                finished_at: 100,
                duration_ms: 13,
                error: None,
            },
        };

        let requests = adapter.encode_remote_probe_result(&result).unwrap();

        assert_eq!(requests.len(), 1);
        let TransportRequest::WebSocketText { body, .. } = &requests[0] else {
            panic!("expected websocket ping result");
        };
        let json: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(json["type"], "ping_result");
        assert_eq!(json["task_id"], 123);
        assert_eq!(json["ping_type"], "tcp");
        assert_eq!(json["value"], 13);
    }
}
