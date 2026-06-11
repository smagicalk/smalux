//! 协议格式 adapter。

use super::{
    ExportDeliveryId, ExportEndpointScheme, InboundProtocolHandler, TransportId, TransportPlan,
    TransportRequest, TransportSpec, build_export_endpoint, komari, ws,
};
use crate::config::model::{ExportConfig, ExportFormat};
use crate::service::outbound::{
    BasicInfoEnvelope, ControlAckEnvelope, ControlErrorEnvelope, RemoteProbeResultEnvelope,
    RemoteTaskResultEnvelope,
};
use smalux_protocol::{
    OutboundReport, encode_ack_as_smalux_json_bytes, encode_outbound_report_as_smalux_json_bytes,
    encode_protocol_error_as_smalux_json_bytes, encode_remote_probe_result_as_smalux_json_bytes,
    encode_remote_task_result_as_smalux_json_bytes,
};

/// Smalux 自有协议主连接路径。
const SMALUX_CONNECT_PATH: &str = "/agent/v1/connect";

/// 协议格式适配器。
pub(crate) trait ProtocolAdapter {
    /// 根据配置声明需要启动的 transport。
    fn transport_plan(&mut self, config: &ExportConfig) -> anyhow::Result<TransportPlan>;

    /// 是否需要 reporter 生成低频 basic info 事件。
    fn needs_basic_info_events(&self) -> bool {
        false
    }

    /// 将内部上报语义编码成零到多条 transport 请求。
    fn encode_report(
        &mut self,
        delivery_id: ExportDeliveryId,
        outbound: &OutboundReport,
    ) -> anyhow::Result<Vec<TransportRequest>>;

    /// 将低频基础信息编码成零到多条 transport 请求。
    fn encode_basic_info(
        &mut self,
        _info: &BasicInfoEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        Ok(vec![])
    }

    /// 将远程任务结果编码成零到多条 transport 请求。
    fn encode_remote_task_result(
        &mut self,
        _result: &RemoteTaskResultEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        Ok(vec![])
    }

    /// 将远程网络探测结果编码成零到多条 transport 请求。
    fn encode_remote_probe_result(
        &mut self,
        _result: &RemoteProbeResultEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        Ok(vec![])
    }

    /// 将控制命令确认编码成零到多条 transport 请求。
    fn encode_control_ack(
        &mut self,
        _ack: &ControlAckEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        Ok(vec![])
    }

    /// 将控制命令错误编码成零到多条 transport 请求。
    fn encode_control_error(
        &mut self,
        _error: &ControlErrorEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        Ok(vec![])
    }
}

/// Smalux 默认 JSON 协议 adapter。
#[derive(Debug, Default)]
struct SmaluxJsonProtocolAdapter;

impl ProtocolAdapter for SmaluxJsonProtocolAdapter {
    /// smalux_json 当前使用主 WebSocket transport。
    fn transport_plan(&mut self, config: &ExportConfig) -> anyhow::Result<TransportPlan> {
        let endpoint = build_export_endpoint(
            &config.base_url,
            ExportEndpointScheme::WebSocket,
            SMALUX_CONNECT_PATH,
        )?;
        Ok(TransportPlan::new(vec![TransportSpec::WebSocket {
            id: TransportId::RealtimeReport,
            config: ws::WebSocketConfig::from_export_endpoint(config, endpoint)?,
            connect_on_start: true,
        }]))
    }

    /// 编码为 smalux JSON bytes，封包和加密由 WebSocket transport 处理。
    fn encode_report(
        &mut self,
        delivery_id: ExportDeliveryId,
        outbound: &OutboundReport,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        if delivery_id != ExportDeliveryId::RealtimeReport {
            anyhow::bail!(
                "smalux_json does not support export delivery: {}",
                delivery_id.as_str()
            );
        }

        let json = encode_outbound_report_as_smalux_json_bytes(outbound)?;
        tracing::debug!(
            sequence = outbound.sequence,
            body_bytes = json.len(),
            "smalux report encoded as websocket binary payload"
        );

        Ok(vec![TransportRequest::WebSocketBinary {
            transport: TransportId::RealtimeReport,
            sequence: outbound.sequence,
            body: json,
        }])
    }

    /// 编码远程任务结果为 smalux JSON bytes。
    fn encode_remote_task_result(
        &mut self,
        result: &RemoteTaskResultEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let json = encode_remote_task_result_as_smalux_json_bytes(
            &result.agent_id,
            result.sequence,
            result.created_at,
            &result.result,
        )?;
        tracing::debug!(
            sequence = result.sequence,
            task_id = %result.result.task_id,
            body_bytes = json.len(),
            "remote task result encoded as websocket binary payload"
        );

        Ok(vec![TransportRequest::WebSocketBinary {
            transport: TransportId::RealtimeReport,
            sequence: result.sequence,
            body: json,
        }])
    }

    /// 编码远程探测结果为 smalux JSON bytes。
    fn encode_remote_probe_result(
        &mut self,
        result: &RemoteProbeResultEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let json = encode_remote_probe_result_as_smalux_json_bytes(
            &result.agent_id,
            result.sequence,
            result.created_at,
            &result.result,
        )?;
        tracing::debug!(
            sequence = result.sequence,
            probe_id = %result.result.display_id(),
            probe_type = result.result.probe_type.as_str(),
            target = %result.result.target,
            body_bytes = json.len(),
            "remote probe result encoded as websocket binary payload"
        );

        Ok(vec![TransportRequest::WebSocketBinary {
            transport: TransportId::RealtimeReport,
            sequence: result.sequence,
            body: json,
        }])
    }

    /// 编码控制命令确认为 smalux JSON bytes。
    fn encode_control_ack(
        &mut self,
        ack: &ControlAckEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let json =
            encode_ack_as_smalux_json_bytes(&ack.agent_id, ack.sequence, ack.created_at, &ack.ack)?;
        tracing::debug!(
            sequence = ack.sequence,
            server_sequence = ack.ack.sequence,
            body_bytes = json.len(),
            "control ack encoded as websocket binary payload"
        );

        Ok(vec![TransportRequest::WebSocketBinary {
            transport: TransportId::RealtimeReport,
            sequence: ack.sequence,
            body: json,
        }])
    }

    /// 编码控制命令错误为 smalux JSON bytes。
    fn encode_control_error(
        &mut self,
        error: &ControlErrorEnvelope,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        let json = encode_protocol_error_as_smalux_json_bytes(
            &error.agent_id,
            error.sequence,
            error.created_at,
            &error.error,
        )?;
        tracing::debug!(
            sequence = error.sequence,
            server_sequence = error.error.sequence,
            code = %error.error.code,
            body_bytes = json.len(),
            "control error encoded as websocket binary payload"
        );

        Ok(vec![TransportRequest::WebSocketBinary {
            transport: TransportId::RealtimeReport,
            sequence: error.sequence,
            body: json,
        }])
    }
}

/// 根据配置创建协议格式 adapter。
pub(crate) fn build_protocol_adapter(
    format: ExportFormat,
) -> Box<dyn ProtocolAdapter + Send + Sync> {
    match format {
        ExportFormat::SmaluxJson => Box::<SmaluxJsonProtocolAdapter>::default(),
        ExportFormat::Komari => Box::<komari::KomariProtocolAdapter>::default(),
    }
}

/// 返回导出格式是否需要 reporter 生成 basic info 事件。
pub(crate) fn export_format_needs_basic_info(format: ExportFormat) -> bool {
    build_protocol_adapter(format).needs_basic_info_events()
}

/// 创建 Komari server 消息入站处理器。
pub(crate) fn build_komari_inbound_handler(
    config_manager: crate::config::ConfigManager,
    commands: crate::service::InboundCommandSender,
) -> Box<dyn InboundProtocolHandler> {
    komari::inbound_handler(config_manager, commands)
}
