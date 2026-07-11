//! 导出路由。
//!
//! Router 负责把内部上报语义交给 adapter 编码，并把编码后的请求投递给 transport hub。
//! 后续要把发送队列移动到长期 worker 时，可以优先改这里，service 层不需要理解每种 transport。

use super::{ExportDeliveryId, ProtocolAdapter, TransportHub, TransportPlan, TransportRequest};
use crate::config::model::ExportConfig;
use crate::service::outbound::{
    BasicInfoEnvelope, ControlAckEnvelope, ControlErrorEnvelope, RemoteJobResultEnvelope,
    RemoteTaskResultEnvelope,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
use smalux_core::log::{
    redact_sensitive_json, redact_sensitive_json_bytes, redact_sensitive_json_text,
    redact_sensitive_text,
};
use smalux_protocol::ClientEvent;

/// 导出日志选项。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ExportLogOptions {
    /// 是否允许 trace 日志输出实际 payload 预览。
    payload_enabled: bool,
    /// payload 预览最大原始字节数。
    payload_max_bytes: usize,
}

impl ExportLogOptions {
    /// 根据启动期日志配置创建导出日志选项。
    pub(crate) const fn new(payload_enabled: bool, payload_max_bytes: usize) -> Self {
        Self {
            payload_enabled,
            payload_max_bytes,
        }
    }
}

/// 导出路由器。
pub(crate) struct ExportRouter {
    /// 当前导出格式 adapter。
    adapter: Box<dyn ProtocolAdapter + Send + Sync>,
    /// 当前导出链路日志选项。
    log_options: ExportLogOptions,
}

impl ExportRouter {
    /// 创建导出路由器。
    pub(crate) fn new(
        adapter: Box<dyn ProtocolAdapter + Send + Sync>,
        log_options: ExportLogOptions,
    ) -> Self {
        Self {
            adapter,
            log_options,
        }
    }

    /// 根据当前 adapter 生成 transport plan。
    pub(crate) fn transport_plan(
        &mut self,
        config: &ExportConfig,
    ) -> anyhow::Result<TransportPlan> {
        self.adapter.transport_plan(config)
    }

    /// 将内部上报语义编码成 transport 请求。
    pub(crate) fn encode_report(
        &mut self,
        delivery_id: ExportDeliveryId,
        outbound: &ClientEvent,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        self.adapter.encode_report(delivery_id, outbound)
    }

    /// 编码并投递单个 delivery 的上报事件。
    pub(crate) async fn send_report(
        &mut self,
        transport_hub: &mut TransportHub,
        delivery_id: ExportDeliveryId,
        outbound: &ClientEvent,
    ) -> anyhow::Result<usize> {
        let requests = self.encode_report(delivery_id, outbound)?;
        let request_count = requests.len();
        tracing::trace!(
            delivery = delivery_id.as_str(),
            sequence = outbound.sequence,
            request_count,
            "export router encoded report"
        );

        // adapter 可以返回 0 条请求，表示当前格式不支持或明确跳过该事件。
        // router 只负责投递，不把“跳过”当错误。
        for request in requests {
            trace_transport_request(delivery_id, outbound.sequence, &request, self.log_options);
            transport_hub.enqueue(delivery_id, outbound.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递低频基础信息事件。
    pub(crate) async fn send_basic_info(
        &mut self,
        transport_hub: &mut TransportHub,
        info: &BasicInfoEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_basic_info(info)?;
        let request_count = requests.len();
        tracing::trace!(
            delivery = ExportDeliveryId::BasicInfo.as_str(),
            sequence = info.sequence,
            request_count,
            "export router encoded basic info"
        );

        for request in requests {
            trace_transport_request(
                ExportDeliveryId::BasicInfo,
                info.sequence,
                &request,
                self.log_options,
            );
            transport_hub.enqueue(ExportDeliveryId::BasicInfo, info.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递远程任务结果。
    pub(crate) async fn send_remote_task_result(
        &mut self,
        transport_hub: &mut TransportHub,
        result: &RemoteTaskResultEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_remote_task_result(result)?;
        let request_count = requests.len();
        tracing::trace!(
            delivery = ExportDeliveryId::RemoteTaskResult.as_str(),
            sequence = result.sequence,
            request_count,
            "export router encoded remote task result"
        );

        for request in requests {
            trace_transport_request(
                ExportDeliveryId::RemoteTaskResult,
                result.sequence,
                &request,
                self.log_options,
            );
            transport_hub.enqueue(ExportDeliveryId::RemoteTaskResult, result.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递通用远程 job 结果。
    pub(crate) async fn send_remote_job_result(
        &mut self,
        transport_hub: &mut TransportHub,
        result: &RemoteJobResultEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_remote_job_result(result)?;
        let request_count = requests.len();
        tracing::trace!(
            delivery = ExportDeliveryId::JobResult.as_str(),
            sequence = result.sequence,
            request_count,
            "export router encoded remote job result"
        );

        for request in requests {
            trace_transport_request(
                ExportDeliveryId::JobResult,
                result.sequence,
                &request,
                self.log_options,
            );
            transport_hub.enqueue(ExportDeliveryId::JobResult, result.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递控制命令确认。
    pub(crate) async fn send_control_ack(
        &mut self,
        transport_hub: &mut TransportHub,
        ack: &ControlAckEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_control_ack(ack)?;
        let request_count = requests.len();
        tracing::trace!(
            delivery = ExportDeliveryId::ControlAck.as_str(),
            sequence = ack.sequence,
            request_count,
            "export router encoded control ack"
        );

        for request in requests {
            trace_transport_request(
                ExportDeliveryId::ControlAck,
                ack.sequence,
                &request,
                self.log_options,
            );
            transport_hub.enqueue(ExportDeliveryId::ControlAck, ack.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递控制命令错误。
    pub(crate) async fn send_control_error(
        &mut self,
        transport_hub: &mut TransportHub,
        error: &ControlErrorEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_control_error(error)?;
        let request_count = requests.len();
        tracing::trace!(
            delivery = ExportDeliveryId::ControlError.as_str(),
            sequence = error.sequence,
            request_count,
            "export router encoded control error"
        );

        for request in requests {
            trace_transport_request(
                ExportDeliveryId::ControlError,
                error.sequence,
                &request,
                self.log_options,
            );
            transport_hub.enqueue(ExportDeliveryId::ControlError, error.sequence, request)?;
        }

        Ok(request_count)
    }
}

/// 记录单条 transport 请求的形态，不输出请求体内容。
fn trace_transport_request(
    delivery_id: ExportDeliveryId,
    sequence: u64,
    request: &TransportRequest,
    log_options: ExportLogOptions,
) {
    match request {
        TransportRequest::WebSocketText { transport, body } => {
            let preview = log_options
                .payload_enabled
                .then(|| payload_preview_text(body, log_options.payload_max_bytes));
            tracing::trace!(
                delivery = delivery_id.as_str(),
                sequence,
                transport = transport.as_str(),
                request_kind = "websocket_text",
                body_bytes = body.len(),
                payload_enabled = log_options.payload_enabled,
                payload_preview = preview.as_deref(),
                "export router enqueueing transport request"
            );
        }
        TransportRequest::WebSocketBinary {
            transport,
            sequence: request_sequence,
            body,
        } => {
            let preview = log_options
                .payload_enabled
                .then(|| payload_preview_binary(body, log_options.payload_max_bytes));
            tracing::trace!(
                delivery = delivery_id.as_str(),
                sequence,
                request_sequence,
                transport = transport.as_str(),
                request_kind = "websocket_binary",
                body_bytes = body.len(),
                payload_enabled = log_options.payload_enabled,
                payload_preview = preview.as_deref(),
                "export router enqueueing transport request"
            );
        }
        TransportRequest::HttpJson {
            transport,
            method,
            body,
            ..
        } => {
            let body_bytes = json_body_len(body);
            let preview = log_options
                .payload_enabled
                .then(|| payload_preview_json(body, log_options.payload_max_bytes));
            tracing::trace!(
                delivery = delivery_id.as_str(),
                sequence,
                transport = transport.as_str(),
                method = method.as_str(),
                request_kind = "http_json",
                body_bytes,
                payload_enabled = log_options.payload_enabled,
                payload_preview = preview.as_deref(),
                "export router enqueueing transport request"
            );
        }
    }
}

/// 计算 JSON 请求体序列化后的字节数。
fn json_body_len(body: &Value) -> usize {
    serde_json::to_vec(body)
        .map(|bytes| bytes.len())
        .unwrap_or_else(|_| body.to_string().len())
}

/// 生成文本 payload 预览，按原始字节截断并用 UTF-8 lossy 展示。
fn payload_preview_text(body: &str, max_bytes: usize) -> String {
    if let Some(redacted) = redact_sensitive_json_text(body) {
        return payload_preview_from_bytes(
            "json",
            body.len(),
            redacted.value.as_bytes(),
            max_bytes,
            PreviewEncoding::Utf8Lossy,
            redacted.redacted,
        );
    }

    let redacted = redact_sensitive_text(body);
    payload_preview_from_bytes(
        "text",
        body.len(),
        redacted.value.as_bytes(),
        max_bytes,
        PreviewEncoding::Utf8Lossy,
        redacted.redacted,
    )
}

/// 生成 JSON payload 预览。
fn payload_preview_json(body: &Value, max_bytes: usize) -> String {
    payload_preview_json_with_original_len(body, json_body_len(body), max_bytes)
}

/// 生成 JSON payload 预览，并保留原始 body 字节数用于日志判断。
fn payload_preview_json_with_original_len(
    body: &Value,
    original_len: usize,
    max_bytes: usize,
) -> String {
    let redacted = redact_sensitive_json(body);
    let bytes = serde_json::to_vec(&redacted.value)
        .unwrap_or_else(|_| redacted.value.to_string().into_bytes());
    payload_preview_from_bytes(
        "json",
        original_len,
        &bytes,
        max_bytes,
        PreviewEncoding::Utf8Lossy,
        redacted.redacted,
    )
}

/// 生成二进制 payload 预览；使用 base64，避免不可见字节污染日志。
fn payload_preview_binary(body: &[u8], max_bytes: usize) -> String {
    if let Ok(text) = std::str::from_utf8(body) {
        if let Some(redacted) = redact_sensitive_json_bytes(body) {
            return payload_preview_from_bytes(
                "json",
                body.len(),
                redacted.value.as_bytes(),
                max_bytes,
                PreviewEncoding::Utf8Lossy,
                redacted.redacted,
            );
        }

        let redacted = redact_sensitive_text(text);
        return payload_preview_from_bytes(
            "binary_text",
            body.len(),
            redacted.value.as_bytes(),
            max_bytes,
            PreviewEncoding::Utf8Lossy,
            redacted.redacted,
        );
    }

    payload_preview_from_bytes(
        "binary",
        body.len(),
        body,
        max_bytes,
        PreviewEncoding::Base64,
        false,
    )
}

/// payload 预览编码方式。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum PreviewEncoding {
    /// 文本用 UTF-8 lossy 展示。
    Utf8Lossy,
    /// 二进制用 base64 展示。
    Base64,
}

/// 构造统一 payload 预览字符串，明确类型、原始长度、截断状态和编码方式。
fn payload_preview_from_bytes(
    kind: &str,
    original_len: usize,
    display_bytes: &[u8],
    max_bytes: usize,
    encoding: PreviewEncoding,
    redacted: bool,
) -> String {
    let take_len = display_bytes.len().min(max_bytes);
    let truncated = display_bytes.len() > take_len;
    let preview = match encoding {
        PreviewEncoding::Utf8Lossy => {
            String::from_utf8_lossy(&display_bytes[..take_len]).to_string()
        }
        PreviewEncoding::Base64 => STANDARD.encode(&display_bytes[..take_len]),
    };
    let encoding = match encoding {
        PreviewEncoding::Utf8Lossy => "utf8_lossy",
        PreviewEncoding::Base64 => "base64",
    };

    format!(
        "kind={kind}; encoding={encoding}; bytes={original_len}; redacted={redacted}; truncated={truncated}; preview={preview}",
    )
}

#[cfg(test)]
mod tests {
    //! 导出路由测试。

    use super::*;
    use crate::config::model::ExportFormat;
    use crate::export::{TransportRequest, build_protocol_adapter};
    use smalux_core::model::info::AgentReport;

    /// 验证 router 会复用 adapter 的编码结果。
    #[test]
    fn router_encodes_report_with_current_adapter() {
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();
        let outbound = smalux_protocol::ClientEvent::snapshot(1, 100, report);
        let mut router = ExportRouter::new(
            build_protocol_adapter(ExportFormat::SmaluxJson),
            ExportLogOptions::new(false, 4096),
        );

        let requests = router
            .encode_report(ExportDeliveryId::RealtimeReport, &outbound)
            .unwrap();

        assert!(matches!(
            requests.as_slice(),
            [TransportRequest::WebSocketBinary { sequence: 1, .. }]
        ));
    }

    /// 验证文本 payload 预览按字节截断并标出截断状态。
    #[test]
    fn payload_preview_text_truncates_by_bytes() {
        let preview = payload_preview_text("abcdef", 3);

        assert!(preview.contains("kind=text"));
        assert!(preview.contains("bytes=6"));
        assert!(preview.contains("truncated=true"));
        assert!(preview.contains("preview=abc"));
    }

    /// 验证二进制 payload 预览使用 base64。
    #[test]
    fn payload_preview_binary_uses_base64() {
        let preview = payload_preview_binary(&[0xff, 0x00, 0x01, 0x02], 2);

        assert!(preview.contains("kind=binary"));
        assert!(preview.contains("encoding=base64"));
        assert!(preview.contains("bytes=4"));
        assert!(preview.contains("redacted=false"));
        assert!(preview.contains("truncated=true"));
        assert!(preview.contains("preview=/wA="));
    }

    /// 验证 JSON payload 会递归脱敏敏感字段。
    #[test]
    fn payload_preview_json_redacts_sensitive_fields() {
        let body = serde_json::json!({
            "token": "secret-token",
            "nested": {
                "api_key": "secret-key",
                "normal": "visible"
            },
            "stdout": "command output"
        });

        let preview = payload_preview_json(&body, 4096);

        assert!(preview.contains("redacted=true"));
        assert!(preview.contains(r#""token":"<redacted>""#));
        assert!(preview.contains(r#""api_key":"<redacted>""#));
        assert!(preview.contains(r#""stdout":"<redacted>""#));
        assert!(preview.contains(r#""normal":"visible""#));
        assert!(!preview.contains("secret-token"));
        assert!(!preview.contains("secret-key"));
        assert!(!preview.contains("command output"));
    }

    /// 验证 shell stream 的 data 字段会按上下文脱敏。
    #[test]
    fn payload_preview_json_redacts_shell_stream_data() {
        let body = serde_json::json!({
            "type": "output",
            "session_id": "shell-1",
            "data": "base64-output"
        });

        let preview = payload_preview_json(&body, 4096);

        assert!(preview.contains("redacted=true"));
        assert!(preview.contains(r#""data":"<redacted>""#));
        assert!(!preview.contains("base64-output"));
    }

    /// 验证非 JSON 文本 payload 会按 key-value 关键词脱敏。
    #[test]
    fn payload_preview_text_redacts_sensitive_assignments() {
        let preview = payload_preview_text("token=secret&name=node command: whoami", 4096);

        assert!(preview.contains("redacted=true"));
        assert!(preview.contains("token=<redacted>"));
        assert!(preview.contains("name=node"));
        assert!(preview.contains("command: <redacted>"));
        assert!(!preview.contains("secret"));
        assert!(!preview.contains("whoami"));
    }

    /// 验证二进制 UTF-8 JSON payload 会先按 JSON 脱敏。
    #[test]
    fn payload_preview_binary_json_redacts_before_preview() {
        let body = br#"{ "authorization": "Bearer secret", "value": 1 }"#;

        let preview = payload_preview_binary(body, 4096);

        assert!(preview.contains("kind=json"));
        assert!(preview.contains("redacted=true"));
        assert!(preview.contains(r#""authorization":"<redacted>""#));
        assert!(!preview.contains("Bearer secret"));
    }
}
