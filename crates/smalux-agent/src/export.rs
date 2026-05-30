//! Agent 数据导出抽象。
//!
//! 这里定义发送端和消息监听器的通用 trait，具体协议实现放在子模块。service 层只关心
//! “要发送什么语义”，adapter 决定“编码成什么格式”，transport 决定“怎么发出去”。

use crate::config::model::{ExportConfig, ExportFormat, JobConfig, JobsConfig};
use crate::service::outbound::{
    ControlAckEnvelope, ControlErrorEnvelope, RemoteProbeResultEnvelope, RemoteTaskResultEnvelope,
};
use smalux_protocol::{
    OutboundReport, encode_ack_as_smalux_json_bytes, encode_outbound_report_as_smalux_json_bytes,
    encode_protocol_error_as_smalux_json_bytes, encode_remote_probe_result_as_smalux_json_bytes,
    encode_remote_task_result_as_smalux_json_bytes,
};
use std::pin::Pin;
use std::time::Duration;

/// HTTP 导出实现。
mod http;
/// Komari 兼容导出实现。
mod komari;
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

pub(crate) use router::ExportRouter;
pub(crate) use worker::{TransportEvent, TransportEventReceiver, transport_event_channel};
use worker::{TransportEventSender, TransportWorkerHandle};

/// 已编码完成、可以交给 transport 发送的导出消息。
///
/// `Binary` 的 body 是业务 payload，不一定是最终 WebSocket frame。Smalux 自有协议会在
/// WebSocket transport 中再封成 `WirePacket`，并按 `wire_mode` 决定是否加密。
pub(crate) enum EncodedExportMessage {
    /// 文本消息。
    Text(String),
    /// 二进制业务数据，由具体 transport 决定是否封包或加密。
    Binary {
        /// 业务序号，用于 wire packet 和排查日志。
        sequence: u64,
        /// 业务 payload。
        body: Vec<u8>,
    },
}

/// 从 transport 收到的导出消息。
pub(crate) enum ExportInboundMessage {
    /// 文本消息。
    Text(String),
    /// 二进制消息。
    Binary(Vec<u8>),
}

/// 把入站消息统一转成 UTF-8 文本。
pub(crate) fn inbound_message_into_string(msg: ExportInboundMessage) -> anyhow::Result<String> {
    match msg {
        ExportInboundMessage::Text(text) => Ok(text),
        ExportInboundMessage::Binary(bytes) => Ok(String::from_utf8(bytes)?),
    }
}

/// 导出 transport 标识。
///
/// 当前实时上报和低频基础信息各自有稳定 transport ID。
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) enum TransportId {
    /// 实时上报通道。
    RealtimeReport,
    /// 低频基础信息通道。
    BasicInfo,
}

impl TransportId {
    /// 返回稳定日志名称。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RealtimeReport => "realtime_report",
            Self::BasicInfo => "basic_info",
        }
    }
}

/// 单个 transport 的启动规格。
pub(crate) enum TransportSpec {
    /// WebSocket 长连接规格。
    WebSocket {
        /// transport ID。
        id: TransportId,
        /// WebSocket 配置。
        config: ws::WebSocketConfig,
        /// 是否在 pipeline 连接阶段立即建立 WebSocket。
        connect_on_start: bool,
    },
    /// HTTP 短请求规格。
    Http {
        /// transport ID。
        id: TransportId,
        /// HTTP transport 配置。
        config: http::HttpConfig,
    },
}

impl TransportSpec {
    /// 返回 transport ID。
    fn id(&self) -> TransportId {
        match self {
            Self::WebSocket { id, .. } => *id,
            Self::Http { id, .. } => *id,
        }
    }
}

/// 导出 job ID。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportJobId {
    /// 实时上报 job。
    RealtimeReport,
    /// Komari basic info 低频上报 job。
    BasicInfo,
    /// 远程任务结果即时回传。
    RemoteTaskResult,
    /// 远程网络探测结果即时回传。
    RemoteProbeResult,
    /// 控制命令确认即时回传。
    ControlAck,
    /// 控制命令错误即时回传。
    ControlError,
}

impl ExportJobId {
    /// 返回日志使用的 job 名称。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RealtimeReport => "realtime_report",
            Self::BasicInfo => "basic_info",
            Self::RemoteTaskResult => "remote_task_result",
            Self::RemoteProbeResult => "remote_probe_result",
            Self::ControlAck => "control_ack",
            Self::ControlError => "control_error",
        }
    }
}

/// 导出 job 触发方式。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportJobTrigger {
    /// 最新 report 更新后触发。
    OnLatestReport,
    /// 按固定间隔触发。
    Interval(Duration),
}

/// 导出 job 失败处理策略。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportJobFailurePolicy {
    /// 失败后重建导出 pipeline，适合长连接实时上报。
    ReconnectPipeline,
    /// 失败只记录日志，等待下一次调度，适合低频辅助 HTTP 请求。
    LogAndContinue,
}

/// adapter 需要运行的导出 job。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ExportJobSpec {
    /// job ID。
    pub(crate) id: ExportJobId,
    /// job 触发方式。
    pub(crate) trigger: ExportJobTrigger,
    /// 是否在拿到第一份 report 后立即运行。
    pub(crate) run_on_start: bool,
    /// 失败处理策略。
    pub(crate) failure_policy: ExportJobFailurePolicy,
}

impl ExportJobSpec {
    /// 创建跟随最新 report 的实时上报 job。
    pub(crate) fn on_latest_report(id: ExportJobId) -> Self {
        Self {
            id,
            trigger: ExportJobTrigger::OnLatestReport,
            run_on_start: true,
            failure_policy: ExportJobFailurePolicy::ReconnectPipeline,
        }
    }

    /// 创建固定间隔上报 job。
    pub(crate) fn interval(
        id: ExportJobId,
        interval: Duration,
        failure_policy: ExportJobFailurePolicy,
    ) -> Self {
        Self {
            id,
            trigger: ExportJobTrigger::Interval(interval),
            run_on_start: true,
            failure_policy,
        }
    }
}

/// adapter 需要启动的 transport 和 job 集合。
///
/// 这个 plan 是导出层的扩展点：同一份内部 report 可以被不同 adapter 拆成不同 transport
/// 和 job，例如 Smalux 默认只用实时 WebSocket，而 Komari 同时需要 WebSocket report 和
/// HTTP basic info。
pub(crate) struct TransportPlan {
    /// 所有 transport 规格。
    transports: Vec<TransportSpec>,
    /// 所有导出 job 规格。
    jobs: Vec<ExportJobSpec>,
}

impl TransportPlan {
    /// 创建默认只有实时上报 job 的 plan。
    fn new(transports: Vec<TransportSpec>) -> Self {
        Self {
            transports,
            jobs: vec![ExportJobSpec::on_latest_report(ExportJobId::RealtimeReport)],
        }
    }

    /// 创建带自定义 job 的 plan。
    fn with_jobs(transports: Vec<TransportSpec>, jobs: Vec<ExportJobSpec>) -> Self {
        Self { transports, jobs }
    }

    /// 返回 job 列表。
    pub(crate) fn jobs(&self) -> &[ExportJobSpec] {
        &self.jobs
    }

    /// 应用运行时 job 配置，禁用的 job 会从 plan 中移除。
    pub(crate) fn apply_job_config(&mut self, config: &JobsConfig) {
        self.jobs.retain_mut(|job| {
            let Some(job_config) = match_job_config(config, job.id) else {
                return true;
            };
            if !job_config.enabled {
                tracing::info!(job = job.id.as_str(), "export job disabled");
                return false;
            }

            job.trigger = ExportJobTrigger::Interval(job_config.interval);
            job.run_on_start = job_config.run_on_start;
            true
        });
    }
}

/// 根据 job ID 读取对应配置。
fn match_job_config(config: &JobsConfig, job_id: ExportJobId) -> Option<&JobConfig> {
    match job_id {
        ExportJobId::RealtimeReport => Some(&config.realtime_report),
        ExportJobId::BasicInfo => Some(&config.basic_info),
        ExportJobId::RemoteTaskResult
        | ExportJobId::RemoteProbeResult
        | ExportJobId::ControlAck
        | ExportJobId::ControlError => None,
    }
}

/// adapter 编码后的单次发送请求。
#[derive(Debug)]
pub(crate) enum TransportRequest {
    /// 发送 WebSocket 文本消息。
    WebSocketText {
        /// 目标 transport。
        transport: TransportId,
        /// 文本内容。
        body: String,
    },
    /// 发送 WebSocket 二进制消息。
    WebSocketBinary {
        /// 目标 transport。
        transport: TransportId,
        /// 业务序号。
        sequence: u64,
        /// 二进制内容。
        body: Vec<u8>,
    },
    /// 发送 HTTP JSON 请求。
    HttpJson {
        /// 目标 transport。
        transport: TransportId,
        /// HTTP method。
        method: http::HttpMethod,
        /// 完整请求 URL。
        url: String,
        /// JSON body。
        body: serde_json::Value,
    },
}

impl TransportRequest {
    /// 返回目标 transport。
    fn transport_id(&self) -> TransportId {
        match self {
            Self::WebSocketText { transport, .. } | Self::WebSocketBinary { transport, .. } => {
                *transport
            }
            Self::HttpJson { transport, .. } => *transport,
        }
    }
}

/// 导出格式适配器。
pub(crate) trait ExportAdapter {
    /// 根据配置声明需要启动的 transport。
    fn transport_plan(&mut self, config: &ExportConfig) -> anyhow::Result<TransportPlan>;

    /// 将内部上报语义编码成零到多条 transport 请求。
    fn encode_report(
        &mut self,
        job_id: ExportJobId,
        outbound: &OutboundReport,
    ) -> anyhow::Result<Vec<TransportRequest>>;

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

/// Smalux 默认 JSON adapter。
#[derive(Debug, Default)]
struct SmaluxJsonAdapter;

impl ExportAdapter for SmaluxJsonAdapter {
    /// smalux_json 当前使用主 WebSocket transport。
    fn transport_plan(&mut self, config: &ExportConfig) -> anyhow::Result<TransportPlan> {
        match ExportProtocol::from_server_url(&config.server_url)? {
            ExportProtocol::WebSocket => Ok(TransportPlan::new(vec![TransportSpec::WebSocket {
                id: TransportId::RealtimeReport,
                config: ws::WebSocketConfig::try_from(config)?,
                connect_on_start: true,
            }])),
            ExportProtocol::Http => {
                anyhow::bail!("smalux_json format does not support http transport yet")
            }
        }
    }

    /// 编码为 smalux JSON bytes，封包和加密由 WebSocket transport 处理。
    fn encode_report(
        &mut self,
        job_id: ExportJobId,
        outbound: &OutboundReport,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        if job_id != ExportJobId::RealtimeReport {
            anyhow::bail!(
                "smalux_json does not support export job: {}",
                job_id.as_str()
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
            task_id = %crate::service::display_probe_task_id(&result.result.task_id),
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

/// 根据配置创建导出格式 adapter。
pub(crate) fn build_export_adapter(format: ExportFormat) -> Box<dyn ExportAdapter + Send + Sync> {
    match format {
        ExportFormat::SmaluxJson => Box::<SmaluxJsonAdapter>::default(),
        ExportFormat::Komari => Box::<komari::KomariAdapter>::default(),
    }
}

/// 创建 Komari server 消息监听器。
pub(crate) fn build_komari_message_listener(
    config_manager: crate::config::ConfigManager,
    commands: crate::service::InboundCommandSender,
) -> Box<dyn ExportMessageListener> {
    komari::message_listener(config_manager, commands)
}

/// 当前可用的导出协议。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportProtocol {
    /// WebSocket 导出协议。
    WebSocket,
    /// HTTP 导出协议。
    Http,
}

impl ExportProtocol {
    /// 根据导出地址推断协议类型。
    fn from_server_url(server_url: &str) -> anyhow::Result<Self> {
        let Some((scheme, _rest)) = server_url.trim().split_once(':') else {
            anyhow::bail!("export.server_url must include a protocol scheme");
        };

        match scheme.to_ascii_lowercase().as_str() {
            "ws" | "wss" => Ok(Self::WebSocket),
            "http" | "https" => Ok(Self::Http),
            other => anyhow::bail!("unsupported export protocol: {other}"),
        }
    }

    /// 协议名称，用于结构化日志。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WebSocket => "websocket",
            Self::Http => "http",
        }
    }
}

/// 协议无关的导出 transport client。
enum ExportTransportClient {
    /// WebSocket transport。
    WebSocket(ws::WebSocketClient),
    /// HTTP transport。
    Http(http::HttpClient),
}

impl ExportTransportClient {
    /// 返回当前 transport 使用的协议。
    fn protocol(&self) -> ExportProtocol {
        match self {
            Self::WebSocket(_client) => ExportProtocol::WebSocket,
            Self::Http(_client) => ExportProtocol::Http,
        }
    }
}

/// 可动态分发的消息监听器。
///
/// trait 方法返回 boxed future，避免 `async fn` 直接出现在 trait object 中导致无法 dyn 兼容。
pub(crate) trait ExportMessageListener: Send + Sync + 'static {
    /// 收到 transport 消息后的回调。
    fn on_message(
        &self,
        msg: ExportInboundMessage,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + '_>>;
}

/// 导出端统一发送接口。
///
/// 后续无论是 WebSocket、HTTP 还是本地文件，都可以按这个接口接入 pipeline。
pub(crate) trait ExportTransport {
    /// 建立连接并启动后台收发任务。
    async fn connect(&mut self) -> anyhow::Result<()>;

    /// 发送已经序列化好的文本消息。
    async fn send_text_message(&mut self, msg: &str) -> anyhow::Result<()>;

    /// 发送已经编码好的消息。
    async fn send_encoded_export_message(
        &mut self,
        msg: EncodedExportMessage,
    ) -> anyhow::Result<()>;

    /// 设置服务端消息监听器。
    async fn set_listener(
        &mut self,
        listener: Box<dyn ExportMessageListener>,
    ) -> anyhow::Result<()>;

    /// 主动关闭连接。
    async fn close(&mut self) -> anyhow::Result<()>;
}

impl ExportTransport for ExportTransportClient {
    /// 建立协议连接。
    async fn connect(&mut self) -> anyhow::Result<()> {
        match self {
            Self::WebSocket(client) => client.connect().await,
            Self::Http(_client) => Ok(()),
        }
    }

    /// 发送文本消息。
    async fn send_text_message(&mut self, msg: &str) -> anyhow::Result<()> {
        match self {
            Self::WebSocket(client) => client.send_text_message(msg).await,
            Self::Http(_client) => {
                anyhow::bail!("http transport does not support raw text messages")
            }
        }
    }

    /// 发送已编码消息。
    async fn send_encoded_export_message(
        &mut self,
        msg: EncodedExportMessage,
    ) -> anyhow::Result<()> {
        match self {
            Self::WebSocket(client) => client.send_encoded_export_message(msg).await,
            Self::Http(_client) => match msg {
                EncodedExportMessage::Text(_text) => {
                    anyhow::bail!("http transport does not support raw text messages")
                }
                EncodedExportMessage::Binary { .. } => {
                    anyhow::bail!("http transport does not support raw binary messages")
                }
            },
        }
    }

    /// 设置服务端消息监听器。
    async fn set_listener(
        &mut self,
        listener: Box<dyn ExportMessageListener>,
    ) -> anyhow::Result<()> {
        match self {
            Self::WebSocket(client) => client.set_listener(listener).await,
            Self::Http(_client) => Ok(()),
        }
    }

    /// 主动关闭连接。
    async fn close(&mut self) -> anyhow::Result<()> {
        match self {
            Self::WebSocket(client) => client.close().await,
            Self::Http(_client) => Ok(()),
        }
    }
}

/// 单个导出 transport 的运行状态。
struct TransportEntry {
    /// 是否在 pipeline 连接阶段立即连接。
    connect_on_start: bool,
    /// transport worker。
    worker: TransportWorkerHandle,
}

/// 导出 transport 管理器。
///
/// 按 adapter 的 `TransportPlan` 管理多个 transport。
pub(crate) struct TransportHub {
    /// 已启动或待启动的 transport。
    transports: Vec<TransportEntry>,
}

impl TransportHub {
    /// 根据 adapter 声明的 plan 创建 transport hub。
    pub(crate) fn from_plan(
        plan: TransportPlan,
        event_tx: TransportEventSender,
    ) -> anyhow::Result<Self> {
        let transports = plan
            .transports
            .into_iter()
            .map(|spec| {
                let id = spec.id();
                let (client, connect_on_start) = match spec {
                    TransportSpec::WebSocket {
                        config,
                        connect_on_start,
                        ..
                    } => (
                        ExportTransportClient::WebSocket(ws::WebSocketClient::new_with_config(
                            config,
                        )),
                        connect_on_start,
                    ),
                    TransportSpec::Http { config, .. } => (
                        ExportTransportClient::Http(http::HttpClient::new(config)?),
                        false,
                    ),
                };
                let worker = TransportWorkerHandle::spawn(id, client, event_tx.clone());
                Ok(TransportEntry {
                    connect_on_start,
                    worker,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self { transports })
    }

    /// 返回 transport 概要，用于日志。
    pub(crate) fn summary(&self) -> String {
        self.transports
            .iter()
            .map(|entry| {
                format!(
                    "{}:{}",
                    entry.worker.id().as_str(),
                    entry.worker.protocol().as_str()
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// 给实时上报 transport 设置 server 消息监听器。
    pub(crate) async fn set_realtime_report_listener(
        &mut self,
        listener: Box<dyn ExportMessageListener>,
    ) -> anyhow::Result<()> {
        let Some(entry) = self.find_transport_mut(TransportId::RealtimeReport) else {
            anyhow::bail!("realtime report export transport is not configured");
        };

        entry.worker.set_listener(listener).await
    }

    /// 连接所有长连接 transport。
    pub(crate) async fn connect_all(&mut self) -> anyhow::Result<()> {
        for entry in &mut self.transports {
            if entry.connect_on_start {
                entry.worker.connect().await?;
            } else {
                tracing::debug!(
                    transport = entry.worker.id().as_str(),
                    protocol = entry.worker.protocol().as_str(),
                    "export transport connection deferred"
                );
            }
        }
        Ok(())
    }

    /// 投递单条 transport request 到对应 transport worker。
    pub(crate) fn enqueue(
        &mut self,
        job_id: ExportJobId,
        sequence: u64,
        request: TransportRequest,
    ) -> anyhow::Result<()> {
        let transport_id = request.transport_id();
        let Some(entry) = self.find_transport_mut(transport_id) else {
            anyhow::bail!(
                "export transport is not configured: {}",
                transport_id.as_str()
            );
        };

        entry.worker.send(job_id, sequence, request)
    }

    /// 关闭所有 transport。
    pub(crate) async fn close_all(&mut self) -> anyhow::Result<()> {
        let mut first_error = None;
        let transports = std::mem::take(&mut self.transports);
        for entry in transports {
            if let Err(err) = entry.worker.shutdown().await {
                tracing::warn!(error = ?err, "export transport close failed");
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    /// 查找指定 transport。
    fn find_transport_mut(&mut self, id: TransportId) -> Option<&mut TransportEntry> {
        self.transports
            .iter_mut()
            .find(|entry| entry.worker.id() == id)
    }
}

#[cfg(test)]
mod tests {
    //! 导出协议选择测试。

    use super::*;
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
