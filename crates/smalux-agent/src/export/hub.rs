//! 导出 transport client 和 hub。

use super::worker::{TransportEventSender, TransportWorkerHandle};
use super::{
    EncodedExportMessage, ExportDeliveryId, ExportMessageListener, ExportProtocol, ExportTransport,
    TransportId, TransportPlan, TransportRequest, TransportSpec, http, ws,
};

/// 协议无关的导出 transport client。
pub(crate) enum ExportTransportClient {
    /// WebSocket transport。
    WebSocket(ws::WebSocketClient),
    /// HTTP transport。
    Http(http::HttpClient),
}

impl ExportTransportClient {
    /// 返回当前 transport 使用的协议。
    pub(crate) fn protocol(&self) -> ExportProtocol {
        match self {
            Self::WebSocket(_client) => ExportProtocol::WebSocket,
            Self::Http(_client) => ExportProtocol::Http,
        }
    }
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
            .into_transports()
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
        delivery_id: ExportDeliveryId,
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

        entry.worker.send(delivery_id, sequence, request)
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
