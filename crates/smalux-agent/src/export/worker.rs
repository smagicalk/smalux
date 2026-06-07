//! Transport 后台发送 worker。
//!
//! worker 串行持有具体 transport client，并把真实发送结果回传给 export supervisor。

use super::{
    EncodedTransportMessage, ExportProtocol, ExportTransport, ExportTransportClient,
    InboundProtocolHandler, TransportId, TransportRequest,
};
use crate::export::ExportDeliveryId;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// 单个 transport worker 的发送队列容量。
const TRANSPORT_WORKER_QUEUE_CAPACITY: usize = 128;
/// transport 事件队列容量。
const TRANSPORT_EVENT_QUEUE_CAPACITY: usize = 128;

/// transport worker 事件发送端。
pub(crate) type TransportEventSender = mpsc::Sender<TransportEvent>;
/// transport worker 事件接收端。
pub(crate) type TransportEventReceiver = mpsc::Receiver<TransportEvent>;

/// 创建 transport 事件队列。
pub(crate) fn transport_event_channel() -> (TransportEventSender, TransportEventReceiver) {
    mpsc::channel(TRANSPORT_EVENT_QUEUE_CAPACITY)
}

/// transport worker 回传给 supervisor 的发送结果事件。
#[derive(Debug)]
pub(crate) enum TransportEvent {
    /// 单条请求已经真实发送成功。
    Sent {
        /// transport ID。
        transport: TransportId,
        /// delivery ID。
        delivery: ExportDeliveryId,
        /// 上报序号。
        sequence: u64,
    },
    /// 单条请求真实发送失败。
    Failed {
        /// transport ID。
        transport: TransportId,
        /// delivery ID。
        delivery: ExportDeliveryId,
        /// 上报序号。
        sequence: u64,
        /// 错误文本。
        error: String,
    },
}

/// transport worker 控制命令。
enum TransportWorkerCommand {
    /// 建立连接。
    Connect {
        /// 连接结果回传。
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    /// 设置服务端入站协议处理器。
    SetInboundHandler {
        /// handler 实例。
        handler: Box<dyn InboundProtocolHandler>,
        /// 设置结果回传。
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
    /// 发送单条请求。
    Send {
        /// delivery ID。
        delivery: ExportDeliveryId,
        /// 上报序号。
        sequence: u64,
        /// transport 请求。
        request: TransportRequest,
    },
    /// 关闭连接并退出 worker。
    Shutdown {
        /// 关闭结果回传。
        result_tx: oneshot::Sender<anyhow::Result<()>>,
    },
}

/// 单个 transport worker 句柄。
pub(crate) struct TransportWorkerHandle {
    /// transport ID。
    id: TransportId,
    /// transport 协议。
    protocol: ExportProtocol,
    /// worker 命令发送端。
    commands: mpsc::Sender<TransportWorkerCommand>,
    /// worker 任务句柄。
    task: JoinHandle<()>,
}

impl TransportWorkerHandle {
    /// 启动 worker。
    pub(crate) fn spawn(
        id: TransportId,
        client: ExportTransportClient,
        event_tx: TransportEventSender,
    ) -> Self {
        let protocol = client.protocol();
        let (commands, command_rx) =
            mpsc::channel::<TransportWorkerCommand>(TRANSPORT_WORKER_QUEUE_CAPACITY);
        let task = tokio::spawn(transport_worker_loop(id, client, event_tx, command_rx));

        Self {
            id,
            protocol,
            commands,
            task,
        }
    }

    /// 返回 transport ID。
    pub(crate) fn id(&self) -> TransportId {
        self.id
    }

    /// 返回 transport 协议。
    pub(crate) fn protocol(&self) -> ExportProtocol {
        self.protocol
    }

    /// 请求 worker 建立连接。
    pub(crate) async fn connect(&self) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .send(TransportWorkerCommand::Connect { result_tx })
            .await
            .map_err(|_| anyhow::anyhow!("transport worker channel is closed"))?;
        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("transport worker connect result channel is closed"))?
    }

    /// 请求 worker 设置入站协议处理器。
    pub(crate) async fn set_inbound_handler(
        &self,
        handler: Box<dyn InboundProtocolHandler>,
    ) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        self.commands
            .send(TransportWorkerCommand::SetInboundHandler { handler, result_tx })
            .await
            .map_err(|_| anyhow::anyhow!("transport worker channel is closed"))?;
        result_rx.await.map_err(|_| {
            anyhow::anyhow!("transport worker set inbound handler result channel is closed")
        })?
    }

    /// 投递发送请求；这里不等待真实发送结果，结果由 `TransportEvent` 回传。
    pub(crate) fn send(
        &self,
        delivery: ExportDeliveryId,
        sequence: u64,
        request: TransportRequest,
    ) -> anyhow::Result<()> {
        self.commands
            .try_send(TransportWorkerCommand::Send {
                delivery,
                sequence,
                request,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => anyhow::anyhow!(
                    "export transport queue is full: transport={}, capacity={}",
                    self.id.as_str(),
                    TRANSPORT_WORKER_QUEUE_CAPACITY
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("transport worker channel is closed")
                }
            })
    }

    /// 关闭 worker。
    pub(crate) async fn shutdown(self) -> anyhow::Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let send_result = self
            .commands
            .send(TransportWorkerCommand::Shutdown { result_tx })
            .await;
        if send_result.is_err() {
            let _ = self.task.await;
            anyhow::bail!("transport worker channel is closed");
        }

        let close_result = result_rx
            .await
            .map_err(|_| anyhow::anyhow!("transport worker shutdown result channel is closed"))?;
        let _ = self.task.await;
        close_result
    }
}

/// transport worker 主循环。
async fn transport_worker_loop(
    id: TransportId,
    mut client: ExportTransportClient,
    event_tx: TransportEventSender,
    mut command_rx: mpsc::Receiver<TransportWorkerCommand>,
) {
    let mut connected = matches!(client, ExportTransportClient::Http(_));
    tracing::debug!(
        transport = id.as_str(),
        protocol = client.protocol().as_str(),
        queue_capacity = TRANSPORT_WORKER_QUEUE_CAPACITY,
        "transport worker started"
    );

    while let Some(command) = command_rx.recv().await {
        match command {
            TransportWorkerCommand::Connect { result_tx } => {
                let result = connect_client(id, &mut client, &mut connected).await;
                let _ = result_tx.send(result);
            }
            TransportWorkerCommand::SetInboundHandler { handler, result_tx } => {
                let result = client.set_inbound_handler(handler).await;
                let _ = result_tx.send(result);
            }
            TransportWorkerCommand::Send {
                delivery,
                sequence,
                request,
            } => {
                let result = send_worker_request(id, &mut client, &mut connected, request).await;
                let event = match result {
                    Ok(()) => TransportEvent::Sent {
                        transport: id,
                        delivery,
                        sequence,
                    },
                    Err(error) => TransportEvent::Failed {
                        transport: id,
                        delivery,
                        sequence,
                        error: error.to_string(),
                    },
                };

                if event_tx.send(event).await.is_err() {
                    tracing::warn!(
                        transport = id.as_str(),
                        "transport event channel closed; worker stopping"
                    );
                    break;
                }
            }
            TransportWorkerCommand::Shutdown { result_tx } => {
                let result = close_client(&mut client, &mut connected).await;
                let _ = result_tx.send(result);
                break;
            }
        }
    }

    tracing::debug!(transport = id.as_str(), "transport worker stopped");
}

/// 确保 transport 已连接。
async fn connect_client(
    id: TransportId,
    client: &mut ExportTransportClient,
    connected: &mut bool,
) -> anyhow::Result<()> {
    if *connected {
        return Ok(());
    }

    tracing::info!(
        transport = id.as_str(),
        protocol = client.protocol().as_str(),
        "export transport connecting"
    );
    client.connect().await?;
    *connected = true;
    Ok(())
}

/// 关闭 transport。
async fn close_client(
    client: &mut ExportTransportClient,
    connected: &mut bool,
) -> anyhow::Result<()> {
    if !*connected {
        return Ok(());
    }

    client.close().await?;
    *connected = false;
    Ok(())
}

/// 发送单条 transport 请求。
async fn send_worker_request(
    id: TransportId,
    client: &mut ExportTransportClient,
    connected: &mut bool,
    request: TransportRequest,
) -> anyhow::Result<()> {
    connect_client(id, client, connected).await?;

    match (client, request) {
        (
            ExportTransportClient::WebSocket(transport),
            TransportRequest::WebSocketText { body, .. },
        ) => {
            transport
                .send_encoded_transport_message(EncodedTransportMessage::Text(body))
                .await
        }
        (
            ExportTransportClient::WebSocket(transport),
            TransportRequest::WebSocketBinary { sequence, body, .. },
        ) => {
            transport
                .send_encoded_transport_message(EncodedTransportMessage::Binary { sequence, body })
                .await
        }
        (
            ExportTransportClient::Http(transport),
            TransportRequest::HttpJson {
                method, url, body, ..
            },
        ) => transport.send_json(method, &url, &body).await,
        (transport, request) => {
            anyhow::bail!(
                "transport request does not match transport: transport={}, request={}",
                transport.protocol().as_str(),
                request.transport_id().as_str()
            )
        }
    }
}
