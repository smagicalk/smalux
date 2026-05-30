//! WebSocket 客户端连接状态机。

use super::config::WebSocketConfig;
use super::request::{build_connect_request, build_connect_url, redact_url};
use super::tasks::{
    TASK_SHUTDOWN_TIMEOUT, WebSocketCommand, WebSocketWireState, spawn_websocket_tasks,
    wait_for_task,
};
use crate::config::model::ExportWireMode;
use crate::export::security;
use crate::export::wire::{self, WirePacket, WirePacketKind};
use crate::export::{EncodedExportMessage, ExportMessageListener, ExportTransport};
use anyhow::anyhow;
use futures_util::{SinkExt, StreamExt};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};
use tokio_tungstenite::{connect_async, connect_async_tls_with_config};

/// 默认连接超时时间。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
/// 业务发送 channel 的缓冲大小。
const COMMAND_CHANNEL_BUFFER: usize = 1024;
/// 安全握手超时时间。
const SECURE_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(8);

/// WebSocket 客户端状态。
pub(crate) struct WebSocketClient {
    /// WebSocket 客户端配置。
    config: WebSocketConfig,
    /// 后台写任务的发送入口。
    sender: Arc<Mutex<Option<tokio::sync::mpsc::Sender<WebSocketCommand>>>>,
    /// 后台收发任务句柄，用于 close 时等待任务退出。
    task: Option<JoinHandle<()>>,
    /// 监听器 worker 任务句柄。
    listener_task: Option<JoinHandle<()>>,
    /// 服务端文本消息监听器。
    listener: Arc<tokio::sync::RwLock<Option<Arc<dyn ExportMessageListener>>>>,
}

impl Debug for WebSocketClient {
    /// 输出关键连接配置，便于调试连接问题。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketClient")
            .field("config", &self.config)
            .finish()
    }
}

impl WebSocketClient {
    /// 根据完整配置创建 WebSocket 客户端。
    pub(crate) fn new_with_config(config: WebSocketConfig) -> Self {
        Self {
            config,
            sender: Arc::new(tokio::sync::Mutex::new(None)),
            task: None,
            listener_task: None,
            listener: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// 启动后台收发任务。
    fn start_listener(
        &mut self,
        websocket: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        receive: tokio::sync::mpsc::Receiver<WebSocketCommand>,
        wire_state: WebSocketWireState,
    ) {
        let (task, listener_task) = spawn_websocket_tasks(
            websocket,
            receive,
            self.config.heartbeat,
            wire_state,
            self.listener.clone(),
        );
        self.task = Some(task);
        self.listener_task = Some(listener_task);
    }

    /// 关闭已启动的后台任务。
    async fn wait_for_shutdown_tasks(&mut self) {
        if let Some(task) = self.task.take() {
            wait_for_task(task, TASK_SHUTDOWN_TIMEOUT, "websocket background task").await;
        }

        if let Some(task) = self.listener_task.take() {
            wait_for_task(task, TASK_SHUTDOWN_TIMEOUT, "listener worker").await;
        }
    }

    /// 根据配置准备 wire 状态；secure_psk 会先在裸 websocket 上完成 Noise 握手。
    async fn prepare_wire_state(
        &self,
        websocket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> anyhow::Result<WebSocketWireState> {
        match self.config.wire_mode {
            ExportWireMode::BinaryPlain => Ok(WebSocketWireState::BinaryPlain {
                session_id: uuid::Uuid::new_v4().into_bytes(),
            }),
            ExportWireMode::SecurePsk => self.secure_psk_handshake(websocket).await,
        }
    }

    /// 执行 secure_psk 握手并返回可用的 Noise transport 状态。
    async fn secure_psk_handshake(
        &self,
        websocket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> anyhow::Result<WebSocketWireState> {
        let session_id = uuid::Uuid::new_v4().into_bytes();
        let key = self
            .config
            .secure_key
            .as_ref()
            .ok_or_else(|| anyhow!("secure key is required for secure_psk wire mode"))?;
        tracing::info!(
            key_id = %key.key_id,
            "websocket secure_psk handshake starting"
        );

        let mut handshake = security::build_noise_initiator(&key.psk)?;
        send_wire_packet(
            websocket,
            WirePacket::new(
                WirePacketKind::Hello,
                session_id,
                0,
                security::encode_secure_hello(&key.key_id)?,
            ),
        )
        .await?;

        // NNpsk0 不传静态公钥，认证完全来自双方是否能用同一个 PSK 完成握手。
        // 第 1 条 handshake 由 agent 发出，第 2 条必须由 server responder 返回。
        let first = security::write_handshake_message(&mut handshake, b"")?;
        send_wire_packet(
            websocket,
            WirePacket::new(WirePacketKind::Handshake, session_id, 1, first),
        )
        .await?;

        let response = timeout(SECURE_HANDSHAKE_TIMEOUT, receive_wire_packet(websocket)).await??;
        if response.kind != WirePacketKind::Handshake {
            anyhow::bail!(
                "unexpected secure handshake response packet kind: {}",
                response.kind.as_str()
            );
        }
        if response.session_id != session_id {
            anyhow::bail!("secure handshake response session_id mismatch");
        }
        // read_handshake_message 会校验 responder 是否持有正确 PSK；失败就不能进入 transport mode。
        security::read_handshake_message(&mut handshake, &response.payload)?;
        let transport = handshake.into_transport_mode()?;

        tracing::info!(
            key_id = %key.key_id,
            "websocket secure_psk handshake completed"
        );
        Ok(WebSocketWireState::SecurePsk {
            session_id,
            transport,
        })
    }

    /// 发送关闭命令，并容忍后台任务已经退出的情况。
    async fn send_close_command(&mut self) {
        match self.sender.lock().await.take() {
            None => {
                tracing::debug!("websocket close requested without active connection");
            }
            Some(sender) => {
                tracing::info!("websocket close requested; sending close command");
                if let Err(e) = sender.send(WebSocketCommand::Close).await {
                    tracing::debug!(
                        error = %e,
                        "websocket close command could not be delivered; continuing cleanup"
                    );
                }
            }
        }
    }

    /// Drop 路径无法 await 当前对象方法，因此用独立异步任务完成清理。
    fn spawn_drop_cleanup(
        sender: Arc<Mutex<Option<tokio::sync::mpsc::Sender<WebSocketCommand>>>>,
        task: Option<JoinHandle<()>>,
        listener_task: Option<JoinHandle<()>>,
    ) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Some(sender) = sender.lock().await.take() {
                    let _ = sender.send(WebSocketCommand::Close).await;
                    drop(sender);
                }

                if let Some(task) = task {
                    wait_for_task(task, TASK_SHUTDOWN_TIMEOUT, "websocket background task").await;
                }

                if let Some(task) = listener_task {
                    wait_for_task(task, TASK_SHUTDOWN_TIMEOUT, "listener worker").await;
                }
            });
        } else {
            abort_task(task);
            abort_task(listener_task);
        }
    }
}

/// 在握手阶段直接发送 wire packet。
async fn send_wire_packet(
    websocket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    packet: WirePacket,
) -> anyhow::Result<()> {
    let kind = packet.kind.as_str();
    let sequence = packet.sequence;
    let bytes = wire::encode_wire_packet(&packet)?;
    websocket
        .send(tungstenite::Message::Binary(bytes::Bytes::from(bytes)))
        .await?;
    tracing::debug!(packet_kind = kind, sequence, "websocket wire packet sent");
    Ok(())
}

/// 在握手阶段直接读取下一条 binary wire packet。
async fn receive_wire_packet(
    websocket: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
) -> anyhow::Result<WirePacket> {
    loop {
        let Some(message) = websocket.next().await else {
            anyhow::bail!("websocket closed during secure handshake");
        };
        match message? {
            tungstenite::Message::Binary(bytes) => return Ok(wire::decode_wire_packet(&bytes)?),
            tungstenite::Message::Ping(bytes) => {
                websocket.send(tungstenite::Message::Pong(bytes)).await?;
            }
            tungstenite::Message::Close(frame) => {
                anyhow::bail!("websocket closed during secure handshake: {frame:?}");
            }
            other => {
                tracing::debug!(?other, "websocket non-binary handshake message ignored");
            }
        }
    }
}

/// 无 Tokio runtime 时只能直接 abort 后台任务。
fn abort_task(task: Option<JoinHandle<()>>) {
    if let Some(task) = task {
        task.abort();
    }
}

impl WebSocketClient {
    /// 将文本内容送入后台发送队列。
    async fn enqueue_text(&mut self, msg: String) -> anyhow::Result<()> {
        self.enqueue_command(WebSocketCommand::SendText(msg)).await
    }

    /// 将二进制内容送入后台发送队列。
    async fn enqueue_binary(&mut self, sequence: u64, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.enqueue_command(WebSocketCommand::SendBinary { sequence, bytes })
            .await
    }

    /// 将命令送入后台发送队列。
    async fn enqueue_command(&mut self, command: WebSocketCommand) -> anyhow::Result<()> {
        match self.sender.lock().await.as_mut() {
            None => {
                anyhow::bail!("client not connected");
            }
            Some(sender) => {
                sender.send(command).await?;
            }
        }
        Ok(())
    }
}

impl ExportTransport for WebSocketClient {
    /// 建立 WebSocket 连接，认证成功后启动后台收发任务。
    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.sender.lock().await.is_some() || self.task.is_some() || self.listener_task.is_some()
        {
            anyhow::bail!("websocket client is already connected");
        }

        tracing::info!(
            url = %redact_url(&build_connect_url(&self.config)?),
            auth = self.config.auth.kind(),
            token_set = self.config.auth.token_set(),
            unsafe_cert = self.config.unsafe_cert,
            heartbeat_secs = self.config.heartbeat,
            "websocket connecting"
        );

        let request = build_connect_request(&self.config)?;
        let mut ws;
        if self.config.unsafe_cert {
            // unsafe_cert 只用于测试或自签名环境，会跳过服务端证书校验。
            tracing::warn!(
                url = %redact_url(&build_connect_url(&self.config)?),
                "unsafe_cert enabled; TLS server certificate verification is disabled"
            );
            let builder = crate::export::rustls::client_config_builder()?;
            let verifier =
                crate::export::rustls::UnsafeNoCertificateVerification::from_client_config_builder(
                    &builder,
                );
            let cfg = builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();
            let _res;
            (ws, _res) = timeout(
                CONNECT_TIMEOUT,
                connect_async_tls_with_config(
                    request,
                    None,
                    false,
                    Some(tokio_tungstenite::Connector::Rustls(Arc::new(cfg))),
                ),
            )
            .await??;
        } else {
            // 默认路径使用 rustls/tungstenite 的正常证书校验。
            let _res;
            (ws, _res) = timeout(CONNECT_TIMEOUT, connect_async(request)).await??;
        }

        let wire_state = match self.prepare_wire_state(&mut ws).await {
            Err(e) => {
                // 安全握手失败时直接关闭裸连接，后台任务此时尚未启动。
                tracing::warn!(error = %e, "websocket wire negotiation failed; closing connection");
                let _ = ws.close(None).await;
                return Err(anyhow!(e));
            }
            Ok(wire_state) => wire_state,
        };

        // wire 准备完成后创建业务发送 channel，再启动后台收发循环。
        let (sender, stop_receiver) = tokio::sync::mpsc::channel(COMMAND_CHANNEL_BUFFER);
        self.start_listener(ws, stop_receiver, wire_state);
        self.sender.lock().await.replace(sender);
        tracing::info!(
            url = %redact_url(&build_connect_url(&self.config)?),
            "websocket connected"
        );
        Ok(())
    }

    /// 发送文本消息。
    async fn send_text_message(&mut self, msg: &str) -> anyhow::Result<()> {
        self.enqueue_text(msg.to_string()).await
    }

    /// 发送已编码消息。
    async fn send_encoded_export_message(
        &mut self,
        msg: EncodedExportMessage,
    ) -> anyhow::Result<()> {
        match msg {
            EncodedExportMessage::Text(text) => self.enqueue_text(text).await,
            EncodedExportMessage::Binary { sequence, body } => {
                self.enqueue_binary(sequence, body).await
            }
        }
    }

    /// 替换当前消息监听器。
    async fn set_listener(
        &mut self,
        listener: Box<dyn ExportMessageListener>,
    ) -> anyhow::Result<()> {
        self.listener.write().await.replace(Arc::from(listener));
        tracing::debug!("websocket listener updated");
        Ok(())
    }

    /// 关闭连接并清空发送入口。
    async fn close(&mut self) -> anyhow::Result<()> {
        self.send_close_command().await;
        self.wait_for_shutdown_tasks().await;
        Ok(())
    }
}

impl Drop for WebSocketClient {
    /// 对象释放时尽量通知后台任务关闭连接。
    fn drop(&mut self) {
        let sender = self.sender.clone();
        let task = self.task.take();
        let listener_task = self.listener_task.take();

        Self::spawn_drop_cleanup(sender, task, listener_task);
    }
}
