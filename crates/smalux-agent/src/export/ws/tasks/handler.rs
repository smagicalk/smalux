//! WebSocket 后台任务消息处理逻辑。

use super::{
    CLOSE_HANDSHAKE_TIMEOUT, InboundMessageEvent, LISTENER_QUEUE_CAPACITY, WebSocketCommand,
    WebSocketWireState, WebSocketWriter,
};
use crate::config::model::ExportWireMode;
use crate::export::security;
use crate::export::wire::{self, WirePacket, WirePacketKind};
use crate::export::{ExportInboundMessage, ExportMessageListener};
use futures_util::SinkExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite;

/// 处理服务端发来的消息。
pub(super) async fn handle_incoming_message(
    msg: Option<Result<tungstenite::protocol::Message, tungstenite::Error>>,
    write: &mut WebSocketWriter,
    wire_state: &mut WebSocketWireState,
    listener_lock: &Arc<RwLock<Option<Arc<dyn ExportMessageListener>>>>,
    listener_tx: &mpsc::Sender<InboundMessageEvent>,
    close_requested: &mut bool,
    mut close_timeout: std::pin::Pin<&mut tokio::time::Sleep>,
) -> bool {
    match msg {
        Some(Ok(tungstenite::protocol::Message::Text(msg_bytes))) => {
            if matches!(wire_state.mode(), ExportWireMode::SecurePsk) {
                tracing::warn!("websocket text message rejected in secure_psk wire mode");
                return false;
            }
            handle_data_message(
                ExportInboundMessage::Text(msg_bytes.to_string()),
                "text",
                msg_bytes.len(),
                write,
                listener_lock,
                listener_tx,
                close_requested,
                close_timeout.as_mut(),
            )
            .await
        }
        Some(Ok(tungstenite::protocol::Message::Binary(bytes))) => {
            let len = bytes.len();
            match decode_binary_payload(bytes.as_ref(), wire_state) {
                Ok((packet, payload)) => handle_data_message(
                    ExportInboundMessage::Binary(payload),
                    "binary",
                    len,
                    write,
                    listener_lock,
                    listener_tx,
                    close_requested,
                    close_timeout.as_mut(),
                )
                .await
                .then(|| {
                    tracing::debug!(
                        packet_kind = packet.kind.as_str(),
                        sequence = packet.sequence,
                        payload_bytes = packet.payload.len(),
                        "websocket binary wire packet received"
                    );
                    true
                })
                .unwrap_or(false),
                Err(err) => {
                    tracing::warn!(error = %err, "websocket binary packet decode failed");
                    false
                }
            }
        }
        Some(Ok(tungstenite::protocol::Message::Close(frame))) => {
            log_close_frame(frame, *close_requested);
            if let Err(e) = write.flush().await {
                tracing::debug!(error = %e, "websocket close response flush failed");
            }
            false
        }
        Some(Ok(tungstenite::protocol::Message::Pong(msg))) => {
            tracing::trace!(bytes = msg.len(), "websocket pong received");
            true
        }
        Some(Ok(tungstenite::protocol::Message::Ping(msg))) if !*close_requested => {
            tracing::trace!(bytes = msg.len(), "websocket ping received; sending pong");
            if let Err(e) = write.send(tungstenite::protocol::Message::Pong(msg)).await {
                tracing::error!(error = %e, "websocket pong failed; stopping background task");
                return false;
            }
            true
        }
        Some(Ok(tungstenite::protocol::Message::Ping(msg))) => {
            tracing::trace!(bytes = msg.len(), "websocket is closing; ping ignored");
            true
        }
        Some(Ok(tungstenite::protocol::Message::Frame(_))) => {
            tracing::warn!("websocket raw frame received; ignored");
            true
        }
        Some(Err(e)) => {
            tracing::error!(error = %e, "websocket read failed; stopping background task");
            false
        }
        None => {
            tracing::info!("websocket read stream ended; stopping background task");
            false
        }
    }
}

/// 处理外部发送或关闭命令。
pub(super) async fn handle_command(
    receive_msg: Option<WebSocketCommand>,
    write: &mut WebSocketWriter,
    wire_state: &mut WebSocketWireState,
    close_requested: &mut bool,
    mut close_timeout: std::pin::Pin<&mut tokio::time::Sleep>,
) -> bool {
    match receive_msg {
        None => {
            tracing::debug!("websocket command channel closed; sending close frame");
            send_close_frame(write, close_requested, close_timeout.as_mut()).await
        }
        Some(WebSocketCommand::SendText(msg)) => {
            tracing::trace!(bytes = msg.len(), "websocket sending text message");
            if let Err(e) = write
                .send(tungstenite::Message::from(msg.to_string()))
                .await
            {
                tracing::error!(
                    error = %e,
                    bytes = msg.len(),
                    "websocket text message send failed; stopping background task"
                );
                return false;
            }
            true
        }
        Some(WebSocketCommand::SendBinary { sequence, bytes }) => {
            let message_bytes = match encode_binary_payload(wire_state, sequence, bytes) {
                Ok(message_bytes) => message_bytes,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        sequence,
                        "websocket binary message encode failed; stopping background task"
                    );
                    return false;
                }
            };
            tracing::trace!(
                bytes = message_bytes.len(),
                sequence,
                "websocket sending binary message"
            );
            if let Err(e) = write
                .send(tungstenite::Message::Binary(bytes::Bytes::from(
                    message_bytes,
                )))
                .await
            {
                tracing::error!(
                    error = %e,
                    "websocket binary message send failed; stopping background task"
                );
                return false;
            }
            true
        }
        Some(WebSocketCommand::Close) => {
            tracing::info!("websocket close command received; sending close frame");
            send_close_frame(write, close_requested, close_timeout.as_mut()).await
        }
    }
}

/// 根据 wire 状态编码业务 payload。
fn encode_binary_payload(
    wire_state: &mut WebSocketWireState,
    sequence: u64,
    payload: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let packet = match wire_state {
        WebSocketWireState::BinaryPlain { session_id } => {
            WirePacket::plain_data(*session_id, sequence, payload)
        }
        WebSocketWireState::SecurePsk {
            session_id,
            transport,
        } => {
            let encrypted = security::encrypt_payload(transport, &payload)?;
            WirePacket::secure_data(*session_id, sequence, encrypted)
        }
    };

    Ok(wire::encode_wire_packet(&packet)?)
}

/// 根据 wire 状态解码业务 payload。
fn decode_binary_payload(
    input: &[u8],
    wire_state: &mut WebSocketWireState,
) -> anyhow::Result<(WirePacket, Vec<u8>)> {
    let packet = wire::decode_wire_packet(input)?;
    let payload = match wire_state {
        WebSocketWireState::BinaryPlain { .. } => {
            if packet.kind != WirePacketKind::PlainData {
                anyhow::bail!(
                    "unexpected wire packet kind for binary_plain: {}",
                    packet.kind.as_str()
                );
            }
            packet.payload.clone()
        }
        WebSocketWireState::SecurePsk { transport, .. } => {
            if packet.kind != WirePacketKind::SecureData {
                anyhow::bail!(
                    "unexpected wire packet kind for secure_psk: {}",
                    packet.kind.as_str()
                );
            }
            security::decrypt_payload(transport, &packet.payload)?
        }
    };

    Ok((packet, payload))
}

/// 处理服务端数据消息。
async fn handle_data_message(
    payload: ExportInboundMessage,
    message_kind: &'static str,
    bytes: usize,
    write: &mut WebSocketWriter,
    listener_lock: &Arc<RwLock<Option<Arc<dyn ExportMessageListener>>>>,
    listener_tx: &mpsc::Sender<InboundMessageEvent>,
    close_requested: &mut bool,
    mut close_timeout: std::pin::Pin<&mut tokio::time::Sleep>,
) -> bool {
    tracing::debug!(bytes, message_kind, "websocket data message received");
    if *close_requested {
        tracing::debug!(
            bytes,
            message_kind,
            "websocket is closing; data message ignored"
        );
        return true;
    }

    // 只在锁内克隆当前 listener 快照，业务回调不持有锁。
    let listener = listener_lock.read().await.clone();
    let Some(listener) = listener else {
        tracing::warn!(
            bytes,
            message_kind,
            "websocket data message received without listener"
        );
        return true;
    };

    let event = InboundMessageEvent { payload, listener };

    match listener_tx.try_send(event) {
        Ok(()) => true,
        Err(TrySendError::Full(_event)) => {
            tracing::warn!(
                queue_capacity = LISTENER_QUEUE_CAPACITY,
                "listener queue full; closing websocket"
            );
            if let Err(e) = write.send(tungstenite::Message::Close(None)).await {
                tracing::debug!(
                    error = %e,
                    "failed to send close frame after listener queue full"
                );
                return false;
            }
            *close_requested = true;
            close_timeout
                .as_mut()
                .reset(Instant::now() + CLOSE_HANDSHAKE_TIMEOUT);
            true
        }
        Err(TrySendError::Closed(_event)) => {
            tracing::warn!("listener worker channel closed; stopping websocket task");
            false
        }
    }
}

/// 发送 close frame 并切换到等待对端确认状态。
async fn send_close_frame(
    write: &mut WebSocketWriter,
    close_requested: &mut bool,
    mut close_timeout: std::pin::Pin<&mut tokio::time::Sleep>,
) -> bool {
    if let Err(e) = write.send(tungstenite::Message::Close(None)).await {
        tracing::debug!(error = %e, "websocket close frame send failed");
        return false;
    }
    *close_requested = true;
    close_timeout
        .as_mut()
        .reset(Instant::now() + CLOSE_HANDSHAKE_TIMEOUT);
    true
}

/// 输出 close frame 日志。
fn log_close_frame(frame: Option<tungstenite::protocol::CloseFrame>, close_requested: bool) {
    if close_requested {
        tracing::info!(?frame, "websocket close acknowledgement received");
    } else {
        tracing::info!(?frame, "websocket close frame received");
    }
}
