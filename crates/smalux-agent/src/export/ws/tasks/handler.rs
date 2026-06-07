//! WebSocket 后台任务消息处理逻辑。

use super::{
    CLOSE_HANDSHAKE_TIMEOUT, InboundMessageEvent, LISTENER_QUEUE_CAPACITY, WebSocketCommand,
    WebSocketWireState, WebSocketWriter,
};
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
            // secure_psk 模式下业务 JSON 必须走 SecureData，拒绝 text 可以避免控制命令
            // 绕过加密通道进入 ServiceControlListener。
            if wire_state.is_secure_psk() {
                tracing::warn!("websocket text message rejected in secure_psk wire mode");
                return false;
            }
            handle_data_message(
                ExportInboundMessage::Text(msg_bytes.to_string()),
                "text",
                msg_bytes.len(),
                IncomingDataContext {
                    write,
                    listener_lock,
                    listener_tx,
                    close_requested,
                    close_timeout: close_timeout.as_mut(),
                },
            )
            .await
        }
        Some(Ok(tungstenite::protocol::Message::Binary(bytes))) => {
            let len = bytes.len();
            // 非 raw 模式下 binary frame 先按 Smalux wire 解包；payload 才是后续解析
            // 自有协议 ServerFrame 的 UTF-8 JSON bytes；第三方兼容消息由对应 listener 处理。
            match decode_binary_payload(bytes.as_ref(), wire_state) {
                Ok((packet, payload)) => {
                    let payload_len = payload.len();
                    let handled = handle_data_message(
                        ExportInboundMessage::Binary(payload),
                        "binary",
                        len,
                        IncomingDataContext {
                            write,
                            listener_lock,
                            listener_tx,
                            close_requested,
                            close_timeout: close_timeout.as_mut(),
                        },
                    )
                    .await;

                    if handled {
                        match packet {
                            Some(packet) => {
                                tracing::debug!(
                                    packet_kind = packet.kind.as_str(),
                                    sequence = packet.sequence,
                                    payload_bytes = packet.payload.len(),
                                    "websocket binary wire packet received"
                                );
                            }
                            None => {
                                tracing::debug!(
                                    payload_bytes = payload_len,
                                    "websocket raw binary message received"
                                );
                            }
                        }
                    }

                    handled
                }
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
        Some(WebSocketCommand::SendRawBinary(bytes)) => {
            tracing::trace!(bytes = bytes.len(), "websocket sending raw binary message");
            if let Err(e) = write
                .send(tungstenite::Message::Binary(bytes::Bytes::from(bytes)))
                .await
            {
                tracing::error!(
                    error = %e,
                    "websocket raw binary message send failed; stopping background task"
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
    // transport 不解析 JSON 内容，只按 wire_mode 给业务 payload 加壳或加密。
    // 这样 report、ack/error、remote task result 和 shell stream event 可以复用同一发送路径。
    let packet = match wire_state {
        WebSocketWireState::RawBinary => return Ok(payload),
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
) -> anyhow::Result<(Option<WirePacket>, Vec<u8>)> {
    if matches!(wire_state, WebSocketWireState::RawBinary) {
        return Ok((None, input.to_vec()));
    }

    let packet = wire::decode_wire_packet(input)?;
    let payload = match wire_state {
        WebSocketWireState::RawBinary => unreachable!("handled before wire decode"),
        WebSocketWireState::BinaryPlain { .. } => {
            // binary_plain 只接受 PlainData，避免握手包或密文包被误当成明文控制消息。
            if packet.kind != WirePacketKind::PlainData {
                anyhow::bail!(
                    "unexpected wire packet kind for binary_plain: {}",
                    packet.kind.as_str()
                );
            }
            packet.payload.clone()
        }
        WebSocketWireState::SecurePsk { transport, .. } => {
            // secure_psk 只接受 SecureData；解密失败会停止连接并交给 export supervisor 重连。
            if packet.kind != WirePacketKind::SecureData {
                anyhow::bail!(
                    "unexpected wire packet kind for secure_psk: {}",
                    packet.kind.as_str()
                );
            }
            security::decrypt_payload(transport, &packet.payload)?
        }
    };

    Ok((Some(packet), payload))
}

/// 单条入站数据消息处理所需的读循环上下文。
struct IncomingDataContext<'a> {
    /// WebSocket 写半边，用于队列满或关闭时主动发送 close frame。
    write: &'a mut WebSocketWriter,
    /// 当前业务 listener 快照来源。
    listener_lock: &'a Arc<RwLock<Option<Arc<dyn ExportMessageListener>>>>,
    /// listener 串行执行队列。
    listener_tx: &'a mpsc::Sender<InboundMessageEvent>,
    /// 当前连接是否已经进入关闭握手。
    close_requested: &'a mut bool,
    /// 关闭握手超时计时器。
    close_timeout: std::pin::Pin<&'a mut tokio::time::Sleep>,
}

/// 处理服务端数据消息。
async fn handle_data_message(
    payload: ExportInboundMessage,
    message_kind: &'static str,
    bytes: usize,
    mut ctx: IncomingDataContext<'_>,
) -> bool {
    tracing::debug!(bytes, message_kind, "websocket data message received");
    if *ctx.close_requested {
        tracing::debug!(
            bytes,
            message_kind,
            "websocket is closing; data message ignored"
        );
        return true;
    }

    // 只在锁内克隆当前 listener 快照，业务回调不持有锁。
    let listener = ctx.listener_lock.read().await.clone();
    let Some(listener) = listener else {
        tracing::warn!(
            bytes,
            message_kind,
            "websocket data message received without listener"
        );
        return true;
    };

    let event = InboundMessageEvent { payload, listener };

    // listener 回调放进独立有界队列串行执行。WebSocket 读循环只负责收包和解包；
    // 如果业务处理变慢，背压会在这里显式表现为队列满，而不是无界占用内存。
    match ctx.listener_tx.try_send(event) {
        Ok(()) => true,
        Err(TrySendError::Full(_event)) => {
            tracing::warn!(
                queue_capacity = LISTENER_QUEUE_CAPACITY,
                "listener queue full; closing websocket"
            );
            if let Err(e) = ctx.write.send(tungstenite::Message::Close(None)).await {
                tracing::debug!(
                    error = %e,
                    "failed to send close frame after listener queue full"
                );
                return false;
            }
            *ctx.close_requested = true;
            ctx.close_timeout
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
