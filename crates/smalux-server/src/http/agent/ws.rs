//! agent 主连接 WebSocket upgrade。

pub(crate) mod connection_registry;
pub(crate) mod session;

use crate::state::AppState;
use axum::{
    Error,
    extract::{State, WebSocketUpgrade},
    response::Response,
};
use futures_util::SinkExt;

enum AgentWireAction {
    Continue,
    SendBinary(Vec<u8>),
    Close,
}

async fn handle_agent_wire_packet(bytes: &[u8]) -> anyhow::Result<AgentWireAction> {
    let packet = smalux_protocol::wire::decode_wire_packet(bytes)?;

    tracing::debug!(
        wire_kind = packet.kind.as_str(),
        wire_sequence = packet.sequence,
        payload_bytes = packet.payload.len(),
        "agent websocket received wire packet"
    );

    match packet.kind {
        smalux_protocol::wire::WirePacketKind::PlainData => {
            let client_frame = smalux_protocol::decode_client_frame_bytes(&packet.payload)?;
            Ok(AgentWireAction::Continue)
        }
        smalux_protocol::wire::WirePacketKind::Hello => {
            // TODO: secure_psk 握手：decode hello -> 查 key -> responder -> 回 handshake
            let hello = smalux_protocol::secure::decode_secure_hello(&packet.payload)?;
            // let key
            tracing::debug!(
                "agent websocket received secure hello before
              secure handler is implemented"
            );
            Ok(AgentWireAction::Close)
        }
        smalux_protocol::wire::WirePacketKind::Handshake => {
            // TODO: secure_psk 握手第二阶段
            tracing::debug!(
                "agent websocket received secure handshake
              before secure handler is implemented"
            );
            Ok(AgentWireAction::Close)
        }
        smalux_protocol::wire::WirePacketKind::SecureData => {
            // TODO: decrypt payload -> decode ClientFrame
            tracing::debug!(
                "agent websocket received secure payload
              before secure handler is implemented"
            );
            Ok(AgentWireAction::Close)
        }
        smalux_protocol::wire::WirePacketKind::Close => {
            tracing::info!(
                wire_sequence = packet.sequence,
                "agent
              websocket received wire close packet"
            );
            Ok(AgentWireAction::Close)
        }
    }
}

/// 最小 agent WebSocket upgrade 入口。
///
/// 当前只验证 upgrade 链路能正常建立，不进入真实的 wire/frame 读写逻辑。
/// 后续再在这里接入认证、连接注册、读写循环和 secure_psk。
pub async fn upgrade_agent_ws(ws: WebSocketUpgrade, State(_state): State<AppState>) -> Response {
    ws.on_upgrade(|mut socket| async move {
        tracing::info!("agent websocket upgraded");

        // 这里先只建立连接并立即等待关闭，后续再补真实收发循环。
        while let Some(message) = socket.recv().await {
            match message {
                Ok(axum::extract::ws::Message::Text(_)) => {
                    tracing::debug!("agent websocket received placeholder message");
                }
                Ok(axum::extract::ws::Message::Binary(client_frame_bytes)) => {}
                Ok(axum::extract::ws::Message::Ping(data)) => {
                    match socket.send(axum::extract::ws::Message::Pong(data)).await {
                        Ok(_) => {
                            tracing::debug!("agent websocket sent pong");
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "agent websocket send pong failed");
                            break;
                        }
                    }
                }
                Ok(axum::extract::ws::Message::Pong(_)) => {
                    tracing::debug!("agent websocket received pong");
                }
                Ok(axum::extract::ws::Message::Close(_)) => {
                    tracing::info!("agent websocket closed by peer");
                    match socket.close().await {
                        Ok(_) => {
                            tracing::debug!("agent websocket closed bye");
                        }
                        Err(_) => {
                            tracing::error!("agent websocket closed with error");
                        }
                    }
                    break;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "agent websocket receive failed");
                    break;
                }
            }
        }
    })
}
