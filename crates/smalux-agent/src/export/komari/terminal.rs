//! Komari terminal 消息解析。
//!
//! Komari 的远程终端会先通过 report WebSocket 下发 `message=terminal` 和
//! `request_id`，然后 agent 再打开独立 terminal WebSocket 并复用 remote shell manager。

use crate::config::model::{ExportAuthMode, ExportConfig};
use crate::export::TransportInboundMessage;
use crate::export::ws::WebSocketConfig;
use crate::service::shell::{
    RemoteShellFrame, RemoteShellInput, RemoteShellStreamCodec, RemoteShellStreamEvent,
    command_to_shell_input, parse_stream_command,
};
use serde::Deserialize;

/// Komari terminal 请求。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(super) struct TerminalRequest {
    /// Komari terminal 会话 ID。
    pub(super) request_id: String,
}

/// Komari server 文本消息。
#[derive(Debug, Deserialize)]
struct KomariServerMessage {
    /// 消息类型。
    message: String,
    /// terminal 请求 ID。
    request_id: Option<String>,
}

/// 解析 Komari terminal 请求。
pub(super) fn parse_terminal_request(input: &str) -> anyhow::Result<TerminalRequest> {
    let message: KomariServerMessage = serde_json::from_str(input)?;
    if message.message != "terminal" {
        anyhow::bail!("not a komari terminal message");
    }

    let request_id = message
        .request_id
        .filter(|request_id| !request_id.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("komari terminal request_id is required"))?;

    Ok(TerminalRequest { request_id })
}

/// Komari terminal stream codec。
#[derive(Debug, Default)]
pub(super) struct KomariTerminalCodec;

impl RemoteShellStreamCodec for KomariTerminalCodec {
    /// 返回 codec 名称。
    fn name(&self) -> &'static str {
        "komari_terminal"
    }

    /// Komari terminal stream 使用裸 binary frame，不经过 Smalux wire。
    fn configure_websocket(&self, config: WebSocketConfig) -> WebSocketConfig {
        config.with_raw_binary_frames(true)
    }

    /// 将 Komari terminal 入站消息转换为内部 shell 输入。
    fn decode_inbound(
        &self,
        msg: TransportInboundMessage,
    ) -> anyhow::Result<Option<RemoteShellInput>> {
        match msg {
            TransportInboundMessage::Binary(bytes) => Ok(Some(RemoteShellInput::Input(bytes))),
            TransportInboundMessage::Text(text) => decode_terminal_text_message(text),
        }
    }

    /// 将内部 shell 事件转换为 Komari terminal frame。
    fn encode_event(
        &self,
        event: RemoteShellStreamEvent,
    ) -> anyhow::Result<Option<RemoteShellFrame>> {
        match event {
            RemoteShellStreamEvent::Opened { .. } | RemoteShellStreamEvent::Exit { .. } => Ok(None),
            RemoteShellStreamEvent::Output {
                session_id, data, ..
            } => {
                let bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)?;
                tracing::trace!(
                    session_id = %session_id,
                    bytes = bytes.len(),
                    "remote shell komari raw output encoded"
                );
                Ok(Some(RemoteShellFrame::RawBinary(bytes)))
            }
            RemoteShellStreamEvent::Error { message, .. } => {
                Ok(Some(RemoteShellFrame::Text(message)))
            }
        }
    }
}

/// Komari terminal text frame 中可能出现的兼容命令。
///
/// 当前核心 Smalux stream command 使用 `data` 字段；这里额外接受 Komari 兼容层可能出现的
/// `input` 字段，避免把第三方字段 alias 泄漏到核心 shell 协议模型。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum KomariTerminalTextCommand {
    /// 写入 PTY 输入。
    Input {
        /// Komari 兼容输入字段。
        input: String,
    },
    /// 调整 PTY 终端尺寸。
    Resize {
        /// 终端列数。
        cols: u16,
        /// 终端行数。
        rows: u16,
    },
    /// 请求关闭会话。
    Close,
    /// stream 保活消息。
    Heartbeat,
}

/// 解码 Komari terminal text frame。
fn decode_terminal_text_message(text: String) -> anyhow::Result<Option<RemoteShellInput>> {
    match parse_stream_command(&text) {
        Ok(command) => return command_to_shell_input(command),
        Err(err) => {
            tracing::trace!(error = %err, bytes = text.len(), "komari terminal text is not smalux stream command");
        }
    }

    match serde_json::from_str::<KomariTerminalTextCommand>(&text) {
        Ok(command) => Ok(komari_text_command_to_shell_input(command)),
        Err(err) => {
            tracing::debug!(
                error = %err,
                bytes = text.len(),
                "komari terminal text message is not structured; forwarding raw input"
            );
            Ok(Some(RemoteShellInput::Input(text.into_bytes())))
        }
    }
}

/// 把 Komari terminal 兼容命令转换为内部 shell 输入。
fn komari_text_command_to_shell_input(
    command: KomariTerminalTextCommand,
) -> Option<RemoteShellInput> {
    let input = match command {
        KomariTerminalTextCommand::Input { input } => RemoteShellInput::Input(input.into_bytes()),
        KomariTerminalTextCommand::Resize { cols, rows } => RemoteShellInput::Resize { cols, rows },
        KomariTerminalTextCommand::Close => RemoteShellInput::Close,
        KomariTerminalTextCommand::Heartbeat => {
            tracing::debug!("komari terminal heartbeat received");
            return None;
        }
    };

    Some(input)
}

/// Komari terminal URL 已经包含 token/id/query，交给 shell manager 时避免重复追加认证 query。
pub(super) fn terminal_stream_export_config(mut export_config: ExportConfig) -> ExportConfig {
    export_config.auth_mode = ExportAuthMode::None;
    export_config.token = None;
    export_config.query.clear();
    export_config
}

#[cfg(test)]
mod tests {
    //! Komari terminal 消息解析测试。

    use super::*;

    /// 验证 terminal 消息可以解析 request_id。
    #[test]
    fn parse_terminal_request_reads_request_id() {
        let request =
            parse_terminal_request(r#"{ "message": "terminal", "request_id": "term-1" }"#).unwrap();

        assert_eq!(request.request_id, "term-1");
    }

    /// 验证非 terminal 消息会被忽略。
    #[test]
    fn parse_terminal_request_rejects_non_terminal_message() {
        let error = parse_terminal_request(r#"{ "message": "ping" }"#).unwrap_err();

        assert!(error.to_string().contains("not a komari terminal message"));
    }

    /// 验证 Komari terminal codec 会启用 raw binary frame。
    #[test]
    fn komari_terminal_codec_enables_raw_binary_frames() {
        let codec = KomariTerminalCodec;
        let config = codec.configure_websocket(WebSocketConfig::new("ws://127.0.0.1".to_string()));

        assert!(config.raw_binary_frames);
    }

    /// 验证 Komari terminal codec 会把 raw binary 转为 shell input。
    #[test]
    fn komari_terminal_codec_decodes_raw_binary_input() {
        let codec = KomariTerminalCodec;
        let input = codec
            .decode_inbound(TransportInboundMessage::Binary(b"echo ok\r\n".to_vec()))
            .unwrap()
            .unwrap();

        let RemoteShellInput::Input(bytes) = input else {
            panic!("expected input bytes");
        };

        assert_eq!(bytes, b"echo ok\r\n");
    }

    /// 验证 Komari terminal codec 会在兼容层解析 input 字段。
    #[test]
    fn komari_terminal_codec_decodes_input_alias_text() {
        let codec = KomariTerminalCodec;
        let input = codec
            .decode_inbound(TransportInboundMessage::Text(
                r#"{ "type": "input", "input": "echo komari\n" }"#.to_string(),
            ))
            .unwrap()
            .unwrap();

        let RemoteShellInput::Input(bytes) = input else {
            panic!("expected input bytes");
        };

        assert_eq!(bytes, b"echo komari\n");
    }
}
