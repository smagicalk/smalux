//! 远程 shell stream codec。
//!
//! shell manager 只处理统一的 `RemoteShellInput` / `RemoteShellStreamEvent` 语义；
//! 不同服务端协议通过 codec 把外部 frame 转换成这些内部语义。

use super::message::{
    RemoteShellStreamCommand, RemoteShellStreamEvent, decode_input_bytes, encode_stream_event,
    parse_stream_command,
};
use crate::export::ws::WebSocketConfig;
use crate::export::{ExportInboundMessage, inbound_message_into_string};

/// shell stream codec 共享引用。
pub(crate) type RemoteShellStreamCodecRef = std::sync::Arc<dyn RemoteShellStreamCodec>;

/// stream 中收到的输入事件。
pub(crate) enum RemoteShellInput {
    /// 写入 PTY 输入。
    Input(Vec<u8>),
    /// 调整终端尺寸。
    Resize { cols: u16, rows: u16 },
    /// 关闭会话。
    Close,
}

/// codec 输出给 WebSocket transport 的 frame。
pub(crate) enum RemoteShellFrame {
    /// 使用 Smalux binary wire 发送，是否加密由 WebSocketConfig 的 wire_mode 决定。
    SmaluxWire(Vec<u8>),
    /// 发送 WebSocket text frame。
    Text(String),
    /// 发送不经过 Smalux wire 的 WebSocket raw binary frame。
    RawBinary(Vec<u8>),
}

/// 远程 shell stream codec。
///
/// codec 只负责协议 frame 和内部 shell 语义之间的转换；加密、重连和 WebSocket
/// close 处理仍然属于 transport 层。
pub(crate) trait RemoteShellStreamCodec: Send + Sync + std::fmt::Debug {
    /// codec 名称，用于结构化日志。
    fn name(&self) -> &'static str;

    /// 根据协议需要调整临时 WebSocket 配置。
    fn configure_websocket(&self, config: WebSocketConfig) -> WebSocketConfig {
        config
    }

    /// 将入站 WebSocket 消息转换为 shell 输入。
    fn decode_inbound(&self, msg: ExportInboundMessage)
    -> anyhow::Result<Option<RemoteShellInput>>;

    /// 将 shell 事件转换为 WebSocket frame。
    fn encode_event(
        &self,
        event: RemoteShellStreamEvent,
    ) -> anyhow::Result<Option<RemoteShellFrame>>;
}

/// Smalux 自有 shell stream codec。
#[derive(Debug, Default)]
pub(crate) struct SmaluxShellCodec;

impl RemoteShellStreamCodec for SmaluxShellCodec {
    /// 返回 codec 名称。
    fn name(&self) -> &'static str {
        "smalux_shell"
    }

    /// Smalux shell stream 入站消息必须是 JSON command。
    fn decode_inbound(
        &self,
        msg: ExportInboundMessage,
    ) -> anyhow::Result<Option<RemoteShellInput>> {
        let msg = inbound_message_into_string(msg)?;
        let command = parse_stream_command(&msg)?;
        command_to_shell_input(command)
    }

    /// Smalux shell stream 出站事件走 Smalux binary wire。
    fn encode_event(
        &self,
        event: RemoteShellStreamEvent,
    ) -> anyhow::Result<Option<RemoteShellFrame>> {
        let text = encode_stream_event(&event)?;
        Ok(Some(RemoteShellFrame::SmaluxWire(text.into_bytes())))
    }
}

/// 把结构化 stream command 转换为 PTY 输入事件。
pub(crate) fn command_to_shell_input(
    command: RemoteShellStreamCommand,
) -> anyhow::Result<Option<RemoteShellInput>> {
    let input = match command {
        RemoteShellStreamCommand::Resize { cols, rows } => RemoteShellInput::Resize { cols, rows },
        RemoteShellStreamCommand::Close => RemoteShellInput::Close,
        RemoteShellStreamCommand::Heartbeat => {
            tracing::debug!("remote shell stream heartbeat received");
            return Ok(None);
        }
        command => RemoteShellInput::Input(decode_input_bytes(command)?),
    };
    Ok(Some(input))
}

#[cfg(test)]
mod tests {
    //! 远程 shell stream codec 测试。

    use super::*;

    /// 验证 Smalux codec 会把输出事件编码为 Smalux wire payload。
    #[test]
    fn smalux_codec_encodes_event_as_wire_payload() {
        let codec = SmaluxShellCodec;
        let frame = codec
            .encode_event(RemoteShellStreamEvent::Opened {
                session_id: "shell-1".to_string(),
            })
            .unwrap()
            .unwrap();

        let RemoteShellFrame::SmaluxWire(payload) = frame else {
            panic!("expected smalux wire frame");
        };
        let text = String::from_utf8(payload).unwrap();

        assert!(text.contains(r#""type":"opened""#));
    }

    /// 验证 heartbeat 会被转换为空输入。
    #[test]
    fn command_to_shell_input_ignores_heartbeat() {
        let input = command_to_shell_input(RemoteShellStreamCommand::Heartbeat).unwrap();

        assert!(input.is_none());
    }
}
