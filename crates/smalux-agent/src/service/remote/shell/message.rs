//! 远程 shell 控制消息和 stream 辅助逻辑。
//!
//! 稳定 JSON 模型定义在 `smalux-protocol`，本模块只保留 agent 侧默认值、校验和
//! PTY 字节转换，避免 server 和 agent 各自维护一份协议结构。

pub(crate) use smalux_protocol::{
    RemoteShellDataEncoding, RemoteShellOpenRequest, RemoteShellStreamCommand,
    RemoteShellStreamEvent,
};

/// 默认 PTY 列数。
pub(crate) const DEFAULT_PTY_COLS: u16 = 80;
/// 默认 PTY 行数。
pub(crate) const DEFAULT_PTY_ROWS: u16 = 24;

/// 校验打开请求的基础字段。
pub(crate) fn validate_open_request(request: &RemoteShellOpenRequest) -> anyhow::Result<()> {
    if request.session_id.trim().is_empty() {
        anyhow::bail!("remote_shell.session_id cannot be empty");
    }
    if request.stream_url.trim().is_empty() {
        anyhow::bail!("remote_shell.stream_url cannot be empty");
    }
    if request.cols == Some(0) {
        anyhow::bail!("remote_shell.cols must be greater than 0");
    }
    if request.rows == Some(0) {
        anyhow::bail!("remote_shell.rows must be greater than 0");
    }
    Ok(())
}

/// 返回打开 PTY 时使用的初始尺寸。
pub(crate) fn open_request_initial_size(request: &RemoteShellOpenRequest) -> (u16, u16) {
    (
        request.cols.unwrap_or(DEFAULT_PTY_COLS),
        request.rows.unwrap_or(DEFAULT_PTY_ROWS),
    )
}

/// 解析 shell stream command。
pub(crate) fn parse_stream_command(text: &str) -> anyhow::Result<RemoteShellStreamCommand> {
    Ok(smalux_protocol::decode_remote_shell_stream_command(text)?)
}

/// 把输入 command 解码成 PTY 字节。
pub(crate) fn decode_input_bytes(command: RemoteShellStreamCommand) -> anyhow::Result<Vec<u8>> {
    match command {
        RemoteShellStreamCommand::Input { data, encoding } => {
            decode_data(data, encoding.unwrap_or(RemoteShellDataEncoding::Utf8))
        }
        RemoteShellStreamCommand::Resize { .. }
        | RemoteShellStreamCommand::Close
        | RemoteShellStreamCommand::Heartbeat => {
            anyhow::bail!("remote shell command does not carry input bytes")
        }
    }
}

/// 编码 shell stream event。
pub(crate) fn encode_stream_event(event: &RemoteShellStreamEvent) -> anyhow::Result<String> {
    Ok(smalux_protocol::encode_remote_shell_stream_event(event)?)
}

/// 把 PTY 输出编码成 stream event。
pub(crate) fn output_event(session_id: String, data: &[u8]) -> RemoteShellStreamEvent {
    RemoteShellStreamEvent::Output {
        session_id,
        data: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, data),
        encoding: RemoteShellDataEncoding::Base64,
    }
}

/// 按指定编码解码 stream 数据。
fn decode_data(data: String, encoding: RemoteShellDataEncoding) -> anyhow::Result<Vec<u8>> {
    match encoding {
        RemoteShellDataEncoding::Utf8 => Ok(data.into_bytes()),
        RemoteShellDataEncoding::Base64 => Ok(base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            data,
        )?),
    }
}

#[cfg(test)]
mod tests {
    //! 远程 shell 消息模型测试。

    use super::*;

    /// 验证 input command 可以解析。
    #[test]
    fn parse_stream_command_reads_input() {
        let command = parse_stream_command(r#"{ "type": "input", "data": "echo ok\n" }"#).unwrap();

        assert_eq!(
            command,
            RemoteShellStreamCommand::Input {
                data: "echo ok\n".to_string(),
                encoding: None
            }
        );
    }

    /// 验证 base64 input 可以保留原始字节。
    #[test]
    fn decode_input_bytes_accepts_base64() {
        let command =
            parse_stream_command(r#"{ "type": "input", "encoding": "base64", "data": "AAEC" }"#)
                .unwrap();

        assert_eq!(decode_input_bytes(command).unwrap(), vec![0, 1, 2]);
    }

    /// 验证第三方 input 字段不会泄漏进 Smalux 核心 stream 协议。
    #[test]
    fn parse_stream_command_rejects_input_alias_without_data() {
        let error = parse_stream_command(r#"{ "type": "input", "input": "echo third-party\n" }"#)
            .unwrap_err();

        assert!(error.to_string().contains("missing field"));
    }

    /// 验证 stream heartbeat 可以解析并忽略额外字段。
    #[test]
    fn parse_stream_command_reads_heartbeat() {
        let command =
            parse_stream_command(r#"{ "type": "heartbeat", "timestamp": 1780660703 }"#).unwrap();

        assert_eq!(command, RemoteShellStreamCommand::Heartbeat);
    }

    /// 验证打开请求会拒绝空字段。
    #[test]
    fn open_request_rejects_blank_fields() {
        let request = RemoteShellOpenRequest {
            session_id: " ".to_string(),
            stream_url: "ws://127.0.0.1/shell".to_string(),
            cols: None,
            rows: None,
        };
        let error = validate_open_request(&request).unwrap_err();

        assert!(error.to_string().contains("session_id"));
    }

    /// 验证打开请求会使用默认 PTY 尺寸。
    #[test]
    fn open_request_uses_default_pty_size() {
        let request = RemoteShellOpenRequest {
            session_id: "shell-1".to_string(),
            stream_url: "ws://127.0.0.1/shell".to_string(),
            cols: None,
            rows: None,
        };

        assert_eq!(
            open_request_initial_size(&request),
            (DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS)
        );
    }

    /// 验证 PTY 尺寸不能为 0。
    #[test]
    fn open_request_rejects_zero_pty_size() {
        let cols_request = RemoteShellOpenRequest {
            session_id: "shell-1".to_string(),
            stream_url: "ws://127.0.0.1/shell".to_string(),
            cols: Some(0),
            rows: Some(24),
        };
        let cols_error = validate_open_request(&cols_request).unwrap_err();
        let rows_request = RemoteShellOpenRequest {
            session_id: "shell-1".to_string(),
            stream_url: "ws://127.0.0.1/shell".to_string(),
            cols: Some(80),
            rows: Some(0),
        };
        let rows_error = validate_open_request(&rows_request).unwrap_err();

        assert!(cols_error.to_string().contains("cols"));
        assert!(rows_error.to_string().contains("rows"));
    }

    /// 验证 event 使用 snake_case 类型字段。
    #[test]
    fn stream_event_encodes_snake_case_type() {
        let event = output_event("s1".to_string(), b"ok");
        let json = encode_stream_event(&event).unwrap();

        assert!(json.contains(r#""type":"output""#));
        assert!(json.contains(r#""encoding":"base64""#));
        assert!(json.contains(r#""session_id":"s1""#));
    }
}
