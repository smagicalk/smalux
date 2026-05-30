//! 远程 shell 控制消息和 stream 消息模型。
//!
//! 控制消息走主 WebSocket；stream 消息走每个 shell 会话独立的临时 WebSocket。

use serde::{Deserialize, Serialize};

/// 默认 PTY 列数。
pub(crate) const DEFAULT_PTY_COLS: u16 = 80;
/// 默认 PTY 行数。
pub(crate) const DEFAULT_PTY_ROWS: u16 = 24;

/// 主控制通道中的远程 shell 打开请求。
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub(crate) struct RemoteShellOpenRequest {
    /// 本次 shell 会话 ID，由 server 生成并在 stream 消息中回显。
    pub session_id: String,
    /// 本次 shell 会话使用的临时 WebSocket stream 地址。
    pub stream_url: String,
    /// 初始终端列数。
    #[serde(default)]
    pub cols: Option<u16>,
    /// 初始终端行数。
    #[serde(default)]
    pub rows: Option<u16>,
}

impl RemoteShellOpenRequest {
    /// 校验打开请求的基础字段。
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.session_id.trim().is_empty() {
            anyhow::bail!("remote_shell.session_id cannot be empty");
        }
        if self.stream_url.trim().is_empty() {
            anyhow::bail!("remote_shell.stream_url cannot be empty");
        }
        if self.cols == Some(0) {
            anyhow::bail!("remote_shell.cols must be greater than 0");
        }
        if self.rows == Some(0) {
            anyhow::bail!("remote_shell.rows must be greater than 0");
        }
        Ok(())
    }

    /// 返回打开 PTY 时使用的初始尺寸。
    pub(crate) fn initial_size(&self) -> (u16, u16) {
        (
            self.cols.unwrap_or(DEFAULT_PTY_COLS),
            self.rows.unwrap_or(DEFAULT_PTY_ROWS),
        )
    }
}

/// stream 数据编码。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RemoteShellDataEncoding {
    /// UTF-8 文本。
    Utf8,
    /// base64 编码的原始字节。
    Base64,
}

/// shell stream 上 server 发给 agent 的消息。
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum RemoteShellStreamCommand {
    /// 写入 PTY 输入。
    Input {
        /// 输入数据。
        data: String,
        /// 输入数据编码，缺省按 UTF-8 文本处理。
        #[serde(default)]
        encoding: Option<RemoteShellDataEncoding>,
    },
    /// 调整 PTY 终端尺寸。
    Resize {
        /// 终端列数。
        cols: u16,
        /// 终端行数。
        rows: u16,
    },
    /// 请求关闭 shell 会话。
    Close,
}

/// shell stream 上 agent 发给 server 的消息。
#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum RemoteShellStreamEvent {
    /// shell 会话已经启动。
    Opened {
        /// shell 会话 ID。
        session_id: String,
    },
    /// PTY 输出。
    Output {
        /// shell 会话 ID。
        session_id: String,
        /// 输出数据，当前使用 base64 保留原始字节。
        data: String,
        /// 输出数据编码。
        encoding: RemoteShellDataEncoding,
    },
    /// shell 进程退出。
    Exit {
        /// shell 会话 ID。
        session_id: String,
        /// 退出码；被系统信号或强制关闭时可能为空。
        code: Option<i32>,
    },
    /// shell 会话错误。
    Error {
        /// shell 会话 ID。
        session_id: String,
        /// 错误信息。
        message: String,
    },
}

/// 解析 shell stream command。
pub(crate) fn parse_stream_command(text: &str) -> anyhow::Result<RemoteShellStreamCommand> {
    Ok(serde_json::from_str(text)?)
}

/// 把输入 command 解码成 PTY 字节。
pub(crate) fn decode_input_bytes(command: RemoteShellStreamCommand) -> anyhow::Result<Vec<u8>> {
    match command {
        RemoteShellStreamCommand::Input { data, encoding } => {
            decode_data(data, encoding.unwrap_or(RemoteShellDataEncoding::Utf8))
        }
        RemoteShellStreamCommand::Resize { .. } | RemoteShellStreamCommand::Close => {
            anyhow::bail!("remote shell command does not carry input bytes")
        }
    }
}

/// 编码 shell stream event。
pub(crate) fn encode_stream_event(event: &RemoteShellStreamEvent) -> anyhow::Result<String> {
    Ok(serde_json::to_string(event)?)
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

    /// 验证打开请求会拒绝空字段。
    #[test]
    fn open_request_rejects_blank_fields() {
        let error = RemoteShellOpenRequest {
            session_id: " ".to_string(),
            stream_url: "ws://127.0.0.1/shell".to_string(),
            cols: None,
            rows: None,
        }
        .validate()
        .unwrap_err();

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

        assert_eq!(request.initial_size(), (DEFAULT_PTY_COLS, DEFAULT_PTY_ROWS));
    }

    /// 验证 PTY 尺寸不能为 0。
    #[test]
    fn open_request_rejects_zero_pty_size() {
        let cols_error = RemoteShellOpenRequest {
            session_id: "shell-1".to_string(),
            stream_url: "ws://127.0.0.1/shell".to_string(),
            cols: Some(0),
            rows: Some(24),
        }
        .validate()
        .unwrap_err();
        let rows_error = RemoteShellOpenRequest {
            session_id: "shell-1".to_string(),
            stream_url: "ws://127.0.0.1/shell".to_string(),
            cols: Some(80),
            rows: Some(0),
        }
        .validate()
        .unwrap_err();

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
