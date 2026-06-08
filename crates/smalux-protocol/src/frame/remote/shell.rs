//! 交互式远程 shell 协议模型。

use serde::{Deserialize, Serialize};

/// 远程 shell 打开请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteShellOpenRequest {
    /// 本次 shell 会话 ID，由 server 生成并在 stream 消息中回显。
    pub session_id: String,
    /// 本次 shell 会话使用的临时 WebSocket stream 地址。
    pub stream_url: String,
    /// 初始终端列数；缺省由 agent 使用平台默认值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cols: Option<u16>,
    /// 初始终端行数；缺省由 agent 使用平台默认值。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u16>,
}

/// 远程 shell stream 数据编码。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteShellDataEncoding {
    /// UTF-8 文本。
    Utf8,
    /// base64 编码的原始字节。
    Base64,
}

/// shell stream 上 server 发给 agent 的消息。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteShellStreamCommand {
    /// 写入 PTY 输入。
    Input {
        /// 输入数据。
        data: String,
        /// 输入数据编码，缺省按 UTF-8 文本处理。
        #[serde(default, skip_serializing_if = "Option::is_none")]
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
    /// stream 保活消息，不产生 PTY 输入。
    Heartbeat,
}

/// shell stream 上 agent 发给 server 的消息。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteShellStreamEvent {
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
