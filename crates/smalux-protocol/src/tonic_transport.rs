//! 在 Tonic 双向流之上运行 Noise 握手和加密业务消息的适配层。
//!
//! Client 通过 [`AgentProtocolClient`] 完成首次注册（XXpsk3）或后续连接（IK）；
//! Server 通过 [`ServerSessionAcceptor`] 解析同一条 `OpenSession` 流并返回待授权会话。
//! 握手成功后，双方都使用 [`TonicNoiseSession`] 收发加密消息、心跳及密钥轮换控制帧。
//! 需要并发提交上报和接收命令时，可把会话交给 [`SessionDriver`]，业务层仅持有可克隆
//! [`SessionHandle`] 与单消费者 [`SessionEventReceiver`]。
//!
//! 本模块不保存 Token、长期私钥或授权记录。调用方应自行持久化 `noise` 模块的 snapshot。

mod client;
mod driver;
mod server;
mod session;

pub use client::{
    AgentPendingRegistration, AgentProtocolClient, AgentRegistration, AgentTransportRpcClient,
};
pub use driver::{
    RunningSession, SessionDriver, SessionDriverConfig, SessionEventReceiver, SessionHandle,
};
pub use server::{
    IncomingSession, ServerAuthentication, ServerPendingSession, ServerRegistration,
    ServerSessionAcceptor, validate_registration_token_id,
};
pub use session::{
    HeartbeatPolicy, HeartbeatSample, HeartbeatStats, MaintenanceResult, MaintenanceStatus,
    RekeyPolicy, SessionEvent, TonicNoiseSession,
};

use std::{borrow::Cow, fmt};

use crate::agent::v1::{ProtocolError, ProtocolErrorCode, SecureErrorCode};
use crate::noise::NoiseError;

/// 日志中允许直接展示的外部标识最大字节数。
///
/// 该上限只约束日志表示，不改变协议字段或业务存储格式。
const MAX_EXTERNAL_LOG_LABEL_BYTES: usize = 128;

/// 返回适合写入日志的注册 Token ID。
///
/// Token ID 虽然是公开选择器，但 Server 在认证前就会收到它，因此仍然属于不可信输入。
/// 这里只保留短的 ASCII 标识；控制字符、非 ASCII 字符和超长值统一替换为长度摘要，防止
/// 伪造日志行或用单个握手帧放大日志文件。
fn registration_token_id_log_label(token_id: &str) -> Cow<'_, str> {
    if token_id.is_empty() {
        return Cow::Borrowed("<none>");
    }
    let is_safe = token_id.len() <= MAX_EXTERNAL_LOG_LABEL_BYTES
        && token_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if is_safe {
        Cow::Borrowed(token_id)
    } else {
        Cow::Owned(format!("<invalid-token-id:{}-bytes>", token_id.len()))
    }
}

#[derive(Debug)]
/// Tonic + Noise 调用过程中对业务层稳定暴露的错误分类。
pub enum TransportError {
    /// 与具体网络无关的 Noise 核心错误。
    Noise(NoiseError),
    /// gRPC RPC 返回的状态错误。
    Status(tonic::Status),
    /// 建立 HTTP/2/TLS channel 时的传输错误。
    Transport(tonic::transport::Error),
    /// Endpoint 或带 prefix origin 不是合法 URI。
    InvalidUri(http::uri::InvalidUri),
    /// 指定握手阶段超过配置的时间上限。
    Timeout(&'static str),
    /// 对端关闭 gRPC stream 或本地 channel sender 已关闭。
    Closed,
    /// IK 指定的 Server key ID 不在当前 keyring 中。
    UnknownKeyId,
    /// Server 已要求 initiator 在当前会话发起同步 rekey。
    RekeyRequired,
    /// 超过心跳策略允许的最长无入站时间。
    HeartbeatTimeout,
    /// 本地检测到的帧顺序或控制消息错误。
    Protocol(String),
    /// 未建立 Noise 前收到的远端外层安全错误。
    RemoteProtocol(String),
    /// 已建立 Noise 会话后收到的加密业务错误。
    RemoteSecure(SecureErrorCode, String),
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Noise(error) => write!(formatter, "{error}"),
            Self::Status(error) => write!(formatter, "gRPC status: {error}"),
            Self::Transport(error) => write!(formatter, "gRPC transport: {error}"),
            Self::InvalidUri(error) => write!(formatter, "invalid endpoint: {error}"),
            Self::Timeout(stage) => write!(formatter, "timed out {stage}"),
            Self::Closed => formatter.write_str("gRPC stream closed"),
            Self::UnknownKeyId => formatter.write_str("Server key ID is not active"),
            Self::RekeyRequired => formatter.write_str("Server requested session rekey"),
            Self::HeartbeatTimeout => formatter.write_str("Noise session heartbeat timed out"),
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Self::RemoteProtocol(message) => write!(formatter, "remote protocol error: {message}"),
            Self::RemoteSecure(code, message) => {
                write!(formatter, "remote secure error {code:?}: {message}")
            }
        }
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Noise(error) => Some(error),
            Self::Status(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::InvalidUri(error) => Some(error),
            _ => None,
        }
    }
}

impl TransportError {
    /// 转换为可安全放在 Noise 握手外层的通用错误，不泄露 Token 或密钥细节。
    pub fn protocol_error(&self) -> ProtocolError {
        // 外层只保留粗粒度错误码；Token、key bytes 和 snow 细节绝不回显。
        let code = match self {
            Self::Timeout(_) => ProtocolErrorCode::HandshakeTimeout,
            Self::Closed => ProtocolErrorCode::InvalidFrame,
            Self::UnknownKeyId => ProtocolErrorCode::UnknownKeyId,
            Self::Noise(NoiseError::AuthenticationFailed | NoiseError::Crypto(_)) => {
                ProtocolErrorCode::AuthenticationFailed
            }
            Self::Protocol(_) | Self::Noise(_) => ProtocolErrorCode::InvalidFrame,
            _ => ProtocolErrorCode::Unspecified,
        };
        ProtocolError {
            code: code as i32,
            message: match code {
                ProtocolErrorCode::HandshakeTimeout => "Noise handshake timed out",
                ProtocolErrorCode::InvalidFrame if matches!(self, Self::Closed) => {
                    "Noise handshake ended before completion"
                }
                ProtocolErrorCode::UnknownKeyId => "Server key ID is not active",
                ProtocolErrorCode::AuthenticationFailed => "Noise authentication failed",
                ProtocolErrorCode::UnsupportedHandshake => "Noise handshake is not supported",
                _ => "Noise protocol failed",
            }
            .to_owned(),
        }
    }
}

impl From<tonic::Status> for TransportError {
    /// 允许所有 Tonic streaming 调用使用 `?` 统一上抛。
    fn from(error: tonic::Status) -> Self {
        Self::Status(error)
    }
}

impl From<tonic::transport::Error> for TransportError {
    /// 允许 channel/TLS 建连错误使用 `?` 统一上抛。
    fn from(error: tonic::transport::Error) -> Self {
        Self::Transport(error)
    }
}

impl From<http::uri::InvalidUri> for TransportError {
    /// 允许 endpoint/prefix origin 解析错误使用 `?` 统一上抛。
    fn from(error: http::uri::InvalidUri) -> Self {
        Self::InvalidUri(error)
    }
}

#[cfg(test)]
mod tests {
    use super::registration_token_id_log_label;

    #[test]
    fn registration_token_log_label_preserves_short_safe_ids() {
        assert_eq!(
            registration_token_id_log_label("0123456789abcdef0123456789abcdef"),
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(registration_token_id_log_label(""), "<none>");
    }

    #[test]
    fn registration_token_log_label_redacts_untrusted_text() {
        assert_eq!(
            registration_token_id_log_label("valid-prefix\nforged-log-line"),
            "<invalid-token-id:28-bytes>"
        );
        assert_eq!(
            registration_token_id_log_label(&"a".repeat(129)),
            "<invalid-token-id:129-bytes>"
        );
    }
}
