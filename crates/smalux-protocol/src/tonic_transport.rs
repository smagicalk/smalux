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
    ServerSessionAcceptor,
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

/// 注册 Token ID 在握手外层允许的最大字节数。
const MAX_REGISTRATION_TOKEN_ID_BYTES: usize = 128;

/// 标准注册 PSK 的固定字节数。
const REGISTRATION_PSK_BYTES: usize = 32;

/// Server 为 Agent 指定的展示名称最大字节数。
const MAX_AGENT_DISPLAY_NAME_BYTES: usize = 64;

/// 校验握手首帧中用于选择 PSK 的公开 Token ID。
///
/// 该字段虽然不是秘密，但在 Noise 建立前完全来自网络，必须在进入异步 resolver
/// 和数据库查询前限制长度与字符集。实际签发器使用 32 位十六进制 ID；协议层保留
/// 128 字节上限，兼容未来使用 UUID 或带版本前缀的 ID。
pub fn validate_registration_token_id(token_id: &str) -> Result<(), TransportError> {
    if token_id.is_empty()
        || token_id.len() > MAX_REGISTRATION_TOKEN_ID_BYTES
        || !token_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        tracing::warn!(
            token_id_len = token_id.len(),
            "rejected invalid registration Token ID"
        );
        return Err(TransportError::Protocol(
            "registration Token ID is invalid".to_owned(),
        ));
    }
    Ok(())
}

/// 解析标准 `token_id.64位十六进制PSK` 注册凭据。
///
/// 返回值只包含公开 Token ID 和解码后的固定长度 PSK；错误文本不会回显原始凭据，
/// 因而 Agent 配置层与 Server 注册层可以共享同一条 wire invariant，而不会复制安全逻辑。
pub fn parse_registration_credential(
    credential: &str,
) -> Result<(&str, [u8; REGISTRATION_PSK_BYTES]), TransportError> {
    let (token_id, encoded_psk) = credential.split_once('.').ok_or_else(|| {
        TransportError::Protocol("registration token must use the token_id.psk format".to_owned())
    })?;
    validate_registration_token_id(token_id)?;
    if encoded_psk.len() != REGISTRATION_PSK_BYTES * 2 {
        return Err(TransportError::Protocol(
            "registration PSK must contain 64 hexadecimal characters".to_owned(),
        ));
    }

    let mut psk = [0_u8; REGISTRATION_PSK_BYTES];
    for (index, pair) in encoded_psk.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or_else(invalid_registration_psk)?;
        let low = hex_nibble(pair[1]).ok_or_else(invalid_registration_psk)?;
        psk[index] = (high << 4) | low;
    }
    Ok((token_id, psk))
}

/// 校验 Server 签发注册 Token 时绑定的 Agent 展示名称。
///
/// 展示名称不是身份键，也不由 Agent 上报；Server 可在签发 Token 时提供该值。
/// 名称会进入日志、标签和管理界面，因此仍在公共协议库中集中约束格式。
pub fn validate_agent_display_name(display_name: &str) -> Result<(), TransportError> {
    if display_name.is_empty()
        || display_name.len() > MAX_AGENT_DISPLAY_NAME_BYTES
        || !display_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(TransportError::Protocol(
            "Agent display name must be 1-64 ASCII letters, digits, '-', '_' or '.'".to_owned(),
        ));
    }
    Ok(())
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn invalid_registration_psk() -> TransportError {
    TransportError::Protocol("registration PSK contains a non-hexadecimal character".to_owned())
}

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
    use super::{
        parse_registration_credential, registration_token_id_log_label,
        validate_agent_display_name, validate_registration_token_id,
    };

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

    #[test]
    fn registration_credential_parser_returns_public_id_and_exact_psk() {
        let credential = format!("agent-token.{}", "aB".repeat(32));

        let (token_id, psk) = parse_registration_credential(&credential).unwrap();

        assert_eq!(token_id, "agent-token");
        assert_eq!(psk, [0xab; 32]);
    }

    #[test]
    fn registration_credential_parser_rejects_invalid_shapes_without_echoing_secrets() {
        for credential in [
            "missing-separator",
            "agent.00",
            "agent.not-hex",
            "bad id.00",
        ] {
            let error = parse_registration_credential(credential).unwrap_err();
            assert!(!error.to_string().contains(credential));
        }
    }

    #[test]
    fn registration_token_id_validation_rejects_untrusted_values() {
        assert!(validate_registration_token_id("0123456789abcdef").is_ok());
        assert!(validate_registration_token_id("token-id.v1").is_ok());
        assert!(validate_registration_token_id("").is_err());
        assert!(validate_registration_token_id(&"a".repeat(129)).is_err());
        assert!(validate_registration_token_id("token id").is_err());
        assert!(validate_registration_token_id("token\n-id").is_err());
    }

    #[test]
    fn agent_display_name_validation_matches_the_server_contract() {
        assert!(validate_agent_display_name("agent-1.test").is_ok());
        for value in ["", "has space", "中文", &"a".repeat(65)] {
            assert!(validate_agent_display_name(value).is_err());
        }
    }
}
