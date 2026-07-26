//! Noise 核心层的稳定错误分类。
//!
//! 本模块不会把密钥、PSK 或解密后的业务内容写进错误文本。调用方可以记录错误类别，
//! 但认证失败时不应向未认证的对端返回底层密码学细节。

use std::fmt;

/// Noise 协议核心的稳定错误分类。
#[derive(Debug)]
pub enum NoiseError {
    /// 静态公钥、私钥或 key ID 不是 Noise 25519 要求的 32 字节。
    InvalidKeyLength,
    /// XXpsk3 的预共享密钥不是恰好 32 字节。
    InvalidPskLength,
    /// 收到的握手类型与当前状态机期待的类型不一致。
    InvalidHandshakeType,
    /// 握手完成后收到的外层帧不是 Noise ciphertext。
    InvalidFrame,
    /// PSK、公钥或握手认证标签校验失败；故意不进一步区分原因。
    AuthenticationFailed,
    /// 握手结束后没有得到协议要求的对端静态公钥。
    MissingRemoteKey,
    /// 对端指定的 Server key ID 不在当前可接受密钥集合中。
    UnknownKeyId,
    /// 当前状态已经存在一笔尚未完成的静态密钥轮换。
    RotationAlreadyInProgress,
    /// promote、cancel 或 retire 时没有对应的待处理状态。
    NoPendingRotation,
    /// 网络消息携带的 rotation ID 与本地持久化状态不匹配。
    RotationIdMismatch,
    /// `snow` 返回的握手、AEAD 或 transport mode 错误。
    Crypto(snow::Error),
    /// Noise 解密成功，但内层字节不是合法的 `SecureMessage`。
    Encode(prost::DecodeError),
    /// 操作系统安全随机源不可用。
    Random(getrandom::Error),
}

impl fmt::Display for NoiseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidKeyLength => "Noise key must contain 32 bytes",
            Self::InvalidPskLength => "Noise PSK must contain 32 bytes",
            Self::InvalidHandshakeType => "unexpected Noise handshake type",
            Self::InvalidFrame => "unexpected protocol frame",
            Self::AuthenticationFailed => "Noise authentication failed",
            Self::MissingRemoteKey => "Noise handshake did not authenticate a remote static key",
            Self::UnknownKeyId => "Noise responder key ID is not active",
            Self::RotationAlreadyInProgress => "a key rotation is already in progress",
            Self::NoPendingRotation => "no key rotation is pending",
            Self::RotationIdMismatch => "key rotation ID does not match",
            Self::Crypto(_) => "Noise cryptographic operation failed",
            Self::Encode(_) => "encrypted protobuf message is invalid",
            Self::Random(_) => "secure random generation failed",
        })
    }
}

impl std::error::Error for NoiseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Crypto(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::Random(error) => Some(error),
            _ => None,
        }
    }
}

impl From<snow::Error> for NoiseError {
    /// 保留 `snow` 原始错误作为 source，同时对外只展示稳定的通用描述。
    fn from(error: snow::Error) -> Self {
        Self::Crypto(error)
    }
}

impl From<prost::DecodeError> for NoiseError {
    /// 把内层 Protobuf 解码失败归入协议核心错误。
    fn from(error: prost::DecodeError) -> Self {
        Self::Encode(error)
    }
}

impl From<getrandom::Error> for NoiseError {
    /// 把系统随机源失败归入协议核心错误。
    fn from(error: getrandom::Error) -> Self {
        Self::Random(error)
    }
}
