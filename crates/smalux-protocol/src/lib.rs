//! Smalux 通用双端协议模型、编解码、安全边界和会话状态机。
//!
//! 本 crate 不负责建立网络连接或业务授权；安全层内置 Noise XX/IK，也允许调用方扩展其他实现。

mod codec;
mod error;
mod negotiation;
mod security;
mod session;

/// Protobuf 生成的 v1 协议模型。
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/smalux.protocol.v1.rs"));
}

pub use codec::{
    ABSOLUTE_MAX_FRAME_BYTES, CodecLimits, DEFAULT_MAX_FRAME_BYTES, FrameCodec, MIN_FRAME_BYTES,
    pack_any, unpack_any,
};
pub use error::{Error, Result};
pub use negotiation::{
    MAX_CAPABILITIES, MAX_CAPABILITY_NAME_BYTES, MAX_SECURITY_SCHEMES, NegotiatedParameters,
    NegotiationPolicy, negotiate_server_hello,
};
pub use security::{
    AllowUnknownRemoteKey, CallbackRemoteKeyVerifier, HandshakeMessage, NOISE_IK_SCHEME,
    NOISE_RECORD_PLAINTEXT_BYTES, NOISE_REKEY_INTERVAL_RECORDS, NOISE_XX_SCHEME, NoiseKeypair,
    NoiseProvider, NoiseProviderBuilder, NoiseRemoteKeyVerifier, PinnedRemoteKey, SecurityContext,
    SecurityProvider, SecurityRole, SecuritySession, SecurityState, protect_session_content,
    unprotect_session_content,
};
pub use session::{
    MAX_CLOSE_REASON_BYTES, MAX_ERROR_MESSAGE_BYTES, MAX_HANDSHAKE_PAYLOAD_BYTES, ProtocolSession,
    SessionRole, SessionState,
};
pub use v1::*;
