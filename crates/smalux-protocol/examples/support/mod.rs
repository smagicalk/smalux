//! WSS 和 gRPC Echo 示例共用的业务消息与协议构造函数。

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use prost::{Message, Name};
use smalux_protocol::{
    ApplicationEnvelope, ClientFrame, ClientHello, Close, CloseCode, CodecLimits, Error,
    HandshakeMessage, NegotiationPolicy, NoiseKeypair, NoiseProvider, PinnedRemoteKey,
    ProtocolSession, Result as ProtocolResult, SecurityHandshake, SecurityProvider, SecurityRole,
    SecuritySession, ServerFrame, SessionContent, VersionRange, client_frame, pack_any,
    protect_session_content, server_frame, session_content, unprotect_session_content,
};

pub const CAPABILITY: &str = "smalux.example.echo.v1";

/// 示例自己的 Echo 业务消息。
#[derive(Clone, PartialEq, Message)]
pub struct EchoMessage {
    /// Client 输入或 Server 回显的文本。
    #[prost(string, tag = "1")]
    pub text: String,
}

impl Name for EchoMessage {
    const NAME: &'static str = "EchoMessage";
    const PACKAGE: &'static str = "smalux.example.echo.v1";
}

/// 生成临时静态密钥，并为 Client/Server 创建互相 Pin 的 Noise XX Provider。
pub fn noise_providers() -> ProtocolResult<(NoiseProvider, NoiseProvider)> {
    let client_keys = NoiseKeypair::generate()?;
    let server_keys = NoiseKeypair::generate()?;
    let client_public = client_keys.public_key();
    let server_public = server_keys.public_key();
    let client = NoiseProvider::builder(
        SecurityRole::Client,
        client_keys,
        PinnedRemoteKey::new(server_public),
    )
    .enable_xx()
    .build()?;
    let server = NoiseProvider::builder(
        SecurityRole::Server,
        server_keys,
        PinnedRemoteKey::new(client_public),
    )
    .enable_xx()
    .build()?;
    Ok((client, server))
}

/// 构造两个传输示例使用的 ClientHello。
pub fn client_hello(limits: CodecLimits, provider: &NoiseProvider) -> ClientHello {
    ClientHello {
        supported_versions: vec![VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 0,
        }],
        supported_capabilities: vec![CAPABILITY.to_owned()],
        required_capabilities: vec![CAPABILITY.to_owned()],
        supported_security_schemes: provider.supported_schemes().to_vec(),
        max_frame_bytes: limits.max_frame_bytes() as u32,
        nonce: vec![1; 32],
    }
}

/// 构造两个传输示例使用的 Server 协商策略。
pub fn server_policy(limits: CodecLimits, provider: &NoiseProvider) -> NegotiationPolicy {
    NegotiationPolicy {
        supported_versions: vec![VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 0,
        }],
        supported_capabilities: vec![CAPABILITY.to_owned()],
        required_capabilities: vec![CAPABILITY.to_owned()],
        security_schemes: provider.supported_schemes().to_vec(),
        codec_limits: limits,
    }
}

/// 使用 Hello 协商结果创建当前连接独享的 Noise 会话。
pub fn start_noise_session(
    session: &ProtocolSession,
    role: SecurityRole,
    provider: &NoiseProvider,
) -> ProtocolResult<Arc<dyn SecuritySession>> {
    let negotiated = session.negotiated().ok_or_else(|| {
        Error::Negotiation("cannot start Noise before Hello negotiation completes".to_owned())
    })?;
    provider.start(negotiated.security_context(role))
}

/// 将 Noise 的下一步握手数据包装为 ClientFrame。
pub fn client_handshake_frame(
    security: &dyn SecuritySession,
    message: HandshakeMessage,
) -> ClientFrame {
    ClientFrame {
        body: Some(client_frame::Body::SecurityHandshake(SecurityHandshake {
            scheme: security.scheme().to_owned(),
            step: message.step,
            payload: message.payload,
        })),
    }
}

/// 将 Noise 的下一步握手数据包装为 ServerFrame。
pub fn server_handshake_frame(
    security: &dyn SecuritySession,
    message: HandshakeMessage,
) -> ServerFrame {
    ServerFrame {
        body: Some(server_frame::Body::SecurityHandshake(SecurityHandshake {
            scheme: security.scheme().to_owned(),
            step: message.step,
            payload: message.payload,
        })),
    }
}

/// 校验并交给 Noise 处理 Client 方向握手帧。
pub fn receive_client_handshake(
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
    frame: &ClientFrame,
) -> ProtocolResult<()> {
    session.on_client_frame(frame)?;
    let handshake = match frame.body.as_ref() {
        Some(client_frame::Body::SecurityHandshake(handshake)) => handshake,
        _ => {
            return Err(Error::InvalidField {
                field: "client_frame.body",
                detail: "expected Noise SecurityHandshake".to_owned(),
            });
        }
    };
    security.receive_handshake(HandshakeMessage {
        step: handshake.step,
        payload: handshake.payload.clone(),
    })
}

/// 校验并交给 Noise 处理 Server 方向握手帧。
pub fn receive_server_handshake(
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
    frame: &ServerFrame,
) -> ProtocolResult<()> {
    session.on_server_frame(frame)?;
    let handshake = match frame.body.as_ref() {
        Some(server_frame::Body::SecurityHandshake(handshake)) => handshake,
        _ => {
            return Err(Error::InvalidField {
                field: "server_frame.body",
                detail: "expected Noise SecurityHandshake".to_owned(),
            });
        }
    };
    security.receive_handshake(HandshakeMessage {
        step: handshake.step,
        payload: handshake.payload.clone(),
    })
}

/// 加密 Client 方向内容，并同步推进本地协议状态机。
pub fn protected_client_frame(
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
    content: &SessionContent,
) -> ProtocolResult<ClientFrame> {
    let frame = ClientFrame {
        body: Some(client_frame::Body::ProtectedPayload(
            protect_session_content(security, content)?,
        )),
    };
    session.on_client_frame(&frame)?;
    session.on_decrypted_client_content(content)?;
    Ok(frame)
}

/// 加密 Server 方向内容，并同步推进本地协议状态机。
pub fn protected_server_frame(
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
    content: &SessionContent,
) -> ProtocolResult<ServerFrame> {
    let frame = ServerFrame {
        body: Some(server_frame::Body::ProtectedPayload(
            protect_session_content(security, content)?,
        )),
    };
    session.on_server_frame(&frame)?;
    session.on_decrypted_server_content(content)?;
    Ok(frame)
}

/// 解密并校验 Client 方向受保护内容。
pub fn receive_client_content(
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
    frame: &ClientFrame,
) -> ProtocolResult<SessionContent> {
    session.on_client_frame(frame)?;
    let payload = match frame.body.as_ref() {
        Some(client_frame::Body::ProtectedPayload(payload)) => payload,
        _ => {
            return Err(Error::InvalidField {
                field: "client_frame.body",
                detail: "expected Noise ProtectedPayload".to_owned(),
            });
        }
    };
    let content = unprotect_session_content(security, payload)?;
    session.on_decrypted_client_content(&content)?;
    Ok(content)
}

/// 解密并校验 Server 方向受保护内容。
pub fn receive_server_content(
    session: &mut ProtocolSession,
    security: &dyn SecuritySession,
    frame: &ServerFrame,
) -> ProtocolResult<SessionContent> {
    session.on_server_frame(frame)?;
    let payload = match frame.body.as_ref() {
        Some(server_frame::Body::ProtectedPayload(payload)) => payload,
        _ => {
            return Err(Error::InvalidField {
                field: "server_frame.body",
                detail: "expected Noise ProtectedPayload".to_owned(),
            });
        }
    };
    let content = unprotect_session_content(security, payload)?;
    session.on_decrypted_server_content(&content)?;
    Ok(content)
}

/// 将 EchoMessage 包装为带 sequence 和关联 ID 的业务内容。
pub fn application_content(
    message_id: Vec<u8>,
    correlation_id: Option<Vec<u8>>,
    sequence: u64,
    text: String,
) -> SessionContent {
    SessionContent {
        body: Some(session_content::Body::Application(ApplicationEnvelope {
            message_id,
            correlation_id,
            sequence,
            sent_at_unix_ms: unix_time_ms(),
            payload: Some(pack_any(&EchoMessage { text })),
        })),
    }
}

/// 构造正常结束示例会话的 Close 内容。
pub fn close_content() -> SessionContent {
    SessionContent {
        body: Some(session_content::Body::Close(Close {
            code: CloseCode::Normal as i32,
            reason: "example finished".to_owned(),
            retryable: false,
        })),
    }
}

/// 生成固定 16 字节且便于观察的示例消息 ID。
pub fn id_from_sequence(marker: u8, sequence: u64) -> Vec<u8> {
    let mut id = vec![0_u8; 16];
    id[0] = marker;
    id[8..].copy_from_slice(&sequence.to_be_bytes());
    id
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
