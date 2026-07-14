//! Client/Server 共用的协议生命周期、握手顺序和业务 sequence 状态机。
//!
//! 状态机不持有网络连接和密钥。受保护 Frame 必须先校验外层，再解密并调用对应的
//! `on_decrypted_*_content`，两步缺一不可。

use crate::negotiation::{validate_client_hello, validate_exact_bytes, validate_server_selection};
use crate::{
    ClientFrame, ClientHello, CodecLimits, Error, Result, SecurityHandshake, SecuritySession,
    SecurityState, ServerFrame, SessionContent, client_frame, negotiation::NegotiatedParameters,
    server_frame, session_content,
};

/// 单条安全握手载荷上限：64 KiB。
pub const MAX_HANDSHAKE_PAYLOAD_BYTES: usize = 64 * 1024;
/// 可发送给对端的协议错误文本上限。
pub const MAX_ERROR_MESSAGE_BYTES: usize = 1024;
/// 会话关闭原因文本上限。
pub const MAX_CLOSE_REASON_BYTES: usize = 512;
const MESSAGE_ID_BYTES: usize = 16;
const MAX_TYPE_URL_BYTES: usize = 256;

/// 当前协议状态机代表的本地一方。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRole {
    /// 主动建立连接并首先发送 ClientHello。
    Client,
    /// 接收连接并等待 ClientHello。
    Server,
}

/// 通用协议会话的生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// 等待本次会话唯一的 ClientHello。
    AwaitingClientHello,
    /// 已收到 ClientHello，等待 ServerHello。
    AwaitingServerHello,
    /// Hello 已完成，具体安全实现正在握手。
    NegotiatingSecurity,
    /// 可以交换明文或受保护的会话内容。
    Ready,
    /// 至少一方已发送 Close，等待底层连接结束。
    Closing,
    /// 底层连接已经正常结束。
    Closed,
    /// 协议顺序或字段发生不可恢复错误。
    Failed,
}

impl SessionState {
    fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingClientHello => "awaiting_client_hello",
            Self::AwaitingServerHello => "awaiting_server_hello",
            Self::NegotiatingSecurity => "negotiating_security",
            Self::Ready => "ready",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }
}

/// 校验协议消息顺序并保存当前连接的协商和 sequence 状态。
#[derive(Debug)]
pub struct ProtocolSession {
    role: SessionRole,
    state: SessionState,
    limits: CodecLimits,
    client_hello: Option<ClientHello>,
    negotiated: Option<NegotiatedParameters>,
    next_client_sequence: u64,
    next_server_sequence: u64,
    next_handshake_step: u32,
    protects_content: Option<bool>,
}

impl ProtocolSession {
    /// 创建尚未交换 ClientHello 的会话状态机。
    pub fn new(role: SessionRole, limits: CodecLimits) -> Result<Self> {
        CodecLimits::new(limits.max_frame_bytes())?;
        Ok(Self {
            role,
            state: SessionState::AwaitingClientHello,
            limits,
            client_hello: None,
            negotiated: None,
            next_client_sequence: 1,
            next_server_sequence: 1,
            next_handshake_step: 0,
            protects_content: None,
        })
    }

    /// 返回本地在当前会话中的角色。
    pub const fn role(&self) -> SessionRole {
        self.role
    }

    /// 返回当前生命周期状态。
    pub const fn state(&self) -> SessionState {
        self.state
    }

    /// 返回完成 Hello 后的协商参数。
    pub fn negotiated(&self) -> Option<&NegotiatedParameters> {
        self.negotiated.as_ref()
    }

    /// 记录一条已经发送或收到的 Client 方向 Frame。
    pub fn on_client_frame(&mut self, frame: &ClientFrame) -> Result<()> {
        let result = self.handle_client_frame(frame);
        self.fail_on_error(result)
    }

    /// 记录一条已经发送或收到的 Server 方向 Frame。
    pub fn on_server_frame(&mut self, frame: &ServerFrame) -> Result<()> {
        let result = self.handle_server_frame(frame);
        self.fail_on_error(result)
    }

    /// 校验安全实现已经完成握手，并将会话推进到 Ready。
    pub fn complete_security(&mut self, security: &dyn SecuritySession) -> Result<()> {
        if self.state != SessionState::NegotiatingSecurity {
            return self.fail(Error::InvalidState {
                state: self.state.as_str(),
                detail: "security can only complete after hello negotiation",
            });
        }
        let negotiated = self.negotiated.as_ref().ok_or(Error::InvalidState {
            state: self.state.as_str(),
            detail: "security completion requires negotiated parameters",
        })?;
        if security.scheme() != negotiated.security_scheme() {
            return self.fail(Error::Negotiation(
                "security session scheme differs from server selection".to_owned(),
            ));
        }
        if security.state() != SecurityState::Ready {
            return self.fail(Error::InvalidState {
                state: self.state.as_str(),
                detail: "security session is not ready",
            });
        }
        self.protects_content = Some(security.protects_content());
        self.state = SessionState::Ready;
        Ok(())
    }

    /// 解密 Client ProtectedPayload 后校验其中的 SessionContent。
    pub fn on_decrypted_client_content(&mut self, content: &SessionContent) -> Result<()> {
        let result = self
            .ensure_content_protection(true)
            .and_then(|()| self.handle_content(Direction::Client, content));
        self.fail_on_error(result)
    }

    /// 解密 Server ProtectedPayload 后校验其中的 SessionContent。
    pub fn on_decrypted_server_content(&mut self, content: &SessionContent) -> Result<()> {
        let result = self
            .ensure_content_protection(true)
            .and_then(|()| self.handle_content(Direction::Server, content));
        self.fail_on_error(result)
    }

    /// 底层连接正常结束后，将 Closing 会话标记为 Closed。
    pub fn mark_closed(&mut self) -> Result<()> {
        if self.state == SessionState::Closed {
            return Ok(());
        }
        if self.state != SessionState::Closing {
            return self.fail(Error::InvalidState {
                state: self.state.as_str(),
                detail: "session can only close after a close message",
            });
        }
        self.state = SessionState::Closed;
        Ok(())
    }

    fn handle_client_frame(&mut self, frame: &ClientFrame) -> Result<()> {
        let body = frame.body.as_ref().ok_or(Error::MissingBody {
            message: "ClientFrame",
        })?;
        match (self.state, body) {
            (SessionState::AwaitingClientHello, client_frame::Body::Hello(hello)) => {
                validate_client_hello(hello)?;
                self.client_hello = Some(hello.clone());
                self.state = SessionState::AwaitingServerHello;
                Ok(())
            }
            (
                SessionState::NegotiatingSecurity,
                client_frame::Body::SecurityHandshake(handshake),
            ) => Self::validate_handshake(
                self.negotiated.as_ref(),
                &mut self.next_handshake_step,
                handshake,
            ),
            (SessionState::Ready, client_frame::Body::PlaintextContent(content)) => {
                self.ensure_content_protection(false)?;
                self.handle_content(Direction::Client, content)
            }
            (SessionState::Ready, client_frame::Body::ProtectedPayload(payload)) => {
                self.ensure_content_protection(true)?;
                validate_protected_payload(payload)
            }
            (SessionState::Closing, client_frame::Body::PlaintextContent(content)) => {
                self.ensure_content_protection(false)?;
                self.handle_content(Direction::Client, content)
            }
            (SessionState::Closing, client_frame::Body::ProtectedPayload(payload)) => {
                self.ensure_content_protection(true)?;
                validate_protected_payload(payload)
            }
            _ => Err(Error::InvalidState {
                state: self.state.as_str(),
                detail: "client frame is not allowed in the current state",
            }),
        }
    }

    fn handle_server_frame(&mut self, frame: &ServerFrame) -> Result<()> {
        let body = frame.body.as_ref().ok_or(Error::MissingBody {
            message: "ServerFrame",
        })?;
        match (self.state, body) {
            (SessionState::AwaitingServerHello, server_frame::Body::Hello(hello)) => {
                let client = self.client_hello.as_ref().ok_or(Error::InvalidState {
                    state: self.state.as_str(),
                    detail: "server hello requires a recorded client hello",
                })?;
                let negotiated = validate_server_selection(client, hello, self.limits)?;
                self.negotiated = Some(negotiated);
                self.state = SessionState::NegotiatingSecurity;
                Ok(())
            }
            (
                SessionState::NegotiatingSecurity,
                server_frame::Body::SecurityHandshake(handshake),
            ) => Self::validate_handshake(
                self.negotiated.as_ref(),
                &mut self.next_handshake_step,
                handshake,
            ),
            (SessionState::Ready, server_frame::Body::PlaintextContent(content)) => {
                self.ensure_content_protection(false)?;
                self.handle_content(Direction::Server, content)
            }
            (SessionState::Ready, server_frame::Body::ProtectedPayload(payload)) => {
                self.ensure_content_protection(true)?;
                validate_protected_payload(payload)
            }
            (SessionState::Closing, server_frame::Body::PlaintextContent(content)) => {
                self.ensure_content_protection(false)?;
                self.handle_content(Direction::Server, content)
            }
            (SessionState::Closing, server_frame::Body::ProtectedPayload(payload)) => {
                self.ensure_content_protection(true)?;
                validate_protected_payload(payload)
            }
            _ => Err(Error::InvalidState {
                state: self.state.as_str(),
                detail: "server frame is not allowed in the current state",
            }),
        }
    }

    fn handle_content(&mut self, direction: Direction, content: &SessionContent) -> Result<()> {
        if !matches!(self.state, SessionState::Ready | SessionState::Closing) {
            return Err(Error::InvalidState {
                state: self.state.as_str(),
                detail: "session content requires a ready session",
            });
        }
        let body = content.body.as_ref().ok_or(Error::MissingBody {
            message: "SessionContent",
        })?;
        if self.state == SessionState::Closing {
            if let session_content::Body::Close(close) = body {
                validate_close(close)?;
                self.state = SessionState::Closed;
                return Ok(());
            }
            return Err(Error::InvalidState {
                state: self.state.as_str(),
                detail: "only a close response is allowed while closing",
            });
        }
        match body {
            session_content::Body::Application(envelope) => {
                validate_envelope(envelope)?;
                let next = match direction {
                    Direction::Client => &mut self.next_client_sequence,
                    Direction::Server => &mut self.next_server_sequence,
                };
                if envelope.sequence != *next {
                    return Err(Error::InvalidSequence {
                        direction: direction.as_str(),
                        expected: *next,
                        actual: envelope.sequence,
                    });
                }
                *next = next.checked_add(1).ok_or(Error::InvalidSequence {
                    direction: direction.as_str(),
                    expected: *next,
                    actual: envelope.sequence,
                })?;
            }
            session_content::Body::Close(close) => {
                validate_close(close)?;
                self.state = SessionState::Closing;
            }
            session_content::Body::Error(error) if error.fatal => {
                validate_protocol_error(error)?;
                self.state = SessionState::Failed;
            }
            session_content::Body::Ping(_) | session_content::Body::Pong(_) => {}
            session_content::Body::Error(error) => validate_protocol_error(error)?,
        }
        Ok(())
    }

    fn validate_handshake(
        negotiated: Option<&NegotiatedParameters>,
        next_step: &mut u32,
        handshake: &SecurityHandshake,
    ) -> Result<()> {
        let negotiated = negotiated.ok_or(Error::InvalidState {
            state: SessionState::NegotiatingSecurity.as_str(),
            detail: "security handshake requires negotiated parameters",
        })?;
        if handshake.scheme != negotiated.security_scheme() {
            return Err(Error::Negotiation(
                "security handshake scheme differs from server selection".to_owned(),
            ));
        }
        if handshake.payload.len() > MAX_HANDSHAKE_PAYLOAD_BYTES {
            return Err(Error::InvalidField {
                field: "security_handshake.payload",
                detail: format!("payload exceeds {MAX_HANDSHAKE_PAYLOAD_BYTES} bytes"),
            });
        }
        if handshake.step != *next_step {
            return Err(Error::InvalidField {
                field: "security_handshake.step",
                detail: format!("expected step {}, received {}", *next_step, handshake.step),
            });
        }
        *next_step = next_step.checked_add(1).ok_or(Error::InvalidField {
            field: "security_handshake.step",
            detail: "handshake step overflow".to_owned(),
        })?;
        Ok(())
    }

    fn ensure_content_protection(&self, received_protected: bool) -> Result<()> {
        let expected_protected = self.protects_content.ok_or(Error::InvalidState {
            state: self.state.as_str(),
            detail: "content received before security completion",
        })?;
        if expected_protected != received_protected {
            return Err(Error::InvalidState {
                state: self.state.as_str(),
                detail: "content protection differs from negotiated security session",
            });
        }
        Ok(())
    }

    fn fail_on_error(&mut self, result: Result<()>) -> Result<()> {
        if result.is_err() {
            self.state = SessionState::Failed;
        }
        result
    }

    fn fail<T>(&mut self, error: Error) -> Result<T> {
        self.state = SessionState::Failed;
        Err(error)
    }
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Client,
    Server,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Server => "server",
        }
    }
}

fn validate_envelope(envelope: &crate::ApplicationEnvelope) -> Result<()> {
    validate_exact_bytes(
        "application.message_id",
        &envelope.message_id,
        MESSAGE_ID_BYTES,
    )?;
    if let Some(correlation_id) = &envelope.correlation_id {
        validate_exact_bytes(
            "application.correlation_id",
            correlation_id,
            MESSAGE_ID_BYTES,
        )?;
    }
    if envelope.sequence == 0 {
        return Err(Error::InvalidField {
            field: "application.sequence",
            detail: "sequence starts at 1".to_owned(),
        });
    }
    let payload = envelope.payload.as_ref().ok_or(Error::InvalidField {
        field: "application.payload",
        detail: "payload is required".to_owned(),
    })?;
    if payload.type_url.is_empty() || payload.type_url.len() > MAX_TYPE_URL_BYTES {
        return Err(Error::InvalidField {
            field: "application.payload.type_url",
            detail: format!("type URL length must be within 1..={MAX_TYPE_URL_BYTES}"),
        });
    }
    Ok(())
}

fn validate_protocol_error(error: &crate::ProtocolError) -> Result<()> {
    if error.message.len() > MAX_ERROR_MESSAGE_BYTES {
        return Err(Error::InvalidField {
            field: "protocol_error.message",
            detail: format!("message exceeds {MAX_ERROR_MESSAGE_BYTES} bytes"),
        });
    }
    if let Some(correlation_id) = &error.correlation_id {
        validate_exact_bytes(
            "protocol_error.correlation_id",
            correlation_id,
            MESSAGE_ID_BYTES,
        )?;
    }
    Ok(())
}

fn validate_close(close: &crate::Close) -> Result<()> {
    if close.reason.len() > MAX_CLOSE_REASON_BYTES {
        return Err(Error::InvalidField {
            field: "close.reason",
            detail: format!("reason exceeds {MAX_CLOSE_REASON_BYTES} bytes"),
        });
    }
    Ok(())
}

fn validate_protected_payload(payload: &crate::ProtectedPayload) -> Result<()> {
    if payload.ciphertext.is_empty() {
        return Err(Error::InvalidField {
            field: "protected_payload.ciphertext",
            detail: "ciphertext must not be empty".to_owned(),
        });
    }
    Ok(())
}
