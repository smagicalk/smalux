//! Server 侧 Tonic stream 接收器与业务授权边界。

use std::{future::Future, time::Duration};

use tokio::{sync::mpsc, time::timeout};
use tonic::{Status, Streaming};
use tracing::{debug, info, trace, warn};

use crate::{
    agent::v1::{
        NoiseHandshake, ProtocolFrame, RegistrationCommitted, RegistrationMessage,
        RegistrationPrepared, RegistrationRequest, SecureError, SecureMessage,
        noise_handshake::HandshakeType, protocol_frame, registration_message, secure_message,
    },
    noise::{
        HandshakeMode, KeyId, NoisePublicKey, ServerIkHandshake, ServerKeyRing, ServerXxHandshake,
    },
};

use super::{TonicNoiseSession, TransportError, registration_token_id_log_label};

/// 完成 Noise 握手后得到的强类型 Server 入口。
///
/// 首次 XXpsk3 连接进入注册阶段，IK 连接进入已登记身份的业务授权阶段。该枚举只表达
/// 协议阶段，不替调用方验证 Token、查询数据库或决定是否授权。
pub enum IncomingSession {
    /// 首次注册，需要读取加密注册请求并完成 prepare/commit/committed。
    Registration(ServerRegistration),
    /// 后续 IK 连接，需要使用认证公钥查询已登记 Agent。
    Authentication(ServerAuthentication),
}

/// XXpsk3 已完成，等待 Server 处理可靠注册状态机的会话。
pub struct ServerRegistration {
    /// XXpsk3 首帧中已经用于解析 PSK 的公开 Token ID。
    registration_token_id: String,
    peer_public_key: NoisePublicKey,
    session: TonicNoiseSession,
}

impl ServerRegistration {
    /// 返回已经完成 Noise 校验的公开 Token ID。
    ///
    /// 业务层必须要求密文 `RegistrationRequest.token` 使用相同 ID，避免握手凭据和
    /// 注册资料来自两条不同 Token。
    pub fn registration_token_id(&self) -> &str {
        &self.registration_token_id
    }

    /// 返回 XXpsk3 已认证的 Agent 静态公钥，注册记录必须绑定该公钥。
    pub fn peer_public_key(&self) -> NoisePublicKey {
        self.peer_public_key
    }

    /// 读取首次注册请求；其他业务或控制类型会作为协议错误返回。
    pub async fn receive_request(&mut self) -> Result<RegistrationRequest, TransportError> {
        debug!("waiting for encrypted Agent RegistrationRequest");
        let message = self
            .session
            .receive()
            .await?
            .ok_or(TransportError::Closed)?;
        match message.body {
            Some(secure_message::Body::RegistrationMessage(RegistrationMessage {
                body: Some(registration_message::Body::Request(request)),
            })) => {
                info!(
                    agent_name_len = request.agent_name.len(),
                    "received encrypted Agent registration request"
                );
                Ok(request)
            }
            _ => {
                warn!("XXpsk3 session received a non-registration request message");
                Err(TransportError::Protocol(
                    "XXpsk3 session requires RegistrationRequest".to_owned(),
                ))
            }
        }
    }

    /// Server 持久化 pending 注册后，向 Agent 返回稳定业务 ID 和幂等事务 ID。
    pub async fn prepare(
        &mut self,
        registration_id: [u8; 16],
        agent_id: impl Into<String>,
    ) -> Result<(), TransportError> {
        let agent_id = agent_id.into();
        info!(registration_id = ?registration_id, agent_id = %agent_id, "sending encrypted registration preparation");
        self.session
            .send(registration(registration_message::Body::Prepared(
                RegistrationPrepared {
                    registration_id: registration_id.to_vec(),
                    agent_id,
                },
            )))
            .await
    }

    /// 等待 Agent 确认本地身份材料已经持久化，并校验事务 ID。
    pub async fn wait_for_commit(
        &mut self,
        expected_registration_id: [u8; 16],
        limit: Duration,
    ) -> Result<(), TransportError> {
        debug!(registration_id = ?expected_registration_id, "waiting for encrypted registration commit");
        let message = timeout(limit, self.session.receive())
            .await
            .map_err(|_| TransportError::Timeout("waiting for registration commit"))??
            .ok_or(TransportError::Closed)?;
        match message.body {
            Some(secure_message::Body::RegistrationMessage(RegistrationMessage {
                body: Some(registration_message::Body::Commit(commit)),
            })) if commit.registration_id == expected_registration_id => {
                info!(registration_id = ?expected_registration_id, "received matching registration commit");
                Ok(())
            }
            _ => {
                warn!("registration commit did not match the pending transaction");
                Err(TransportError::Protocol(
                    "expected matching RegistrationCommit".to_owned(),
                ))
            }
        }
    }

    /// Server 激活 Agent 并消费 Token 后发送最终确认，返回可继续复用的 XX 会话。
    pub async fn complete(
        mut self,
        registration_id: [u8; 16],
    ) -> Result<TonicNoiseSession, TransportError> {
        info!(registration_id = ?registration_id, "sending encrypted registration committed response");
        self.session
            .send(registration(registration_message::Body::Committed(
                RegistrationCommitted {
                    registration_id: registration_id.to_vec(),
                },
            )))
            .await?;
        Ok(self.session)
    }

    /// 发送加密拒绝原因并结束本地注册阶段。
    pub async fn reject(mut self, error: SecureError) -> Result<(), TransportError> {
        warn!(
            code = error.code,
            "rejecting Agent registration in encrypted session"
        );
        self.session
            .send(SecureMessage {
                body: Some(secure_message::Body::Error(error)),
            })
            .await
    }
}

/// IK 已完成，等待调用方根据认证公钥执行数据库授权的会话。
pub struct ServerAuthentication {
    peer_public_key: NoisePublicKey,
    session: TonicNoiseSession,
}

impl ServerAuthentication {
    /// 返回 IK 握手已经认证的 Agent 静态公钥。
    pub fn peer_public_key(&self) -> NoisePublicKey {
        self.peer_public_key
    }

    /// 数据库授权成功后取得业务会话。
    pub fn authorize(self) -> TonicNoiseSession {
        info!(peer_key_id = ?self.peer_public_key.key_id(), "authorizing authenticated Agent session");
        self.session
    }

    /// 数据库授权失败时发送加密错误并结束本地授权阶段。
    pub async fn reject(mut self, error: SecureError) -> Result<(), TransportError> {
        warn!(code = error.code, "rejecting authenticated Agent session");
        self.session
            .send(SecureMessage {
                body: Some(secure_message::Body::Error(error)),
            })
            .await
    }
}

/// Noise 握手已完成、但业务层尚未决定授权或拒绝的会话。
pub struct ServerPendingSession {
    /// 首次注册 XXpsk3 或后续认证 IK。
    mode: HandshakeMode,
    /// XXpsk3 首帧的公开 Token ID；IK 会话没有注册 Token。
    registration_token_id: Option<String>,
    /// 握手密码学认证得到的 Agent 长期静态公钥。
    peer_public_key: NoisePublicKey,
    /// 已建立但尚未交给业务处理的加密会话。
    session: TonicNoiseSession,
}

impl ServerPendingSession {
    /// 返回握手模式，业务层据此选择注册或已注册授权分支。
    pub fn handshake_mode(&self) -> HandshakeMode {
        self.mode
    }

    /// 返回 Noise 握手已经认证的 Agent 静态公钥，供业务层查询授权记录。
    pub fn peer_public_key(&self) -> NoisePublicKey {
        self.peer_public_key
    }

    /// 授权通过后消费待授权对象，取得可收发加密业务消息的会话。
    pub fn authorize(self) -> TonicNoiseSession {
        self.session
    }

    /// 通过已建立的 Noise 会话发送加密拒绝原因，然后结束本地待授权状态。
    pub async fn reject(mut self, error: SecureError) -> Result<(), TransportError> {
        warn!(code = error.code, "rejecting pending Agent session");
        self.session
            .send(SecureMessage {
                body: Some(secure_message::Body::Error(error)),
            })
            .await
    }
}

/// 接受 `OpenSession` 双向流并完成 Noise responder 握手。
pub struct ServerSessionAcceptor {
    /// 每一条预期握手帧的最大等待时间。
    handshake_timeout: Duration,
}

impl Default for ServerSessionAcceptor {
    /// 创建默认 5 秒握手超时的接收器。
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

impl ServerSessionAcceptor {
    /// 创建使用指定握手超时的接收器。
    pub fn new(handshake_timeout: Duration) -> Self {
        Self { handshake_timeout }
    }

    /// 读取首帧、选择 XXpsk3/IK 与 Server 私钥，并返回待业务授权会话。
    ///
    /// `sender` 与 `inbound` 必须属于同一个 RPC；`registration_psk` 只用于 XXpsk3。
    pub async fn accept_session(
        &self,
        inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
        keyring: &ServerKeyRing,
        registration_psk: &[u8],
    ) -> Result<ServerPendingSession, TransportError> {
        let registration_psk: [u8; 32] = registration_psk.try_into().map_err(|_| {
            TransportError::Protocol("registration PSK must contain 32 bytes".into())
        })?;
        self.accept_session_with_psk_resolver(inbound, sender, keyring, move |_| async move {
            Ok(registration_psk)
        })
        .await
    }

    /// 按 XXpsk3 首帧中的公开 Token ID 异步选择独立注册 PSK。
    ///
    /// resolver 接收拥有所有权的 Token ID，因此它可以安全跨越数据库查询等 `await`
    /// 边界。解析过程与握手帧共用握手超时，避免数据库停顿无限占用一条 RPC。
    pub async fn accept_session_with_psk_resolver<F, Fut>(
        &self,
        mut inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
        keyring: &ServerKeyRing,
        resolve_psk: F,
    ) -> Result<ServerPendingSession, TransportError>
    where
        F: FnOnce(String) -> Fut + Send,
        Fut: Future<Output = Result<[u8; 32], TransportError>> + Send,
    {
        debug!(?self.handshake_timeout, "waiting for first Agent Noise handshake frame");
        let first = next_handshake(&mut inbound, self.handshake_timeout).await?;
        let kind = HandshakeType::try_from(first.r#type)
            .map_err(|_| TransportError::Protocol("unknown handshake type".to_owned()))?;
        let registration_token_id = first.registration_token_id.clone();
        let token_id_label = registration_token_id_log_label(&first.registration_token_id);
        info!(handshake = ?kind, token_id = %token_id_label, payload_len = first.payload.len(), "received Agent Noise handshake start");
        let established = match kind {
            HandshakeType::XxPsk3 => {
                validate_registration_token_id(&first.registration_token_id)?;
                let registration_psk = timeout(
                    self.handshake_timeout,
                    resolve_psk(first.registration_token_id.clone()),
                )
                .await
                .map_err(|_| TransportError::Timeout("resolving registration PSK"))??;
                debug!(token_id = %token_id_label, "resolved registration PSK without logging secret bytes");
                // 首次注册尚无 pinned key，固定使用 keyring 的当前首选身份响应。
                let identity =
                    keyring.active_keys().into_iter().next().ok_or_else(|| {
                        TransportError::Protocol("Server keyring is empty".into())
                    })?;
                let (waiting, second) =
                    ServerXxHandshake::receive_message1(identity, &registration_psk, first)?;
                send_handshake(&sender, second).await?;
                let third = next_handshake(&mut inbound, self.handshake_timeout).await?;
                debug!("received Agent XXpsk3 message 3; validating registration handshake");
                waiting.receive_message3(third)?
            }
            HandshakeType::Ik => {
                // IK Client 明确指定 responder key ID，轮换期据此选择 current/next/previous。
                let key_id = KeyId::from_bytes(&first.responder_key_id)?;
                debug!(server_key_id = ?key_id, "selecting Server key for Agent IK handshake");
                let identity = keyring
                    .find_active(key_id)
                    .ok_or(TransportError::UnknownKeyId)?;
                let (established, second) = ServerIkHandshake::receive_message1(identity, first)?;
                send_handshake(&sender, second).await?;
                info!(peer_key_id = ?established.remote_static_key.key_id(), "Agent IK handshake completed");
                established
            }
            HandshakeType::Unspecified => {
                warn!("Agent handshake did not specify a supported mode");
                return Err(TransportError::Protocol(
                    "handshake type is required".to_owned(),
                ));
            }
        };
        info!(mode = ?established.mode, peer_key_id = ?established.remote_static_key.key_id(), "Noise handshake established on Server");
        Ok(ServerPendingSession {
            mode: established.mode,
            registration_token_id: (established.mode == HandshakeMode::RegistrationXxPsk3)
                .then_some(registration_token_id),
            peer_public_key: established.remote_static_key,
            session: TonicNoiseSession::server(sender, inbound, established.session),
        })
    }

    /// 完成 Noise responder 握手，并把结果分类为首次注册或 IK 业务授权阶段。
    pub async fn accept_incoming(
        &self,
        inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
        keyring: &ServerKeyRing,
        registration_psk: &[u8],
    ) -> Result<IncomingSession, TransportError> {
        let pending = self
            .accept_session(inbound, sender, keyring, registration_psk)
            .await?;
        match pending.mode {
            HandshakeMode::RegistrationXxPsk3 => {
                Ok(IncomingSession::Registration(ServerRegistration {
                    registration_token_id: pending.registration_token_id.ok_or_else(|| {
                        TransportError::Protocol("registration Token ID is missing".to_owned())
                    })?,
                    peer_public_key: pending.peer_public_key,
                    session: pending.session,
                }))
            }
            HandshakeMode::AuthenticatedIk => {
                Ok(IncomingSession::Authentication(ServerAuthentication {
                    peer_public_key: pending.peer_public_key,
                    session: pending.session,
                }))
            }
        }
    }

    /// 使用异步多 Token resolver 完成握手并分类注册或 IK 会话。
    pub async fn accept_incoming_with_psk_resolver<F, Fut>(
        &self,
        inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
        keyring: &ServerKeyRing,
        resolve_psk: F,
    ) -> Result<IncomingSession, TransportError>
    where
        F: FnOnce(String) -> Fut + Send,
        Fut: Future<Output = Result<[u8; 32], TransportError>> + Send,
    {
        let pending = self
            .accept_session_with_psk_resolver(inbound, sender, keyring, resolve_psk)
            .await?;
        match pending.mode {
            HandshakeMode::RegistrationXxPsk3 => {
                Ok(IncomingSession::Registration(ServerRegistration {
                    registration_token_id: pending.registration_token_id.ok_or_else(|| {
                        TransportError::Protocol("registration Token ID is missing".to_owned())
                    })?,
                    peer_public_key: pending.peer_public_key,
                    session: pending.session,
                }))
            }
            HandshakeMode::AuthenticatedIk => {
                Ok(IncomingSession::Authentication(ServerAuthentication {
                    peer_public_key: pending.peer_public_key,
                    session: pending.session,
                }))
            }
        }
    }
}

/// 校验握手首帧中用于选择 PSK 的公开 Token ID。
///
/// 该字段虽然不是秘密，但在 Noise 建立前完全来自网络，必须在进入异步 resolver
/// 和数据库查询前限制长度与字符集。实际签发器使用 32 位十六进制 ID；协议层保留
/// 128 字节上限，兼容未来使用 UUID 或带版本前缀的 ID。
pub fn validate_registration_token_id(token_id: &str) -> Result<(), TransportError> {
    if token_id.is_empty()
        || token_id.len() > 128
        || !token_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        warn!(
            token_id_len = token_id.len(),
            "rejected invalid registration Token ID"
        );
        return Err(TransportError::Protocol(
            "registration Token ID is invalid".to_owned(),
        ));
    }
    Ok(())
}

/// 构造一条加密注册状态消息。
fn registration(body: registration_message::Body) -> SecureMessage {
    SecureMessage {
        body: Some(secure_message::Body::RegistrationMessage(
            RegistrationMessage { body: Some(body) },
        )),
    }
}

/// 在指定上限内读取下一条握手帧。
async fn next_handshake(
    inbound: &mut Streaming<ProtocolFrame>,
    limit: Duration,
) -> Result<NoiseHandshake, TransportError> {
    let frame = timeout(limit, inbound.message())
        .await
        .map_err(|_| {
            warn!(?limit, "timed out waiting for Agent handshake frame");
            TransportError::Timeout("waiting for handshake frame")
        })??
        .ok_or_else(|| {
            warn!("Agent closed the gRPC stream while sending a handshake frame");
            TransportError::Closed
        })?;
    match frame.body {
        Some(protocol_frame::Body::Handshake(handshake)) => {
            trace!(
                payload_len = handshake.payload.len(),
                "received Agent handshake frame"
            );
            Ok(handshake)
        }
        _ => {
            warn!("received a non-handshake frame during Server handshake");
            Err(TransportError::Protocol(
                "expected handshake frame".to_owned(),
            ))
        }
    }
}

/// 通过 Tonic 响应 channel 发送一条握手帧。
async fn send_handshake(
    sender: &mpsc::Sender<Result<ProtocolFrame, Status>>,
    handshake: NoiseHandshake,
) -> Result<(), TransportError> {
    trace!(
        handshake = ?handshake.r#type,
        payload_len = handshake.payload.len(),
        "sending Server handshake frame"
    );
    sender
        .send(Ok(ProtocolFrame {
            body: Some(protocol_frame::Body::Handshake(handshake)),
        }))
        .await
        .map_err(|_| TransportError::Closed)
}

#[cfg(test)]
mod tests {
    use super::validate_registration_token_id;

    #[test]
    fn registration_token_id_validation_rejects_untrusted_values() {
        assert!(validate_registration_token_id("0123456789abcdef").is_ok());
        assert!(validate_registration_token_id("token-id.v1").is_ok());
        assert!(validate_registration_token_id("").is_err());
        assert!(validate_registration_token_id(&"a".repeat(129)).is_err());
        assert!(validate_registration_token_id("token id").is_err());
        assert!(validate_registration_token_id("token\n-id").is_err());
    }
}
