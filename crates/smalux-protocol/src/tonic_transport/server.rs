//! Server 侧 Tonic stream 接收器与业务授权边界。

use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tonic::{Status, Streaming};

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

use super::{TonicNoiseSession, TransportError};

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
    peer_public_key: NoisePublicKey,
    session: TonicNoiseSession,
}

impl ServerRegistration {
    /// 返回 XXpsk3 已认证的 Agent 静态公钥，注册记录必须绑定该公钥。
    pub fn peer_public_key(&self) -> NoisePublicKey {
        self.peer_public_key
    }

    /// 读取首次注册请求；其他业务或控制类型会作为协议错误返回。
    pub async fn receive_request(&mut self) -> Result<RegistrationRequest, TransportError> {
        let message = self
            .session
            .receive()
            .await?
            .ok_or(TransportError::Closed)?;
        match message.body {
            Some(secure_message::Body::RegistrationMessage(RegistrationMessage {
                body: Some(registration_message::Body::Request(request)),
            })) => Ok(request),
            _ => Err(TransportError::Protocol(
                "XXpsk3 session requires RegistrationRequest".to_owned(),
            )),
        }
    }

    /// Server 持久化 pending 注册后，向 Agent 返回稳定业务 ID 和幂等事务 ID。
    pub async fn prepare(
        &mut self,
        registration_id: [u8; 16],
        agent_id: impl Into<String>,
    ) -> Result<(), TransportError> {
        self.session
            .send(registration(registration_message::Body::Prepared(
                RegistrationPrepared {
                    registration_id: registration_id.to_vec(),
                    agent_id: agent_id.into(),
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
        let message = timeout(limit, self.session.receive())
            .await
            .map_err(|_| TransportError::Timeout("waiting for registration commit"))??
            .ok_or(TransportError::Closed)?;
        match message.body {
            Some(secure_message::Body::RegistrationMessage(RegistrationMessage {
                body: Some(registration_message::Body::Commit(commit)),
            })) if commit.registration_id == expected_registration_id => Ok(()),
            _ => Err(TransportError::Protocol(
                "expected matching RegistrationCommit".to_owned(),
            )),
        }
    }

    /// Server 激活 Agent 并消费 Token 后发送最终确认，返回可继续复用的 XX 会话。
    pub async fn complete(
        mut self,
        registration_id: [u8; 16],
    ) -> Result<TonicNoiseSession, TransportError> {
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
        self.session
    }

    /// 数据库授权失败时发送加密错误并结束本地授权阶段。
    pub async fn reject(mut self, error: SecureError) -> Result<(), TransportError> {
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
        mut inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
        keyring: &ServerKeyRing,
        registration_psk: &[u8],
    ) -> Result<ServerPendingSession, TransportError> {
        let first = next_handshake(&mut inbound, self.handshake_timeout).await?;
        let kind = HandshakeType::try_from(first.r#type)
            .map_err(|_| TransportError::Protocol("unknown handshake type".to_owned()))?;
        let established = match kind {
            HandshakeType::XxPsk3 => {
                // 首次注册尚无 pinned key，固定使用 keyring 的当前首选身份响应。
                let identity =
                    keyring.active_keys().into_iter().next().ok_or_else(|| {
                        TransportError::Protocol("Server keyring is empty".into())
                    })?;
                let (waiting, second) =
                    ServerXxHandshake::receive_message1(identity, registration_psk, first)?;
                send_handshake(&sender, second).await?;
                let third = next_handshake(&mut inbound, self.handshake_timeout).await?;
                waiting.receive_message3(third)?
            }
            HandshakeType::Ik => {
                // IK Client 明确指定 responder key ID，轮换期据此选择 current/next/previous。
                let key_id = KeyId::from_bytes(&first.responder_key_id)?;
                let identity = keyring
                    .find_active(key_id)
                    .ok_or(TransportError::UnknownKeyId)?;
                let (established, second) = ServerIkHandshake::receive_message1(identity, first)?;
                send_handshake(&sender, second).await?;
                established
            }
            HandshakeType::Unspecified => {
                return Err(TransportError::Protocol(
                    "handshake type is required".to_owned(),
                ));
            }
        };
        Ok(ServerPendingSession {
            mode: established.mode,
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
        .map_err(|_| TransportError::Timeout("waiting for handshake frame"))??
        .ok_or(TransportError::Closed)?;
    match frame.body {
        Some(protocol_frame::Body::Handshake(handshake)) => Ok(handshake),
        _ => Err(TransportError::Protocol(
            "expected handshake frame".to_owned(),
        )),
    }
}

/// 通过 Tonic 响应 channel 发送一条握手帧。
async fn send_handshake(
    sender: &mpsc::Sender<Result<ProtocolFrame, Status>>,
    handshake: NoiseHandshake,
) -> Result<(), TransportError> {
    sender
        .send(Ok(ProtocolFrame {
            body: Some(protocol_frame::Body::Handshake(handshake)),
        }))
        .await
        .map_err(|_| TransportError::Closed)
}
