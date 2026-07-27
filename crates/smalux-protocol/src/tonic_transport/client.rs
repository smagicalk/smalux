//! Agent 侧 Tonic Client、XXpsk3 注册和 IK 建连流程。

use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::{
    agent::v1::{
        ProtocolFrame, RegistrationCommit, RegistrationMessage, RegistrationRequest,
        SecureErrorCode, SecureMessage, agent_transport_client::AgentTransportClient,
        protocol_frame, registration_message, secure_message,
    },
    noise::{ClientIkHandshake, ClientXxHandshake, NoiseIdentity, NoisePublicKey},
};

use super::{TonicNoiseSession, TransportError};

/// 首次注册成功后，业务层需要处理和持久化的完整结果。
pub struct AgentRegistration {
    /// Server 在加密 RegistrationPrepared 中分配的 Agent 业务 ID。
    pub agent_id: String,
    /// 本次注册使用的 Agent 长期 Noise 身份；含私钥。
    pub agent_identity: NoiseIdentity,
    /// XXpsk3 认证后学到的 Server 静态公钥，后续 IK 必须固定它。
    pub server_public_key: NoisePublicKey,
    /// Server 分配的注册事务 ID；可用于诊断或持久化审计记录。
    pub registration_id: [u8; 16],
    /// 注册完成后仍然可用的当前加密流；首次业务应直接复用，断线后再改用 IK 重连。
    pub session: TonicNoiseSession,
}

/// Server 已准备注册、等待 Agent 完成本地持久化并提交确认的阶段。
///
/// 调用方取得本对象后，应先保存 `agent_identity`、`server_public_key`、`agent_id` 和
/// `registration_id`，保存成功后再调用 [`Self::commit`]。如果保存失败，直接丢弃本对象；
/// 下一次用相同 Token 和 Agent 公钥重新注册时，Server 应返回相同 pending 事务。
pub struct AgentPendingRegistration {
    /// Server 分配的稳定业务 ID。
    pub agent_id: String,
    /// 本次注册使用的 Agent 长期 Noise 身份；含私钥。
    pub agent_identity: NoiseIdentity,
    /// XXpsk3 认证后学到的 Server 静态公钥。
    pub server_public_key: NoisePublicKey,
    /// Server 分配的 16 字节幂等注册事务 ID。
    pub registration_id: [u8; 16],
    /// 等待最终提交确认的当前 XXpsk3 加密流。
    session: TonicNoiseSession,
    /// 等待最终确认的时间上限。
    confirmation_timeout: Duration,
}

impl AgentPendingRegistration {
    /// 通知 Server 本地身份已经保存，并等待最终 `RegistrationCommitted`。
    pub async fn commit(mut self) -> Result<AgentRegistration, TransportError> {
        self.session
            .send(SecureMessage {
                body: Some(secure_message::Body::RegistrationMessage(
                    RegistrationMessage {
                        body: Some(registration_message::Body::Commit(RegistrationCommit {
                            registration_id: self.registration_id.to_vec(),
                        })),
                    },
                )),
            })
            .await?;
        let response = timeout(self.confirmation_timeout, self.session.receive())
            .await
            .map_err(|_| TransportError::Timeout("waiting for registration commit"))??
            .ok_or(TransportError::Closed)?;
        match response.body {
            Some(secure_message::Body::RegistrationMessage(RegistrationMessage {
                body: Some(registration_message::Body::Committed(committed)),
            })) if committed.registration_id == self.registration_id => {}
            Some(secure_message::Body::Error(error)) => {
                let code =
                    SecureErrorCode::try_from(error.code).unwrap_or(SecureErrorCode::Unspecified);
                return Err(TransportError::RemoteSecure(code, error.message));
            }
            _ => {
                return Err(TransportError::Protocol(
                    "expected matching encrypted RegistrationCommitted".to_owned(),
                ));
            }
        }
        Ok(AgentRegistration {
            agent_id: self.agent_id,
            agent_identity: self.agent_identity,
            server_public_key: self.server_public_key,
            registration_id: self.registration_id,
            session: self.session,
        })
    }
}

/// Agent 建立正式 `AgentTransport/OpenSession` 的高层入口。
pub struct AgentProtocolClient {
    /// 不含 gRPC service path 的源站地址，例如 `https://agent.example.com`。
    endpoint: String,
    /// Axum nest 或反向代理使用的可选统一路径前缀。
    grpc_prefix: Option<String>,
    /// channel 打开和每一步 Noise 握手的时间上限。
    handshake_timeout: Duration,
}

impl AgentProtocolClient {
    /// 创建 Client 配置；默认握手超时为 5 秒且没有额外 gRPC prefix。
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            grpc_prefix: None,
            handshake_timeout: Duration::from_secs(5),
        }
    }

    /// 修改建连和握手等待上限，不影响建立后的长期业务流。
    pub fn set_handshake_timeout(&mut self, value: Duration) {
        self.handshake_timeout = value;
    }

    /// 设置反向代理或 Axum nest 使用的统一 gRPC 前缀，例如 `/api/v1/grpc`。
    pub fn set_grpc_prefix(&mut self, prefix: impl Into<String>) {
        self.grpc_prefix = Some(prefix.into());
    }

    /// 首次注册便捷入口：依次执行 prepare 和 commit，直到收到最终加密确认。
    ///
    /// Client 不需要预置 Server 公钥。只有返回 [`AgentRegistration`] 后，调用方才应持久化
    /// `server_public_key` 并把 Agent 标记为注册成功。
    pub async fn register_agent(
        &self,
        identity: NoiseIdentity,
        psk: &[u8],
        token: String,
        agent_name: String,
    ) -> Result<AgentRegistration, TransportError> {
        self.prepare_registration(identity, psk, token, agent_name)
            .await?
            .commit()
            .await
    }

    /// 首次注册的准备阶段：完成 XXpsk3、发送注册请求并等待 Server 的 pending 结果。
    ///
    /// 本方法不会发送最终 commit。调用方必须先持久化返回对象中的身份材料，再调用
    /// [`AgentPendingRegistration::commit`]。不需要控制持久化时序的简单程序可直接使用
    /// [`Self::register_agent`] 完成两步调用。
    pub async fn prepare_registration(
        &self,
        identity: NoiseIdentity,
        psk: &[u8],
        token: String,
        agent_name: String,
    ) -> Result<AgentPendingRegistration, TransportError> {
        // 第一步只生成 XXpsk3 message 1；此时尚未信任任何 Server 静态公钥。
        let (waiting, first) = ClientXxHandshake::start(&identity, psk)?;
        // 打开 gRPC 双向流，并把 message 1 作为首帧发送。
        let (sender, mut inbound) = self.open(first).await?;
        // 每个握手阶段单独受 handshake_timeout 约束，避免半开 RPC 永久占用资源。
        let second = next_handshake(&mut inbound, self.handshake_timeout).await?;
        // message 2 验证成功后学到 Server 公钥，并产生必须回传的 message 3。
        let (established, third) = waiting.receive_message2(second)?;
        sender
            .send(handshake_frame(third))
            .await
            .map_err(|_| TransportError::Closed)?;
        // 只有 Noise transcript 验证成功后，远端静态公钥才可进入待持久化结果。
        let server_public_key = established.remote_static_key;
        let mut session = TonicNoiseSession::client(sender, inbound, established.session);
        // Token 放在 Noise transport 密文中；TLS/CDN 只能看到 ciphertext 帧。
        session
            .send(SecureMessage {
                body: Some(secure_message::Body::RegistrationMessage(
                    RegistrationMessage {
                        body: Some(registration_message::Body::Request(RegistrationRequest {
                            token,
                            agent_name,
                        })),
                    },
                )),
            })
            .await?;
        // 这里仅等待 Server 持久化 pending；最终成功还需要 Agent commit 和 Server committed。
        let response = timeout(self.handshake_timeout, session.receive())
            .await
            .map_err(|_| TransportError::Timeout("waiting for registration preparation"))??
            .ok_or(TransportError::Closed)?;
        let response = match response.body {
            Some(secure_message::Body::RegistrationMessage(RegistrationMessage {
                body: Some(registration_message::Body::Prepared(response)),
            })) => response,
            Some(secure_message::Body::Error(error)) => {
                let code =
                    SecureErrorCode::try_from(error.code).unwrap_or(SecureErrorCode::Unspecified);
                return Err(TransportError::RemoteSecure(code, error.message));
            }
            _ => {
                return Err(TransportError::Protocol(
                    "expected encrypted RegistrationPrepared".to_owned(),
                ));
            }
        };
        let registration_id: [u8; 16] = response.registration_id.try_into().map_err(|_| {
            TransportError::Protocol("registration ID must contain exactly 16 bytes".to_owned())
        })?;
        Ok(AgentPendingRegistration {
            agent_id: response.agent_id,
            agent_identity: identity,
            server_public_key,
            registration_id,
            session,
            confirmation_timeout: self.handshake_timeout,
        })
    }

    /// 已注册连接：使用 Agent 身份和固定 Server 公钥执行两消息 IK。
    pub async fn connect(
        &self,
        identity: &NoiseIdentity,
        server_key: NoisePublicKey,
    ) -> Result<TonicNoiseSession, TransportError> {
        // IK message 1 已包含对 Agent 静态身份的密码学证明，并指定目标 Server key ID。
        let (waiting, first) = ClientIkHandshake::start(identity, server_key)?;
        let (sender, mut inbound) = self.open(first).await?;
        // Server message 2 完成双向静态身份认证，随后直接进入 transport mode。
        let second = next_handshake(&mut inbound, self.handshake_timeout).await?;
        let established = waiting.receive_message2(second)?;
        Ok(TonicNoiseSession::client(
            sender,
            inbound,
            established.session,
        ))
    }

    /// 按给定 Server 公钥顺序逐一尝试 IK，首个成功结果立即返回。
    ///
    /// 适用于 Server 换钥窗口；全部失败时返回最后一次握手错误。
    pub async fn connect_with_candidates(
        &self,
        identity: &NoiseIdentity,
        candidates: &[NoisePublicKey],
    ) -> Result<TonicNoiseSession, TransportError> {
        // 轮换窗口通常依次尝试 pending/current/previous；每次尝试都创建独立 RPC。
        let mut last = None;
        for key in candidates {
            match self.connect(identity, *key).await {
                Ok(session) => return Ok(session),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or_else(|| TransportError::Protocol("no Server key candidates".into())))
    }

    /// 创建 Tonic channel、发送握手首帧并打开正式双向 RPC。
    async fn open(
        &self,
        first: crate::agent::v1::NoiseHandshake,
    ) -> Result<(mpsc::Sender<ProtocolFrame>, tonic::Streaming<ProtocolFrame>), TransportError>
    {
        // endpoint scheme 决定使用 HTTPS/TLS 还是本地 h2c。
        let channel = connect_channel(&self.endpoint).await?;
        // 有界 channel 把应用发送速度反压到 gRPC request stream。
        let (sender, receiver) = mpsc::channel(16);
        sender
            .send(handshake_frame(first))
            .await
            .map_err(|_| TransportError::Closed)?;
        // with_origin 只改变生成的 gRPC :path 前缀，不改变服务和方法的 Protobuf 名称。
        let mut client = if let Some(prefix) = &self.grpc_prefix {
            let origin = format!("{}{}", self.endpoint.trim_end_matches('/'), prefix).parse()?;
            AgentTransportClient::with_origin(channel, origin)
        } else {
            AgentTransportClient::new(channel)
        };
        let response = timeout(
            self.handshake_timeout,
            client.open_session(ReceiverStream::new(receiver)),
        )
        .await
        .map_err(|_| TransportError::Timeout("opening gRPC session"))??;
        Ok((sender, response.into_inner()))
    }
}

/// 根据 endpoint scheme 创建 Tonic channel。
async fn connect_channel(endpoint: &str) -> Result<Channel, TransportError> {
    // Endpoint 解析失败属于不可重试配置错误。
    let builder = Endpoint::from_shared(endpoint.to_owned())?;
    let builder = if endpoint.starts_with("https://") {
        builder.tls_config(ClientTlsConfig::new().with_native_roots())?
    } else {
        builder
    };
    Ok(builder.connect().await?)
}

/// 在给定上限内读取下一条握手响应，并识别远端外层协议错误。
async fn next_handshake(
    inbound: &mut tonic::Streaming<ProtocolFrame>,
    limit: Duration,
) -> Result<crate::agent::v1::NoiseHandshake, TransportError> {
    let frame = timeout(limit, inbound.message())
        .await
        .map_err(|_| TransportError::Timeout("waiting for handshake frame"))??
        .ok_or(TransportError::Closed)?;
    match frame.body {
        Some(protocol_frame::Body::Handshake(handshake)) => Ok(handshake),
        Some(protocol_frame::Body::ProtocolError(error)) => {
            Err(TransportError::RemoteProtocol(error.message))
        }
        _ => Err(TransportError::Protocol(
            "expected handshake frame".to_owned(),
        )),
    }
}

/// 把 Noise 核心产生的握手消息包装为正式外层帧。
fn handshake_frame(handshake: crate::agent::v1::NoiseHandshake) -> ProtocolFrame {
    ProtocolFrame {
        body: Some(protocol_frame::Body::Handshake(handshake)),
    }
}
