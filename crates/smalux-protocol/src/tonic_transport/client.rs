//! Agent 侧 Tonic Client、XXpsk3 注册和 IK 建连流程。

use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::{
    agent::v1::{
        ProtocolFrame, SecureErrorCode, SecureMessage, TokenMessage,
        agent_transport_client::AgentTransportClient, protocol_frame, secure_message,
        token_message,
    },
    noise::{ClientIkHandshake, ClientXxHandshake, NoiseIdentity, NoisePublicKey},
};

use super::{TonicNoiseSession, TransportError};

/// 首次注册成功后，业务层需要处理和持久化的完整结果。
pub struct EnrollmentOutcome {
    /// Server 在加密 TokenResponse 中确认的 Agent 业务 ID。
    pub agent_id: String,
    /// 本次注册使用的 Agent 长期 Noise 身份；含私钥。
    pub agent_identity: NoiseIdentity,
    /// XXpsk3 认证后学到的 Server 静态公钥，后续 IK 必须固定它。
    pub server_public_key: NoisePublicKey,
    /// 注册完成后仍然可用的当前加密流；调用方也可以直接丢弃并改用 IK 重连。
    pub session: TonicNoiseSession,
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

    /// 首次注册：执行 XXpsk3，发送加密 TokenRequest，并等待加密 TokenResponse。
    ///
    /// Client 不需要预置 Server 公钥。只有返回 `EnrollmentOutcome` 后，调用方才应持久化
    /// `server_public_key` 并把 Agent 标记为注册成功。
    pub async fn enroll(
        &self,
        identity: NoiseIdentity,
        psk: &[u8],
        token: String,
        agent_name: String,
    ) -> Result<EnrollmentOutcome, TransportError> {
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
                body: Some(secure_message::Body::TokenMessage(TokenMessage {
                    body: Some(token_message::Body::Request(
                        crate::agent::v1::TokenRequest { token, agent_name },
                    )),
                })),
            })
            .await?;
        // 注册只有收到加密 TokenResponse 才算成功；中途关闭不会返回 EnrollmentOutcome。
        let response = session.receive().await?.ok_or(TransportError::Closed)?;
        let response = match response.body {
            Some(secure_message::Body::TokenMessage(TokenMessage {
                body: Some(token_message::Body::Response(response)),
            })) => response,
            Some(secure_message::Body::Error(error)) => {
                let code =
                    SecureErrorCode::try_from(error.code).unwrap_or(SecureErrorCode::Unspecified);
                return Err(TransportError::RemoteSecure(code, error.message));
            }
            _ => {
                return Err(TransportError::Protocol(
                    "expected encrypted TokenResponse".to_owned(),
                ));
            }
        };
        Ok(EnrollmentOutcome {
            agent_id: response.agent_id,
            agent_identity: identity,
            server_public_key,
            session,
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
