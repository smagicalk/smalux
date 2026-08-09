//! Agent 侧 Tonic Client、XXpsk3 注册和 IK 建连流程。

use std::time::Duration;

use tokio::{sync::mpsc, time::timeout};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    client::Grpc,
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use tracing::{debug, info, trace, warn};

use crate::{
    agent::v1::{
        HealthRequest, HealthResponse, ProtocolFrame, RegistrationCommit, RegistrationMessage,
        RegistrationRequest, SecureErrorCode, SecureMessage, protocol_frame, registration_message,
        secure_message,
    },
    noise::{ClientIkHandshake, ClientXxHandshake, NoiseIdentity, NoisePublicKey},
};

use super::{
    TonicNoiseSession, TransportError, registration_token_id_log_label,
    validate_registration_token_id,
};

const AGENT_TRANSPORT_SERVICE: &str = "smalux.agent.v1.AgentTransport";

/// Agent gRPC 的路径感知 Client。
///
/// Tonic 生成的 `AgentTransportClient` 会为每个 RPC 使用以 `/` 开头的固定路径，
/// 因而不能通过 endpoint URL 保留 Axum `nest` 或反向代理前缀。本类型统一生成完整
/// 路径，例如 `/api/v1/grpc/smalux.agent.v1.AgentTransport/OpenSession`，调用方无需
/// 直接接触 `tonic::client::Grpc`、Prost codec 或方法 URI。
pub struct AgentTransportRpcClient {
    inner: Grpc<Channel>,
    grpc_prefix: Option<String>,
}

impl AgentTransportRpcClient {
    /// 连接 gRPC 源站，并可选指定 Axum 或代理使用的统一路径前缀。
    ///
    /// `endpoint` 只能包含 scheme、host 与 port，例如 `https://agent.example.com`；
    /// 路径前缀应单独通过 `grpc_prefix` 传入，避免 Tonic 覆盖 endpoint URL 的 path。
    pub async fn connect(
        endpoint: &str,
        grpc_prefix: Option<&str>,
    ) -> Result<Self, TransportError> {
        let endpoint_label = endpoint_log_label(endpoint);
        let grpc_prefix_label = grpc_prefix_log_label(grpc_prefix);
        info!(
            endpoint = %endpoint_label,
            grpc_prefix = %grpc_prefix_label,
            "connecting Agent gRPC client"
        );
        let channel = connect_channel(endpoint).await?;
        debug!(endpoint = %endpoint_label, "Agent gRPC channel connected");
        Ok(Self::new(channel, grpc_prefix))
    }

    /// 基于已建立的 Channel 创建 Client，适合调用方自行配置连接池或 TLS。
    pub fn new(channel: Channel, grpc_prefix: Option<&str>) -> Self {
        Self {
            inner: Grpc::new(channel),
            grpc_prefix: grpc_prefix.map(normalize_grpc_prefix),
        }
    }

    /// 调用未认证的健康检查 RPC。
    pub async fn health_check(
        &mut self,
        request: HealthRequest,
    ) -> Result<tonic::Response<HealthResponse>, TransportError> {
        debug!("sending Agent HealthCheck RPC");
        self.ready().await?;
        let result = self
            .inner
            .unary(
                tonic::Request::new(request),
                self.rpc_path("HealthCheck")?,
                tonic_prost::ProstCodec::<HealthRequest, HealthResponse>::default(),
            )
            .await
            .map_err(TransportError::Status);
        match &result {
            Ok(_) => trace!("Agent HealthCheck RPC completed"),
            Err(error) => warn!(error = %error, "Agent HealthCheck RPC failed"),
        }
        result
    }

    /// 打开一条 Agent 双向流；流内容由调用方决定是 Noise 握手还是测试帧。
    pub async fn open_session(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = ProtocolFrame>,
    ) -> Result<tonic::Response<tonic::Streaming<ProtocolFrame>>, TransportError> {
        debug!("opening Agent Noise gRPC session");
        self.ready().await?;
        let result = self
            .inner
            .streaming(
                request.into_streaming_request(),
                self.rpc_path("OpenSession")?,
                tonic_prost::ProstCodec::<ProtocolFrame, ProtocolFrame>::default(),
            )
            .await
            .map_err(TransportError::Status);
        match &result {
            Ok(_) => info!("Agent Noise gRPC session opened"),
            Err(error) => warn!(error = %error, "Agent Noise gRPC session failed to open"),
        }
        result
    }

    /// 等待 Channel 可用，并把 tower 层错误归类为可诊断的本地协议错误。
    async fn ready(&mut self) -> Result<(), TransportError> {
        self.inner.ready().await.map_err(|error| {
            warn!(error = %error, "Agent gRPC channel is not ready");
            TransportError::Protocol(format!("gRPC client was not ready: {error}"))
        })
    }

    /// 返回 service 与 method 对应的完整 HTTP/2 `:path`。
    fn rpc_path(&self, method: &str) -> Result<http::uri::PathAndQuery, TransportError> {
        let prefix = self.grpc_prefix.as_deref().unwrap_or_default();
        let path = format!("{prefix}/{AGENT_TRANSPORT_SERVICE}/{method}");
        path.parse().map_err(TransportError::InvalidUri)
    }
}

/// 统一去除末尾 `/`，确保 `rpc_path` 只产生一个路径分隔符。
fn normalize_grpc_prefix(prefix: &str) -> String {
    let prefix = prefix.trim();
    if prefix.is_empty() || prefix == "/" {
        String::new()
    } else {
        format!("/{}", prefix.trim_matches('/'))
    }
}

/// 生成不含查询参数、控制字符或超长输入的 gRPC 前缀日志标签。
fn grpc_prefix_log_label(prefix: Option<&str>) -> String {
    let Some(prefix) = prefix else {
        return "<none>".to_owned();
    };
    let normalized = normalize_grpc_prefix(prefix);
    if normalized.is_empty() {
        return "/".to_owned();
    }
    if normalized.len() > 256 || normalized.chars().any(char::is_control) {
        return "<invalid-grpc-prefix>".to_owned();
    }
    normalized
        .parse::<http::uri::PathAndQuery>()
        .map(|value| value.path().to_owned())
        .unwrap_or_else(|_| "<invalid-grpc-prefix>".to_owned())
}

/// 从标准 `token_id.psk` 凭据中提取公开 Token ID。
///
/// PSK 本身仍只会进入 Noise 握手和密文注册请求；这里仅取出首个分隔符之前的公开选择器。
fn registration_token_id_from_credential(token: &str) -> Result<String, TransportError> {
    token
        .split_once('.')
        .map(|(token_id, _)| token_id.to_owned())
        .ok_or_else(|| {
            TransportError::Protocol(
                "registration token must use the token_id.psk format".to_owned(),
            )
        })
}

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
        info!(
            registration_id = ?self.registration_id,
            "sending encrypted registration commit"
        );
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
                warn!(code = error.code, "Server rejected registration commit");
                let code =
                    SecureErrorCode::try_from(error.code).unwrap_or(SecureErrorCode::Unspecified);
                return Err(TransportError::RemoteSecure(code, error.message));
            }
            _ => {
                warn!("registration commit response had an unexpected message type");
                return Err(TransportError::Protocol(
                    "expected matching encrypted RegistrationCommitted".to_owned(),
                ));
            }
        }
        info!(agent_id = %self.agent_id, "Agent registration committed");
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

    /// 使用公开 Token ID 完成首次注册的便捷入口。
    ///
    /// 当调用方已经把 Token ID 与密钥分开保存，推荐使用此方法；如果 Token 采用标准
    /// `token_id.psk` 字符串，普通的 [`Self::register_agent`] 也会自动提取同一个 ID。
    pub async fn register_agent_with_token_id(
        &self,
        identity: NoiseIdentity,
        psk: &[u8],
        token_id: impl Into<String>,
        token: String,
        agent_name: String,
    ) -> Result<AgentRegistration, TransportError> {
        self.prepare_registration_with_token_id(identity, psk, token_id, token, agent_name)
            .await?
            .commit()
            .await
    }

    /// 首次注册的准备阶段：完成 XXpsk3、发送注册请求并等待 Server 的 pending 结果。
    ///
    /// 本方法不会发送最终 commit。调用方必须先持久化返回对象中的身份材料，再调用
    /// [`AgentPendingRegistration::commit`]。Token 必须采用标准的 `token_id.psk` 格式；如果
    /// 调用方已经把 ID 和密钥分开保存，请改用 [`Self::prepare_registration_with_token_id`]。
    pub async fn prepare_registration(
        &self,
        identity: NoiseIdentity,
        psk: &[u8],
        token: String,
        agent_name: String,
    ) -> Result<AgentPendingRegistration, TransportError> {
        let token_id = registration_token_id_from_credential(&token)?;
        self.prepare_registration_with_token_id(identity, psk, token_id, token, agent_name)
            .await
    }

    /// 使用公开 Token ID 选择 Server 侧独立 PSK，支持并发签发多条注册凭据。
    pub async fn prepare_registration_with_token_id(
        &self,
        identity: NoiseIdentity,
        psk: &[u8],
        token_id: impl Into<String>,
        token: String,
        agent_name: String,
    ) -> Result<AgentPendingRegistration, TransportError> {
        let agent_name_len = agent_name.len();
        let token_id = token_id.into();
        validate_registration_token_id(&token_id)?;
        let token_id_label = registration_token_id_log_label(&token_id);
        info!(
            token_id = %token_id_label,
            agent_name_len,
            "starting Agent XXpsk3 registration"
        );
        // 第一步只生成 XXpsk3 message 1；此时尚未信任任何 Server 静态公钥。
        let (waiting, mut first) = ClientXxHandshake::start(&identity, psk)?;
        first.registration_token_id = token_id;
        // 打开 gRPC 双向流，并把 message 1 作为首帧发送。
        let (sender, mut inbound) = self.open(first).await?;
        // 每个握手阶段单独受 handshake_timeout 约束，避免半开 RPC 永久占用资源。
        let second = next_handshake(&mut inbound, self.handshake_timeout).await?;
        // message 2 验证成功后学到 Server 公钥，并产生必须回传的 message 3。
        let (established, third) = waiting.receive_message2(second)?;
        debug!("Agent XXpsk3 handshake completed; sending message 3");
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
                warn!(
                    code = error.code,
                    "Server rejected encrypted registration request"
                );
                let code =
                    SecureErrorCode::try_from(error.code).unwrap_or(SecureErrorCode::Unspecified);
                return Err(TransportError::RemoteSecure(code, error.message));
            }
            _ => {
                warn!("registration preparation response had an unexpected message type");
                return Err(TransportError::Protocol(
                    "expected encrypted RegistrationPrepared".to_owned(),
                ));
            }
        };
        let registration_id: [u8; 16] = response.registration_id.try_into().map_err(|_| {
            TransportError::Protocol("registration ID must contain exactly 16 bytes".to_owned())
        })?;
        info!(
            agent_id = %response.agent_id,
            registration_id = ?registration_id,
            "Agent registration prepared; local persistence is required before commit"
        );
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
        info!(server_key_id = ?server_key.key_id(), "starting Agent IK connection");
        // IK message 1 已包含对 Agent 静态身份的密码学证明，并指定目标 Server key ID。
        let (waiting, first) = ClientIkHandshake::start(identity, server_key)?;
        let (sender, mut inbound) = self.open(first).await?;
        // Server message 2 完成双向静态身份认证，随后直接进入 transport mode。
        let second = next_handshake(&mut inbound, self.handshake_timeout).await?;
        let established = waiting.receive_message2(second)?;
        info!(server_key_id = ?server_key.key_id(), "Agent IK connection authenticated");
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
            debug!(server_key_id = ?key.key_id(), "trying Agent IK key candidate");
            match self.connect(identity, *key).await {
                Ok(session) => return Ok(session),
                Err(error) => {
                    warn!(server_key_id = ?key.key_id(), error = %error, "Agent IK key candidate failed");
                    last = Some(error);
                }
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
        let endpoint_label = endpoint_log_label(&self.endpoint);
        trace!(
            endpoint = %endpoint_label,
            "creating Agent Noise gRPC request stream"
        );
        // endpoint scheme 决定使用 HTTPS/TLS 还是本地 h2c。
        // 有界 channel 把应用发送速度反压到 gRPC request stream。
        let (sender, receiver) = mpsc::channel(16);
        sender
            .send(handshake_frame(first))
            .await
            .map_err(|_| TransportError::Closed)?;
        // 由路径感知 Client 显式保留 Axum nest 或反向代理前缀。
        let mut client =
            AgentTransportRpcClient::connect(&self.endpoint, self.grpc_prefix.as_deref()).await?;
        let response = timeout(
            self.handshake_timeout,
            client.open_session(ReceiverStream::new(receiver)),
        )
        .await
        .map_err(|_| TransportError::Timeout("opening gRPC session"))??;
        debug!("Agent Noise gRPC request stream is ready for handshake responses");
        Ok((sender, response.into_inner()))
    }
}

/// 根据 endpoint scheme 创建 Tonic channel。
async fn connect_channel(endpoint: &str) -> Result<Channel, TransportError> {
    let endpoint_label = endpoint_log_label(endpoint);
    debug!(
        endpoint = %endpoint_label,
        tls = endpoint.starts_with("https://"),
        "building Agent gRPC channel"
    );
    // Endpoint 解析失败属于不可重试配置错误。
    let builder = Endpoint::from_shared(endpoint.to_owned())?;
    let builder = if endpoint.starts_with("https://") {
        builder.tls_config(ClientTlsConfig::new().with_native_roots())?
    } else {
        builder
    };
    Ok(builder.connect().await?)
}

/// 生成用于日志的 endpoint 摘要，只保留 scheme、host 和 port。
///
/// Endpoint 可能来自外部配置，不能把完整 URI（尤其是 userinfo、path 或 query）写入日志。
fn endpoint_log_label(endpoint: &str) -> String {
    let Ok(uri) = endpoint.parse::<http::Uri>() else {
        return "<invalid-endpoint>".to_owned();
    };
    let scheme = uri.scheme_str().unwrap_or("<unknown-scheme>");
    let authority = uri
        .authority()
        .map(|value| value.as_str())
        .unwrap_or("<missing-authority>");
    // URI authority 理论上不应包含 userinfo；若调用方传入，则只保留 @ 后的 host:port。
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    format!("{scheme}://{authority}")
}

/// 在给定上限内读取下一条握手响应，并识别远端外层协议错误。
async fn next_handshake(
    inbound: &mut tonic::Streaming<ProtocolFrame>,
    limit: Duration,
) -> Result<crate::agent::v1::NoiseHandshake, TransportError> {
    let frame = timeout(limit, inbound.message())
        .await
        .map_err(|_| {
            warn!(?limit, "timed out waiting for Server handshake frame");
            TransportError::Timeout("waiting for handshake frame")
        })??
        .ok_or_else(|| {
            warn!("Server closed the gRPC stream while sending a handshake frame");
            TransportError::Closed
        })?;
    match frame.body {
        Some(protocol_frame::Body::Handshake(handshake)) => {
            trace!(
                payload_len = handshake.payload.len(),
                "received Server handshake frame"
            );
            Ok(handshake)
        }
        Some(protocol_frame::Body::ProtocolError(error)) => {
            warn!(code = error.code, "Server returned an outer protocol error");
            Err(TransportError::RemoteProtocol(error.message))
        }
        _ => {
            warn!("received a non-handshake frame during Client handshake");
            Err(TransportError::Protocol(
                "expected handshake frame".to_owned(),
            ))
        }
    }
}

/// 把 Noise 核心产生的握手消息包装为正式外层帧。
fn handshake_frame(handshake: crate::agent::v1::NoiseHandshake) -> ProtocolFrame {
    ProtocolFrame {
        body: Some(protocol_frame::Body::Handshake(handshake)),
    }
}

#[cfg(test)]
mod tests {
    use tonic::transport::Endpoint;

    use super::{
        AgentTransportRpcClient, endpoint_log_label, grpc_prefix_log_label,
        registration_token_id_from_credential,
    };

    #[tokio::test]
    async fn rpc_client_preserves_configured_prefix_in_method_path() {
        let channel = Endpoint::from_static("http://127.0.0.1:12345").connect_lazy();
        let client = AgentTransportRpcClient::new(channel, Some("/api/v1/grpc/"));

        assert_eq!(
            client.rpc_path("OpenSession").unwrap().as_str(),
            "/api/v1/grpc/smalux.agent.v1.AgentTransport/OpenSession"
        );
    }

    #[tokio::test]
    async fn rpc_client_uses_generated_grpc_path_without_prefix() {
        let channel = Endpoint::from_static("http://127.0.0.1:12345").connect_lazy();
        let client = AgentTransportRpcClient::new(channel, None);

        assert_eq!(
            client.rpc_path("HealthCheck").unwrap().as_str(),
            "/smalux.agent.v1.AgentTransport/HealthCheck"
        );
    }

    #[test]
    fn endpoint_log_label_redacts_path_query_and_userinfo() {
        assert_eq!(
            endpoint_log_label("https://user:secret@example.com:8443/api?token=hidden"),
            "https://example.com:8443"
        );
        assert_eq!(endpoint_log_label("not a uri"), "<invalid-endpoint>");
    }

    #[test]
    fn grpc_prefix_log_label_omits_query_and_invalid_text() {
        assert_eq!(grpc_prefix_log_label(None), "<none>");
        assert_eq!(
            grpc_prefix_log_label(Some("/api/v1/grpc/?token=hidden")),
            "/api/v1/grpc/"
        );
        assert_eq!(
            grpc_prefix_log_label(Some("bad\nforged-log-line")),
            "<invalid-grpc-prefix>"
        );
    }

    #[test]
    fn registration_token_id_is_extracted_without_logging_the_secret() {
        assert_eq!(
            registration_token_id_from_credential("agent-token.0123456789abcdef").unwrap(),
            "agent-token"
        );
        assert!(registration_token_id_from_credential("missing-separator").is_err());
    }
}
