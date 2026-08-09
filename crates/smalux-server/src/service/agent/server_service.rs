use crate::service::agent::{agent_registrar::PrepareRegistrationError, state::AgentState};
use smalux_protocol::agent::v1::agent_transport_server::AgentTransport;
use smalux_protocol::agent::v1::{
    EchoResponse, HealthRequest, HealthResponse, Messages, MessagesResponse, ProtocolFrame,
    SecureError, SecureErrorCode, SecureMessage, messages, messages_request, messages_response,
    protocol_frame, secure_message,
};
use smalux_protocol::tonic_transport::{
    IncomingSession, ServerSessionAcceptor, TonicNoiseSession, TransportError,
};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::Stream;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ProtocolFrame, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct AgentServer {
    /// Agent 领域共享状态，包含数据库、密钥环和注册中心。
    pub(crate) state: Arc<AgentState>,
    pub(crate) next_session_id: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl AgentTransport for AgentServer {
    async fn health_check(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        tracing::trace!("Agent health check requested");
        Ok(Response::new(HealthResponse {
            message: "ok".to_string(),
            code: 200,
        }))
    }

    type OpenSessionStream = ResponseStream;

    async fn open_session(
        &self,
        request: Request<Streaming<ProtocolFrame>>,
    ) -> Result<Response<Self::OpenSessionStream>, Status> {
        let session_permit = self.state.try_acquire_session().map_err(|_| {
            tracing::warn!("Agent gRPC session limit reached");
            Status::resource_exhausted("Agent session capacity is exhausted")
        })?;
        let (sender, receiver) = mpsc::channel::<Result<ProtocolFrame, Status>>(8);
        let inbound = request.into_inner();
        let session_id = self
            .next_session_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let keyring = self
            .state
            .keyring_manager
            .current_keyring()
            .map_err(|error| {
                tracing::error!(session_id, error = %error, "failed to read Server Noise keyring");
                Status::internal("Server Noise keyring is unavailable")
            })?;
        tracing::info!(
            session_id,
            active_server_keys = keyring.active_keys().len(),
            database_backend = self.state.database.backend_label(),
            "Agent gRPC session accepted"
        );

        let service = self.clone();
        let registrar = Arc::clone(&self.state.agent_registrar);
        let shutdown = self.state.shutdown.clone();
        tokio::spawn(async move {
            let _session_permit = session_permit;
            tracing::debug!(session_id, "Agent gRPC session worker started");
            // ServerSessionAcceptor 负责读取首帧、完成 XXpsk3/IK Noise responder 握手，
            // 并把注册 Token ID 交给注册中心解析 PSK。PSK 本身绝不写入日志。
            let acceptor = ServerSessionAcceptor::default();
            let incoming = tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(session_id, "Agent gRPC session cancelled during handshake");
                    return;
                }
                incoming = acceptor
                    .accept_incoming_with_psk_resolver(
                        inbound,
                        sender.clone(),
                        keyring.as_ref(),
                        move |token_id| async move {
                            registrar
                                .resolve_registration_psk(&token_id)
                                .await
                                .map_err(|error| TransportError::Protocol(error.to_string()))
                        },
                    ) => incoming,
            };

            // Noise 尚未建立时只能返回粗粒度 ProtocolError；不能把 Token、PSK 或 snow
            // 的内部错误直接回显给 Client。握手成功后则只发送加密 SecureError。
            let incoming = match incoming {
                Ok(incoming) => incoming,
                Err(error) => {
                    tracing::warn!(
                        session_id,
                        error = %error,
                        "Agent Noise handshake failed"
                    );
                    if sender
                        .send(Ok(ProtocolFrame {
                            body: Some(protocol_frame::Body::ProtocolError(error.protocol_error())),
                        }))
                        .await
                        .is_err()
                    {
                        tracing::debug!(
                            session_id,
                            "Agent client already closed; handshake error could not be delivered"
                        );
                    }
                    return;
                }
            };

            let result = tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!(session_id, "Agent gRPC session cancelled");
                    return;
                }
                result = service.handle_established_session(session_id, incoming) => result,
            };
            match result {
                Ok(()) => tracing::info!(session_id, "Agent gRPC session completed"),
                Err(error) => tracing::warn!(
                    session_id,
                    error = %error,
                    "Agent encrypted session aborted"
                ),
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

impl AgentServer {
    /// 使用启动阶段创建的 Agent 领域状态创建 gRPC 服务。
    pub fn new(state: Arc<AgentState>) -> Self {
        let keyring = state.keyring_manager.current_keyring();
        let current_key_id = keyring.as_ref().ok().and_then(|keyring| {
            keyring
                .active_keys()
                .first()
                .map(|identity| identity.key_id())
        });
        let active_keys = keyring
            .as_ref()
            .map(|keyring| keyring.active_keys().len())
            .unwrap_or_default();
        tracing::info!(
            key_id = ?current_key_id,
            active_keys,
            database_backend = state.database.backend_label(),
            "creating Agent server service"
        );
        Self {
            state,
            next_session_id: Arc::new(Default::default()),
        }
    }

    /// 处理已经完成 Noise 握手的会话。
    ///
    /// XXpsk3 必须先完成四阶段注册状态机；IK 必须先检查吊销状态和业务授权。只有
    /// 这些检查成功后，才把会话交给加密业务循环。注册资料错误会映射为对应的
    /// 加密协议错误；数据库故障只返回 `Internal`，不会泄露存储细节。
    async fn handle_established_session(
        &self,
        session_id: u64,
        incoming: IncomingSession,
    ) -> Result<(), TransportError> {
        let (agent_id, mut session) = match incoming {
            IncomingSession::Registration(mut registration) => {
                let registration_permit = match self.state.try_acquire_registration() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!(session_id, "Agent registration capacity reached");
                        registration
                            .reject(SecureError {
                                code: SecureErrorCode::ResourceExhausted as i32,
                                message: "registration capacity is temporarily exhausted"
                                    .to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };
                let _registration_permit = registration_permit;
                tracing::info!(
                    session_id,
                    "Agent XXpsk3 handshake completed; entering registration"
                );
                let peer_public_key = registration.peer_public_key();
                let request = match registration.receive_request().await {
                    Ok(request) => request,
                    Err(error) => {
                        tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent registration request is invalid"
                        );
                        registration
                            .reject(SecureError {
                                code: SecureErrorCode::InvalidMessage as i32,
                                message: "registration request is invalid".to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };

                let pending = match self
                    .state
                    .agent_registrar
                    .prepare_registration(
                        registration.registration_token_id(),
                        &request.token,
                        peer_public_key,
                        &request.agent_name,
                    )
                    .await
                {
                    Ok(pending) => pending,
                    Err(error) => {
                        let (code, message) = registration_prepare_error(&error);
                        tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent registration preparation was rejected"
                        );
                        registration
                            .reject(SecureError {
                                code: code as i32,
                                message: message.to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };

                tracing::debug!(
                    session_id,
                    registration_id = ?pending.registration_id,
                    agent_id_len = pending.agent_id.len(),
                    "sending encrypted registration preparation"
                );
                registration
                    .prepare(pending.registration_id, pending.agent_id.clone())
                    .await?;
                if let Err(error) = registration
                    .wait_for_commit(pending.registration_id, Duration::from_secs(10))
                    .await
                {
                    tracing::warn!(
                        session_id,
                        error = %error,
                        "Agent did not complete registration commit"
                    );
                    registration
                        .reject(SecureError {
                            code: SecureErrorCode::InvalidMessage as i32,
                            message: "registration commit was not completed".to_owned(),
                        })
                        .await?;
                    return Ok(());
                }

                if let Err(error) = self
                    .state
                    .agent_registrar
                    .commit_registration(&pending)
                    .await
                {
                    tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent registration commit failed"
                    );
                    registration
                        .reject(SecureError {
                            code: SecureErrorCode::Internal as i32,
                            message: "registration service is unavailable".to_owned(),
                        })
                        .await?;
                    return Ok(());
                }

                let session = registration.complete(pending.registration_id).await?;
                tracing::info!(
                    session_id,
                    agent_id_len = pending.agent_id.len(),
                    "Agent registration committed"
                );
                (pending.agent_id, session)
            }
            IncomingSession::Authentication(authentication) => {
                tracing::info!(
                    session_id,
                    "Agent IK handshake completed; entering authorization"
                );
                let peer_public_key = authentication.peer_public_key();
                let revoked = match self.state.agent_registrar.is_revoked(peer_public_key).await {
                    Ok(revoked) => revoked,
                    Err(error) => {
                        tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent revocation lookup failed"
                        );
                        authentication
                            .reject(SecureError {
                                code: SecureErrorCode::Internal as i32,
                                message: "authorization service is unavailable".to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };
                if revoked {
                    tracing::warn!(session_id, "Agent is revoked");
                    authentication
                        .reject(SecureError {
                            code: SecureErrorCode::AgentNotAuthorized as i32,
                            message: "agent is not authorized".to_owned(),
                        })
                        .await?;
                    return Ok(());
                }

                let authorized = match self
                    .state
                    .agent_registrar
                    .authorize_agent(peer_public_key)
                    .await
                {
                    Ok(authorized) => authorized,
                    Err(error) => {
                        tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent authorization was rejected"
                        );
                        authentication
                            .reject(SecureError {
                                code: SecureErrorCode::AgentNotAuthorized as i32,
                                message: "agent is not authorized".to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };
                tracing::info!(
                    session_id,
                    agent_key_id = ?authorized.public_key.key_id(),
                    agent_id_len = authorized.agent_id.len(),
                    "Agent authorization succeeded"
                );
                (authorized.agent_id, authentication.authorize())
            }
        };

        self.handle_business_messages(session_id, &agent_id, &mut session)
            .await
    }

    /// 处理 Noise transport mode 内的业务消息。
    ///
    /// `TonicNoiseSession::receive` 会自动处理心跳和对称密钥 rekey；这里仅负责业务
    /// `MessagesRequest` 的回显。未来接入 Job/Task 时，应在这个边界分派到对应服务。
    async fn handle_business_messages(
        &self,
        session_id: u64,
        agent_id: &str,
        session: &mut TonicNoiseSession,
    ) -> Result<(), TransportError> {
        while let Some(message) = session.receive().await? {
            let Some(secure_message::Body::Messages(Messages {
                body: Some(messages::Body::Request(request)),
            })) = message.body
            else {
                tracing::warn!(
                    session_id,
                    agent_id_len = agent_id.len(),
                    "Agent sent an unexpected encrypted business message"
                );
                session
                    .send(SecureMessage {
                        body: Some(secure_message::Body::Error(SecureError {
                            code: SecureErrorCode::InvalidMessage as i32,
                            message: "business session requires MessagesRequest".to_owned(),
                        })),
                    })
                    .await?;
                continue;
            };

            tracing::debug!(
                session_id,
                agent_id_len = agent_id.len(),
                sequence = request.sequence,
                "received encrypted Agent business request"
            );
            let response_payload = request.payload.map(|payload| match payload {
                messages_request::Payload::BytesPayload(value) => {
                    messages_response::Payload::BytesPayload(value)
                }
                messages_request::Payload::StringPayload(value) => {
                    messages_response::Payload::StringPayload(value)
                }
                messages_request::Payload::EchoRequest(value) => {
                    messages_response::Payload::EchoResponse(EchoResponse {
                        payload: value.payload,
                    })
                }
            });
            session
                .send(SecureMessage {
                    body: Some(secure_message::Body::Messages(Messages {
                        body: Some(messages::Body::Response(MessagesResponse {
                            acknowledged_sequence: request.sequence,
                            payload: response_payload,
                        })),
                    })),
                })
                .await?;
            tracing::trace!(
                session_id,
                sequence = request.sequence,
                "sent encrypted Agent business response"
            );
        }
        tracing::info!(session_id, "Agent encrypted session disconnected");
        Ok(())
    }
}

/// 把注册中心的结构化拒绝原因转换为对端可见的安全错误。
///
/// `Database` 和 `Internal` 的具体原因只进入 Server 日志，返回文本保持固定。
fn registration_prepare_error(error: &PrepareRegistrationError) -> (SecureErrorCode, &'static str) {
    match error {
        PrepareRegistrationError::InvalidToken => (
            SecureErrorCode::InvalidToken,
            "registration token is invalid",
        ),
        PrepareRegistrationError::TokenAlreadyUsed => (
            SecureErrorCode::TokenAlreadyUsed,
            "registration token is already used",
        ),
        PrepareRegistrationError::AgentAlreadyRegistered => (
            SecureErrorCode::AgentAlreadyRegistered,
            "Agent identity is already registered",
        ),
        PrepareRegistrationError::InvalidAgentName => {
            (SecureErrorCode::InvalidMessage, "Agent name is invalid")
        }
        PrepareRegistrationError::Database(_) | PrepareRegistrationError::Internal(_) => (
            SecureErrorCode::Internal,
            "registration service is unavailable",
        ),
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::DbErr;

    use super::{PrepareRegistrationError, SecureErrorCode, registration_prepare_error};

    #[test]
    fn prepare_failures_map_to_safe_protocol_errors() {
        let cases = [
            (
                PrepareRegistrationError::InvalidToken,
                SecureErrorCode::InvalidToken,
            ),
            (
                PrepareRegistrationError::TokenAlreadyUsed,
                SecureErrorCode::TokenAlreadyUsed,
            ),
            (
                PrepareRegistrationError::AgentAlreadyRegistered,
                SecureErrorCode::AgentAlreadyRegistered,
            ),
            (
                PrepareRegistrationError::InvalidAgentName,
                SecureErrorCode::InvalidMessage,
            ),
            (
                PrepareRegistrationError::Database(DbErr::Custom(
                    "sensitive database detail".to_owned(),
                )),
                SecureErrorCode::Internal,
            ),
        ];

        for (error, expected_code) in cases {
            let (actual_code, message) = registration_prepare_error(&error);
            assert_eq!(actual_code, expected_code);
            assert!(!message.contains("sensitive database detail"));
        }
    }
}
