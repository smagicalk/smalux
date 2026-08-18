use crate::service::agent::{agent_registrar::PrepareRegistrationError, state::AgentState};
use smalux_protocol::agent::v1::agent_transport_server::AgentTransport;
use smalux_protocol::agent::v1::{
    HealthRequest, HealthResponse, ProtocolFrame, SecureErrorCode, protocol_frame,
};
use smalux_protocol::tonic_transport::{ServerSessionAcceptor, TransportError};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::Stream;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

mod session;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<ProtocolFrame, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct AgentServer {
    /// Agent 领域共享状态，包含数据库、密钥环和注册中心。
    state: Arc<AgentState>,
    next_session_id: Arc<AtomicU64>,
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
            database_backend = self.state.database_backend,
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
        Self {
            state,
            next_session_id: Arc::new(Default::default()),
        }
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
    use crate::database::DatabaseError;

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
                PrepareRegistrationError::Database(DatabaseError::InvalidAgentRegistration(
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
