//! Tonic `AgentTransport` 的 Server 适配层。
//!
//! RPC handler 只建立双向流、容量 permit 和 Session 目录记录；Noise 握手、注册、IK 授权
//! 与业务循环位于 `session` 子模块。这样 Tonic 接口层不会拥有完整协议状态机。

use crate::service::agent::{agent_registry::PrepareRegistrationError, state::AgentState};
use smalux_protocol::agent::v1::agent_transport_server::AgentTransport;
use smalux_protocol::agent::v1::{HealthRequest, HealthResponse, ProtocolFrame, SecureErrorCode};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::Stream;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

mod session;

/// Tonic 要求的服务端响应流类型；实际消息由 Session worker 写入有界 channel。
type ResponseStream = Pin<Box<dyn Stream<Item = Result<ProtocolFrame, Status>> + Send + 'static>>;

#[derive(Clone)]
/// Agent gRPC 服务实现；克隆只会克隆共享状态和 Session ID 计数器。
pub struct AgentTransportService {
    /// Agent 领域共享状态，包含数据库、密钥环和注册中心。
    state: Arc<AgentState>,
    /// 仅在当前进程内递增的诊断 ID，不作为持久化身份或安全凭据。
    next_session_id: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl AgentTransport for AgentTransportService {
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
        // permit 由 worker 持有到整个流结束，因此限制的是并发连接数，不是历史 Agent 数。
        let session_permit = self.state.try_acquire_session().map_err(|_| {
            tracing::warn!("Agent gRPC session limit reached");
            Status::resource_exhausted("Agent session capacity is exhausted")
        })?;
        // 有界响应队列向慢客户端施加背压，避免 Session worker 无界累积帧。
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

        let session_cancellation = self.state.sessions.register(session_id).await;
        let sessions = self.state.sessions.clone();
        let service = AgentTransportService::clone(self);

        // RPC handler 只负责建立 Tonic 的双向流边界；握手、授权和业务生命周期由
        // 独立 worker 方法拥有，避免在 trait adapter 中内嵌完整状态机。
        tokio::spawn(async move {
            service
                .run_session_worker(
                    session_id,
                    inbound,
                    sender,
                    keyring,
                    session_permit,
                    session_cancellation,
                )
                .await;
            sessions.remove(session_id).await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

impl AgentTransportService {
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
fn map_registration_prepare_error(
    error: &PrepareRegistrationError,
) -> (SecureErrorCode, &'static str) {
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
        PrepareRegistrationError::Database(_) | PrepareRegistrationError::Internal(_) => (
            SecureErrorCode::Internal,
            "registration service is unavailable",
        ),
    }
}

#[cfg(test)]
mod tests {
    use crate::database::DatabaseError;

    use super::{PrepareRegistrationError, SecureErrorCode, map_registration_prepare_error};

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
                PrepareRegistrationError::Database(DatabaseError::InvalidAgentRegistration(
                    "sensitive database detail".to_owned(),
                )),
                SecureErrorCode::Internal,
            ),
        ];

        for (error, expected_code) in cases {
            let (actual_code, message) = map_registration_prepare_error(&error);
            assert_eq!(actual_code, expected_code);
            assert!(!message.contains("sensitive database detail"));
        }
    }
}
