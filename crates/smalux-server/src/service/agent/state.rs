//! Agent 领域共享状态。
//!
//! 该状态与 Axum 的顶层 `AppState` 解耦：gRPC service 没有 Axum `State` extractor，
//! 因此由 `AgentServer` 直接持有这个状态。Noise 密钥环和注册中心在
//! Server 启动时创建一次，所有 Agent 会话共享它们。

use std::sync::Arc;

use crate::config::RuntimeConfig;
use crate::database::ServerDatabase;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::agent_registrar::AgentRegistrar;
use super::keyring_manager::ServerKeyRingManager;

/// Agent 协议服务使用的共享依赖。
#[derive(Clone)]
pub struct AgentState {
    /// 脱敏数据库后端标签，仅用于会话诊断日志。
    pub(crate) database_backend: &'static str,
    /// Server Noise 密钥环管理器；所有握手和轮换都通过它取得一致句柄。
    pub(crate) keyring_manager: Arc<ServerKeyRingManager>,
    /// Agent 注册中心；负责 Token、注册事务、Agent 激活、授权和吊销查询。
    pub(crate) agent_registrar: Arc<AgentRegistrar>,
    /// 限制所有 Agent gRPC 流同时占用的服务资源。
    pub(crate) session_slots: Arc<Semaphore>,
    /// 限制同时执行注册业务提交的会话数量。
    pub(crate) registration_slots: Arc<Semaphore>,
    /// Tonic 单条 protobuf 消息的大小上限。
    pub(crate) max_grpc_message_bytes: usize,
    /// Server 关闭时通知握手、注册和业务循环退出。
    pub(crate) shutdown: CancellationToken,
}

impl AgentState {
    /// 使用共享数据库和密钥环管理器创建 Agent 状态。
    pub(crate) fn new(
        database: Arc<ServerDatabase>,
        keyring_manager: Arc<ServerKeyRingManager>,
        runtime_config: &RuntimeConfig,
        shutdown: CancellationToken,
    ) -> Self {
        tracing::info!(
            backend = database.backend_label(),
            "creating Agent shared state"
        );
        let database_backend = database.backend_label();
        let agent_registrar = Arc::new(AgentRegistrar::new(Arc::clone(&database)));
        Self {
            database_backend,
            keyring_manager,
            agent_registrar,
            session_slots: Arc::new(Semaphore::new(runtime_config.max_agent_sessions)),
            registration_slots: Arc::new(Semaphore::new(runtime_config.max_registration_sessions)),
            max_grpc_message_bytes: runtime_config.max_grpc_message_bytes,
            shutdown,
        }
    }

    /// 尝试占用一个 Agent 会话槽位；槽位随 permit 生命周期自动释放。
    pub(crate) fn try_acquire_session(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        Arc::clone(&self.session_slots).try_acquire_owned()
    }

    /// 尝试占用一个注册业务槽位；握手完成后才进入此限制。
    pub(crate) fn try_acquire_registration(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        Arc::clone(&self.registration_slots).try_acquire_owned()
    }
}
