//! Server 应用级状态组装。
//!
//! `AppState` 保存公共数据库和 Agent 领域状态。Axum handler 通过 `FromRef` 提取
//! 自己需要的数据库句柄；gRPC service 不使用 Axum extractor，而是直接持有
//! `Arc<AgentState>`。

use std::sync::Arc;

use crate::config::RuntimeConfig;
use crate::database::ServerDatabase;
use crate::service::agent::keyring_manager::{
    DEFAULT_SERVER_KEYRING_SYNC_INTERVAL, ServerKeyRingManager,
};
use crate::service::agent::state::AgentState;
use axum::extract::FromRef;
use smalux_core::config::default::DEFAULT_AGENT_PRIFIX;
use tokio_util::sync::CancellationToken;

/// Axum 顶层共享状态。
#[derive(Clone)]
pub(crate) struct AppState {
    /// SeaORM 数据库连接池；整个 Server 只建立一次。
    pub(crate) database: Arc<ServerDatabase>,
    /// Agent 子状态；gRPC service 也持有同一个 Arc。
    pub(crate) agent: Arc<AgentState>,
    /// 进程级关闭通知，长期 Session 和后台任务共享同一个取消源。
    pub(crate) shutdown: CancellationToken,
    /// 只有最后一个 AppState 所有者释放时才取消后台任务，避免普通 Clone 提前关闭。
    _shutdown_owner: Arc<ShutdownOwner>,
}

/// AppState 生命周期结束时触发后台任务取消。
struct ShutdownOwner(CancellationToken);

impl Drop for ShutdownOwner {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl AppState {
    /// 连接完成并执行迁移后，创建所有领域共享状态。
    pub(crate) async fn build(
        runtime_config: RuntimeConfig,
        database: ServerDatabase,
    ) -> anyhow::Result<Self> {
        let database = Arc::new(database);
        let shutdown = CancellationToken::new();
        let shutdown_owner = Arc::new(ShutdownOwner(shutdown.clone()));

        // 管理器负责原子初始化、revision CAS 和跨进程轮询；Agent 会话只复制它维护
        // 的当前 `Arc<ServerKeyRing>`，不会直接操作数据库或替换共享状态。
        let keyring_manager =
            Arc::new(ServerKeyRingManager::load_or_create(Arc::clone(&database)).await?);
        // 用 Weak 引用启动后台同步；任务同时监听进程级取消令牌，关闭时会立即退出。
        keyring_manager
            .start_sync_task_with_shutdown(DEFAULT_SERVER_KEYRING_SYNC_INTERVAL, shutdown.clone());
        let agent = Arc::new(AgentState::new(
            Arc::clone(&database),
            keyring_manager,
            &runtime_config,
            shutdown.clone(),
        ));
        agent.agent_registrar.start_cleanup_task(shutdown.clone());

        tracing::debug!(
            agent_prefix = DEFAULT_AGENT_PRIFIX,
            address = %runtime_config.address,
            port = runtime_config.port,
            "Server application state assembled"
        );
        Ok(Self {
            database,
            agent,
            shutdown,
            _shutdown_owner: shutdown_owner,
        })
    }
}

/// 从根状态提取 Agent 子状态。
impl FromRef<AppState> for Arc<AgentState> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.agent)
    }
}

/// 需要直接执行公共查询的 handler 可以提取共享数据库句柄。
impl FromRef<AppState> for Arc<ServerDatabase> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.database)
    }
}
