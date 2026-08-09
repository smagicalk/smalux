//! Server 应用级状态组装。
//!
//! `CommonState` 保存所有路由共享的基础设施；`FrontendState` 和 `AgentState` 分别
//! 保存领域专属依赖。Axum 最终只注入一个 `AppState`，各 handler 通过 `FromRef`
//! 提取自己需要的子状态。gRPC service 不使用 Axum extractor，而是直接持有
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

/// 所有领域共享的基础设施状态。
#[derive(Clone)]
pub(crate) struct CommonState {
    /// SeaORM 数据库连接池；整个 Server 只建立一次。
    pub(crate) database: Arc<ServerDatabase>,
    /// 已脱敏的运行态配置；不包含数据库 URL、用户名或密码。
    pub(crate) config: Arc<RuntimeConfig>,
}

/// 前端和普通 HTTP 路由的专属状态。
#[derive(Clone)]
pub(crate) struct FrontendState {
    /// 前端需要数据库时从这里取得公共连接池。
    pub(crate) common: Arc<CommonState>,
}

/// Axum 顶层共享状态。
#[derive(Clone)]
pub(crate) struct AppState {
    /// 公共数据库和配置。
    pub(crate) common: Arc<CommonState>,
    /// 前端子状态。
    pub(crate) frontend: Arc<FrontendState>,
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
        let common = Arc::new(CommonState {
            database: Arc::new(database),
            config: Arc::new(runtime_config),
        });
        let shutdown = CancellationToken::new();
        let shutdown_owner = Arc::new(ShutdownOwner(shutdown.clone()));

        // 管理器负责原子初始化、revision CAS 和跨进程轮询；Agent 会话只复制它维护
        // 的当前 `Arc<ServerKeyRing>`，不会直接操作数据库或替换共享状态。
        let keyring_manager =
            Arc::new(ServerKeyRingManager::load_or_create(Arc::clone(&common.database)).await?);
        // 用 Weak 引用启动后台同步；任务同时监听进程级取消令牌，关闭时会立即退出。
        keyring_manager
            .start_sync_task_with_shutdown(DEFAULT_SERVER_KEYRING_SYNC_INTERVAL, shutdown.clone());
        let agent = Arc::new(AgentState::new(
            Arc::clone(&common.database),
            keyring_manager,
            &common.config,
            shutdown.clone(),
        ));
        agent.agent_registrar.start_cleanup_task(shutdown.clone());
        let frontend = Arc::new(FrontendState {
            common: Arc::clone(&common),
        });

        tracing::debug!(
            agent_prefix = DEFAULT_AGENT_PRIFIX,
            address = %common.config.address,
            port = common.config.port,
            "Server application state assembled"
        );
        Ok(Self {
            common,
            frontend,
            agent,
            shutdown,
            _shutdown_owner: shutdown_owner,
        })
    }
}

/// 从根状态提取前端子状态。
impl FromRef<AppState> for Arc<FrontendState> {
    fn from_ref(state: &AppState) -> Self {
        Arc::clone(&state.frontend)
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
        Arc::clone(&state.common.database)
    }
}
