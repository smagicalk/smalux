//! server 共享状态模块，负责定义 axum handler 和后台服务共享的 AppState。

use sea_orm::DatabaseConnection;

use crate::config::model::FrontendConfig;

/// HTTP handler 和后台服务共享的最小状态。
///
/// 当前保存数据库连接和前端托管配置；后续再逐步加入 repository、
/// connection registry、event publisher 和 session/auth 相关状态。
#[derive(Clone)]
pub struct AppState {
    /// SeaORM 数据库连接，启动时初始化并在整个进程内共享。
    pub database: DatabaseConnection,
    /// 前端托管配置，router 使用它决定是否挂载静态资源 fallback。
    pub frontend: FrontendConfig,

    pub registry:
        std::sync::Arc<crate::http::agent::ws::connection_registry::AgentConnectionRegistry>,
}

impl AppState {
    /// 创建最小共享状态。
    pub fn new(database: DatabaseConnection, frontend: FrontendConfig) -> Self {
        Self {
            database,
            frontend,
            registry: crate::http::agent::ws::connection_registry::AgentConnectionRegistry::new(),
        }
    }
}
