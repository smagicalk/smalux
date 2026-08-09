//! 前端/普通 HTTP 路由。
//!
//! 前端路由使用 `FrontendState`，但通过 `FromRef<AppState>` 共享公共数据库连接池。
//! Agent gRPC 路由不会把前端状态带入服务，两个领域的状态边界保持独立。

use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};

use crate::state::{AppState, FrontendState};

/// 普通 HTTP 健康检查路径；不涉及 Agent Noise 会话。
pub(crate) const HEALTH_PATH: &str = "/api/v1/health";

/// 装配前端/普通 HTTP 路由。
pub(crate) fn get_route() -> Router<AppState> {
    tracing::debug!(path = HEALTH_PATH, "mounting frontend HTTP routes");
    Router::new().route(HEALTH_PATH, get(health))
}

/// 返回当前 Server UTC 日期；HTTP 200 本身表示路由和进程可以响应。
async fn health(State(state): State<Arc<FrontendState>>) -> Json<serde_json::Value> {
    tracing::trace!(
        database_backend = state.common.database.backend_label(),
        "HTTP health check requested"
    );
    Json(serde_json::json!({
        "date": time::OffsetDateTime::now_utc().date().to_string(),
    }))
}
