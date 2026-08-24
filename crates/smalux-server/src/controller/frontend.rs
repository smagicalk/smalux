//! 前端/普通 HTTP 路由。
//!
//! 前端 handler 通过 `FromRef<AppState>` 提取公共数据库连接池。Agent gRPC 路由
//! 直接持有 Agent 领域状态，不会把完整的 Axum 状态带入长期会话。

use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};

use crate::{database::ServerDatabase, state::AppState};

/// 普通 HTTP 健康检查路径；不涉及 Agent Noise 会话。
pub(crate) const HEALTH_PATH: &str = "/api/v1/health";

/// 装配前端/普通 HTTP 路由。
pub(crate) fn router() -> Router<AppState> {
    tracing::debug!(path = HEALTH_PATH, "mounting frontend HTTP routes");
    Router::new().route(HEALTH_PATH, get(health))
}

/// 返回当前 Server UTC 日期；HTTP 200 本身表示路由和进程可以响应。
async fn health(State(database): State<Arc<ServerDatabase>>) -> Json<serde_json::Value> {
    tracing::trace!(
        database_backend = database.backend_label(),
        "HTTP health check requested"
    );
    Json(serde_json::json!({
        "date": time::OffsetDateTime::now_utc().date().to_string(),
    }))
}
