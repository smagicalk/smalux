//! 健康检查接口。

use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;

use crate::state::AppState;

/// 最小健康检查响应。
#[derive(Debug, Clone, Serialize)]
struct HealthResponse {
    /// 服务状态。
    status: &'static str,
}

/// 构建健康检查相关路由。
pub fn router() -> Router<AppState> {
    Router::new().route("/api/v1/health", get(health))
}

/// 最小健康检查接口。
async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let _database = &state.database;
    Json(HealthResponse { status: "ok" })
}
