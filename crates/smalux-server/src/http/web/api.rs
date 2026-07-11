//! HTTP API 入口，负责聚合 `/api/v1/*` 下的 REST 和 realtime 路由。

pub mod health;
pub mod realtime;

/// 构建当前最小 API 路由。
pub fn build_router() -> axum::Router<crate::state::AppState> {
    crate::http::middleware::apply_rest_middleware(
        axum::Router::new()
            .merge(health::router())
            .merge(realtime::build_router()),
    )
}
