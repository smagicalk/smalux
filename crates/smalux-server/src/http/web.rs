//! 前端 Web 面 HTTP 入口，负责聚合 API、realtime 和静态前端。

pub mod api;
pub mod frontend;

/// 构建前端 Web 面总路由。
pub fn build_router(
    config: &crate::config::model::FrontendConfig,
) -> axum::Router<crate::state::AppState> {
    api::build_router().merge(frontend::build_frontend_router(config))
}
