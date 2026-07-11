//! HTTP 路由组装模块，负责组合 agent transport 和前端 Web 面。

use axum::Router;

use crate::state::AppState;

/// 构建当前最小可运行 router。
///
/// 总入口只负责组合 agent transport 和前端 Web 面并挂公共 middleware。
///
/// 前端资源必须最后合并，因为它包含 fallback；这样不会抢走 `/api/v1/*`、
/// `/agent/v1/connect` 或未来 `/api/v1/realtime/*` 这类明确后端入口。
pub fn build_router(state: AppState) -> Router {
    let frontend = state.frontend.clone();
    let router = Router::new()
        .merge(crate::http::agent::build_router())
        .merge(crate::http::web::build_router(&frontend));

    crate::http::middleware::apply_common_middleware(router).with_state(state)
}
