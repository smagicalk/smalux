//! API realtime 通道模块，负责 `/api/v1/realtime/*` 下的实时订阅入口。

/// 构建当前最小 realtime 路由。
///
/// 当前先不挂具体 endpoint，后续在这里增加 `/api/v1/realtime/*` 的 SSE 或 WebSocket。
pub fn build_router() -> axum::Router<crate::state::AppState> {
    axum::Router::new()
}
