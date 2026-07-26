/// 装配正式 Server 的全部顶层路由。
pub(crate) fn build_app_router() -> anyhow::Result<axum::routing::Router> {
    Ok(
        axum::Router::new()
    )
}
