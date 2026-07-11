//! HTTP 中间件模块，负责日志、请求 ID、敏感头和 CORS 这类通用横切能力。
//!
//! 当前先只接最小可用组合：
//! - Trace
//! - Request ID
//! - Sensitive request headers
//! - CORS
//!
//! timeout、限流、session 和认证后续再按路由粒度补，不在最小骨架阶段一口气全挂。

use std::time::Duration;

use axum::{BoxError, Router, http::StatusCode};
use http::{HeaderName, Method, header};
use tower::ServiceBuilder;
use tower::timeout::TimeoutLayer;
use tower_http::{
    cors::{Any, CorsLayer},
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    sensitive_headers::SetSensitiveRequestHeadersLayer,
    trace::TraceLayer,
};

/// 统一的请求 ID header 名。
pub const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
/// REST API 默认超时。
const DEFAULT_REST_TIMEOUT: Duration = Duration::from_secs(15);

/// 构建当前最小 HTTP middleware 组合。
///
/// 这里不直接处理业务认证；只提供通用观测和安全基础设施，后续按路由再追加
/// timeout、rate limit、session/auth 等更具体的中间件。
pub fn apply_common_middleware<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(tower_http::catch_panic::CatchPanicLayer::new())
        .layer(PropagateRequestIdLayer::new(REQUEST_ID_HEADER.clone()))
        .layer(SetRequestIdLayer::new(
            REQUEST_ID_HEADER.clone(),
            MakeRequestUuid,
        ))
        .layer(SetSensitiveRequestHeadersLayer::new([
            header::AUTHORIZATION,
            header::COOKIE,
        ]))
        .layer(build_cors_layer())
        .layer(TraceLayer::new_for_http())
}

/// 给普通 REST 路由追加默认超时。
///
/// 当前只给短请求接口使用，不挂到 WebSocket upgrade 或未来的 SSE/流式响应上。
pub fn apply_rest_middleware<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(
        ServiceBuilder::new()
            .layer(axum::error_handling::HandleErrorLayer::new(
                |_error: BoxError| async { StatusCode::REQUEST_TIMEOUT },
            ))
            .layer(TimeoutLayer::new(DEFAULT_REST_TIMEOUT)),
    )
}

// 命令类接口的限流层暂时不提前定义成空函数。
// 等真正接 `POST /api/v1/agents/{agent_id}/commands` 时，再在 router 里单独挂
// governor 或其它 rate limit layer，避免留下未使用的中间件入口。

/// 构建当前最小 CORS 规则。
///
/// 当前先放宽到开发可用，后续管理端 session/cookie 真接入时再收紧到明确 origin 列表。
fn build_cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ])
        .allow_headers(Any)
}
