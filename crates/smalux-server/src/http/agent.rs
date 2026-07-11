//! agent HTTP 接入模块入口，负责组织 agent WebSocket upgrade、连接生命周期和控制帧发送。
//!
//! 这里保留入口文件，具体连接类型或 stream 处理放到同名目录下。

use axum::{Router, routing::get};

pub mod ws;

/// 构建 agent transport 路由。
pub fn build_router() -> Router<crate::state::AppState> {
    Router::new().route("/agent/v1/connect", get(ws::upgrade_agent_ws))
}
