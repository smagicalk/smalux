use std::sync::Arc;

use crate::service::agent::state::AgentState;
use smalux_core::config::default::DEFAULT_AGENT_PRIFIX;
use smalux_protocol::agent::v1::agent_transport_server::AgentTransportServer;
use tonic::service::Routes;
use tower_http::trace::TraceLayer;

pub(crate) fn get_route(agent_state: Arc<AgentState>) -> anyhow::Result<axum::routing::Router> {
    tracing::debug!(prefix = DEFAULT_AGENT_PRIFIX, "mounting Agent gRPC route");
    let agent_route = get_agent_route(agent_state)?;
    Ok(axum::Router::new().nest(DEFAULT_AGENT_PRIFIX, agent_route))
}

pub(crate) fn get_agent_route(
    agent_state: Arc<AgentState>,
) -> anyhow::Result<axum::routing::Router> {
    let active_keys = agent_state
        .keyring_manager
        .active_key_count()
        .map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(active_keys, "Server Noise keyring ready for Agent service");

    let max_message_bytes = agent_state.max_grpc_message_bytes;
    let service = AgentTransportServer::new(
        crate::service::agent::server_service::AgentServer::new(agent_state),
    )
    .max_decoding_message_size(max_message_bytes)
    .max_encoding_message_size(max_message_bytes);
    Ok(Routes::new(service)
        .into_axum_router()
        // AgentTransport 同时包含 unary RPC 和长期双向流；gRPC 分类器能正确记录
        // RPC 建立、流结束和流失败，不应使用只面向普通 HTTP 响应的分类器。
        .layer(TraceLayer::new_for_grpc()))
}
