mod routes;

use std::sync::Arc;

use crate::service::agent::state::AgentState;

pub(crate) fn get_route(agent_state: Arc<AgentState>) -> anyhow::Result<axum::routing::Router> {
    tracing::debug!("building Agent controller routes");
    match routes::get_route(agent_state) {
        Ok(router) => Ok(router),
        Err(error) => {
            tracing::error!(error = %error, "failed to build Agent controller routes");
            Err(error)
        }
    }
}
