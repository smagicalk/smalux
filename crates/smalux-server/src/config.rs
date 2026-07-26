use serde::{Deserialize, Serialize};

pub(crate) mod default;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(crate) struct ServerConfig {
    pub(crate) address: String,
    pub(crate) port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: default::DEFAULT_ADDRESS.to_string(),
            port: default::DEFAULT_PORT,
        }
    }
}
