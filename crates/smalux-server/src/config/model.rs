//! server 配置模型模块入口，负责导出稳定运行配置类型。

pub mod database;
pub mod frontend;
pub mod http;
pub mod log;
pub mod server;

pub use database::{DatabaseConfig, DatabaseDriver};
pub use frontend::{FrontendConfig, FrontendSlotConfig, FrontendSlotMode};
pub use http::HttpConfig;
pub use log::LogConfig;
pub use server::ServerConfig;
