//! Smalux Server 正式入口，业务服务将在这里逐步实现。

use crate::bootstrap::run_server;

mod bootstrap;
mod config;
pub(crate) mod route;
mod controller;
mod service;

/// 启动正式 Server。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_server(config::ServerConfig::default()).await
}
