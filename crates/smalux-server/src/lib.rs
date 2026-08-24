//! Smalux Server 可复用的服务实现。
//!
//! 二进制入口负责装配和运行，集成测试或其他 workspace crate 可复用这里公开的服务类型。

mod bootstrap;
pub mod cli;
mod commands;
mod config;
mod controller;
pub mod management;
pub(crate) mod route;
/// Server 业务服务，包括 Agent 注册、密钥环、Session 和 gRPC transport。
pub mod service;
mod state;

/// SeaORM 连接、实体和滚动迁移层。
pub mod database;

/// 从环境变量读取启动配置并运行 Server。
///
/// 该兼容入口供集成测试或嵌入调用只使用环境变量启动；正式二进制通过 [`execute`]
/// 处理 CLI 覆盖和管理子命令。
pub async fn run_from_env() -> anyhow::Result<()> {
    let server_config = crate::config::ServerConfig::from_env().map_err(|error| {
        tracing::error!(error = %error, "failed to load Server configuration");
        anyhow::Error::from(error)
    })?;
    crate::bootstrap::run_server(server_config, crate::cli::default_control_endpoint_value()).await
}

/// 执行已经由 Clap 解析的 Server 启动或本地管理命令。
pub async fn execute(cli: crate::cli::Cli) -> anyhow::Result<()> {
    crate::commands::execute(cli).await
}

// 测试库启动前安装全局 tracing subscriber，使 Server 路由和数据库测试在
// `cargo test -- --nocapture` 下也能输出生命周期日志。
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init_test_tracing() {
    smalux_core::logs::init_test_tracing();
}
