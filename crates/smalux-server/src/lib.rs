//! Smalux Server 可复用的服务实现。
//!
//! 二进制入口负责装配和运行，集成测试或其他 workspace crate 可复用这里公开的服务类型。

mod bootstrap;
mod config;
mod controller;
pub(crate) mod route;
pub mod service;
mod state;

/// SeaORM 连接、实体和滚动迁移层。
pub mod database;

/// 从环境变量读取启动配置并运行 Server。
///
/// 该函数是二进制入口和测试之间的运行 seam；未来加入 CLI 后，`main.rs` 可以只负责
/// 解析 CLI 与环境变量，再调用一个接收已构造配置的公开运行函数。
pub async fn run_from_env() -> anyhow::Result<()> {
    let server_config = crate::config::ServerConfig::from_env().map_err(|error| {
        tracing::error!(error = %error, "failed to load Server configuration");
        anyhow::Error::from(error)
    })?;
    crate::bootstrap::run_server(server_config).await
}

// 测试库启动前安装全局 tracing subscriber，使 Server 路由和数据库测试在
// `cargo test -- --nocapture` 下也能输出生命周期日志。
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init_test_tracing() {
    smalux_core::logs::init_test_tracing();
}
