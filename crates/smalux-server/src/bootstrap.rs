//! server 启动编排模块，负责串联日志、CLI、配置、数据库、HTTP server 和后台任务。

use clap::Parser;
use smalux_core::log::init_tracing;
use tokio::net::TcpListener;

use crate::cli::args::ServerArgs;
use crate::config::validation::validate_server_config;
use crate::state::AppState;

/// 执行 server 启动流程。
///
/// 当前最小启动流程会完成：
/// - CLI 解析
/// - 配置校验
/// - 日志初始化
/// - 数据库初始化
/// - 启动最小 axum HTTP/WS 骨架
pub async fn run() -> anyhow::Result<()> {
    let config = ServerArgs::parse().into_config()?;
    validate_server_config(&config)?;
    init_tracing(
        config.log.file.clone(),
        config.log.retention_files,
        config.log.max_size_mb,
    )?;
    log_startup_config(&config)?;

    let database = crate::storage::init_database(&config.database).await?;
    let state = AppState::new(database, config.frontend.clone());
    let router = crate::http::router::build_router(state);
    let listener = TcpListener::bind(config.http.socket_addr()).await?;

    tracing::info!(
        listen_addr = %config.http.socket_addr(),
        "server bootstrap finished, starting axum listener"
    );

    axum::serve(listener, router).await?;
    Ok(())
}

/// 输出启动配置摘要。
///
/// 这里不打印数据库明文密码，只输出脱敏连接地址，方便后续排查 CLI/config 映射问题。
fn log_startup_config(config: &crate::config::model::ServerConfig) -> anyhow::Result<()> {
    // 启动阶段先生成一次真实连接地址，提前发现配置组合错误；该值不写入日志。
    let _connection_url = crate::storage::database_connection_url(&config.database)?;
    let database_url = crate::storage::redacted_database_connection_url(&config.database)?;

    tracing::debug!(
        bind_addr = %config.http.bind_addr,
        bind_port = config.http.bind_port,
        listen_addr = %config.http.socket_addr(),
        database_url = %database_url,
        serve_frontend = config.frontend.serve_frontend,
        site_mode = ?config.frontend.site.mode,
        site_dir = config
            .frontend
            .site
            .directory
            .as_ref()
            .map(|dir| dir.display().to_string())
            .as_deref()
            .unwrap_or("<none>"),
        site_external_url = config
            .frontend
            .site
            .external_url
            .as_deref()
            .unwrap_or("<none>"),
        admin_mode = ?config.frontend.admin.mode,
        admin_dir = config
            .frontend
            .admin
            .directory
            .as_ref()
            .map(|dir| dir.display().to_string())
            .as_deref()
            .unwrap_or("<none>"),
        admin_external_url = config
            .frontend
            .admin
            .external_url
            .as_deref()
            .unwrap_or("<none>"),
        frontend_spa_fallback = config.frontend.spa_fallback,
        log_file = %config.log.file.display(),
        log_retention_files = config.log.retention_files,
        log_max_size_mb = config.log.max_size_mb,
        "server startup config prepared"
    );

    Ok(())
}
