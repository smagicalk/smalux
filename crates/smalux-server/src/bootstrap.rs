//! server 启动编排模块，负责串联日志、CLI、配置、数据库、HTTP server 和后台任务。

use clap::Parser;
use sea_orm::Database;
use sea_orm_migration::MigratorTrait;

use crate::cli::args::ServerArgs;
use crate::config::validation::validate_server_config;

/// 执行 server 启动流程。
///
/// 当前只接入 CLI 解析，确保 `--help` 和启动参数结构可用；后续在这里继续串联
/// 配置校验、日志初始化、数据库连接和 HTTP server。
pub async fn run() -> anyhow::Result<()> {
    let config = ServerArgs::parse().into_config()?;
    validate_server_config(&config)?;
    log_startup_config(&config)?;
    let database = Database::connect(config.database.connection_url()?).await?;
    crate::storage::migration::Migrator::up(&database, None).await?;
    tracing::info!("server bootstrap finished");
    Ok(())
}

/// 输出启动配置摘要。
///
/// 这里不打印数据库明文密码，只输出脱敏连接地址，方便后续排查 CLI/config 映射问题。
fn log_startup_config(config: &crate::config::model::ServerConfig) -> anyhow::Result<()> {
    // 启动阶段先生成一次真实连接地址，提前发现配置组合错误；该值不写入日志。
    let _connection_url = config.database.connection_url()?;
    let database_url = config.database.redacted_connection_url()?;

    tracing::debug!(
        bind_addr = %config.http.bind_addr,
        bind_port = config.http.bind_port,
        listen_addr = %config.http.socket_addr(),
        database_url = %database_url,
        serve_frontend = config.frontend.serve_frontend,
        frontend_dir = %config.frontend.dir.display(),
        frontend_spa_fallback = config.frontend.spa_fallback,
        log_file = %config.log.file.display(),
        log_retention_files = config.log.retention_files,
        log_max_size_mb = config.log.max_size_mb,
        "server startup config prepared"
    );

    Ok(())
}
