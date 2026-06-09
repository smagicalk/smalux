//! server CLI 参数模块，负责定义 clap 参数结构和启动输入解析规则。

use std::path::PathBuf;

use clap::Parser;

use crate::config::defaults::{
    DEFAULT_BIND_ADDR, DEFAULT_DATABASE_URL, DEFAULT_FRONTEND_DIR, DEFAULT_LOG_FILE,
    DEFAULT_LOG_MAX_SIZE_MB, DEFAULT_LOG_RETENTION_FILES,
};

/// server 启动参数，负责承接命令行和环境变量中的原始输入。
///
/// CLI 只配置 server 进程启动所需的静态运行环境，不配置 agent token/key 或 agent 执行能力。
/// agent 凭据应在“添加 agent”流程中动态生成，并存储到数据库。
#[derive(Clone, Debug, Parser)]
#[command(
    name = "smalux-server",
    version,
    about = "Smalux monitoring server",
    long_about = "Smalux monitoring server for agent ingestion, REST API, dashboard realtime events, and optional frontend hosting."
)]
pub struct ServerArgs {
    /// HTTP 监听地址，包含 IP 和端口，例如 127.0.0.1:3000。
    #[arg(short = 'b', long = "bind", env = "SMALUX_SERVER_BIND", default_value = DEFAULT_BIND_ADDR)]
    pub bind_addr: String,

    /// 数据库连接地址，支持 sqlite、postgres、postgresql 和 mysql。
    #[arg(short = 'd', long = "database-url", env = "SMALUX_SERVER_DATABASE_URL", default_value = DEFAULT_DATABASE_URL)]
    pub database_url: String,

    /// 是否由 server 托管前端静态资源；不传时默认只提供 API 和 agent 接入。
    #[arg(
        long = "serve-frontend",
        env = "SMALUX_SERVER_SERVE_FRONTEND",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = clap::value_parser!(bool)
    )]
    pub serve_frontend: bool,

    /// 未编译内置前端资源时，server 托管前端所使用的静态资源目录。
    #[arg(long = "frontend-dir", env = "SMALUX_SERVER_FRONTEND_DIR", default_value = DEFAULT_FRONTEND_DIR)]
    pub frontend_dir: PathBuf,

    /// 是否为 React/Vite 这类 SPA 启用 index.html fallback，需要显式传入 true 或 false。
    #[arg(
        long = "frontend-spa-fallback",
        env = "SMALUX_SERVER_FRONTEND_SPA_FALLBACK",
        default_value_t = true,
        action = clap::ArgAction::Set,
        value_parser = clap::value_parser!(bool)
    )]
    pub frontend_spa_fallback: bool,

    /// 滚动日志文件路径；日志级别仍然只读取 RUST_LOG。
    #[arg(long = "log-file", env = "SMALUX_SERVER_LOG_FILE", default_value = DEFAULT_LOG_FILE)]
    pub log_file: PathBuf,

    /// 滚动日志最多保留的文件数量。
    #[arg(short = 'L', long = "log-retention-files", env = "SMALUX_SERVER_LOG_RETENTION_FILES", default_value_t = DEFAULT_LOG_RETENTION_FILES)]
    pub log_retention_files: usize,

    /// 单个滚动日志文件最大大小，单位 MB。
    #[arg(long = "log-max-size-mb", env = "SMALUX_SERVER_LOG_MAX_SIZE_MB", default_value_t = DEFAULT_LOG_MAX_SIZE_MB)]
    pub log_max_size_mb: u64,
}
