//! smalux-server 二进制入口。
//!
//! 这里只负责启动期的最小装配；HTTP 路由、写入、查询、存储等逻辑放到独立模块。

mod config;
mod http;
mod ingest;
mod query;
mod storage;

/// server 默认日志文件路径；滚动后的历史文件会追加序号后缀。
const LOG_FILE: &str = "logs/smalux-server.log";
/// server 默认保留的滚动日志文件数。
const LOG_RETENTION_FILES: usize = 14;
/// server 默认单个日志文件最大大小，单位 MB。
const LOG_MAX_SIZE_MB: u64 = 64;

/// server 主入口。
///
/// 当前仍是骨架阶段，后续会在这里启动 HTTP 服务。
fn main() -> anyhow::Result<()> {
    smalux_core::log::init_tracing(LOG_FILE, LOG_RETENTION_FILES, LOG_MAX_SIZE_MB)?;
    tracing::info!("smalux-server starting");
    Ok(())
}
