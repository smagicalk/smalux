//! server 启动编排模块，负责串联日志、CLI、配置、数据库、HTTP server 和后台任务。

use clap::Parser;

use crate::cli::args::ServerArgs;

/// 执行 server 启动流程。
///
/// 当前只接入 CLI 解析，确保 `--help` 和启动参数结构可用；后续在这里继续串联
/// 配置校验、日志初始化、数据库连接和 HTTP server。
pub fn run() -> anyhow::Result<()> {
    let _args = ServerArgs::parse();
    Ok(())
}
