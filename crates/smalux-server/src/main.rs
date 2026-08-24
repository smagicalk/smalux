//! Smalux Server 二进制入口。
//!
//! 业务实现位于 `smalux-server` library；这里只负责解析 CLI、初始化进程级日志并调用
//! library 的统一命令入口，不复制配置、IPC 或管理业务。

/// 启动正式 Server。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = smalux_server::cli::parse();
    smalux_core::logs::init_tracing();
    tracing::info!("smalux server process started");
    let result = smalux_server::execute(cli).await;
    match &result {
        Ok(()) => tracing::info!("smalux server process stopped"),
        Err(error) => tracing::error!(error = %error, "smalux server process failed"),
    }
    result
}
