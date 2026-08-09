//! Smalux Server 二进制入口。
//!
//! 业务实现位于 `smalux-server` library；这里只负责初始化进程级日志并调用公开运行
//! seam。未来加入 CLI 后，参数解析也集中在这里，不再复制 library 的模块树。

/// 启动正式 Server。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    smalux_core::logs::init_tracing();
    tracing::info!("smalux server process started");
    let result = smalux_server::run_from_env().await;
    match &result {
        Ok(()) => tracing::info!("smalux server process stopped"),
        Err(error) => tracing::error!(error = %error, "smalux server process failed"),
    }
    result
}
