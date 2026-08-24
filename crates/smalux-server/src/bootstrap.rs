//! Server 进程的装配、监听和优雅关闭流程。
//!
//! 启动分为三步：验证后的配置构建数据库与 `AppState`，创建本地管理端点和 Axum Router，
//! 最后绑定 TCP listener。任一装配步骤失败都会取消共享 shutdown token，避免遗留后台任务。

use std::{sync::Arc, time::Duration};

use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// 绑定配置地址、装配当前应用路由并持续运行 Server。
pub(crate) async fn run_server(
    server_config: crate::config::ServerConfig,
    control_endpoint: std::path::PathBuf,
) -> anyhow::Result<()> {
    tracing::info!(
        address = %server_config.address,
        port = server_config.port,
        "starting smalux server"
    );

    let runtime = build_runtime(&server_config, control_endpoint).await?;
    let listener = match TcpListener::bind(&runtime.bind_address).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(
                listen_address = %runtime.bind_address,
                error = %error,
                "failed to bind server listener"
            );
            runtime.shutdown.cancel();
            return Err(error.into());
        }
    };

    serve_runtime(runtime, listener).await
}

/// 完成 I/O 监听前已经装配好的运行资源。
struct ServerRuntime {
    /// 同时承载普通 HTTP、WebSocket 路由和 gRPC 路由的 Axum Router。
    router: Router,
    /// HTTP、Agent Session、后台任务和本地管理端点共享的关闭信号。
    shutdown: CancellationToken,
    /// 收到关闭信号后等待 Axum 长连接退出的最长时间。
    shutdown_grace: Duration,
    /// 经过配置拼装的 TCP 绑定地址，仅供监听和日志使用。
    bind_address: String,
    /// 本地管理 Named Pipe/Unix Socket 的拥有任务。
    control_task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// 只负责数据库、应用状态和路由装配；未完成装配时不创建监听器。
async fn build_runtime(
    config: &crate::config::ServerConfig,
    control_endpoint: std::path::PathBuf,
) -> anyhow::Result<ServerRuntime> {
    let database = match crate::database::ServerDatabase::connect(config.database.clone()).await {
        Ok(database) => database,
        Err(error) => {
            tracing::error!(error = %error, "failed to initialize Server database");
            return Err(error.into());
        }
    };
    tracing::debug!(
        backend = database.backend_label(),
        "Server database layer initialized"
    );

    let app_state = match crate::state::AppState::build(config.runtime_config(), database).await {
        Ok(app_state) => app_state,
        Err(error) => {
            tracing::error!(error = %error, "failed to build Server application state");
            return Err(error);
        }
    };
    let shutdown = app_state.shutdown.clone();
    let control_task = tokio::spawn(crate::management::serve(
        control_endpoint,
        Arc::clone(&app_state.management),
        shutdown.clone(),
    ));
    let router = match crate::route::build_app_router(app_state) {
        Ok(router) => router,
        Err(error) => {
            tracing::error!(error = %error, "failed to build server routes");
            shutdown.cancel();
            return Err(error);
        }
    };
    tracing::debug!("server routes assembled");

    Ok(ServerRuntime {
        router,
        shutdown,
        shutdown_grace: Duration::from_secs(config.shutdown_grace_seconds),
        bind_address: format!("{}:{}", config.address, config.port),
        control_task,
    })
}

/// 绑定完成后只处理监听、信号和优雅关闭，不再夹带数据库或路由逻辑。
async fn serve_runtime(runtime: ServerRuntime, listener: TcpListener) -> anyhow::Result<()> {
    let ServerRuntime {
        router,
        shutdown,
        shutdown_grace,
        bind_address,
        mut control_task,
    } = runtime;

    tracing::info!(listen_address = %bind_address, "server listening");

    let signal_shutdown = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %error, "failed to listen for Server shutdown signal");
        }
        tracing::info!("Server shutdown signal received");
        signal_shutdown.cancel();
    });

    let shutdown_for_server = shutdown.clone();
    let shutdown_future = async move {
        shutdown_for_server.cancelled().await;
    };
    let server = axum::serve(listener, router).with_graceful_shutdown(shutdown_future);
    let mut server = Box::pin(server.into_future());
    let serve_result = tokio::select! {
        result = &mut server => result.map_err(anyhow::Error::from),
        result = &mut control_task => {
            let requested_shutdown = shutdown.is_cancelled();
            shutdown.cancel();
            if requested_shutdown {
                Ok(())
            } else { match result {
                Ok(Ok(())) => Err(anyhow::anyhow!("Server management endpoint stopped unexpectedly")),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(anyhow::anyhow!("Server management task failed: {error}")),
            } }
        }
        _ = shutdown.cancelled() => {
            tracing::info!(?shutdown_grace, "waiting for Server sessions to stop");
            match tokio::time::timeout(shutdown_grace, &mut server).await {
                Ok(result) => result.map_err(anyhow::Error::from),
                Err(_) => {
                    tracing::warn!(?shutdown_grace, "Server shutdown grace period elapsed");
                    Ok(())
                }
            }
        }
    };
    signal_task.abort();
    shutdown.cancel();
    if !control_task.is_finished() {
        let _ = control_task.await;
    }
    serve_result?;
    tracing::info!("server stopped gracefully");

    Ok(())
}
