use std::time::Duration;

/// 绑定配置地址、装配当前应用路由并持续运行 Server。
pub(crate) async fn run_server(server_config: crate::config::ServerConfig) -> anyhow::Result<()> {
    let address = server_config.address.clone();
    let port = server_config.port;
    let bind_address = format!("{address}:{port}");
    let shutdown_grace = Duration::from_secs(server_config.shutdown_grace_seconds);

    tracing::info!(
        address = %address,
        port,
        "starting smalux server"
    );

    // 数据库连接和迁移在路由装配前完成；迁移失败时不启动一个半可用的 Server。
    let database =
        match crate::database::ServerDatabase::connect(server_config.database.clone()).await {
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

    let app_state =
        match crate::state::AppState::build(server_config.runtime_config(), database).await {
            Ok(app_state) => app_state,
            Err(error) => {
                tracing::error!(error = %error, "failed to build Server application state");
                return Err(error);
            }
        };
    let shutdown = app_state.shutdown.clone();
    let app_router = match crate::route::build_app_router(app_state) {
        Ok(router) => router,
        Err(error) => {
            tracing::error!(error = %error, "failed to build server routes");
            shutdown.cancel();
            return Err(error);
        }
    };
    tracing::debug!("server routes assembled");

    let listener = match tokio::net::TcpListener::bind(&bind_address).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(
                listen_address = %bind_address,
                error = %error,
                "failed to bind server listener"
            );
            shutdown.cancel();
            return Err(error.into());
        }
    };

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
    let server = axum::serve(listener, app_router).with_graceful_shutdown(shutdown_future);
    let mut server = Box::pin(server.into_future());
    let serve_result = tokio::select! {
        result = &mut server => result.map_err(anyhow::Error::from),
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
    serve_result?;
    tracing::info!("server stopped gracefully");

    Ok(())
}
