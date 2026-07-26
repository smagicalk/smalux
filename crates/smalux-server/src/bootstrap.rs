/// 绑定配置地址、装配当前应用路由并持续运行 Server。
pub(crate) async fn run_server(server_config: crate::config::ServerConfig) -> anyhow::Result<()> {
    let address = server_config.address;
    let port = server_config.port;

    let app_router = crate::route::build_app_router()?;

    let listener = tokio::net::TcpListener::bind(format!("{address}:{port}")).await?;

    println!("[server] listening on http://{address}:{port}");

    axum::serve(listener, app_router).await?;

    Ok(())
}
