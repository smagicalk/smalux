mod auth;
mod bootstrap;
mod cli;
mod config;
mod http;
mod ingest;
mod service;
mod state;
mod storage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bootstrap::run().await
}
