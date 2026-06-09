mod auth;
mod bootstrap;
mod cli;
mod config;
mod http;
mod ingest;
mod service;
mod state;
mod storage;

fn main() -> anyhow::Result<()> {
    bootstrap::run()
}
