//! Echo Plus Worker 进程入口。

use smalux_plus_core::worker::PlusWorker;
use smalux_plus_echo::{EchoTask, PLUGIN_ID};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    PlusWorker::builder(PLUGIN_ID)
        .max_concurrency(4)
        .register(EchoTask)
        .run_stdio()
        .await?;
    Ok(())
}
