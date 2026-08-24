//! 可嵌入的 Smalux Agent Client、调度器和远程 Job 控制器。
//!
//! 二进制入口只负责初始化日志和组装运行时；需要把 Agent 集成到其他进程时，
//! 应直接依赖本库的 [`client::SmaluxClient`]，不需要启动本项目的 `main`。

pub(crate) mod config;

pub mod client;
pub mod management;
pub mod plugins;
pub mod remote_jobs;
pub mod scheduler;
pub mod tasks;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init_test_tracing() {
    smalux_core::logs::init_test_tracing();
}
