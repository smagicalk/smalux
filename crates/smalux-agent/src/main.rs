//! Smalux agent 最小入口，业务实现由后续设计补充。

mod cli;
mod client;
mod config;
pub mod job_control;
pub mod scheduler;
pub mod tasks;

// 测试二进制启动前安装全局 tracing subscriber。
// 执行 `cargo test -- --nocapture` 时，可直接看到集成测试中 Server 的连接与断开日志。
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init_test_tracing() {
    smalux_core::logs::init_test_tracing();
}

/// Agent 进程入口；当前分支仅注册模块，运行流程由后续集成阶段接入。
fn main() {
    // Agent 的业务模块只产生 tracing 事件；由进程入口安装公共控制台和滚动文件层。
    smalux_core::logs::init_tracing();
    tracing::info!("smalux agent process started");
}
