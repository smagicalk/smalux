//! Smalux agent 最小入口，业务实现由后续设计补充。

mod cli;
pub mod job_control;
pub mod scheduler;
pub mod tasks;

/// Agent 进程入口；当前分支仅注册模块，运行流程由后续集成阶段接入。
fn main() {}
