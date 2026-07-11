//! agent 服务入口，负责聚合 agent 输入处理、连接状态和命令调度。

pub mod auth;
pub mod command;
pub mod connection;
pub mod input;
pub mod state;
