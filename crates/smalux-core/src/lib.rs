//! smalux 共享核心库。
//!
//! 这里放 agent 和 server 都会使用的模型、单位换算、日志初始化和通用工具。

/// 流量/容量单位换算工具。
pub mod flow;
/// 统一 tracing 初始化。
pub mod log;
/// 内部领域模型。
pub mod model;
/// 通用工具模块。
pub mod utils;
