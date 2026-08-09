//! Smalux 跨 crate 公共能力。
//!
//! `config::paths` 提供配置、数据和缓存目录的统一解析，
//! 让 agent、server 以及后续扩展模块不必各自拼接平台相关路径。

pub mod config;
pub mod logs;

// 常用类型在 crate 根导出，调用方可以直接使用 `smalux_core::AppDirectories`。
pub use config::paths::{AppDirectories, DirectoryError};
