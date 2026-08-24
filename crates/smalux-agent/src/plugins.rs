//! Agent 端 Plus 插件目录、会话运行时状态和后续 Worker 管理入口。
//!
//! 本模块不负责下载、安装或持久化 Server 业务配置。`catalog` 只发现管理员手工放入
//! 数据目录的插件版本；`runtime` 只保留当前 Noise 会话已经确认的运行时快照。Worker
//! 子进程和插件 Job Adapter 会在此模块下继续扩展，避免污染 client 与 scheduler 边界。

mod catalog;
mod manager;
mod runtime;
mod task;
mod worker;

pub use catalog::{InstalledPlugin, PluginCatalog, PluginCatalogError};
pub use manager::{PluginManager, PluginRuntimeLimits, PluginStatusSnapshot};
pub use runtime::{PluginRuntimeState, RuntimeSnapshotResult};
pub use task::PluginReportingTask;
pub use worker::{PluginWorkerClient, PluginWorkerError, WorkerTaskOutput};
