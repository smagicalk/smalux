//! Agent 本地管理模块。

mod ipc;
pub mod protocol;
pub mod service;

pub use ipc::{endpoint_is_active, request, serve};
pub use protocol::*;
pub use service::{ManagementState, run_server};
