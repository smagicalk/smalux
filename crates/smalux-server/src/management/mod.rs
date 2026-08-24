//! Server 本地管理模块。

mod ipc;
pub mod protocol;
pub(crate) mod service;

pub use ipc::request;
pub(crate) use ipc::serve;
pub use protocol::*;
pub(crate) use service::AdminService;
