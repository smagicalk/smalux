//! 远程交互式 shell 模块入口。
//!
//! 当前实现 smalux 自有控制消息的第一版接入：主 WebSocket 只负责收到
//! `remote_shell_open`，真正的 PTY input/output/resize 走独立临时 WebSocket stream。

mod manager;
mod message;
mod options;

pub(crate) use self::manager::RemoteShellManager;
pub(crate) use self::message::RemoteShellOpenRequest;
pub(crate) use self::options::RemoteShellOptions;
