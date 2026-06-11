//! 远程任务、网络探测和交互 shell 协议模型入口。

/// 远程网络探测协议模型。
mod probe;
/// 交互式远程 shell 协议模型。
mod shell;
/// 远程非交互任务协议模型。
mod task;

pub use self::probe::{
    RemoteProbeApplyRequest, RemoteProbeId, RemoteProbeJob, RemoteProbeOnceRequest,
    RemoteProbeOperation, RemoteProbeResult, RemoteProbeResultSource, RemoteProbeResultStatus,
    RemoteProbeType,
};
pub use self::shell::{
    RemoteShellDataEncoding, RemoteShellOpenRequest, RemoteShellStreamCommand,
    RemoteShellStreamEvent,
};
pub use self::task::{RemoteTaskRequest, RemoteTaskResult, RemoteTaskStatus};
