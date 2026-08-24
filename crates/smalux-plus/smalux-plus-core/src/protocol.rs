//! Agent 与 Plus Worker 之间的 Protobuf IPC 消息。
//!
//! stdout 只允许写入 [`WorkerFrame`]，日志必须写到 stderr。消息本身不携带路径、命令行
//! 或 Server endpoint；Worker 只执行已安装 Manifest 声明的 Task kind。

use prost::Message;

/// Worker 协议主版本。
pub const WORKER_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, PartialEq, Message)]
pub struct WorkerFrame {
    #[prost(oneof = "worker_frame::Body", tags = "1, 2")]
    pub body: Option<worker_frame::Body>,
}

pub mod worker_frame {
    use super::{WorkerRequest, WorkerResponse};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Body {
        #[prost(message, tag = "1")]
        Request(WorkerRequest),
        #[prost(message, tag = "2")]
        Response(WorkerResponse),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct WorkerRequest {
    #[prost(oneof = "worker_request::Body", tags = "1, 2, 3, 4, 5, 6")]
    pub body: Option<worker_request::Body>,
}

pub mod worker_request {
    use super::{CancelTask, ExecuteTask, InitializeWorker};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Body {
        #[prost(message, tag = "1")]
        Hello(super::Hello),
        #[prost(message, tag = "2")]
        Initialize(InitializeWorker),
        #[prost(message, tag = "3")]
        Execute(ExecuteTask),
        #[prost(message, tag = "4")]
        Cancel(CancelTask),
        #[prost(message, tag = "5")]
        Ping(super::Ping),
        #[prost(message, tag = "6")]
        Shutdown(super::Shutdown),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct WorkerResponse {
    #[prost(oneof = "worker_response::Body", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub body: Option<worker_response::Body>,
}

pub mod worker_response {
    use super::{ErrorResponse, TaskResult, WorkerReady};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Body {
        #[prost(message, tag = "1")]
        Ready(WorkerReady),
        #[prost(message, tag = "2")]
        Started(super::TaskStarted),
        #[prost(message, tag = "3")]
        Result(TaskResult),
        #[prost(message, tag = "4")]
        Error(ErrorResponse),
        #[prost(message, tag = "5")]
        Pong(super::Pong),
        #[prost(message, tag = "6")]
        Stopped(super::Stopped),
        #[prost(message, tag = "7")]
        Cancelled(super::TaskCancelled),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct Hello {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(string, tag = "2")]
    pub expected_plugin_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct InitializeWorker {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(string, tag = "2")]
    pub plugin_id: String,
    #[prost(uint64, tag = "3")]
    pub config_revision: u64,
    #[prost(bytes = "bytes", tag = "4")]
    pub runtime_config: Vec<u8>,
    #[prost(uint32, tag = "5")]
    pub max_concurrency: u32,
    #[prost(message, optional, tag = "6")]
    pub agent_context: Option<AgentContextMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AgentContextMessage {
    #[prost(string, tag = "1")]
    pub agent_version: String,
    #[prost(string, tag = "2")]
    pub operating_system: String,
    #[prost(string, tag = "3")]
    pub architecture: String,
    #[prost(string, optional, tag = "4")]
    pub agent_id: Option<String>,
    #[prost(string, tag = "5")]
    pub data_dir: String,
    #[prost(string, tag = "6")]
    pub config_dir: String,
    #[prost(string, tag = "7")]
    pub plugin_dir: String,
    #[prost(string, tag = "8")]
    pub plugin_data_dir: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct WorkerReady {
    #[prost(uint32, tag = "1")]
    pub protocol_version: u32,
    #[prost(string, tag = "2")]
    pub plugin_id: String,
    #[prost(uint64, tag = "3")]
    pub config_revision: u64,
    #[prost(string, repeated, tag = "4")]
    pub task_kinds: Vec<String>,
    /// Worker 实际采用的最大并发，等于插件自身与 Agent 上限中的较小值。
    #[prost(uint32, tag = "5")]
    pub effective_max_concurrency: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExecuteTask {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(bytes = "bytes", tag = "2")]
    pub run_id: Vec<u8>,
    #[prost(string, tag = "3")]
    pub task_kind: String,
    #[prost(uint32, tag = "4")]
    pub schema_version: u32,
    #[prost(bytes = "bytes", tag = "5")]
    pub config: Vec<u8>,
    #[prost(uint64, tag = "6")]
    pub deadline_unix_millis: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct TaskStarted {
    #[prost(string, tag = "1")]
    pub request_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct TaskResult {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(bytes = "bytes", tag = "2")]
    pub run_id: Vec<u8>,
    #[prost(enumeration = "TaskStatus", tag = "3")]
    pub status: i32,
    #[prost(string, tag = "4")]
    pub summary: String,
    #[prost(message, repeated, tag = "5")]
    pub metrics: Vec<Metric>,
    #[prost(bytes = "bytes", tag = "6")]
    pub payload: Vec<u8>,
    #[prost(string, optional, tag = "7")]
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum TaskStatus {
    Unspecified = 0,
    Succeeded = 1,
    Failed = 2,
    Cancelled = 3,
}

#[derive(Clone, PartialEq, Message)]
pub struct Metric {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(double, tag = "2")]
    pub value: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct CancelTask {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(string, tag = "2")]
    pub reason: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Ping {
    #[prost(uint64, tag = "1")]
    pub nonce: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Pong {
    #[prost(uint64, tag = "1")]
    pub nonce: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Shutdown {
    #[prost(string, tag = "1")]
    pub reason: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Stopped {
    #[prost(string, tag = "1")]
    pub reason: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct TaskCancelled {
    #[prost(string, tag = "1")]
    pub request_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct ErrorResponse {
    #[prost(string, tag = "1")]
    pub request_id: String,
    #[prost(string, tag = "2")]
    pub code: String,
    #[prost(string, tag = "3")]
    pub message: String,
}
