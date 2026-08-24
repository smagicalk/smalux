//! Smalux Plus 的稳定边界定义。
//!
//! Smalux Plus 与 Agent 之间的稳定 Worker 契约。
//!
//! Plus 使用独立可执行 Worker，而不是把第三方代码加载进 Agent 进程。这个 crate 同时
//! 提供共享 Manifest、Protobuf IPC 帧、任务 trait 和 Worker 运行时，因此插件作者只需
//! 依赖一个 crate；Agent 只使用其中的 Manifest 和协议部分。

pub mod error;
pub mod framing;
pub mod manifest;
pub mod protocol;
pub mod schema;
pub mod task;
pub mod worker;

pub use error::PluginError;
pub use manifest::{PluginManifest, PluginPlatform, PluginVersion};
pub use protocol::{WorkerFrame, WorkerRequest, WorkerResponse};
pub use schema::{
    FieldControl, PluginFieldSchema, PluginSchemaBundle, PluginSchemaError, PluginTaskSchema,
    SCHEMA_FORMAT_VERSION, SchemaHash,
};
pub use task::{AgentContext, PlusTask, PlusTaskContext, PlusTaskError, PlusTaskOutput};
