//! Smalux agent/server 共享通信协议。
//!
//! 本 crate 定义 agent/server 必须共享的协议结构、JSON codec、二进制 wire packet
//! 和 secure_psk 安全通道工具；具体 WebSocket、HTTP 或 gRPC 连接管理由 agent/server 实现。

/// JSON 编解码。
pub mod codec;
/// 双向通信 frame 和 payload。
pub mod frame;
/// secure_psk 安全通道。
pub mod secure;
/// Smalux 二进制 wire packet。
pub mod wire;

pub use codec::{
    build_client_frame_from_ack, build_client_frame_from_protocol_error,
    build_client_frame_from_remote_job_result, build_client_frame_from_remote_task_result,
    decode_client_frame, decode_client_frame_bytes, decode_server_frame, decode_server_frame_bytes,
    decode_shell_stream_command, encode_client_frame, encode_client_frame_bytes,
    encode_server_frame, encode_server_frame_bytes, encode_shell_stream_event,
};
pub use frame::{
    Ack, ClientEvent, ClientEventKind, ClientFrame, ClientPayload, DeltaReport, Heartbeat,
    MetricCollectionRequest, ProtocolError, RemoteJobApplyRequest, RemoteJobKind,
    RemoteJobOperation, RemoteJobResult, RemoteJobRunRequest, RemoteJobSpec, RemoteProbeId,
    RemoteProbeResult, RemoteProbeResultSource, RemoteProbeResultStatus, RemoteProbeType,
    RemoteShellDataEncoding, RemoteShellOpenRequest, RemoteShellStreamCommand,
    RemoteShellStreamEvent, RemoteTaskRequest, RemoteTaskResult, RemoteTaskStatus,
    SMALUX_PROTOCOL_VERSION, ServerFrame, ServerPayload, SnapshotRequest,
};
