//! Smalux agent/server 共享通信协议。
//!
//! 本 crate 只定义传输无关的消息结构和 codec，不实现 WebSocket、HTTP 或 gRPC。

/// JSON 编解码。
pub mod codec;
/// 双向通信 frame 和 payload。
pub mod frame;

pub use codec::{
    decode_client_frame, decode_remote_shell_stream_command, decode_server_frame,
    encode_ack_as_smalux_json_bytes, encode_client_frame, encode_outbound_report_as_smalux_json,
    encode_outbound_report_as_smalux_json_bytes, encode_protocol_error_as_smalux_json_bytes,
    encode_remote_probe_result_as_smalux_json_bytes, encode_remote_shell_stream_event,
    encode_remote_task_result_as_smalux_json_bytes, encode_server_frame,
};
pub use frame::{
    Ack, ClientFrame, ClientPayload, DeltaReport, Heartbeat, MetricCollectionRequest,
    OutboundReport, OutboundReportKind, ProtocolError, RemoteProbeRequest, RemoteProbeResult,
    RemoteProbeType, RemoteShellDataEncoding, RemoteShellOpenRequest, RemoteShellStreamCommand,
    RemoteShellStreamEvent, RemoteTaskRequest, RemoteTaskResult, RemoteTaskStatus,
    SMALUX_PROTOCOL_VERSION, ServerFrame, ServerPayload, SnapshotRequest,
};
