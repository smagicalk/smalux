//! 协议协商、字段校验、状态机、编解码和安全层共用错误模型。

use thiserror::Error;

/// 协议模块统一返回值。
pub type Result<T> = std::result::Result<T, Error>;

/// 编解码、协商和会话状态错误。
#[derive(Debug, Error)]
pub enum Error {
    /// 本地帧限制超出协议允许范围。
    #[error("frame limit {value} is outside the allowed range {min}..={max}")]
    InvalidFrameLimit {
        /// 用户提供的限制值。
        value: usize,
        /// 协议允许的最小值。
        min: usize,
        /// 协议允许的最大值。
        max: usize,
    },
    /// 单帧编码长度超过本地限制。
    #[error("frame size {actual} exceeds limit {max}")]
    FrameTooLarge {
        /// 实际帧长度。
        actual: usize,
        /// 当前生效的本地限制。
        max: usize,
    },
    /// Protobuf 编码失败。
    #[error("failed to encode protobuf frame: {0}")]
    Encode(#[from] prost::EncodeError),
    /// Protobuf 解码失败。
    #[error("failed to decode protobuf frame: {0}")]
    Decode(#[from] prost::DecodeError),
    /// 流式帧使用了非法长度前缀。
    #[error("invalid protobuf length delimiter")]
    InvalidLengthDelimiter,
    /// Frame 或 SessionContent 没有设置 oneof body。
    #[error("{message} does not contain a body")]
    MissingBody {
        /// 缺失 body 的消息名称。
        message: &'static str,
    },
    /// 消息出现在不允许的会话状态。
    #[error("invalid frame in state {state}: {detail}")]
    InvalidState {
        /// 收到消息时的状态名称。
        state: &'static str,
        /// 具体不合法原因。
        detail: &'static str,
    },
    /// Hello 或业务信封字段不满足协议约束。
    #[error("invalid protocol field {field}: {detail}")]
    InvalidField {
        /// 不合法字段路径。
        field: &'static str,
        /// 约束失败原因。
        detail: String,
    },
    /// 双方找不到可接受的协商结果。
    #[error("protocol negotiation failed: {0}")]
    Negotiation(String),
    /// 当前方向的业务消息序号不连续。
    #[error("invalid {direction} sequence: expected {expected}, received {actual}")]
    InvalidSequence {
        /// Client 或 Server 方向。
        direction: &'static str,
        /// 下一个期望序号。
        expected: u64,
        /// 实际收到的序号。
        actual: u64,
    },
    /// 业务 Any 的 type URL 与目标类型不一致。
    #[error("unexpected Any type URL: expected {expected}, received {actual}")]
    AnyTypeMismatch {
        /// 目标 Rust/Protobuf 类型 URL。
        expected: String,
        /// Any 实际携带的类型 URL。
        actual: String,
    },
    /// 安全提供者或会话实现返回失败。
    #[error("security operation failed: {0}")]
    Security(String),
}
