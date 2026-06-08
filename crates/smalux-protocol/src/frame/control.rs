//! 协议确认和错误模型。

use serde::{Deserialize, Serialize};

/// 协议确认信息。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Ack {
    /// 被确认的消息序号。
    pub sequence: u64,
}

/// 协议级错误。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProtocolError {
    /// 被错误关联的对端消息序号。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// 稳定错误码。
    pub code: String,
    /// 面向日志和调试的错误说明。
    pub message: String,
}
