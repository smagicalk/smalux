//! 远程网络探测协议模型。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 远程网络探测类型。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteProbeType {
    /// TCP 连接耗时探测。
    Tcp,
    /// HTTP/HTTPS 请求耗时探测。
    Http,
    /// ICMP 探测。
    Icmp,
}

impl RemoteProbeType {
    /// 返回稳定字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Http => "http",
            Self::Icmp => "icmp",
        }
    }
}

/// 远程网络探测请求。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeRequest {
    /// server 侧生成的探测任务 ID；兼容协议可能使用数字或字符串。
    pub task_id: Value,
    /// 探测类型。
    pub probe_type: RemoteProbeType,
    /// 探测目标，TCP 使用 `host:port`，HTTP 使用 URL 或 host。
    pub target: String,
}

/// 远程网络探测结果。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteProbeResult {
    /// server 侧生成的探测任务 ID。
    pub task_id: Value,
    /// 探测类型。
    pub probe_type: RemoteProbeType,
    /// 探测目标。
    pub target: String,
    /// 成功时为耗时毫秒，失败、禁用或限频时为 -1。
    pub value: i64,
    /// 开始时间，Unix 时间戳，单位秒。
    pub started_at: u64,
    /// 完成时间，Unix 时间戳，单位秒。
    pub finished_at: u64,
    /// 执行耗时，单位毫秒。
    pub duration_ms: u64,
    /// 失败、禁用或限频原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
