//! agent 上报元信息模型。

use serde::{Deserialize, Serialize};

/// 上报模型版本。
pub const AGENT_REPORT_SCHEMA_VERSION: u16 = 5;

/// agent 上报元信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReportMeta {
    /// 上报模型版本。
    pub schema_version: u16,
    /// agent 版本。
    pub agent_version: String,
    /// 本次上报时间，Unix 时间戳，单位秒。
    pub report_at: u64,
}
