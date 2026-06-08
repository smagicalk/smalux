//! 通用指标状态模型。

use serde::{Deserialize, Serialize};

/// 通用指标采集状态。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricStatus {
    /// 最近一次采集成功。
    Ready,
    /// 保留旧值，但最近一次采集失败。
    Stale,
    /// 采集失败，且没有可用旧值。
    Failed,
    /// 当前平台或运行环境不支持该指标。
    Unsupported,
}

impl Default for MetricStatus {
    /// 默认表示尚未拿到有效数据。
    fn default() -> Self {
        Self::Failed
    }
}

/// 指标采集详细级别。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricLevel {
    /// 只采集总数，适合默认高频上报。
    Count,
    /// 采集轻量聚合或 top 列表，适合临时排查。
    Light,
    /// 采集完整明细，适合按需诊断。
    Details,
}

impl Default for MetricLevel {
    /// 默认只上报总数。
    fn default() -> Self {
        Self::Count
    }
}

impl MetricLevel {
    /// 是否需要轻量信息。
    pub fn includes_light(self) -> bool {
        matches!(self, Self::Light)
    }

    /// 是否需要完整明细。
    pub fn includes_details(self) -> bool {
        matches!(self, Self::Details)
    }
}

/// 带采样时间的数据。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Stamped<T> {
    /// Unix 时间戳，单位秒。
    pub sampled_at: u64,
    /// 采样值。
    pub value: T,
}
