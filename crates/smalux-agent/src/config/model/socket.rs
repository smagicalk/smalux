//! Socket 采集配置模型。

use serde::{Deserialize, Serialize};
use smalux_core::model::info::MetricLevel;
use std::time::Duration;

/// Socket 采集配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SocketConfig {
    /// 是否启用 Socket 采样。
    pub enabled: bool,
    /// 采样间隔。
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// 采集详细级别。
    pub level: MetricLevel,
    /// details 返回条数上限。
    pub limit: usize,
}

impl SocketConfig {
    /// 创建 Socket 采集配置。
    pub(crate) const fn new(
        enabled: bool,
        interval: Duration,
        level: MetricLevel,
        limit: usize,
    ) -> Self {
        Self {
            enabled,
            interval,
            level,
            limit,
        }
    }
}

/// Socket 采集配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct SocketConfigPatch {
    /// 是否启用 Socket 采样。
    pub enabled: Option<bool>,
    /// 采样间隔。
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// 采集详细级别。
    pub level: Option<MetricLevel>,
    /// details 返回条数上限。
    pub limit: Option<usize>,
}

impl SocketConfigPatch {
    /// 应用 Socket 采集配置 patch。
    pub(crate) fn apply_to(&self, config: &mut SocketConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(interval) = self.interval {
            config.interval = interval;
        }
        if let Some(level) = self.level {
            config.level = level;
        }
        if let Some(limit) = self.limit {
            config.limit = limit;
        }
    }
}
