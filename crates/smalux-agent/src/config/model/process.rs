//! 进程采集配置模型。

use serde::{Deserialize, Serialize};
use smalux_core::model::info::MetricLevel;
use std::time::Duration;

/// 进程采集配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProcessConfig {
    /// 是否启用进程采样。
    pub enabled: bool,
    /// 采样间隔。
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// 采集详细级别。
    pub level: MetricLevel,
    /// light/details 返回条数上限。
    pub limit: usize,
}

impl ProcessConfig {
    /// 创建进程采集配置。
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

/// 进程采集配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ProcessConfigPatch {
    /// 是否启用进程采样。
    pub enabled: Option<bool>,
    /// 采样间隔。
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// 采集详细级别。
    pub level: Option<MetricLevel>,
    /// light/details 返回条数上限。
    pub limit: Option<usize>,
}

impl ProcessConfigPatch {
    /// 应用进程采集配置 patch。
    pub(crate) fn apply_to(&self, config: &mut ProcessConfig) {
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
