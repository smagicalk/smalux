//! 通用采样组配置模型。

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 通用采样组配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GroupConfig {
    /// 是否启用该采样组。
    pub enabled: bool,
    /// 采样间隔。
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

impl GroupConfig {
    /// 创建采样组配置。
    pub(crate) const fn new(enabled: bool, interval: Duration) -> Self {
        Self { enabled, interval }
    }
}

/// 通用采样组配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GroupConfigPatch {
    /// 是否启用该采样组。
    pub enabled: Option<bool>,
    /// 采样间隔。
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
}

impl GroupConfigPatch {
    /// 应用采样组 patch。
    pub(crate) fn apply_to(&self, config: &mut GroupConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(interval) = self.interval {
            config.interval = interval;
        }
    }
}
