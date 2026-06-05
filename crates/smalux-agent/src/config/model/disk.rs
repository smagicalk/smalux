//! 磁盘采样配置模型。

use super::super::defaults::DEFAULT_DISK_INTERVAL;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 磁盘采样配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DiskConfig {
    /// 是否启用磁盘采样。
    pub enabled: bool,
    /// 磁盘采样间隔。
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// 是否上报单磁盘明细。
    pub include_per_device: bool,
}

impl Default for DiskConfig {
    /// 默认启用磁盘采样，并保留单磁盘明细。
    fn default() -> Self {
        Self {
            enabled: true,
            interval: DEFAULT_DISK_INTERVAL,
            include_per_device: true,
        }
    }
}

/// 磁盘采样配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiskConfigPatch {
    /// 是否启用磁盘采样。
    pub enabled: Option<bool>,
    /// 磁盘采样间隔。
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// 是否上报单磁盘明细。
    pub include_per_device: Option<bool>,
}

impl DiskConfigPatch {
    /// 应用磁盘配置 patch。
    pub(crate) fn apply_to(&self, config: &mut DiskConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(interval) = self.interval {
            config.interval = interval;
        }
        if let Some(include_per_device) = self.include_per_device {
            config.include_per_device = include_per_device;
        }
    }
}
