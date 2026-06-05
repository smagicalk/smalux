//! 上报配置模型。

use super::super::defaults::{
    DEFAULT_REPORT_FORCE_SNAPSHOT_MIN_INTERVAL, DEFAULT_REPORT_HEARTBEAT_INTERVAL,
    DEFAULT_REPORT_INTERVAL, DEFAULT_REPORT_SNAPSHOT_INTERVAL,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 上报配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReportConfig {
    /// 是否启用上报。
    pub enabled: bool,
    /// 上报间隔。
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// 是否启用业务级心跳。
    pub heartbeat_enabled: bool,
    /// 业务级心跳间隔。
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
    /// 是否启用 delta 增量上报。
    pub delta_enabled: bool,
    /// 启用 delta 后，强制定期发送完整 snapshot 的间隔。
    #[serde(with = "humantime_serde")]
    pub snapshot_interval: Duration,
    /// server 请求强制 snapshot 的最小响应间隔。
    #[serde(with = "humantime_serde")]
    pub force_snapshot_min_interval: Duration,
}

impl Default for ReportConfig {
    /// 默认启用定时上报。
    fn default() -> Self {
        Self {
            enabled: true,
            interval: DEFAULT_REPORT_INTERVAL,
            heartbeat_enabled: false,
            heartbeat_interval: DEFAULT_REPORT_HEARTBEAT_INTERVAL,
            delta_enabled: false,
            snapshot_interval: DEFAULT_REPORT_SNAPSHOT_INTERVAL,
            force_snapshot_min_interval: DEFAULT_REPORT_FORCE_SNAPSHOT_MIN_INTERVAL,
        }
    }
}

/// 上报配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReportConfigPatch {
    /// 是否启用上报。
    pub enabled: Option<bool>,
    /// 上报间隔。
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// 是否启用业务级心跳。
    pub heartbeat_enabled: Option<bool>,
    /// 业务级心跳间隔。
    #[serde(default, with = "humantime_serde")]
    pub heartbeat_interval: Option<Duration>,
    /// 是否启用 delta 增量上报。
    pub delta_enabled: Option<bool>,
    /// 启用 delta 后，强制定期发送完整 snapshot 的间隔。
    #[serde(default, with = "humantime_serde")]
    pub snapshot_interval: Option<Duration>,
    /// server 请求强制 snapshot 的最小响应间隔。
    #[serde(default, with = "humantime_serde")]
    pub force_snapshot_min_interval: Option<Duration>,
}

impl ReportConfigPatch {
    /// 应用上报配置 patch。
    pub(crate) fn apply_to(&self, config: &mut ReportConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(interval) = self.interval {
            config.interval = interval;
        }
        if let Some(heartbeat_enabled) = self.heartbeat_enabled {
            config.heartbeat_enabled = heartbeat_enabled;
        }
        if let Some(heartbeat_interval) = self.heartbeat_interval {
            config.heartbeat_interval = heartbeat_interval;
        }
        if let Some(delta_enabled) = self.delta_enabled {
            config.delta_enabled = delta_enabled;
        }
        if let Some(snapshot_interval) = self.snapshot_interval {
            config.snapshot_interval = snapshot_interval;
        }
        if let Some(force_snapshot_min_interval) = self.force_snapshot_min_interval {
            config.force_snapshot_min_interval = force_snapshot_min_interval;
        }
    }
}

#[cfg(test)]
mod tests {
    //! 上报配置模型测试。

    use super::*;

    /// 验证默认保持完整快照兼容模式。
    #[test]
    fn default_report_config_keeps_snapshot_mode() {
        let config = ReportConfig::default();

        assert!(config.enabled);
        assert!(!config.heartbeat_enabled);
        assert!(!config.delta_enabled);
        assert_eq!(config.interval, DEFAULT_REPORT_INTERVAL);
    }

    /// 验证 patch 可以启用业务心跳和 delta。
    #[test]
    fn report_patch_updates_policy_fields() {
        let mut config = ReportConfig::default();
        let patch = ReportConfigPatch {
            heartbeat_enabled: Some(true),
            heartbeat_interval: Some(Duration::from_secs(10)),
            delta_enabled: Some(true),
            snapshot_interval: Some(Duration::from_secs(60)),
            force_snapshot_min_interval: Some(Duration::from_secs(5)),
            ..ReportConfigPatch::default()
        };

        patch.apply_to(&mut config);

        assert!(config.heartbeat_enabled);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert!(config.delta_enabled);
        assert_eq!(config.snapshot_interval, Duration::from_secs(60));
        assert_eq!(config.force_snapshot_min_interval, Duration::from_secs(5));
    }
}
