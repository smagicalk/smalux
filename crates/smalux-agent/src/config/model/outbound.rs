//! 出站业务事件配置模型。

use super::super::defaults::DEFAULT_BASIC_INFO_REFRESH_INTERVAL;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 实时 report 出站配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RealtimeReportOutputConfig {
    /// 是否启用实时 report 出站。
    pub enabled: bool,
    /// 第一份 report ready 后是否立即发送。
    pub send_on_start: bool,
}

impl RealtimeReportOutputConfig {
    /// 创建实时 report 出站配置。
    const fn new(enabled: bool, send_on_start: bool) -> Self {
        Self {
            enabled,
            send_on_start,
        }
    }
}

/// basic info 出站配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct BasicInfoOutputConfig {
    /// 是否启用 basic info 出站事件。
    pub enabled: bool,
    /// basic info 刷新事件生成间隔。
    #[serde(with = "humantime_serde")]
    pub refresh_interval: Duration,
    /// 第一份 telemetry ready 后是否立即发送。
    pub send_on_start: bool,
}

impl BasicInfoOutputConfig {
    /// 创建 basic info 出站配置。
    const fn new(enabled: bool, refresh_interval: Duration, send_on_start: bool) -> Self {
        Self {
            enabled,
            refresh_interval,
            send_on_start,
        }
    }
}

/// 所有出站业务事件的动态配置。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct OutboundConfig {
    /// 实时 report 出站配置，通常使用主 WebSocket transport。
    pub realtime_report: RealtimeReportOutputConfig,
    /// basic info 出站配置；当前用于 Komari uploadBasicInfo。
    pub basic_info: BasicInfoOutputConfig,
}

impl Default for OutboundConfig {
    /// 默认启用实时 report 和 basic info；不支持的 adapter 会跳过对应事件。
    fn default() -> Self {
        Self {
            realtime_report: RealtimeReportOutputConfig::new(true, true),
            basic_info: BasicInfoOutputConfig::new(true, DEFAULT_BASIC_INFO_REFRESH_INTERVAL, true),
        }
    }
}

/// 实时 report 出站配置 patch。
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RealtimeReportOutputConfigPatch {
    /// 是否启用实时 report 出站。
    pub enabled: Option<bool>,
    /// 第一份 report ready 后是否立即发送。
    pub send_on_start: Option<bool>,
}

impl RealtimeReportOutputConfigPatch {
    /// 应用实时 report 出站 patch。
    pub(crate) fn apply_to(&self, config: &mut RealtimeReportOutputConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(send_on_start) = self.send_on_start {
            config.send_on_start = send_on_start;
        }
    }
}

/// basic info 出站配置 patch。
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BasicInfoOutputConfigPatch {
    /// 是否启用 basic info 出站事件。
    pub enabled: Option<bool>,
    /// basic info 刷新事件生成间隔。
    #[serde(default, with = "humantime_serde")]
    pub refresh_interval: Option<Duration>,
    /// 第一份 telemetry ready 后是否立即发送。
    pub send_on_start: Option<bool>,
}

impl BasicInfoOutputConfigPatch {
    /// 应用 basic info 出站 patch。
    pub(crate) fn apply_to(&self, config: &mut BasicInfoOutputConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(refresh_interval) = self.refresh_interval {
            config.refresh_interval = refresh_interval;
        }
        if let Some(send_on_start) = self.send_on_start {
            config.send_on_start = send_on_start;
        }
    }
}

/// 所有出站业务事件的配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutboundConfigPatch {
    /// 实时 report 出站 patch。
    pub realtime_report: Option<RealtimeReportOutputConfigPatch>,
    /// basic info 出站 patch。
    pub basic_info: Option<BasicInfoOutputConfigPatch>,
}

impl OutboundConfigPatch {
    /// 应用出站业务事件 patch。
    pub(crate) fn apply_to(&self, config: &mut OutboundConfig) {
        if let Some(realtime_report) = self.realtime_report {
            realtime_report.apply_to(&mut config.realtime_report);
        }
        if let Some(basic_info) = self.basic_info {
            basic_info.apply_to(&mut config.basic_info);
        }
    }
}

#[cfg(test)]
mod tests {
    //! 出站业务事件配置测试。

    use super::*;

    /// 验证默认出站配置符合当前 agent 行为。
    #[test]
    fn default_outbound_keeps_realtime_and_basic_info_enabled() {
        let config = OutboundConfig::default();

        assert!(config.realtime_report.enabled);
        assert!(config.realtime_report.send_on_start);
        assert!(config.basic_info.enabled);
        assert_eq!(
            config.basic_info.refresh_interval,
            DEFAULT_BASIC_INFO_REFRESH_INTERVAL
        );
    }

    /// 验证 patch 可以单独覆盖某个出站事件。
    #[test]
    fn outbound_patch_updates_single_output() {
        let mut config = OutboundConfig::default();
        let patch = OutboundConfigPatch {
            basic_info: Some(BasicInfoOutputConfigPatch {
                enabled: Some(false),
                refresh_interval: Some(Duration::from_secs(60)),
                send_on_start: Some(false),
            }),
            ..OutboundConfigPatch::default()
        };

        patch.apply_to(&mut config);

        assert!(!config.basic_info.enabled);
        assert_eq!(config.basic_info.refresh_interval, Duration::from_secs(60));
        assert!(!config.basic_info.send_on_start);
        assert!(config.realtime_report.enabled);
    }

    /// 验证 realtime report patch 不包含刷新间隔字段。
    #[test]
    fn outbound_patch_updates_realtime_report_without_refresh_interval() {
        let mut config = OutboundConfig::default();
        let patch = OutboundConfigPatch {
            realtime_report: Some(RealtimeReportOutputConfigPatch {
                enabled: Some(false),
                send_on_start: Some(false),
            }),
            ..OutboundConfigPatch::default()
        };

        patch.apply_to(&mut config);

        assert!(!config.realtime_report.enabled);
        assert!(!config.realtime_report.send_on_start);
    }
}
