//! 导出 job 配置模型。

use super::super::defaults::{
    DEFAULT_BASIC_INFO_JOB_INTERVAL, DEFAULT_REALTIME_REPORT_JOB_INTERVAL,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 单个导出 job 的动态配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct JobConfig {
    /// 是否启用该 job。
    pub enabled: bool,
    /// job 调度间隔。
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// 拿到第一份 report 后是否立即运行一次。
    pub run_on_start: bool,
}

impl JobConfig {
    /// 创建导出 job 配置。
    fn new(enabled: bool, interval: Duration, run_on_start: bool) -> Self {
        Self {
            enabled,
            interval,
            run_on_start,
        }
    }
}

/// 所有导出 job 的动态配置。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct JobsConfig {
    /// 实时上报 job，通常使用主 WebSocket transport。
    pub realtime_report: JobConfig,
    /// Komari basic info 低频上报 job；非 Komari 格式下不会被 adapter 使用。
    pub basic_info: JobConfig,
}

impl Default for JobsConfig {
    /// 默认启用实时上报和 basic info；未声明的 job 会被对应 adapter 忽略。
    fn default() -> Self {
        Self {
            realtime_report: JobConfig::new(true, DEFAULT_REALTIME_REPORT_JOB_INTERVAL, true),
            basic_info: JobConfig::new(true, DEFAULT_BASIC_INFO_JOB_INTERVAL, true),
        }
    }
}

/// 单个导出 job 的配置 patch。
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct JobConfigPatch {
    /// 是否启用该 job。
    pub enabled: Option<bool>,
    /// job 调度间隔。
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// 拿到第一份 report 后是否立即运行一次。
    pub run_on_start: Option<bool>,
}

impl JobConfigPatch {
    /// 应用单个 job patch。
    pub(crate) fn apply_to(&self, config: &mut JobConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(interval) = self.interval {
            config.interval = interval;
        }
        if let Some(run_on_start) = self.run_on_start {
            config.run_on_start = run_on_start;
        }
    }
}

/// 所有导出 job 的配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct JobsConfigPatch {
    /// 实时上报 job patch。
    pub realtime_report: Option<JobConfigPatch>,
    /// Komari basic info job patch。
    pub basic_info: Option<JobConfigPatch>,
}

impl JobsConfigPatch {
    /// 应用导出 jobs patch。
    pub(crate) fn apply_to(&self, config: &mut JobsConfig) {
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
    //! 导出 job 配置测试。

    use super::*;

    /// 验证默认 job 配置符合当前 agent 行为。
    #[test]
    fn default_jobs_keep_realtime_and_basic_info_enabled() {
        let config = JobsConfig::default();

        assert!(config.realtime_report.enabled);
        assert_eq!(
            config.realtime_report.interval,
            DEFAULT_REALTIME_REPORT_JOB_INTERVAL
        );
        assert!(config.basic_info.enabled);
        assert_eq!(config.basic_info.interval, DEFAULT_BASIC_INFO_JOB_INTERVAL);
    }

    /// 验证 patch 可以单独覆盖某个 job。
    #[test]
    fn jobs_patch_updates_single_job() {
        let mut config = JobsConfig::default();
        let patch = JobsConfigPatch {
            basic_info: Some(JobConfigPatch {
                enabled: Some(false),
                interval: Some(Duration::from_secs(60)),
                run_on_start: Some(false),
            }),
            ..JobsConfigPatch::default()
        };

        patch.apply_to(&mut config);

        assert!(!config.basic_info.enabled);
        assert_eq!(config.basic_info.interval, Duration::from_secs(60));
        assert!(!config.basic_info.run_on_start);
        assert!(config.realtime_report.enabled);
    }
}
