//! 公网 IP 采集配置模型。

use super::super::defaults::{
    DEFAULT_PUBLIC_IP_MAX_CONCURRENCY, DEFAULT_PUBLIC_IP_REFRESH_INTERVAL,
    DEFAULT_PUBLIC_IP_RETRY_INTERVAL, DEFAULT_PUBLIC_IP_STARTUP_TIMEOUT,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// 公网 IP 采集配置。
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PublicIpConfig {
    /// 是否启用公网 IP 采集。
    pub enabled: bool,
    /// 第一包上报是否必须等待公网 IP。
    pub required_for_first_report: bool,
    /// 是否优先使用网卡上的公网候选地址。
    pub prefer_interface_candidate: bool,
    /// 使用网卡候选地址后是否继续用外部服务校验。
    pub verify_interface_candidate: bool,
    /// 启动时单轮外部探测超时。
    #[serde(with = "humantime_serde")]
    pub startup_timeout: Duration,
    /// 失败后的重试间隔。
    #[serde(with = "humantime_serde")]
    pub retry_interval: Duration,
    /// 成功后的低频刷新间隔。
    #[serde(with = "humantime_serde")]
    pub refresh_interval: Duration,
    /// 外部公网 IP 服务最大并发数。
    pub max_concurrency: usize,
}

impl Default for PublicIpConfig {
    /// 默认采集公网 IP，但不阻塞第一包上报。
    fn default() -> Self {
        Self {
            enabled: true,
            required_for_first_report: false,
            prefer_interface_candidate: true,
            verify_interface_candidate: true,
            startup_timeout: DEFAULT_PUBLIC_IP_STARTUP_TIMEOUT,
            retry_interval: DEFAULT_PUBLIC_IP_RETRY_INTERVAL,
            refresh_interval: DEFAULT_PUBLIC_IP_REFRESH_INTERVAL,
            max_concurrency: DEFAULT_PUBLIC_IP_MAX_CONCURRENCY,
        }
    }
}

/// 公网 IP 采集配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PublicIpConfigPatch {
    /// 是否启用公网 IP 采集。
    pub enabled: Option<bool>,
    /// 第一包上报是否必须等待公网 IP。
    pub required_for_first_report: Option<bool>,
    /// 是否优先使用网卡上的公网候选地址。
    pub prefer_interface_candidate: Option<bool>,
    /// 使用网卡候选地址后是否继续用外部服务校验。
    pub verify_interface_candidate: Option<bool>,
    /// 启动时单轮外部探测超时。
    #[serde(default, with = "humantime_serde")]
    pub startup_timeout: Option<Duration>,
    /// 失败后的重试间隔。
    #[serde(default, with = "humantime_serde")]
    pub retry_interval: Option<Duration>,
    /// 成功后的低频刷新间隔。
    #[serde(default, with = "humantime_serde")]
    pub refresh_interval: Option<Duration>,
    /// 外部公网 IP 服务最大并发数。
    pub max_concurrency: Option<usize>,
}

impl PublicIpConfigPatch {
    /// 应用公网 IP 配置 patch。
    pub(crate) fn apply_to(&self, config: &mut PublicIpConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(required_for_first_report) = self.required_for_first_report {
            config.required_for_first_report = required_for_first_report;
        }
        if let Some(prefer_interface_candidate) = self.prefer_interface_candidate {
            config.prefer_interface_candidate = prefer_interface_candidate;
        }
        if let Some(verify_interface_candidate) = self.verify_interface_candidate {
            config.verify_interface_candidate = verify_interface_candidate;
        }
        if let Some(startup_timeout) = self.startup_timeout {
            config.startup_timeout = startup_timeout;
        }
        if let Some(retry_interval) = self.retry_interval {
            config.retry_interval = retry_interval;
        }
        if let Some(refresh_interval) = self.refresh_interval {
            config.refresh_interval = refresh_interval;
        }
        if let Some(max_concurrency) = self.max_concurrency {
            config.max_concurrency = max_concurrency;
        }
    }
}
