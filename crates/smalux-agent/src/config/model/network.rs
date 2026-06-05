//! 网络采样配置模型。

use super::super::defaults::DEFAULT_NETWORK_INTERVAL;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;

/// 网络采样配置。
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NetworkConfig {
    /// 是否启用网络采样。
    pub enabled: bool,
    /// 网络采样间隔。
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// 是否上报单网卡明细。
    pub include_per_interface: bool,
    /// 只统计这些网卡；为空表示统计全部网卡。
    pub include_interfaces: Vec<String>,
    /// 排除这些网卡；当 include_interfaces 非空时不参与过滤。
    pub exclude_interfaces: Vec<String>,
}

impl Default for NetworkConfig {
    /// 默认启用网络采样，并保留单网卡明细。
    fn default() -> Self {
        Self {
            enabled: true,
            interval: DEFAULT_NETWORK_INTERVAL,
            include_per_interface: true,
            include_interfaces: Vec::new(),
            exclude_interfaces: Vec::new(),
        }
    }
}

/// 网络采样配置 patch。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NetworkConfigPatch {
    /// 是否启用网络采样。
    pub enabled: Option<bool>,
    /// 网络采样间隔。
    #[serde(default, with = "humantime_serde")]
    pub interval: Option<Duration>,
    /// 是否上报单网卡明细。
    pub include_per_interface: Option<bool>,
    /// 只统计这些网卡；为空表示统计全部网卡。
    #[serde(default)]
    pub include_interfaces: Option<Vec<String>>,
    /// 排除这些网卡；include_interfaces 非空时忽略。
    #[serde(default)]
    pub exclude_interfaces: Option<Vec<String>>,
}

impl NetworkConfigPatch {
    /// 应用网络配置 patch。
    pub(crate) fn apply_to(&self, config: &mut NetworkConfig) {
        if let Some(enabled) = self.enabled {
            config.enabled = enabled;
        }
        if let Some(interval) = self.interval {
            config.interval = interval;
        }
        if let Some(include_per_interface) = self.include_per_interface {
            config.include_per_interface = include_per_interface;
        }
        if let Some(include_interfaces) = self.include_interfaces.clone() {
            config.include_interfaces = normalize_interface_names(include_interfaces);
        }
        if let Some(exclude_interfaces) = self.exclude_interfaces.clone() {
            config.exclude_interfaces = normalize_interface_names(exclude_interfaces);
        }
    }
}

/// 规范化网卡名称：去掉首尾空白、过滤空值，并按首次出现顺序去重。
fn normalize_interface_names(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();

    for value in values {
        let value = value.trim().to_string();
        if value.is_empty() {
            continue;
        }
        if seen.insert(value.clone()) {
            normalized.push(value);
        }
    }

    normalized
}

#[cfg(test)]
mod tests {
    //! 网络配置模型测试。

    use super::*;

    /// 验证网络网卡过滤配置会去空白、去空值并保留首次出现顺序。
    #[test]
    fn patch_normalizes_interface_filters() {
        let mut config = NetworkConfig::default();
        let patch = NetworkConfigPatch {
            include_interfaces: Some(vec![
                " Ethernet ".to_string(),
                "Ethernet".to_string(),
                " ".to_string(),
                "Wi-Fi".to_string(),
            ]),
            exclude_interfaces: Some(vec![
                " Loopback ".to_string(),
                "Loopback".to_string(),
                "".to_string(),
            ]),
            ..NetworkConfigPatch::default()
        };

        patch.apply_to(&mut config);

        assert_eq!(config.include_interfaces, ["Ethernet", "Wi-Fi"]);
        assert_eq!(config.exclude_interfaces, ["Loopback"]);
    }

    /// 验证 server patch 可以用空列表清空网卡过滤配置。
    #[test]
    fn patch_can_clear_interface_filters() {
        let mut config = NetworkConfig {
            include_interfaces: vec!["Ethernet".to_string()],
            exclude_interfaces: vec!["Loopback".to_string()],
            ..NetworkConfig::default()
        };
        let patch = NetworkConfigPatch {
            include_interfaces: Some(Vec::new()),
            exclude_interfaces: Some(Vec::new()),
            ..NetworkConfigPatch::default()
        };

        patch.apply_to(&mut config);

        assert!(config.include_interfaces.is_empty());
        assert!(config.exclude_interfaces.is_empty());
    }
}
