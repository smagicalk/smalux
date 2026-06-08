//! 公网 IP 信息模型。

use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// 公网 IP 来源。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicIpSource {
    /// 从网卡公网候选地址获取。
    InterfaceCandidate,
    /// 从外部 HTTP 服务获取。
    ExternalHttp,
}

impl Default for PublicIpSource {
    /// 默认按外部 HTTP 服务处理。
    fn default() -> Self {
        Self::ExternalHttp
    }
}

/// 公网 IP 获取状态。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PublicIpStatus {
    /// 未启用公网 IP 采集。
    Disabled,
    /// 尚未完成首次尝试。
    Pending,
    /// 最近一次获取成功。
    Ready,
    /// 获取失败，且没有可用旧值。
    Failed,
    /// 保留旧公网 IP，但最近一次刷新失败。
    Stale,
}

impl Default for PublicIpStatus {
    /// 默认处于等待采集状态。
    fn default() -> Self {
        Self::Pending
    }
}

/// 公网 IP 信息。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct PublicIpInfo {
    /// 公网 IP 获取状态。
    pub status: PublicIpStatus,
    /// 公网 IP 地址，获取成功或使用旧值时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<IpAddr>,
    /// 公网 IP 来源，获取成功或使用旧值时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<PublicIpSource>,
    /// 成功采样时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at: Option<u64>,
    /// 外部服务校验时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_at: Option<u64>,
    /// 最近一次尝试时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<u64>,
    /// 最近一次失败原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Default for PublicIpInfo {
    /// 默认表示尚未完成公网 IP 采集。
    fn default() -> Self {
        Self {
            status: PublicIpStatus::Pending,
            ip: None,
            source: None,
            sampled_at: None,
            verified_at: None,
            last_attempt_at: None,
            error: None,
        }
    }
}

impl PublicIpInfo {
    /// 构造禁用状态。
    pub fn disabled() -> Self {
        Self {
            status: PublicIpStatus::Disabled,
            ..Self::default()
        }
    }

    /// 构造获取成功状态。
    pub fn ready(
        ip: IpAddr,
        source: PublicIpSource,
        sampled_at: u64,
        verified_at: Option<u64>,
    ) -> Self {
        Self {
            status: PublicIpStatus::Ready,
            ip: Some(ip),
            source: Some(source),
            sampled_at: Some(sampled_at),
            verified_at,
            last_attempt_at: Some(sampled_at),
            error: None,
        }
    }

    /// 构造获取失败状态。
    pub fn failed(error: String, last_attempt_at: u64) -> Self {
        Self {
            status: PublicIpStatus::Failed,
            last_attempt_at: Some(last_attempt_at),
            error: Some(error),
            ..Self::default()
        }
    }

    /// 根据旧值构造 stale；没有旧 IP 时降级为 failed。
    pub fn stale_or_failed(previous: &Self, error: String, last_attempt_at: u64) -> Self {
        let Some(ip) = previous.ip else {
            return Self::failed(error, last_attempt_at);
        };

        Self {
            status: PublicIpStatus::Stale,
            ip: Some(ip),
            source: previous.source.clone(),
            sampled_at: previous.sampled_at,
            verified_at: previous.verified_at,
            last_attempt_at: Some(last_attempt_at),
            error: Some(error),
        }
    }
}
