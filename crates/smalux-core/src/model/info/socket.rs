//! Socket 信息模型。

use super::{MetricLevel, MetricStatus};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Socket 统计来源。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SocketSource {
    /// 通过完整 socket table 统计。
    SocketTable,
    /// 通过平台聚合计数统计。
    FastCounter,
}

impl Default for SocketSource {
    /// 默认使用完整 socket table 语义。
    fn default() -> Self {
        Self::SocketTable
    }
}

/// Socket 统计准确性。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SocketAccuracy {
    /// 聚合计数，成本低但不同平台语义可能略有差异。
    Aggregate,
    /// 来自 socket table 的逐条统计。
    SocketTable,
}

impl Default for SocketAccuracy {
    /// 默认使用 socket table 精度。
    fn default() -> Self {
        Self::SocketTable
    }
}

/// Socket 汇总信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default, Hash, Eq, PartialEq)]
pub struct SocketInfo {
    /// TCP socket 数量。
    pub tcp: u64,
    /// UDP socket 数量。
    pub udp: u64,
    /// 采集状态。
    pub status: MetricStatus,
    /// 统计来源。
    pub source: SocketSource,
    /// 统计准确性。
    pub accuracy: SocketAccuracy,
    /// 本次采集详细级别。
    pub level: MetricLevel,
    /// 轻量 Socket 聚合信息，仅 `level=light` 时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub light: Option<SocketLightInfo>,
    /// Socket 完整明细，仅 `level=details` 时存在。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<SocketDetailInfo>,
    /// 最近一次失败原因。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// TCP 状态计数。
#[derive(Debug, Clone, Serialize, Deserialize, Default, Hash, Eq, PartialEq)]
pub struct TcpStateCount {
    /// TCP 状态名，使用小写 snake_case。
    pub state: String,
    /// 该状态下的 TCP socket 数量。
    pub count: u64,
}

/// Socket 轻量聚合信息。
#[derive(Debug, Clone, Serialize, Deserialize, Default, Hash, Eq, PartialEq)]
pub struct SocketLightInfo {
    /// TCP 状态分布。
    pub tcp_states: Vec<TcpStateCount>,
}

/// Socket 协议。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SocketProtocol {
    /// TCP socket。
    Tcp,
    /// UDP socket。
    Udp,
}

/// Socket 明细。
#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct SocketDetail {
    /// 协议。
    pub protocol: SocketProtocol,
    /// 本地地址。
    pub local_addr: IpAddr,
    /// 本地端口。
    pub local_port: u16,
    /// 远端地址，UDP 通常为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_addr: Option<IpAddr>,
    /// 远端端口，UDP 通常为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_port: Option<u16>,
    /// TCP 状态，UDP 为空。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// 关联进程 ID 列表；平台或权限不支持时为空。
    pub pids: Vec<u32>,
}

/// Socket 明细采样结果。
#[derive(Debug, Clone, Serialize, Deserialize, Default, Hash, Eq, PartialEq)]
pub struct SocketDetailInfo {
    /// 返回条数上限。
    pub limit: usize,
    /// 是否因为上限截断。
    pub truncated: bool,
    /// Socket 明细列表。
    pub items: Vec<SocketDetail>,
}

impl SocketInfo {
    /// 构造成功状态。
    pub fn ready(tcp: u64, udp: u64, source: SocketSource, accuracy: SocketAccuracy) -> Self {
        Self::ready_with_level(tcp, udp, source, accuracy, MetricLevel::Count, None, None)
    }

    /// 构造带详细级别的成功状态。
    pub fn ready_with_level(
        tcp: u64,
        udp: u64,
        source: SocketSource,
        accuracy: SocketAccuracy,
        level: MetricLevel,
        light: Option<SocketLightInfo>,
        details: Option<SocketDetailInfo>,
    ) -> Self {
        Self {
            tcp,
            udp,
            status: MetricStatus::Ready,
            source,
            accuracy,
            level,
            light,
            details,
            error: None,
        }
    }

    /// 根据旧值构造 stale；没有旧值时降级为 failed，并保留当前请求级别。
    pub fn stale_or_failed(previous: Option<&Self>, level: MetricLevel, error: String) -> Self {
        let Some(previous) = previous
            .filter(|value| matches!(value.status, MetricStatus::Ready | MetricStatus::Stale))
        else {
            return Self {
                status: MetricStatus::Failed,
                level,
                error: Some(error),
                ..Self::default()
            };
        };

        Self {
            tcp: previous.tcp,
            udp: previous.udp,
            status: MetricStatus::Stale,
            source: previous.source.clone(),
            accuracy: previous.accuracy.clone(),
            level,
            light: previous
                .light
                .clone()
                .filter(|_| level == MetricLevel::Light),
            details: previous
                .details
                .clone()
                .filter(|_| level == MetricLevel::Details),
            error: Some(error),
        }
    }

    /// 构造当前平台不支持状态，并保留调用方请求的采集级别。
    pub fn unsupported(level: MetricLevel, error: String) -> Self {
        Self {
            status: MetricStatus::Unsupported,
            level,
            error: Some(error),
            ..Self::default()
        }
    }
}
