//! 监控上报和采集控制模型。

use serde::{Deserialize, Serialize};
use smalux_core::model::info::{
    CoreInfo, DiskInfo, IdentityInfo, MetricLevel, NetworkInfo, ProcessInfo, SocketInfo, Stamped,
};

/// 一次性诊断采集请求。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetricCollectionRequest {
    /// 本次采集级别；缺省使用 agent 当前配置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<MetricLevel>,
    /// 本次返回条数上限；缺省使用 agent 当前配置。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// 心跳附加状态。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Heartbeat {
    /// agent 最近一次完整 report 时间，Unix 时间戳，单位秒。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_report_at: Option<u64>,
    /// agent 最近一次完整 report 序号。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_report_sequence: Option<u64>,
}

/// 增量上报内容。
///
/// 字段缺省表示该字段相对上一份状态没有变化；可选采集组字段出现 `null` 表示该组被关闭。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeltaReport {
    /// 增量基于的上一条消息序号。
    pub base_sequence: u64,
    /// 本次增量生成时间，Unix 时间戳，单位秒。
    pub report_at: u64,
    /// 身份信息变化。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityInfo>,
    /// 核心指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub core: Option<Option<Stamped<CoreInfo>>>,
    /// 磁盘指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk: Option<Option<Stamped<DiskInfo>>>,
    /// 网络指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<Option<Stamped<NetworkInfo>>>,
    /// 进程汇总指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processes: Option<Option<Stamped<ProcessInfo>>>,
    /// Socket 汇总指标变化；`Some(None)` 表示该组被关闭。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sockets: Option<Option<Stamped<SocketInfo>>>,
}

/// 请求完整快照。
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// 请求原因，便于 agent 日志定位。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
