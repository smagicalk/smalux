//! agent 监控上报数据模型。

use super::{
    CoreInfo, DiskInfo, IdentityInfo, NetworkInfo, ProcessInfo, ReportMeta, SocketInfo, Stamped,
    SystemInfo,
};
use serde::{Deserialize, Serialize};

/// agent 监控上报数据。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentReport {
    /// 上报元信息。
    pub meta: ReportMeta,
    /// 身份信息。
    pub identity: IdentityInfo,
    /// 静态系统信息。
    pub system: SystemInfo,
    /// 高频核心指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub core: Option<Stamped<CoreInfo>>,
    /// 磁盘指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk: Option<Stamped<DiskInfo>>,
    /// 网络指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<Stamped<NetworkInfo>>,
    /// 进程汇总指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processes: Option<Stamped<ProcessInfo>>,
    /// Socket 汇总指标。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sockets: Option<Stamped<SocketInfo>>,
}
