//! Telemetry 内部事件模型。

use crate::collect::{CoreSample, DiskSample, NetworkSample, ProcessSample, SocketSample};
use smalux_core::model::info::IdentityInfo;
use smalux_protocol::OutboundReport;

/// 采集层提交给 reporter 的最新指标更新。
///
/// 这个事件不是网络发送格式，而是 agent 内部的状态变更。reporter 按收到顺序
/// 更新自己的 latest 缓存，再决定是否生成 snapshot/delta/heartbeat。
#[derive(Debug, Clone)]
pub(crate) enum TelemetryUpdate {
    /// 同一采集调度点产生的一组指标更新。
    Batch(Vec<TelemetryUpdate>),
    /// 公网 IP 低频刷新结果；失败时 reporter 会尽量保留旧公网 IP。
    IdentityRefresh(IdentityInfo),
    /// 核心指标更新。
    Core(CoreSample),
    /// 磁盘指标更新。
    Disk(DiskSample),
    /// 网络指标更新。
    Network(NetworkSample),
    /// 进程指标更新。
    Processes(ProcessSample),
    /// Socket 指标更新。
    Sockets(SocketSample),
}

impl TelemetryUpdate {
    /// 返回稳定日志名称。
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Batch(_) => "batch",
            Self::IdentityRefresh(_) => "identity_refresh",
            Self::Core(_) => "core",
            Self::Disk(_) => "disk",
            Self::Network(_) => "network",
            Self::Processes(_) => "processes",
            Self::Sockets(_) => "sockets",
        }
    }
}

/// 已经完成聚合、等待导出层发送的业务上报事件。
#[derive(Debug, Clone)]
pub(crate) struct ReportEvent {
    /// 待导出的内部协议语义。
    outbound: OutboundReport,
}

impl ReportEvent {
    /// 创建上报事件。
    pub(crate) fn new(outbound: OutboundReport) -> Self {
        Self { outbound }
    }

    /// 取出内部协议语义，交给 export adapter 编码。
    pub(crate) fn into_outbound(self) -> OutboundReport {
        self.outbound
    }
}
