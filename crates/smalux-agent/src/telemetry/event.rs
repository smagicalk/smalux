//! Telemetry 内部事件模型。

use smalux_protocol::OutboundReport;

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
