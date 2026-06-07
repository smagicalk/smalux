//! 导出路由。
//!
//! Router 负责把内部上报语义交给 adapter 编码，并把编码后的请求投递给 transport hub。
//! 后续要把发送队列移动到长期 worker 时，可以优先改这里，service 层不需要理解每种 transport。

use super::{ExportDeliveryId, ProtocolAdapter, TransportHub, TransportPlan, TransportRequest};
use crate::config::model::ExportConfig;
use crate::service::outbound::{
    BasicInfoEnvelope, ControlAckEnvelope, ControlErrorEnvelope, RemoteProbeResultEnvelope,
    RemoteTaskResultEnvelope,
};
use smalux_protocol::OutboundReport;

/// 导出路由器。
pub(crate) struct ExportRouter {
    /// 当前导出格式 adapter。
    adapter: Box<dyn ProtocolAdapter + Send + Sync>,
}

impl ExportRouter {
    /// 创建导出路由器。
    pub(crate) fn new(adapter: Box<dyn ProtocolAdapter + Send + Sync>) -> Self {
        Self { adapter }
    }

    /// 根据当前 adapter 生成 transport plan。
    pub(crate) fn transport_plan(
        &mut self,
        config: &ExportConfig,
    ) -> anyhow::Result<TransportPlan> {
        self.adapter.transport_plan(config)
    }

    /// 将内部上报语义编码成 transport 请求。
    pub(crate) fn encode_report(
        &mut self,
        delivery_id: ExportDeliveryId,
        outbound: &OutboundReport,
    ) -> anyhow::Result<Vec<TransportRequest>> {
        self.adapter.encode_report(delivery_id, outbound)
    }

    /// 编码并投递单个 delivery 的上报事件。
    pub(crate) async fn send_report(
        &mut self,
        transport_hub: &mut TransportHub,
        delivery_id: ExportDeliveryId,
        outbound: &OutboundReport,
    ) -> anyhow::Result<usize> {
        let requests = self.encode_report(delivery_id, outbound)?;
        let request_count = requests.len();

        // adapter 可以返回 0 条请求，表示当前格式不支持或明确跳过该事件。
        // router 只负责投递，不把“跳过”当错误。
        for request in requests {
            transport_hub.enqueue(delivery_id, outbound.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递低频基础信息事件。
    pub(crate) async fn send_basic_info(
        &mut self,
        transport_hub: &mut TransportHub,
        info: &BasicInfoEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_basic_info(info)?;
        let request_count = requests.len();

        for request in requests {
            transport_hub.enqueue(ExportDeliveryId::BasicInfo, info.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递远程任务结果。
    pub(crate) async fn send_remote_task_result(
        &mut self,
        transport_hub: &mut TransportHub,
        result: &RemoteTaskResultEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_remote_task_result(result)?;
        let request_count = requests.len();

        for request in requests {
            transport_hub.enqueue(ExportDeliveryId::RemoteTaskResult, result.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递远程探测结果。
    pub(crate) async fn send_remote_probe_result(
        &mut self,
        transport_hub: &mut TransportHub,
        result: &RemoteProbeResultEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_remote_probe_result(result)?;
        let request_count = requests.len();

        for request in requests {
            transport_hub.enqueue(
                ExportDeliveryId::RemoteProbeResult,
                result.sequence,
                request,
            )?;
        }

        Ok(request_count)
    }

    /// 编码并投递控制命令确认。
    pub(crate) async fn send_control_ack(
        &mut self,
        transport_hub: &mut TransportHub,
        ack: &ControlAckEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_control_ack(ack)?;
        let request_count = requests.len();

        for request in requests {
            transport_hub.enqueue(ExportDeliveryId::ControlAck, ack.sequence, request)?;
        }

        Ok(request_count)
    }

    /// 编码并投递控制命令错误。
    pub(crate) async fn send_control_error(
        &mut self,
        transport_hub: &mut TransportHub,
        error: &ControlErrorEnvelope,
    ) -> anyhow::Result<usize> {
        let requests = self.adapter.encode_control_error(error)?;
        let request_count = requests.len();

        for request in requests {
            transport_hub.enqueue(ExportDeliveryId::ControlError, error.sequence, request)?;
        }

        Ok(request_count)
    }
}

#[cfg(test)]
mod tests {
    //! 导出路由测试。

    use super::*;
    use crate::config::model::ExportFormat;
    use crate::export::{TransportRequest, build_protocol_adapter};
    use smalux_core::model::info::AgentReport;

    /// 验证 router 会复用 adapter 的编码结果。
    #[test]
    fn router_encodes_report_with_current_adapter() {
        let mut report = AgentReport::default();
        report.identity.agent_id = "agent-1".to_string();
        let outbound = smalux_protocol::OutboundReport::snapshot(1, 100, report);
        let mut router = ExportRouter::new(build_protocol_adapter(ExportFormat::SmaluxJson));

        let requests = router
            .encode_report(ExportDeliveryId::RealtimeReport, &outbound)
            .unwrap();

        assert!(matches!(
            requests.as_slice(),
            [TransportRequest::WebSocketBinary { sequence: 1, .. }]
        ));
    }
}
