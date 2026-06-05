//! 导出 plan、delivery 和 transport 请求模型。

use super::{http, ws};
use crate::config::model::OutboundConfig;
use std::time::Duration;

/// 导出 transport 标识。
///
/// 当前实时上报和辅助 HTTP 各自有稳定 transport ID。
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub(crate) enum TransportId {
    /// 实时上报通道。
    RealtimeReport,
    /// 辅助 HTTP 通道，当前承载 Komari basic info 和 task result。
    AuxiliaryHttp,
}

impl TransportId {
    /// 返回稳定日志名称。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RealtimeReport => "realtime_report",
            Self::AuxiliaryHttp => "auxiliary_http",
        }
    }
}

/// 单个 transport 的启动规格。
pub(crate) enum TransportSpec {
    /// WebSocket 长连接规格。
    WebSocket {
        /// transport ID。
        id: TransportId,
        /// WebSocket 配置。
        config: ws::WebSocketConfig,
        /// 是否在 pipeline 连接阶段立即建立 WebSocket。
        connect_on_start: bool,
    },
    /// HTTP 短请求规格。
    Http {
        /// transport ID。
        id: TransportId,
        /// HTTP transport 配置。
        config: http::HttpConfig,
    },
}

impl TransportSpec {
    /// 返回 transport ID。
    pub(crate) fn id(&self) -> TransportId {
        match self {
            Self::WebSocket { id, .. } => *id,
            Self::Http { id, .. } => *id,
        }
    }
}

/// 导出 delivery ID。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportDeliveryId {
    /// 实时上报 delivery。
    RealtimeReport,
    /// Komari basic info 低频上报 delivery。
    BasicInfo,
    /// 远程任务结果即时回传。
    RemoteTaskResult,
    /// 远程网络探测结果即时回传。
    RemoteProbeResult,
    /// 控制命令确认即时回传。
    ControlAck,
    /// 控制命令错误即时回传。
    ControlError,
}

impl ExportDeliveryId {
    /// 返回日志使用的 delivery 名称。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::RealtimeReport => "realtime_report",
            Self::BasicInfo => "basic_info",
            Self::RemoteTaskResult => "remote_task_result",
            Self::RemoteProbeResult => "remote_probe_result",
            Self::ControlAck => "control_ack",
            Self::ControlError => "control_error",
        }
    }
}

/// 导出 delivery 触发方式。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportDeliveryTrigger {
    /// 最新 report 更新后触发。
    OnLatestReport,
    /// 由出站业务事件直接触发。
    EventDriven,
    /// 按固定间隔触发。
    Interval(Duration),
}

/// 导出 delivery 失败处理策略。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum ExportDeliveryFailurePolicy {
    /// 失败后重建导出 pipeline，适合长连接实时上报。
    ReconnectPipeline,
    /// 失败只记录日志，等待下一次调度，适合低频辅助 HTTP 请求。
    LogAndContinue,
}

/// adapter 需要运行的导出 delivery。
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ExportDeliverySpec {
    /// delivery ID。
    pub(crate) id: ExportDeliveryId,
    /// delivery 触发方式。
    pub(crate) trigger: ExportDeliveryTrigger,
    /// 是否在拿到第一份 report 后立即运行。
    pub(crate) send_on_start: bool,
    /// 失败处理策略。
    pub(crate) failure_policy: ExportDeliveryFailurePolicy,
}

impl ExportDeliverySpec {
    /// 创建跟随最新 report 的实时上报 delivery。
    pub(crate) fn on_latest_report(id: ExportDeliveryId) -> Self {
        Self {
            id,
            trigger: ExportDeliveryTrigger::OnLatestReport,
            send_on_start: true,
            failure_policy: ExportDeliveryFailurePolicy::ReconnectPipeline,
        }
    }

    /// 创建固定间隔上报 delivery。
    #[cfg(test)]
    pub(crate) fn interval(
        id: ExportDeliveryId,
        interval: Duration,
        failure_policy: ExportDeliveryFailurePolicy,
    ) -> Self {
        Self {
            id,
            trigger: ExportDeliveryTrigger::Interval(interval),
            send_on_start: true,
            failure_policy,
        }
    }

    /// 创建由出站事件驱动的 delivery。
    pub(crate) fn event_driven(
        id: ExportDeliveryId,
        failure_policy: ExportDeliveryFailurePolicy,
    ) -> Self {
        Self {
            id,
            trigger: ExportDeliveryTrigger::EventDriven,
            send_on_start: true,
            failure_policy,
        }
    }
}

/// adapter 需要启动的 transport 和 delivery 集合。
///
/// 这个 plan 是导出层的扩展点：同一份内部 report 可以被不同 adapter 拆成不同 transport
/// 和 delivery，例如 Smalux 默认只用实时 WebSocket，而 Komari 同时需要 WebSocket report 和
/// HTTP basic info。
pub(crate) struct TransportPlan {
    /// 所有 transport 规格。
    pub(crate) transports: Vec<TransportSpec>,
    /// 所有导出 delivery 规格。
    pub(crate) deliveries: Vec<ExportDeliverySpec>,
}

impl TransportPlan {
    /// 创建默认只有实时上报 delivery 的 plan。
    pub(crate) fn new(transports: Vec<TransportSpec>) -> Self {
        Self {
            transports,
            deliveries: vec![ExportDeliverySpec::on_latest_report(
                ExportDeliveryId::RealtimeReport,
            )],
        }
    }

    /// 创建带自定义 delivery 的 plan。
    pub(crate) fn with_deliveries(
        transports: Vec<TransportSpec>,
        deliveries: Vec<ExportDeliverySpec>,
    ) -> Self {
        Self {
            transports,
            deliveries,
        }
    }

    /// 返回 delivery 列表。
    pub(crate) fn deliveries(&self) -> &[ExportDeliverySpec] {
        &self.deliveries
    }

    /// 消费 plan，返回 transport 规格。
    pub(crate) fn into_transports(self) -> Vec<TransportSpec> {
        self.transports
    }

    /// 应用运行时出站配置，禁用的 delivery 会从 plan 中移除。
    pub(crate) fn apply_outbound_config(&mut self, config: &OutboundConfig) {
        self.deliveries
            .retain_mut(|delivery| apply_matching_outbound_config(delivery, config));
    }
}

/// 将当前配置应用到匹配的导出 delivery。
fn apply_matching_outbound_config(
    delivery: &mut ExportDeliverySpec,
    config: &OutboundConfig,
) -> bool {
    match delivery.id {
        ExportDeliveryId::RealtimeReport => {
            if !config.realtime_report.enabled {
                tracing::info!(delivery = delivery.id.as_str(), "export delivery disabled");
                return false;
            }
            delivery.send_on_start = config.realtime_report.send_on_start;
            true
        }
        ExportDeliveryId::BasicInfo => {
            if !config.basic_info.enabled {
                tracing::info!(delivery = delivery.id.as_str(), "export delivery disabled");
                return false;
            }
            if matches!(delivery.trigger, ExportDeliveryTrigger::Interval(_)) {
                delivery.trigger =
                    ExportDeliveryTrigger::Interval(config.basic_info.refresh_interval);
            }
            delivery.send_on_start = config.basic_info.send_on_start;
            true
        }
        ExportDeliveryId::RemoteTaskResult
        | ExportDeliveryId::RemoteProbeResult
        | ExportDeliveryId::ControlAck
        | ExportDeliveryId::ControlError => true,
    }
}

/// adapter 编码后的单次发送请求。
#[derive(Debug)]
pub(crate) enum TransportRequest {
    /// 发送 WebSocket 文本消息。
    WebSocketText {
        /// 目标 transport。
        transport: TransportId,
        /// 文本内容。
        body: String,
    },
    /// 发送 WebSocket 二进制消息。
    WebSocketBinary {
        /// 目标 transport。
        transport: TransportId,
        /// 业务序号。
        sequence: u64,
        /// 二进制内容。
        body: Vec<u8>,
    },
    /// 发送 HTTP JSON 请求。
    HttpJson {
        /// 目标 transport。
        transport: TransportId,
        /// HTTP method。
        method: http::HttpMethod,
        /// 完整请求 URL。
        url: String,
        /// JSON body。
        body: serde_json::Value,
    },
}

impl TransportRequest {
    /// 返回目标 transport。
    pub(crate) fn transport_id(&self) -> TransportId {
        match self {
            Self::WebSocketText { transport, .. } | Self::WebSocketBinary { transport, .. } => {
                *transport
            }
            Self::HttpJson { transport, .. } => *transport,
        }
    }
}

#[cfg(test)]
mod tests {
    //! 导出 plan delivery 配置测试。

    use super::*;

    /// 验证 realtime report 保持跟随最新 report 触发，并读取 latest-only 配置。
    #[test]
    fn apply_outbound_config_preserves_on_latest_report_trigger() {
        let mut plan = TransportPlan::with_deliveries(
            vec![],
            vec![ExportDeliverySpec::on_latest_report(
                ExportDeliveryId::RealtimeReport,
            )],
        );
        let mut config = OutboundConfig::default();
        config.realtime_report.send_on_start = false;

        plan.apply_outbound_config(&config);

        assert_eq!(
            plan.deliveries[0].trigger,
            ExportDeliveryTrigger::OnLatestReport
        );
        assert!(!plan.deliveries[0].send_on_start);
    }

    /// 验证 interval delivery 仍然读取自己的动态间隔。
    #[test]
    fn apply_outbound_config_updates_interval_delivery_trigger() {
        let mut plan = TransportPlan::with_deliveries(
            vec![],
            vec![ExportDeliverySpec::interval(
                ExportDeliveryId::BasicInfo,
                Duration::from_secs(300),
                ExportDeliveryFailurePolicy::LogAndContinue,
            )],
        );
        let mut config = OutboundConfig::default();
        config.basic_info.refresh_interval = Duration::from_secs(60);

        plan.apply_outbound_config(&config);

        assert_eq!(
            plan.deliveries[0].trigger,
            ExportDeliveryTrigger::Interval(Duration::from_secs(60))
        );
    }
}
