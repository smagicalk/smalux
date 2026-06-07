//! export delivery 运行时调度。

use super::super::outbound::ReportEnvelope;
use crate::export::{
    ExportDeliveryFailurePolicy, ExportDeliverySpec, ExportDeliveryTrigger, ExportRouter,
    TransportHub, TransportPlan,
};
use std::time::Duration;
use tokio::time::Instant;

/// export delivery 调度检查间隔。
pub(super) const EXPORT_DELIVERY_SCHEDULER_TICK: Duration = Duration::from_secs(1);

/// 单个 delivery 的调度状态。
#[derive(Debug, Clone)]
pub(super) struct DeliveryState {
    /// delivery 静态配置。
    pub(super) spec: ExportDeliverySpec,
    /// interval delivery 的下次触发时间。
    next_due: Option<Instant>,
    /// 最近一次发送的 report 序号，用于 latest-only delivery 去重和跳过检测。
    pub(super) last_sent_sequence: Option<u64>,
    /// 最近一次投递给 transport worker 的 report 序号，用于异步发送去重。
    last_queued_sequence: Option<u64>,
}

impl DeliveryState {
    /// 从静态 delivery 配置创建运行状态。
    pub(super) fn from_spec(spec: ExportDeliverySpec) -> Self {
        let next_due = match spec.trigger {
            ExportDeliveryTrigger::OnLatestReport | ExportDeliveryTrigger::EventDriven => None,
            ExportDeliveryTrigger::Interval(interval) => {
                let now = Instant::now();
                Some(if spec.send_on_start {
                    now
                } else {
                    now + interval
                })
            }
        };

        Self {
            spec,
            next_due,
            last_sent_sequence: None,
            last_queued_sequence: None,
        }
    }

    /// 判断 interval delivery 是否到期。
    fn is_due(&self, now: Instant) -> bool {
        self.next_due
            .map(|next_due| next_due <= now)
            .unwrap_or(false)
    }

    /// 标记 interval delivery 已完成一次调度。
    fn mark_interval_scheduled(&mut self, now: Instant) {
        if let ExportDeliveryTrigger::Interval(interval) = self.spec.trigger {
            self.next_due = Some(now + interval);
        }
    }

    /// 对 latest-only delivery 应用 send_on_start=false 语义。
    ///
    /// `OnLatestReport` 没有自己的 interval，到第一份 latest report 时才知道当前序号。
    /// 如果配置要求不在启动时立即发送，就把当前序号记为已观察，等待下一份 report。
    fn skip_initial_latest_report_if_needed(&mut self, report: &ReportEnvelope) -> bool {
        if !matches!(self.spec.trigger, ExportDeliveryTrigger::OnLatestReport) {
            return false;
        }
        if self.spec.send_on_start || self.last_queued_sequence.is_some() {
            return false;
        }

        self.last_queued_sequence = Some(report.sequence);
        true
    }
}

/// 发送所有当前应该运行的 delivery。
pub(super) async fn send_ready_deliveries(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    deliveries: &mut [DeliveryState],
    latest_report: Option<&ReportEnvelope>,
) -> anyhow::Result<()> {
    send_due_interval_deliveries(transport_hub, router, deliveries, latest_report).await?;
    send_on_latest_report_deliveries(transport_hub, router, deliveries, latest_report).await
}

/// 发送所有跟随最新 report 的 delivery。
async fn send_on_latest_report_deliveries(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    deliveries: &mut [DeliveryState],
    latest_report: Option<&ReportEnvelope>,
) -> anyhow::Result<()> {
    let Some(report) = latest_report else {
        return Ok(());
    };

    for delivery in deliveries
        .iter_mut()
        .filter(|delivery| matches!(delivery.spec.trigger, ExportDeliveryTrigger::OnLatestReport))
    {
        if let Err(err) = send_delivery_report(transport_hub, router, delivery, report).await {
            match delivery.spec.failure_policy {
                ExportDeliveryFailurePolicy::ReconnectPipeline => return Err(err),
                ExportDeliveryFailurePolicy::LogAndContinue => tracing::warn!(
                    delivery = delivery.spec.id.as_str(),
                    error = ?err,
                    "export delivery failed; continuing"
                ),
            }
        }
    }

    Ok(())
}

/// 发送所有已到期的 interval delivery。
pub(super) async fn send_due_interval_deliveries(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    deliveries: &mut [DeliveryState],
    latest_report: Option<&ReportEnvelope>,
) -> anyhow::Result<()> {
    let Some(report) = latest_report else {
        return Ok(());
    };
    let now = Instant::now();

    for delivery in deliveries.iter_mut().filter(|delivery| {
        matches!(delivery.spec.trigger, ExportDeliveryTrigger::Interval(_)) && delivery.is_due(now)
    }) {
        if let Err(err) = send_delivery_report(transport_hub, router, delivery, report).await {
            match delivery.spec.failure_policy {
                ExportDeliveryFailurePolicy::ReconnectPipeline => return Err(err),
                ExportDeliveryFailurePolicy::LogAndContinue => tracing::warn!(
                    delivery = delivery.spec.id.as_str(),
                    error = ?err,
                    "export interval delivery failed; continuing"
                ),
            }
        }
        delivery.mark_interval_scheduled(now);
    }

    Ok(())
}

/// 判断当前运行时 delivery 列表是否包含 interval delivery。
pub(super) fn has_interval_deliveries(deliveries: &[DeliveryState]) -> bool {
    deliveries
        .iter()
        .any(|delivery| matches!(delivery.spec.trigger, ExportDeliveryTrigger::Interval(_)))
}

/// 发送单个 delivery 的最新 report。
async fn send_delivery_report(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    delivery: &mut DeliveryState,
    report: &ReportEnvelope,
) -> anyhow::Result<()> {
    if delivery.skip_initial_latest_report_if_needed(report) {
        tracing::debug!(
            delivery = delivery.spec.id.as_str(),
            sequence = report.sequence,
            created_at = report.created_at,
            "export delivery skipped initial latest report"
        );
        return Ok(());
    }

    if matches!(delivery.spec.trigger, ExportDeliveryTrigger::OnLatestReport)
        && Some(report.sequence) == delivery.last_queued_sequence
    {
        return Ok(());
    }

    if matches!(delivery.spec.trigger, ExportDeliveryTrigger::OnLatestReport)
        && let Some(last_sequence) = delivery.last_queued_sequence
    {
        let skipped_reports = report
            .sequence
            .saturating_sub(last_sequence.saturating_add(1));
        if skipped_reports > 0 {
            tracing::warn!(
                delivery = delivery.spec.id.as_str(),
                skipped_reports,
                latest_sequence = report.sequence,
                "export delivery observed report sequence gap"
            );
        }
    }

    let request_count = router
        .send_report(transport_hub, delivery.spec.id, &report.outbound)
        .await?;
    if request_count == 0 {
        delivery.last_queued_sequence = Some(report.sequence);
        delivery.last_sent_sequence = Some(report.sequence);
        tracing::debug!(
            delivery = delivery.spec.id.as_str(),
            sequence = report.sequence,
            created_at = report.created_at,
            "export delivery skipped by adapter"
        );
        return Ok(());
    }

    delivery.last_queued_sequence = Some(report.sequence);
    tracing::debug!(
        delivery = delivery.spec.id.as_str(),
        sequence = report.sequence,
        created_at = report.created_at,
        request_count,
        "export delivery queued"
    );
    Ok(())
}

/// 从 transport plan 创建运行时 delivery 状态。
pub(super) fn delivery_states_from_plan(transport_plan: &TransportPlan) -> Vec<DeliveryState> {
    transport_plan
        .deliveries()
        .iter()
        .cloned()
        .map(DeliveryState::from_spec)
        .collect()
}

#[cfg(test)]
mod tests {
    //! export delivery 调度测试。

    use super::*;
    use crate::export::{ExportDeliveryId, ExportDeliverySpec};
    use smalux_protocol::OutboundReport;

    /// 构造只关心序号的测试 report。
    fn report(sequence: u64) -> ReportEnvelope {
        ReportEnvelope {
            sequence,
            created_at: 100,
            outbound: OutboundReport::heartbeat(
                "agent-test".to_string(),
                sequence,
                100,
                smalux_protocol::Heartbeat {
                    last_report_at: None,
                    last_report_sequence: None,
                },
            ),
        }
    }

    /// 验证 OnLatestReport 的 send_on_start=false 会跳过第一份已有 report。
    #[test]
    fn on_latest_report_respects_send_on_start_false() {
        let mut spec = ExportDeliverySpec::on_latest_report(ExportDeliveryId::RealtimeReport);
        spec.send_on_start = false;
        let mut delivery = DeliveryState::from_spec(spec);

        assert!(delivery.skip_initial_latest_report_if_needed(&report(7)));
        assert_eq!(delivery.last_queued_sequence, Some(7));
        assert!(!delivery.skip_initial_latest_report_if_needed(&report(8)));
    }

    /// 验证 OnLatestReport 默认仍会发送第一份 report。
    #[test]
    fn on_latest_report_send_on_start_true_does_not_skip_initial_report() {
        let spec = ExportDeliverySpec::on_latest_report(ExportDeliveryId::RealtimeReport);
        let mut delivery = DeliveryState::from_spec(spec);

        assert!(!delivery.skip_initial_latest_report_if_needed(&report(7)));
        assert_eq!(delivery.last_queued_sequence, None);
    }
}
