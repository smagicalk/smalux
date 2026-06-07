//! export pending 事件缓存和重发。

use super::super::outbound::{
    BasicInfoEnvelope, ControlAckEnvelope, ControlErrorEnvelope, RemoteProbeResultEnvelope,
    RemoteTaskResultEnvelope, ReportEnvelope,
};
use super::delivery::{DeliveryState, send_ready_deliveries};
use crate::export::{
    ExportDeliveryFailurePolicy, ExportDeliveryId, ExportRouter, TransportEvent, TransportHub,
};
use std::collections::BTreeMap;

/// 处理 transport worker 回传的真实发送结果。
pub(super) fn handle_transport_event(
    event: TransportEvent,
    deliveries: &mut [DeliveryState],
    pending_remote_task_results: &mut BTreeMap<u64, RemoteTaskResultEnvelope>,
    pending_remote_probe_results: &mut BTreeMap<u64, RemoteProbeResultEnvelope>,
    pending_control_acks: &mut BTreeMap<u64, ControlAckEnvelope>,
    pending_control_errors: &mut BTreeMap<u64, ControlErrorEnvelope>,
) -> anyhow::Result<()> {
    match event {
        TransportEvent::Sent {
            transport,
            delivery,
            sequence,
        } => {
            if delivery == ExportDeliveryId::RemoteTaskResult {
                pending_remote_task_results.remove(&sequence);
            }
            if delivery == ExportDeliveryId::RemoteProbeResult {
                pending_remote_probe_results.remove(&sequence);
            }
            if delivery == ExportDeliveryId::ControlAck {
                pending_control_acks.remove(&sequence);
            }
            if delivery == ExportDeliveryId::ControlError {
                pending_control_errors.remove(&sequence);
            }
            if let Some(runtime_delivery) = deliveries
                .iter_mut()
                .find(|runtime_delivery| runtime_delivery.spec.id == delivery)
            {
                runtime_delivery.last_sent_sequence = Some(
                    runtime_delivery
                        .last_sent_sequence
                        .unwrap_or(0)
                        .max(sequence),
                );
            }
            tracing::debug!(
                transport = transport.as_str(),
                delivery = delivery.as_str(),
                sequence,
                "export delivery send confirmed"
            );
            Ok(())
        }
        TransportEvent::Failed {
            transport,
            delivery,
            sequence,
            error,
        } => {
            let policy = if matches!(
                delivery,
                ExportDeliveryId::RemoteTaskResult
                    | ExportDeliveryId::RemoteProbeResult
                    | ExportDeliveryId::ControlAck
                    | ExportDeliveryId::ControlError
            ) {
                ExportDeliveryFailurePolicy::ReconnectPipeline
            } else if delivery == ExportDeliveryId::BasicInfo {
                ExportDeliveryFailurePolicy::LogAndContinue
            } else {
                deliveries
                    .iter()
                    .find(|runtime_delivery| runtime_delivery.spec.id == delivery)
                    .map(|runtime_delivery| runtime_delivery.spec.failure_policy)
                    .unwrap_or(ExportDeliveryFailurePolicy::ReconnectPipeline)
            };

            match policy {
                ExportDeliveryFailurePolicy::ReconnectPipeline => {
                    anyhow::bail!(
                        "export transport send failed: transport={}, delivery={}, sequence={}, error={}",
                        transport.as_str(),
                        delivery.as_str(),
                        sequence,
                        error
                    )
                }
                ExportDeliveryFailurePolicy::LogAndContinue => {
                    tracing::warn!(
                        transport = transport.as_str(),
                        delivery = delivery.as_str(),
                        sequence,
                        error = %error,
                        "export delivery send failed; continuing"
                    );
                    Ok(())
                }
            }
        }
    }
}

/// 投递低频基础信息事件；失败只由调用方记录，不参与 pending 重投。
pub(super) async fn queue_basic_info(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    info: BasicInfoEnvelope,
) -> anyhow::Result<()> {
    let request_count = router.send_basic_info(transport_hub, &info).await?;
    if request_count == 0 {
        tracing::debug!(
            sequence = info.sequence,
            format_skipped = true,
            "basic info skipped by adapter"
        );
        return Ok(());
    }

    tracing::debug!(sequence = info.sequence, request_count, "basic info queued");
    Ok(())
}

/// 重新连接后恢复需要继续发送的事件。
pub(super) struct PendingResumeEvents<'a> {
    /// 等待重新发送的远程任务结果。
    pub(super) remote_task_results: &'a mut BTreeMap<u64, RemoteTaskResultEnvelope>,
    /// 等待重新发送的远程探测结果。
    pub(super) remote_probe_results: &'a mut BTreeMap<u64, RemoteProbeResultEnvelope>,
    /// 等待重新发送的控制确认。
    pub(super) control_acks: &'a mut BTreeMap<u64, ControlAckEnvelope>,
    /// 等待重新发送的控制错误。
    pub(super) control_errors: &'a mut BTreeMap<u64, ControlErrorEnvelope>,
}

/// 重新连接后恢复需要继续发送的事件。
pub(super) async fn send_resume_events(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    deliveries: &mut [DeliveryState],
    latest_report: Option<&ReportEnvelope>,
    pending: PendingResumeEvents<'_>,
) -> anyhow::Result<()> {
    send_ready_deliveries(transport_hub, router, deliveries, latest_report).await?;
    send_pending_remote_task_results(transport_hub, router, pending.remote_task_results).await?;
    send_pending_remote_probe_results(transport_hub, router, pending.remote_probe_results).await?;
    send_pending_control_acks(transport_hub, router, pending.control_acks).await?;
    send_pending_control_errors(transport_hub, router, pending.control_errors).await
}

/// 投递远程任务结果，并在等待发送确认期间保留一份副本。
pub(super) async fn queue_remote_task_result(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_remote_task_results: &mut BTreeMap<u64, RemoteTaskResultEnvelope>,
    result: RemoteTaskResultEnvelope,
) -> anyhow::Result<()> {
    let request_count = router
        .send_remote_task_result(transport_hub, &result)
        .await?;
    if request_count == 0 {
        pending_remote_task_results.remove(&result.sequence);
        tracing::debug!(
            task_id = %result.result.task_id,
            sequence = result.sequence,
            format_skipped = true,
            "remote task result skipped by adapter"
        );
        return Ok(());
    }

    tracing::debug!(
        task_id = %result.result.task_id,
        sequence = result.sequence,
        request_count,
        "remote task result queued"
    );
    pending_remote_task_results.insert(result.sequence, result);
    Ok(())
}

/// 投递远程探测结果，并在等待发送确认期间保留一份副本。
pub(super) async fn queue_remote_probe_result(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_remote_probe_results: &mut BTreeMap<u64, RemoteProbeResultEnvelope>,
    result: RemoteProbeResultEnvelope,
) -> anyhow::Result<()> {
    let request_count = router
        .send_remote_probe_result(transport_hub, &result)
        .await?;
    if request_count == 0 {
        pending_remote_probe_results.remove(&result.sequence);
        tracing::debug!(
            task_id = %crate::service::display_probe_task_id(&result.result.task_id),
            sequence = result.sequence,
            format_skipped = true,
            "remote probe result skipped by adapter"
        );
        return Ok(());
    }

    tracing::debug!(
        task_id = %crate::service::display_probe_task_id(&result.result.task_id),
        sequence = result.sequence,
        request_count,
        "remote probe result queued"
    );
    pending_remote_probe_results.insert(result.sequence, result);
    Ok(())
}

/// 投递控制命令确认，并在等待发送确认期间保留一份副本。
pub(super) async fn queue_control_ack(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_control_acks: &mut BTreeMap<u64, ControlAckEnvelope>,
    ack: ControlAckEnvelope,
) -> anyhow::Result<()> {
    let request_count = router.send_control_ack(transport_hub, &ack).await?;
    if request_count == 0 {
        pending_control_acks.remove(&ack.sequence);
        tracing::debug!(
            sequence = ack.sequence,
            server_sequence = ack.ack.sequence,
            format_skipped = true,
            "control ack skipped by adapter"
        );
        return Ok(());
    }

    tracing::debug!(
        sequence = ack.sequence,
        server_sequence = ack.ack.sequence,
        request_count,
        "control ack queued"
    );
    pending_control_acks.insert(ack.sequence, ack);
    Ok(())
}

/// 投递控制命令错误，并在等待发送确认期间保留一份副本。
pub(super) async fn queue_control_error(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_control_errors: &mut BTreeMap<u64, ControlErrorEnvelope>,
    error: ControlErrorEnvelope,
) -> anyhow::Result<()> {
    let request_count = router.send_control_error(transport_hub, &error).await?;
    if request_count == 0 {
        pending_control_errors.remove(&error.sequence);
        tracing::debug!(
            sequence = error.sequence,
            server_sequence = error.error.sequence,
            code = %error.error.code,
            format_skipped = true,
            "control error skipped by adapter"
        );
        return Ok(());
    }

    tracing::debug!(
        sequence = error.sequence,
        server_sequence = error.error.sequence,
        code = %error.error.code,
        request_count,
        "control error queued"
    );
    pending_control_errors.insert(error.sequence, error);
    Ok(())
}

/// 重连后重投尚未确认发送成功的远程任务结果。
pub(super) async fn send_pending_remote_task_results(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_remote_task_results: &mut BTreeMap<u64, RemoteTaskResultEnvelope>,
) -> anyhow::Result<()> {
    let pending = pending_remote_task_results
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for result in pending {
        let sequence = result.sequence;
        let request_count = router
            .send_remote_task_result(transport_hub, &result)
            .await?;
        if request_count == 0 {
            pending_remote_task_results.remove(&sequence);
            tracing::debug!(
                task_id = %result.result.task_id,
                sequence,
                format_skipped = true,
                "pending remote task result skipped by adapter"
            );
            continue;
        }
        tracing::debug!(
            task_id = %result.result.task_id,
            sequence,
            request_count,
            "pending remote task result requeued"
        );
    }

    Ok(())
}

/// 重连后重投尚未确认发送成功的远程探测结果。
pub(super) async fn send_pending_remote_probe_results(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_remote_probe_results: &mut BTreeMap<u64, RemoteProbeResultEnvelope>,
) -> anyhow::Result<()> {
    let pending = pending_remote_probe_results
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for result in pending {
        let sequence = result.sequence;
        let request_count = router
            .send_remote_probe_result(transport_hub, &result)
            .await?;
        if request_count == 0 {
            pending_remote_probe_results.remove(&sequence);
            tracing::debug!(
                task_id = %crate::service::display_probe_task_id(&result.result.task_id),
                sequence,
                format_skipped = true,
                "pending remote probe result skipped by adapter"
            );
            continue;
        }
        tracing::debug!(
            task_id = %crate::service::display_probe_task_id(&result.result.task_id),
            sequence,
            request_count,
            "pending remote probe result requeued"
        );
    }

    Ok(())
}

/// 重连后重投尚未确认发送成功的控制命令确认。
pub(super) async fn send_pending_control_acks(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_control_acks: &mut BTreeMap<u64, ControlAckEnvelope>,
) -> anyhow::Result<()> {
    let pending = pending_control_acks.values().cloned().collect::<Vec<_>>();
    for ack in pending {
        let sequence = ack.sequence;
        let request_count = router.send_control_ack(transport_hub, &ack).await?;
        if request_count == 0 {
            pending_control_acks.remove(&sequence);
            tracing::debug!(
                sequence,
                server_sequence = ack.ack.sequence,
                format_skipped = true,
                "pending control ack skipped by adapter"
            );
            continue;
        }
        tracing::debug!(
            sequence,
            server_sequence = ack.ack.sequence,
            request_count,
            "pending control ack requeued"
        );
    }

    Ok(())
}

/// 重连后重投尚未确认发送成功的控制命令错误。
pub(super) async fn send_pending_control_errors(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    pending_control_errors: &mut BTreeMap<u64, ControlErrorEnvelope>,
) -> anyhow::Result<()> {
    let pending = pending_control_errors.values().cloned().collect::<Vec<_>>();
    for error in pending {
        let sequence = error.sequence;
        let request_count = router.send_control_error(transport_hub, &error).await?;
        if request_count == 0 {
            pending_control_errors.remove(&sequence);
            tracing::debug!(
                sequence,
                server_sequence = error.error.sequence,
                code = %error.error.code,
                format_skipped = true,
                "pending control error skipped by adapter"
            );
            continue;
        }
        tracing::debug!(
            sequence,
            server_sequence = error.error.sequence,
            code = %error.error.code,
            request_count,
            "pending control error requeued"
        );
    }

    Ok(())
}
