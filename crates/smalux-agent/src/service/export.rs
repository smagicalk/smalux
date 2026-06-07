//! 导出连接监管。

mod delivery;
mod pending;
mod pipeline;

use super::inbound::InboundCommandSender;
use super::outbound::{
    ControlAckEnvelope, ControlErrorEnvelope, OutboundEvent, OutboundReceiver,
    RemoteProbeResultEnvelope, RemoteTaskResultEnvelope, ReportEnvelope,
};
use crate::config::ConfigManager;
use delivery::{
    EXPORT_DELIVERY_SCHEDULER_TICK, has_interval_deliveries, send_due_interval_deliveries,
    send_ready_deliveries,
};
use pending::{
    PendingResumeEvents, handle_transport_event, queue_basic_info, queue_control_ack,
    queue_control_error, queue_remote_probe_result, queue_remote_task_result, send_resume_events,
};
use pipeline::{
    ConnectedExportPipeline, close_transport_hub, connect_export_pipeline,
    rebuild_runtime_deliveries,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{MissedTickBehavior, interval, sleep};

/// 导出连接监管循环。
pub(crate) async fn export_supervisor(
    config_manager: ConfigManager,
    mut outbound_rx: OutboundReceiver,
    inbound_commands: InboundCommandSender,
) -> anyhow::Result<()> {
    let mut config_rx = config_manager.subscribe();
    let mut pipeline =
        connect_export_pipeline(config_manager.clone(), inbound_commands.clone()).await?;
    let mut latest_report: Option<ReportEnvelope> = None;
    let mut pending_events = PendingExportEvents::new();
    // pending 只保存“已经编码并投递给 transport，但还没有收到 Sent 事件”的即时消息。
    // 周期 report 不做 pending，因为 latest_report 会一直保留最新值，重连后按 delivery 再发即可。
    let mut delivery_tick = interval(EXPORT_DELIVERY_SCHEDULER_TICK);
    delivery_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    if let Err(err) = send_ready_deliveries(
        &mut pipeline.transport_hub,
        &mut pipeline.router,
        &mut pipeline.deliveries,
        latest_report.as_ref(),
    )
    .await
    {
        tracing::warn!(error = ?err, "initial export report delivery failed");
    }

    loop {
        tokio::select! {
            event = outbound_rx.recv() => {
                let Some(event) = event else {
                    tracing::warn!("outbound event queue closed; export supervisor stopping");
                    break;
                };
                tracing::debug!(
                    event_kind = event.kind(),
                    sequence = event.sequence(),
                    created_at = event.created_at(),
                    "outbound event received"
                );

                match event {
                    OutboundEvent::Report(report) => {
                        // report 是 latest-state 语义：如果 export 端落后，只保留最新 report，
                        // 避免慢连接导致大量旧 snapshot/delta 堆积。
                        latest_report = Some(report);
                        if let Err(err) = send_ready_deliveries(
                            &mut pipeline.transport_hub,
                            &mut pipeline.router,
                            &mut pipeline.deliveries,
                            latest_report.as_ref(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                                "export report delivery failed; reconnecting"
                            );
                            let reconnect_interval = pipeline.export_config.reconnect_interval;
                            reconnect_pipeline_and_resume(
                                &mut pipeline,
                                &config_manager,
                                &inbound_commands,
                                reconnect_interval,
                                latest_report.as_ref(),
                                &mut pending_events,
                                "report delivery",
                            )
                            .await?;
                        }
                    }
                    OutboundEvent::BasicInfo(info) => {
                        // basic info 是低频兼容事件，下一轮 interval 会自然重试。
                        // 这里不做 pending，也不因为 HTTP 辅助请求失败重建主连接。
                        if let Err(err) = queue_basic_info(
                            &mut pipeline.transport_hub,
                            &mut pipeline.router,
                            *info,
                        ).await {
                            tracing::warn!(error = ?err, "basic info export failed; continuing");
                        }
                    }
                    OutboundEvent::ControlAck(ack) => {
                        // ack/error/task/probe 是一次性结果语义，必须在 Sent 前保留 pending，
                        // 否则重连窗口里会丢失 server 正在等待的命令响应。
                        if let Err(err) = queue_control_ack(
                            &mut pipeline.transport_hub,
                            &mut pipeline.router,
                            &mut pending_events.control_acks,
                            ack.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                                "control ack export failed; reconnecting"
                            );
                            let reconnect_interval = pipeline.export_config.reconnect_interval;
                            reconnect_pipeline_and_resume(
                                &mut pipeline,
                                &config_manager,
                                &inbound_commands,
                                reconnect_interval,
                                latest_report.as_ref(),
                                &mut pending_events,
                                "control ack",
                            ).await?;
                            if let Err(err) = queue_control_ack(
                                &mut pipeline.transport_hub,
                                &mut pipeline.router,
                                &mut pending_events.control_acks,
                                ack,
                            ).await {
                                tracing::warn!(error = ?err, "control ack export failed after reconnect");
                            }
                        }
                    }
                    OutboundEvent::ControlError(error) => {
                        if let Err(err) = queue_control_error(
                            &mut pipeline.transport_hub,
                            &mut pipeline.router,
                            &mut pending_events.control_errors,
                            error.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                                "control error export failed; reconnecting"
                            );
                            let reconnect_interval = pipeline.export_config.reconnect_interval;
                            reconnect_pipeline_and_resume(
                                &mut pipeline,
                                &config_manager,
                                &inbound_commands,
                                reconnect_interval,
                                latest_report.as_ref(),
                                &mut pending_events,
                                "control error",
                            ).await?;
                            if let Err(err) = queue_control_error(
                                &mut pipeline.transport_hub,
                                &mut pipeline.router,
                                &mut pending_events.control_errors,
                                error,
                            ).await {
                                tracing::warn!(error = ?err, "control error export failed after reconnect");
                            }
                        }
                    }
                    OutboundEvent::RemoteTaskResult(result) => {
                        if let Err(err) = queue_remote_task_result(
                            &mut pipeline.transport_hub,
                            &mut pipeline.router,
                            &mut pending_events.remote_task_results,
                            result.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                                "remote task result export failed; reconnecting"
                            );
                            let reconnect_interval = pipeline.export_config.reconnect_interval;
                            reconnect_pipeline_and_resume(
                                &mut pipeline,
                                &config_manager,
                                &inbound_commands,
                                reconnect_interval,
                                latest_report.as_ref(),
                                &mut pending_events,
                                "remote task result",
                            ).await?;
                            if let Err(err) = queue_remote_task_result(
                                &mut pipeline.transport_hub,
                                &mut pipeline.router,
                                &mut pending_events.remote_task_results,
                                result,
                            ).await {
                                tracing::warn!(error = ?err, "remote task result export failed after reconnect");
                            }
                        }
                    }
                    OutboundEvent::RemoteProbeResult(result) => {
                        if let Err(err) = queue_remote_probe_result(
                            &mut pipeline.transport_hub,
                            &mut pipeline.router,
                            &mut pending_events.remote_probe_results,
                            result.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                                "remote probe result export failed; reconnecting"
                            );
                            let reconnect_interval = pipeline.export_config.reconnect_interval;
                            reconnect_pipeline_and_resume(
                                &mut pipeline,
                                &config_manager,
                                &inbound_commands,
                                reconnect_interval,
                                latest_report.as_ref(),
                                &mut pending_events,
                                "remote probe result",
                            ).await?;
                            if let Err(err) = queue_remote_probe_result(
                                &mut pipeline.transport_hub,
                                &mut pipeline.router,
                                &mut pending_events.remote_probe_results,
                                result,
                            ).await {
                                tracing::warn!(error = ?err, "remote probe result export failed after reconnect");
                            }
                        }
                    }
                }
            }
            event = pipeline.transport_events.recv() => {
                let Some(event) = event else {
                    tracing::warn!("transport event channel closed; reconnecting export transport");
                    let reconnect_interval = pipeline.export_config.reconnect_interval;
                    reconnect_pipeline_and_resume(
                        &mut pipeline,
                        &config_manager,
                        &inbound_commands,
                        reconnect_interval,
                        latest_report.as_ref(),
                        &mut pending_events,
                        "transport event channel",
                    ).await?;
                    continue;
                };

                if let Err(err) = handle_transport_event(
                    event,
                    &mut pipeline.deliveries,
                    &mut pending_events.remote_task_results,
                    &mut pending_events.remote_probe_results,
                    &mut pending_events.control_acks,
                    &mut pending_events.control_errors,
                ) {
                    tracing::warn!(
                        error = ?err,
                        reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                        "export transport event failed; reconnecting"
                    );
                    let reconnect_interval = pipeline.export_config.reconnect_interval;
                    reconnect_pipeline_and_resume(
                        &mut pipeline,
                        &config_manager,
                        &inbound_commands,
                        reconnect_interval,
                        latest_report.as_ref(),
                        &mut pending_events,
                        "transport event",
                    ).await?;
                }
            }
            _ = delivery_tick.tick(), if has_interval_deliveries(&pipeline.deliveries) => {
                if let Err(err) = send_due_interval_deliveries(
                    &mut pipeline.transport_hub,
                    &mut pipeline.router,
                    &mut pipeline.deliveries,
                    latest_report.as_ref(),
                ).await {
                    tracing::warn!(
                        error = ?err,
                        reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                        "export interval delivery failed; reconnecting"
                    );
                    let reconnect_interval = pipeline.export_config.reconnect_interval;
                    reconnect_pipeline_and_resume(
                        &mut pipeline,
                        &config_manager,
                        &inbound_commands,
                        reconnect_interval,
                        latest_report.as_ref(),
                        &mut pending_events,
                        "interval delivery",
                    ).await?;
                }
            }
            changed = config_rx.changed() => {
                if changed.is_err() {
                    tracing::warn!("service config channel closed; export supervisor stopping");
                    break;
                }

                let next = config_rx.borrow_and_update().clone();
                if next.export == pipeline.export_config && next.outbound == pipeline.outbound_config {
                    tracing::debug!("service config changed without export delivery changes");
                    continue;
                }
                if next.export == pipeline.export_config {
                    tracing::info!("export outbound config changed; updating export deliveries");
                    let export_config = pipeline.export_config.clone();
                    pipeline.deliveries =
                        rebuild_runtime_deliveries(&mut pipeline.router, &export_config, &next.outbound)?;
                    pipeline.outbound_config = next.outbound.clone();
                    if let Err(err) = send_ready_deliveries(
                        &mut pipeline.transport_hub,
                        &mut pipeline.router,
                        &mut pipeline.deliveries,
                        latest_report.as_ref(),
                    ).await {
                        tracing::warn!(
                            error = ?err,
                            reconnect_interval_ms = pipeline.export_config.reconnect_interval.as_millis(),
                            "export delivery failed after outbound config update; reconnecting"
                        );
                        let reconnect_interval = pipeline.export_config.reconnect_interval;
                        reconnect_pipeline_and_resume(
                            &mut pipeline,
                            &config_manager,
                            &inbound_commands,
                            reconnect_interval,
                            latest_report.as_ref(),
                            &mut pending_events,
                            "outbound config update",
                        ).await?;
                    }
                    continue;
                }

                let reconnect_interval = next.export.reconnect_interval;
                tracing::info!(
                    format = next.export.format.as_str(),
                    reconnect_interval_ms = reconnect_interval.as_millis(),
                    "export config changed; reconnecting export transport"
                );
                reconnect_pipeline_and_resume(
                    &mut pipeline,
                    &config_manager,
                    &inbound_commands,
                    reconnect_interval,
                    latest_report.as_ref(),
                    &mut pending_events,
                    "export config update",
                ).await?;
            }
        }
    }

    close_transport_hub(&mut pipeline.transport_hub).await;
    Ok(())
}

/// 等待发送确认的即时导出事件缓存。
#[derive(Debug, Default)]
struct PendingExportEvents {
    /// 等待确认的远程任务结果。
    remote_task_results: BTreeMap<u64, RemoteTaskResultEnvelope>,
    /// 等待确认的远程探测结果。
    remote_probe_results: BTreeMap<u64, RemoteProbeResultEnvelope>,
    /// 等待确认的控制命令确认。
    control_acks: BTreeMap<u64, ControlAckEnvelope>,
    /// 等待确认的控制命令错误。
    control_errors: BTreeMap<u64, ControlErrorEnvelope>,
}

impl PendingExportEvents {
    /// 创建空 pending 缓存。
    fn new() -> Self {
        Self::default()
    }

    /// 生成重连恢复事件视图，避免 supervisor 到处手写四个 map 引用。
    fn resume_events(&mut self) -> PendingResumeEvents<'_> {
        PendingResumeEvents {
            remote_task_results: &mut self.remote_task_results,
            remote_probe_results: &mut self.remote_probe_results,
            control_acks: &mut self.control_acks,
            control_errors: &mut self.control_errors,
        }
    }
}

/// 关闭旧 transport，按指定间隔等待后重建整套导出 pipeline。
async fn reconnect_pipeline(
    pipeline: &mut ConnectedExportPipeline,
    config_manager: &ConfigManager,
    inbound_commands: &InboundCommandSender,
    reconnect_interval: Duration,
) -> anyhow::Result<()> {
    close_transport_hub(&mut pipeline.transport_hub).await;
    sleep(reconnect_interval).await;
    *pipeline = connect_export_pipeline(config_manager.clone(), inbound_commands.clone()).await?;
    Ok(())
}

/// 重连并恢复最新 report 与所有 pending 即时事件。
async fn reconnect_pipeline_and_resume(
    pipeline: &mut ConnectedExportPipeline,
    config_manager: &ConfigManager,
    inbound_commands: &InboundCommandSender,
    reconnect_interval: Duration,
    latest_report: Option<&ReportEnvelope>,
    pending_events: &mut PendingExportEvents,
    context: &'static str,
) -> anyhow::Result<()> {
    reconnect_pipeline(
        pipeline,
        config_manager,
        inbound_commands,
        reconnect_interval,
    )
    .await?;
    if let Err(err) = send_resume_events(
        &mut pipeline.transport_hub,
        &mut pipeline.router,
        &mut pipeline.deliveries,
        latest_report,
        pending_events.resume_events(),
    )
    .await
    {
        tracing::warn!(
            context,
            error = ?err,
            "export resume events failed after reconnect"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    //! 导出监管事件处理测试。

    use super::*;
    use crate::export::{
        ExportDeliveryFailurePolicy, ExportDeliveryId, ExportDeliverySpec, TransportEvent,
        TransportId,
    };
    use crate::service::export::delivery::DeliveryState;
    use crate::service::outbound::{
        ControlAckEnvelope, RemoteProbeResultEnvelope, RemoteTaskResultEnvelope,
    };
    use std::collections::BTreeMap;

    /// 验证发送成功事件才会更新已发送序号。
    #[test]
    fn transport_sent_event_updates_last_sent_sequence() {
        let mut deliveries = vec![DeliveryState::from_spec(
            ExportDeliverySpec::on_latest_report(ExportDeliveryId::RealtimeReport),
        )];

        handle_transport_event(
            TransportEvent::Sent {
                transport: TransportId::RealtimeReport,
                delivery: ExportDeliveryId::RealtimeReport,
                sequence: 7,
            },
            &mut deliveries,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(deliveries[0].last_sent_sequence, Some(7));
    }

    /// 验证实时上报发送失败会要求重连。
    #[test]
    fn realtime_transport_failed_event_requests_reconnect() {
        let mut deliveries = vec![DeliveryState::from_spec(
            ExportDeliverySpec::on_latest_report(ExportDeliveryId::RealtimeReport),
        )];

        let error = handle_transport_event(
            TransportEvent::Failed {
                transport: TransportId::RealtimeReport,
                delivery: ExportDeliveryId::RealtimeReport,
                sequence: 7,
                error: "send failed".to_string(),
            },
            &mut deliveries,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("export transport send failed"));
    }

    /// 验证低频辅助 delivery 发送失败只记录并继续。
    #[test]
    fn basic_info_transport_failed_event_continues() {
        let mut deliveries = vec![DeliveryState::from_spec(ExportDeliverySpec::event_driven(
            ExportDeliveryId::BasicInfo,
            ExportDeliveryFailurePolicy::LogAndContinue,
        ))];

        handle_transport_event(
            TransportEvent::Failed {
                transport: TransportId::AuxiliaryHttp,
                delivery: ExportDeliveryId::BasicInfo,
                sequence: 7,
                error: "send failed".to_string(),
            },
            &mut deliveries,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )
        .unwrap();
    }

    /// 验证远程任务结果确认发送后会从待确认缓存移除。
    #[test]
    fn remote_task_sent_event_removes_pending_result() {
        let mut pending = BTreeMap::from([(
            9,
            RemoteTaskResultEnvelope {
                agent_id: "agent-1".to_string(),
                sequence: 9,
                created_at: 100,
                result: smalux_protocol::RemoteTaskResult {
                    task_id: "task-1".to_string(),
                    status: smalux_protocol::RemoteTaskStatus::Success,
                    exit_code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                    started_at: 99,
                    finished_at: 100,
                    duration_ms: 1000,
                    timed_out: false,
                    stdout_truncated: false,
                    stderr_truncated: false,
                    error: None,
                },
            },
        )]);

        handle_transport_event(
            TransportEvent::Sent {
                transport: TransportId::RealtimeReport,
                delivery: ExportDeliveryId::RemoteTaskResult,
                sequence: 9,
            },
            &mut [],
            &mut pending,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )
        .unwrap();

        assert!(pending.is_empty());
    }

    /// 验证远程探测结果确认发送后会从待确认缓存移除。
    #[test]
    fn remote_probe_sent_event_removes_pending_result() {
        let mut pending = BTreeMap::from([(
            11,
            RemoteProbeResultEnvelope {
                agent_id: "agent-1".to_string(),
                sequence: 11,
                created_at: 100,
                result: smalux_protocol::RemoteProbeResult {
                    task_id: serde_json::Value::from(7),
                    probe_type: smalux_protocol::RemoteProbeType::Tcp,
                    target: "example.com:443".to_string(),
                    value: 12,
                    started_at: 99,
                    finished_at: 100,
                    duration_ms: 12,
                    error: None,
                },
            },
        )]);

        handle_transport_event(
            TransportEvent::Sent {
                transport: TransportId::RealtimeReport,
                delivery: ExportDeliveryId::RemoteProbeResult,
                sequence: 11,
            },
            &mut [],
            &mut BTreeMap::new(),
            &mut pending,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )
        .unwrap();

        assert!(pending.is_empty());
    }

    /// 验证控制确认发送成功后会从待确认缓存移除。
    #[test]
    fn control_ack_sent_event_removes_pending_ack() {
        let mut pending = BTreeMap::from([(
            10,
            ControlAckEnvelope {
                agent_id: "agent-1".to_string(),
                sequence: 10,
                created_at: 100,
                ack: smalux_protocol::Ack { sequence: 7 },
            },
        )]);

        handle_transport_event(
            TransportEvent::Sent {
                transport: TransportId::RealtimeReport,
                delivery: ExportDeliveryId::ControlAck,
                sequence: 10,
            },
            &mut [],
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut pending,
            &mut BTreeMap::new(),
        )
        .unwrap();

        assert!(pending.is_empty());
    }
}
