//! 导出连接监管。

use super::control::ServiceControlListener;
use super::inbound::InboundCommandSender;
use super::outbound::{
    ControlAckEnvelope, ControlErrorEnvelope, OutboundEvent, OutboundReceiver,
    RemoteProbeResultEnvelope, RemoteTaskResultEnvelope, ReportEnvelope,
};
use crate::config::ConfigManager;
use crate::config::model::{ExportConfig, ExportFormat, JobsConfig};
use crate::export::{
    ExportJobFailurePolicy, ExportJobSpec, ExportJobTrigger, ExportMessageListener, ExportRouter,
    TransportEvent, TransportEventReceiver, TransportHub, TransportPlan, build_export_adapter,
    build_komari_message_listener, transport_event_channel,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};

/// export job 调度检查间隔。
const EXPORT_JOB_SCHEDULER_TICK: Duration = Duration::from_secs(1);

/// 已连接导出 pipeline 的运行时状态。
///
/// 这个 tuple 只在 export supervisor 内部流转，代表“当前 adapter + transport hub +
/// job 调度状态”的一整套连接。export 配置变更时整体重建，单纯 jobs 变更时只重建
/// `RuntimeJob`，避免不必要断开 WebSocket。
type ConnectedExportPipeline = (
    TransportHub,
    TransportEventReceiver,
    ExportRouter,
    ExportConfig,
    JobsConfig,
    Vec<RuntimeJob>,
);

/// 运行时 job 状态。
#[derive(Debug, Clone)]
struct RuntimeJob {
    /// job 静态配置。
    spec: ExportJobSpec,
    /// interval job 的下次触发时间。
    next_due: Option<Instant>,
    /// 最近一次发送的 report 序号，用于 latest-only job 去重和跳过检测。
    last_sent_sequence: Option<u64>,
    /// 最近一次投递给 transport worker 的 report 序号，用于异步发送去重。
    last_queued_sequence: Option<u64>,
}

impl RuntimeJob {
    /// 从静态 job 配置创建运行状态。
    fn from_spec(spec: ExportJobSpec) -> Self {
        let next_due = match spec.trigger {
            ExportJobTrigger::OnLatestReport => None,
            ExportJobTrigger::Interval(interval) => {
                let now = Instant::now();
                Some(if spec.run_on_start {
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

    /// 判断 interval job 是否到期。
    fn is_due(&self, now: Instant) -> bool {
        self.next_due
            .map(|next_due| next_due <= now)
            .unwrap_or(false)
    }

    /// 标记 interval job 已完成一次调度。
    fn mark_interval_scheduled(&mut self, now: Instant) {
        if let ExportJobTrigger::Interval(interval) = self.spec.trigger {
            self.next_due = Some(now + interval);
        }
    }
}

/// 导出连接监管循环。
pub(crate) async fn export_supervisor(
    config_manager: ConfigManager,
    mut outbound_rx: OutboundReceiver,
    inbound_commands: InboundCommandSender,
) -> anyhow::Result<()> {
    let mut config_rx = config_manager.subscribe();
    let (
        mut transport_hub,
        mut transport_events,
        mut router,
        mut current_export,
        mut current_jobs_config,
        mut jobs,
    ) = connect_export_pipeline(config_manager.clone(), inbound_commands.clone()).await?;
    let mut latest_report = None;
    let mut pending_remote_task_results = BTreeMap::new();
    let mut pending_remote_probe_results = BTreeMap::new();
    let mut pending_control_acks = BTreeMap::new();
    let mut pending_control_errors = BTreeMap::new();
    // pending 只保存“已经编码并投递给 transport，但还没有收到 Sent 事件”的即时消息。
    // 周期 report 不做 pending，因为 latest_report 会一直保留最新值，重连后按 job 再发即可。
    let mut job_tick = interval(EXPORT_JOB_SCHEDULER_TICK);
    job_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    if let Err(err) = send_ready_jobs(
        &mut transport_hub,
        &mut router,
        &mut jobs,
        latest_report.as_ref(),
    )
    .await
    {
        tracing::warn!(error = ?err, "initial export report job failed");
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
                        if let Err(err) = send_ready_jobs(
                            &mut transport_hub,
                            &mut router,
                            &mut jobs,
                            latest_report.as_ref(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                                "export report job failed; reconnecting"
                            );
                            close_transport_hub(&mut transport_hub).await;
                            sleep(current_export.reconnect_interval).await;
                            (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                                connect_export_pipeline(
                                    config_manager.clone(),
                                    inbound_commands.clone(),
                                ).await?;
                            if let Err(err) = send_resume_events(
                                &mut transport_hub,
                                &mut router,
                                &mut jobs,
                                latest_report.as_ref(),
                                &mut pending_remote_task_results,
                                &mut pending_remote_probe_results,
                                &mut pending_control_acks,
                                &mut pending_control_errors,
                            ).await {
                                tracing::warn!(error = ?err, "export event failed after reconnect");
                            }
                        }
                    }
                    OutboundEvent::ControlAck(ack) => {
                        // ack/error/task/probe 是一次性结果语义，必须在 Sent 前保留 pending，
                        // 否则重连窗口里会丢失 server 正在等待的命令响应。
                        if let Err(err) = queue_control_ack(
                            &mut transport_hub,
                            &mut router,
                            &mut pending_control_acks,
                            ack.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                                "control ack export failed; reconnecting"
                            );
                            close_transport_hub(&mut transport_hub).await;
                            sleep(current_export.reconnect_interval).await;
                            (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                                connect_export_pipeline(
                                    config_manager.clone(),
                                    inbound_commands.clone(),
                                ).await?;
                            if let Err(err) = queue_control_ack(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_acks,
                                ack,
                            ).await {
                                tracing::warn!(error = ?err, "control ack export failed after reconnect");
                            }
                            if let Err(err) = send_pending_control_acks(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_acks,
                            ).await {
                                tracing::warn!(error = ?err, "pending control acks failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_task_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_task_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote task results failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_probe_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_probe_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote probe results failed after reconnect");
                            }
                        }
                    }
                    OutboundEvent::ControlError(error) => {
                        if let Err(err) = queue_control_error(
                            &mut transport_hub,
                            &mut router,
                            &mut pending_control_errors,
                            error.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                                "control error export failed; reconnecting"
                            );
                            close_transport_hub(&mut transport_hub).await;
                            sleep(current_export.reconnect_interval).await;
                            (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                                connect_export_pipeline(
                                    config_manager.clone(),
                                    inbound_commands.clone(),
                                ).await?;
                            if let Err(err) = queue_control_error(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_errors,
                                error,
                            ).await {
                                tracing::warn!(error = ?err, "control error export failed after reconnect");
                            }
                            if let Err(err) = send_pending_control_errors(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_errors,
                            ).await {
                                tracing::warn!(error = ?err, "pending control errors failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_task_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_task_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote task results failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_probe_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_probe_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote probe results failed after reconnect");
                            }
                        }
                    }
                    OutboundEvent::RemoteTaskResult(result) => {
                        if let Err(err) = queue_remote_task_result(
                            &mut transport_hub,
                            &mut router,
                            &mut pending_remote_task_results,
                            result.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                                "remote task result export failed; reconnecting"
                            );
                            close_transport_hub(&mut transport_hub).await;
                            sleep(current_export.reconnect_interval).await;
                            (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                                connect_export_pipeline(
                                    config_manager.clone(),
                                    inbound_commands.clone(),
                                ).await?;
                            if let Err(err) = queue_remote_task_result(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_task_results,
                                result,
                            ).await {
                                tracing::warn!(error = ?err, "remote task result export failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_task_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_task_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote task results failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_probe_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_probe_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote probe results failed after reconnect");
                            }
                            if let Err(err) = send_pending_control_acks(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_acks,
                            ).await {
                                tracing::warn!(error = ?err, "pending control acks failed after reconnect");
                            }
                            if let Err(err) = send_pending_control_errors(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_errors,
                            ).await {
                                tracing::warn!(error = ?err, "pending control errors failed after reconnect");
                            }
                        }
                    }
                    OutboundEvent::RemoteProbeResult(result) => {
                        if let Err(err) = queue_remote_probe_result(
                            &mut transport_hub,
                            &mut router,
                            &mut pending_remote_probe_results,
                            result.clone(),
                        ).await {
                            tracing::warn!(
                                error = ?err,
                                reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                                "remote probe result export failed; reconnecting"
                            );
                            close_transport_hub(&mut transport_hub).await;
                            sleep(current_export.reconnect_interval).await;
                            (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                                connect_export_pipeline(
                                    config_manager.clone(),
                                    inbound_commands.clone(),
                                ).await?;
                            if let Err(err) = queue_remote_probe_result(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_probe_results,
                                result,
                            ).await {
                                tracing::warn!(error = ?err, "remote probe result export failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_task_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_task_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote task results failed after reconnect");
                            }
                            if let Err(err) = send_pending_remote_probe_results(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_remote_probe_results,
                            ).await {
                                tracing::warn!(error = ?err, "pending remote probe results failed after reconnect");
                            }
                            if let Err(err) = send_pending_control_acks(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_acks,
                            ).await {
                                tracing::warn!(error = ?err, "pending control acks failed after reconnect");
                            }
                            if let Err(err) = send_pending_control_errors(
                                &mut transport_hub,
                                &mut router,
                                &mut pending_control_errors,
                            ).await {
                                tracing::warn!(error = ?err, "pending control errors failed after reconnect");
                            }
                        }
                    }
                }
            }
            event = transport_events.recv() => {
                let Some(event) = event else {
                    tracing::warn!("transport event channel closed; reconnecting export transport");
                    close_transport_hub(&mut transport_hub).await;
                    sleep(current_export.reconnect_interval).await;
                    (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                        connect_export_pipeline(
                            config_manager.clone(),
                            inbound_commands.clone(),
                        ).await?;
                    continue;
                };

                if let Err(err) = handle_transport_event(
                    event,
                    &mut jobs,
                    &mut pending_remote_task_results,
                    &mut pending_remote_probe_results,
                    &mut pending_control_acks,
                    &mut pending_control_errors,
                ) {
                    tracing::warn!(
                        error = ?err,
                        reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                        "export transport event failed; reconnecting"
                    );
                    close_transport_hub(&mut transport_hub).await;
                    sleep(current_export.reconnect_interval).await;
                    (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                        connect_export_pipeline(
                            config_manager.clone(),
                            inbound_commands.clone(),
                        ).await?;
                    if let Err(err) = send_resume_events(
                        &mut transport_hub,
                        &mut router,
                        &mut jobs,
                        latest_report.as_ref(),
                        &mut pending_remote_task_results,
                        &mut pending_remote_probe_results,
                        &mut pending_control_acks,
                        &mut pending_control_errors,
                    ).await {
                        tracing::warn!(error = ?err, "export job failed after transport event reconnect");
                    }
                }
            }
            _ = job_tick.tick() => {
                if let Err(err) = send_due_interval_jobs(
                    &mut transport_hub,
                    &mut router,
                    &mut jobs,
                    latest_report.as_ref(),
                ).await {
                    tracing::warn!(
                        error = ?err,
                        reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                        "export interval job failed; reconnecting"
                    );
                    close_transport_hub(&mut transport_hub).await;
                    sleep(current_export.reconnect_interval).await;
                    (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                        connect_export_pipeline(
                            config_manager.clone(),
                            inbound_commands.clone(),
                        ).await?;
                    if let Err(err) = send_resume_events(
                        &mut transport_hub,
                        &mut router,
                        &mut jobs,
                        latest_report.as_ref(),
                        &mut pending_remote_task_results,
                        &mut pending_remote_probe_results,
                        &mut pending_control_acks,
                        &mut pending_control_errors,
                    ).await {
                        tracing::warn!(error = ?err, "export job failed after interval reconnect");
                    }
                }
            }
            changed = config_rx.changed() => {
                if changed.is_err() {
                    tracing::warn!("service config channel closed; export supervisor stopping");
                    break;
                }

                let next = config_rx.borrow_and_update().clone();
                if next.export == current_export && next.jobs == current_jobs_config {
                    tracing::debug!("service config changed without export job changes");
                    continue;
                }
                if next.export == current_export {
                    tracing::info!("export jobs config changed; updating export jobs");
                    jobs = rebuild_runtime_jobs(&mut router, &current_export, &next.jobs)?;
                    current_jobs_config = next.jobs.clone();
                    if let Err(err) = send_ready_jobs(
                        &mut transport_hub,
                        &mut router,
                        &mut jobs,
                        latest_report.as_ref(),
                    ).await {
                        tracing::warn!(
                            error = ?err,
                            reconnect_interval_ms = current_export.reconnect_interval.as_millis(),
                            "export job failed after job config update; reconnecting"
                        );
                        close_transport_hub(&mut transport_hub).await;
                        sleep(current_export.reconnect_interval).await;
                        (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                            connect_export_pipeline(
                                config_manager.clone(),
                                inbound_commands.clone(),
                            ).await?;
                        if let Err(err) = send_resume_events(
                            &mut transport_hub,
                                &mut router,
                                &mut jobs,
                                latest_report.as_ref(),
                                &mut pending_remote_task_results,
                                &mut pending_remote_probe_results,
                                &mut pending_control_acks,
                                &mut pending_control_errors,
                        ).await {
                            tracing::warn!(error = ?err, "export job failed after job config reconnect");
                        }
                    }
                    continue;
                }

                let reconnect_interval = next.export.reconnect_interval;
                tracing::info!(
                    format = next.export.format.as_str(),
                    reconnect_interval_ms = reconnect_interval.as_millis(),
                    "export config changed; reconnecting export transport"
                );
                close_transport_hub(&mut transport_hub).await;
                sleep(reconnect_interval).await;
                (transport_hub, transport_events, router, current_export, current_jobs_config, jobs) =
                    connect_export_pipeline(
                        config_manager.clone(),
                        inbound_commands.clone(),
                    ).await?;
                if let Err(err) = send_resume_events(
                    &mut transport_hub,
                    &mut router,
                    &mut jobs,
                    latest_report.as_ref(),
                    &mut pending_remote_task_results,
                    &mut pending_remote_probe_results,
                    &mut pending_control_acks,
                    &mut pending_control_errors,
                ).await {
                    tracing::warn!(error = ?err, "export report job failed after export config reconnect");
                }
            }
        }
    }

    close_transport_hub(&mut transport_hub).await;
    Ok(())
}

/// 处理 transport worker 回传的真实发送结果。
fn handle_transport_event(
    event: TransportEvent,
    jobs: &mut [RuntimeJob],
    pending_remote_task_results: &mut BTreeMap<u64, RemoteTaskResultEnvelope>,
    pending_remote_probe_results: &mut BTreeMap<u64, RemoteProbeResultEnvelope>,
    pending_control_acks: &mut BTreeMap<u64, ControlAckEnvelope>,
    pending_control_errors: &mut BTreeMap<u64, ControlErrorEnvelope>,
) -> anyhow::Result<()> {
    match event {
        TransportEvent::Sent {
            transport,
            job,
            sequence,
        } => {
            if job == crate::export::ExportJobId::RemoteTaskResult {
                pending_remote_task_results.remove(&sequence);
            }
            if job == crate::export::ExportJobId::RemoteProbeResult {
                pending_remote_probe_results.remove(&sequence);
            }
            if job == crate::export::ExportJobId::ControlAck {
                pending_control_acks.remove(&sequence);
            }
            if job == crate::export::ExportJobId::ControlError {
                pending_control_errors.remove(&sequence);
            }
            if let Some(runtime_job) = jobs
                .iter_mut()
                .find(|runtime_job| runtime_job.spec.id == job)
            {
                runtime_job.last_sent_sequence =
                    Some(runtime_job.last_sent_sequence.unwrap_or(0).max(sequence));
            }
            tracing::debug!(
                transport = transport.as_str(),
                job = job.as_str(),
                sequence,
                "export job send confirmed"
            );
            Ok(())
        }
        TransportEvent::Failed {
            transport,
            job,
            sequence,
            error,
        } => {
            let policy = if matches!(
                job,
                crate::export::ExportJobId::RemoteTaskResult
                    | crate::export::ExportJobId::RemoteProbeResult
                    | crate::export::ExportJobId::ControlAck
                    | crate::export::ExportJobId::ControlError
            ) {
                ExportJobFailurePolicy::ReconnectPipeline
            } else {
                jobs.iter()
                    .find(|runtime_job| runtime_job.spec.id == job)
                    .map(|runtime_job| runtime_job.spec.failure_policy)
                    .unwrap_or(ExportJobFailurePolicy::ReconnectPipeline)
            };

            match policy {
                ExportJobFailurePolicy::ReconnectPipeline => {
                    anyhow::bail!(
                        "export transport send failed: transport={}, job={}, sequence={}, error={}",
                        transport.as_str(),
                        job.as_str(),
                        sequence,
                        error
                    )
                }
                ExportJobFailurePolicy::LogAndContinue => {
                    tracing::warn!(
                        transport = transport.as_str(),
                        job = job.as_str(),
                        sequence,
                        error = %error,
                        "export job send failed; continuing"
                    );
                    Ok(())
                }
            }
        }
    }
}

/// 重新连接后恢复需要继续发送的事件。
async fn send_resume_events(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    jobs: &mut [RuntimeJob],
    latest_report: Option<&ReportEnvelope>,
    pending_remote_task_results: &mut BTreeMap<u64, RemoteTaskResultEnvelope>,
    pending_remote_probe_results: &mut BTreeMap<u64, RemoteProbeResultEnvelope>,
    pending_control_acks: &mut BTreeMap<u64, ControlAckEnvelope>,
    pending_control_errors: &mut BTreeMap<u64, ControlErrorEnvelope>,
) -> anyhow::Result<()> {
    send_ready_jobs(transport_hub, router, jobs, latest_report).await?;
    send_pending_remote_task_results(transport_hub, router, pending_remote_task_results).await?;
    send_pending_remote_probe_results(transport_hub, router, pending_remote_probe_results).await?;
    send_pending_control_acks(transport_hub, router, pending_control_acks).await?;
    send_pending_control_errors(transport_hub, router, pending_control_errors).await
}

/// 发送所有当前应该运行的 job。
async fn send_ready_jobs(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    jobs: &mut [RuntimeJob],
    latest_report: Option<&ReportEnvelope>,
) -> anyhow::Result<()> {
    send_due_interval_jobs(transport_hub, router, jobs, latest_report).await?;
    send_on_latest_report_jobs(transport_hub, router, jobs, latest_report).await
}

/// 发送所有跟随最新 report 的 job。
async fn send_on_latest_report_jobs(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    jobs: &mut [RuntimeJob],
    latest_report: Option<&ReportEnvelope>,
) -> anyhow::Result<()> {
    let Some(report) = latest_report else {
        return Ok(());
    };

    for job in jobs
        .iter_mut()
        .filter(|job| matches!(job.spec.trigger, ExportJobTrigger::OnLatestReport))
    {
        if let Err(err) = send_job_report(transport_hub, router, job, report).await {
            match job.spec.failure_policy {
                ExportJobFailurePolicy::ReconnectPipeline => return Err(err),
                ExportJobFailurePolicy::LogAndContinue => tracing::warn!(
                    job = job.spec.id.as_str(),
                    error = ?err,
                    "export job failed; continuing"
                ),
            }
        }
    }

    Ok(())
}

/// 发送所有已到期的 interval job。
async fn send_due_interval_jobs(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    jobs: &mut [RuntimeJob],
    latest_report: Option<&ReportEnvelope>,
) -> anyhow::Result<()> {
    let Some(report) = latest_report else {
        return Ok(());
    };
    let now = Instant::now();

    for job in jobs
        .iter_mut()
        .filter(|job| matches!(job.spec.trigger, ExportJobTrigger::Interval(_)) && job.is_due(now))
    {
        if let Err(err) = send_job_report(transport_hub, router, job, report).await {
            match job.spec.failure_policy {
                ExportJobFailurePolicy::ReconnectPipeline => return Err(err),
                ExportJobFailurePolicy::LogAndContinue => tracing::warn!(
                    job = job.spec.id.as_str(),
                    error = ?err,
                    "export interval job failed; continuing"
                ),
            }
        }
        job.mark_interval_scheduled(now);
    }

    Ok(())
}

/// 发送单个 job 的最新 report。
async fn send_job_report(
    transport_hub: &mut TransportHub,
    router: &mut ExportRouter,
    job: &mut RuntimeJob,
    report: &ReportEnvelope,
) -> anyhow::Result<()> {
    if matches!(job.spec.trigger, ExportJobTrigger::OnLatestReport)
        && Some(report.sequence) == job.last_queued_sequence
    {
        return Ok(());
    }

    if matches!(job.spec.trigger, ExportJobTrigger::OnLatestReport) {
        if let Some(last_sequence) = job.last_queued_sequence {
            let skipped_reports = report
                .sequence
                .saturating_sub(last_sequence.saturating_add(1));
            if skipped_reports > 0 {
                tracing::warn!(
                    job = job.spec.id.as_str(),
                    skipped_reports,
                    latest_sequence = report.sequence,
                    "export job observed report sequence gap"
                );
            }
        }
    }

    let request_count = router
        .send_report(transport_hub, job.spec.id, &report.outbound)
        .await?;
    if request_count == 0 {
        job.last_queued_sequence = Some(report.sequence);
        job.last_sent_sequence = Some(report.sequence);
        tracing::debug!(
            job = job.spec.id.as_str(),
            sequence = report.sequence,
            created_at = report.created_at,
            "export job skipped by adapter"
        );
        return Ok(());
    }

    job.last_queued_sequence = Some(report.sequence);
    tracing::debug!(
        job = job.spec.id.as_str(),
        sequence = report.sequence,
        created_at = report.created_at,
        request_count,
        "export job queued"
    );
    Ok(())
}

/// 投递远程任务结果，并在等待发送确认期间保留一份副本。
async fn queue_remote_task_result(
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
async fn queue_remote_probe_result(
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
async fn queue_control_ack(
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
async fn queue_control_error(
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
async fn send_pending_remote_task_results(
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
async fn send_pending_remote_probe_results(
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
async fn send_pending_control_acks(
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
async fn send_pending_control_errors(
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

/// 使用当前配置创建 adapter、transport plan，并连接导出 transport。
async fn connect_export_pipeline(
    config_manager: ConfigManager,
    inbound_commands: InboundCommandSender,
) -> anyhow::Result<ConnectedExportPipeline> {
    loop {
        let config = config_manager.current();
        let export_config = config.export.clone();
        let jobs_config = config.jobs.clone();
        let reconnect_interval = export_config.reconnect_interval;
        let format = export_config.format.as_str();
        let mut router = ExportRouter::new(build_export_adapter(export_config.format));
        let mut transport_plan = router.transport_plan(&export_config)?;
        transport_plan.apply_job_config(&jobs_config);
        let jobs = runtime_jobs_from_plan(&transport_plan);
        let (transport_event_tx, transport_events) = transport_event_channel();
        let mut transport_hub = TransportHub::from_plan(transport_plan, transport_event_tx)?;
        let transport_summary = transport_hub.summary();

        // listener 绑定在 realtime report transport 上；server 控制消息从主长连接进入，
        // 再转换为统一入站命令队列。Komari 和 Smalux 自有协议只在 listener 层分叉。
        transport_hub
            .set_realtime_report_listener(build_export_message_listener(
                export_config.format,
                config_manager.clone(),
                inbound_commands.clone(),
            ))
            .await?;

        match transport_hub.connect_all().await {
            Ok(()) => {
                return Ok((
                    transport_hub,
                    transport_events,
                    router,
                    export_config,
                    jobs_config,
                    jobs,
                ));
            }
            Err(err) => {
                tracing::warn!(
                    transports = %transport_summary,
                    format = format,
                    error = %err,
                    reconnect_interval_ms = reconnect_interval.as_millis(),
                    "export transport connect failed; retrying"
                );
                close_transport_hub(&mut transport_hub).await;
                sleep(reconnect_interval).await;
            }
        }
    }
}

/// 重新读取 adapter job plan 并应用运行时 jobs 配置。
fn rebuild_runtime_jobs(
    router: &mut ExportRouter,
    export_config: &ExportConfig,
    jobs_config: &JobsConfig,
) -> anyhow::Result<Vec<RuntimeJob>> {
    let mut transport_plan = router.transport_plan(export_config)?;
    transport_plan.apply_job_config(jobs_config);
    Ok(runtime_jobs_from_plan(&transport_plan))
}

/// 从 transport plan 创建运行时 job 状态。
fn runtime_jobs_from_plan(transport_plan: &TransportPlan) -> Vec<RuntimeJob> {
    transport_plan
        .jobs()
        .iter()
        .cloned()
        .map(RuntimeJob::from_spec)
        .collect()
}

/// 根据导出格式创建服务端消息监听器。
fn build_export_message_listener(
    format: ExportFormat,
    config_manager: ConfigManager,
    inbound_commands: InboundCommandSender,
) -> Box<dyn ExportMessageListener> {
    match format {
        ExportFormat::SmaluxJson => Box::new(ServiceControlListener::new(inbound_commands)),
        ExportFormat::Komari => build_komari_message_listener(config_manager, inbound_commands),
    }
}

/// 关闭导出 transport hub；关闭失败只记录日志，避免掩盖真正的 supervisor 退出原因。
async fn close_transport_hub(transport_hub: &mut TransportHub) {
    if let Err(err) = transport_hub.close_all().await {
        tracing::warn!(error = ?err, "export transport hub close failed");
    }
}

#[cfg(test)]
mod tests {
    //! 导出监管事件处理测试。

    use super::*;
    use crate::export::{ExportJobId, TransportId};

    /// 验证发送成功事件才会更新已发送序号。
    #[test]
    fn transport_sent_event_updates_last_sent_sequence() {
        let mut jobs = vec![RuntimeJob::from_spec(ExportJobSpec::on_latest_report(
            ExportJobId::RealtimeReport,
        ))];

        handle_transport_event(
            TransportEvent::Sent {
                transport: TransportId::RealtimeReport,
                job: ExportJobId::RealtimeReport,
                sequence: 7,
            },
            &mut jobs,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(jobs[0].last_sent_sequence, Some(7));
    }

    /// 验证实时上报发送失败会要求重连。
    #[test]
    fn realtime_transport_failed_event_requests_reconnect() {
        let mut jobs = vec![RuntimeJob::from_spec(ExportJobSpec::on_latest_report(
            ExportJobId::RealtimeReport,
        ))];

        let error = handle_transport_event(
            TransportEvent::Failed {
                transport: TransportId::RealtimeReport,
                job: ExportJobId::RealtimeReport,
                sequence: 7,
                error: "send failed".to_string(),
            },
            &mut jobs,
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
            &mut BTreeMap::new(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("export transport send failed"));
    }

    /// 验证低频辅助 job 发送失败只记录并继续。
    #[test]
    fn basic_info_transport_failed_event_continues() {
        let mut jobs = vec![RuntimeJob::from_spec(ExportJobSpec::interval(
            ExportJobId::BasicInfo,
            Duration::from_secs(300),
            ExportJobFailurePolicy::LogAndContinue,
        ))];

        handle_transport_event(
            TransportEvent::Failed {
                transport: TransportId::BasicInfo,
                job: ExportJobId::BasicInfo,
                sequence: 7,
                error: "send failed".to_string(),
            },
            &mut jobs,
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
                job: ExportJobId::RemoteTaskResult,
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
                job: ExportJobId::RemoteProbeResult,
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
                job: ExportJobId::ControlAck,
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
