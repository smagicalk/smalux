//! export job 运行时调度。

use super::super::outbound::ReportEnvelope;
use crate::export::{
    ExportJobFailurePolicy, ExportJobSpec, ExportJobTrigger, ExportRouter, TransportHub,
    TransportPlan,
};
use std::time::Duration;
use tokio::time::Instant;

/// export job 调度检查间隔。
pub(super) const EXPORT_JOB_SCHEDULER_TICK: Duration = Duration::from_secs(1);

/// 运行时 job 状态。
#[derive(Debug, Clone)]
pub(super) struct RuntimeJob {
    /// job 静态配置。
    pub(super) spec: ExportJobSpec,
    /// interval job 的下次触发时间。
    next_due: Option<Instant>,
    /// 最近一次发送的 report 序号，用于 latest-only job 去重和跳过检测。
    pub(super) last_sent_sequence: Option<u64>,
    /// 最近一次投递给 transport worker 的 report 序号，用于异步发送去重。
    last_queued_sequence: Option<u64>,
}

impl RuntimeJob {
    /// 从静态 job 配置创建运行状态。
    pub(super) fn from_spec(spec: ExportJobSpec) -> Self {
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

/// 发送所有当前应该运行的 job。
pub(super) async fn send_ready_jobs(
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
pub(super) async fn send_due_interval_jobs(
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

/// 从 transport plan 创建运行时 job 状态。
pub(super) fn runtime_jobs_from_plan(transport_plan: &TransportPlan) -> Vec<RuntimeJob> {
    transport_plan
        .jobs()
        .iter()
        .cloned()
        .map(RuntimeJob::from_spec)
        .collect()
}
