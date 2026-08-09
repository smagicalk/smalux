//! Timer 到期批处理、Pending 准入策略和 Backpressure 恢复。

use futures_util::{FutureExt, StreamExt};

use super::super::timing::{DueSchedule, due_occurrences, next_after};
use super::*;

/// Pending 经过容量与合并策略后的内部结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// Pending 已进入 ReadyQueue。
    Accepted,
    /// Pending 按容量或 misfire 策略被丢弃。
    Skipped,
    /// Pending 已进入 Job.blocked，后续周期暂缓。
    Blocked,
}

impl SchedulerActor {
    /// 把同一轮已经到期的 Timer 聚合为有限批次，并为批内实例分配同一 batch_id。
    ///
    /// 批次上限来自 `due_batch_size`，防止大量同时到期的计时项长时间占用 Actor，
    /// 使 CRUD 命令和任务完成事件仍有机会被 Tokio `select!` 处理。
    pub(super) fn handle_expired_batch(&mut self, first: TimerEntry) {
        self.batch_sequence = self.batch_sequence.wrapping_add(1);
        let batch_id = self.batch_sequence;
        let mut entries = vec![first];
        while entries.len() < self.config.due_batch_size.get() {
            let next = self.timers.next().now_or_never().flatten();
            let Some(expired) = next else { break };
            entries.push(expired.into_inner());
        }
        tracing::debug!(
            batch_id,
            entries = entries.len(),
            "agent scheduler timer batch expired"
        );
        for entry in entries {
            self.handle_timer(entry, batch_id);
        }
    }

    /// 校验 Timer 中的 Job 版本和状态，并转换成 Retry 或正常 Pending。
    ///
    /// 旧版本 Timer 会被直接丢弃，这是更新和删除 Job 时无需遍历所有已到期消息的关键。
    fn handle_timer(&mut self, entry: TimerEntry, batch_id: u64) {
        let Some(job) = self.jobs.get(&entry.job_id) else {
            tracing::trace!(job_id = %entry.job_id, "agent scheduler ignored timer for missing job");
            return;
        };
        if job.version != entry.version || !matches!(job.state, JobState::Enabled) {
            tracing::trace!(
                job_id = %entry.job_id,
                timer_version = entry.version,
                current_version = job.version,
                "agent scheduler ignored stale or disabled timer"
            );
            return;
        }
        match entry.kind {
            TimerKind::Retry {
                run_id,
                scheduled_at,
                attempt,
            } => {
                self.retry_timers.remove(&run_id);
                let pending = PendingExecution {
                    job_id: entry.job_id,
                    version: entry.version,
                    run_id,
                    attempt,
                    scheduled_at,
                    kind: PendingKind::Retry,
                    priority: job.priority,
                    batch_id,
                };
                self.admit(pending, true);
            }
            TimerKind::Normal | TimerKind::RunNow => {
                if let Some(job) = self.jobs.get_mut(&entry.job_id) {
                    job.normal_timer_key = None;
                    job.next_run_at = None;
                }
                let run_now = matches!(entry.kind, TimerKind::RunNow);
                self.handle_normal_trigger(entry.job_id, entry.run_at, batch_id, run_now);
            }
        }
    }

    /// 应用 misfire 策略生成本批 Pending，并在未 Backpressure 时安排下一周期。
    fn handle_normal_trigger(
        &mut self,
        job_id: JobId,
        scheduled_at: DateTime<Utc>,
        batch_id: u64,
        run_now: bool,
    ) {
        let now = Utc::now();
        let Some(job) = self.jobs.get(&job_id) else {
            return;
        };
        let trigger = job.trigger.clone();
        let version = job.version;
        let priority = job.priority;
        let once = matches!(trigger.schedule, Schedule::Once { .. });
        let due = if run_now {
            DueSchedule {
                occurrences: vec![now],
                next: next_after(&trigger.schedule, now).ok().flatten(),
            }
        } else {
            match due_occurrences(&trigger, scheduled_at, now, self.config.maximum_catch_up) {
                Ok(value) => value,
                Err(error) => {
                    tracing::error!(
                        job_id = %job_id,
                        version,
                        error = %error,
                        "agent scheduler failed to calculate due occurrences"
                    );
                    let _ = self.force_disable(job_id, error.to_string());
                    return;
                }
            }
        };

        let mut blocked = false;
        if due.occurrences.is_empty() {
            tracing::debug!(
                job_id = %job_id,
                version,
                scheduled_at = %scheduled_at,
                "agent scheduler skipped trigger occurrences"
            );
            self.emit(SchedulerEventKind::TriggerSkipped {
                job_id,
                version,
                scheduled_at,
                reason: "misfire policy skipped expired occurrences".to_owned(),
            });
        }
        for occurrence in due.occurrences {
            let pending = PendingExecution {
                job_id,
                version,
                run_id: RunId::new_v4(),
                attempt: 1,
                scheduled_at: occurrence,
                kind: if once {
                    PendingKind::Once
                } else if run_now {
                    PendingKind::RunNow
                } else {
                    PendingKind::Normal
                },
                priority,
                batch_id,
            };
            blocked |= matches!(self.admit(pending, once), Admission::Blocked);
        }
        if !blocked && let Some(next) = due.next {
            self.schedule_normal(job_id, next, TimerKind::Normal);
        }
    }

    /// 按合并和容量策略尝试把 Pending 放入 ReadyQueue。
    ///
    /// Once、RunNow 和 Retry 强制使用 Backpressure，保证不可丢失实例不会被普通周期
    /// 的 `SkipNewest` 或 `ReplaceOldestTrigger` 策略意外删除。
    fn admit(&mut self, pending: PendingExecution, force_backpressure: bool) -> Admission {
        let Some(job) = self.jobs.get(&pending.job_id) else {
            return Admission::Skipped;
        };
        let job_id = pending.job_id;
        let version = pending.version;
        let coalescing = job.coalescing;
        let capacity = if force_backpressure || !pending.kind.is_replaceable_trigger() {
            CapacityPolicy::Backpressure
        } else {
            job.capacity
        };
        let job_limit = job
            .max_pending
            .unwrap_or(self.config.default_job_max_pending);

        if coalescing == TriggerCoalescing::KeepLatest && pending.kind.is_replaceable_trigger() {
            let removed = self.ready.remove_normal_triggers(job_id);
            let mut removed_blocked = Vec::new();
            if let Some(job) = self.jobs.get_mut(&job_id) {
                job.blocked.retain(|item| {
                    if item.kind.is_replaceable_trigger() {
                        removed_blocked.push(item.run_id);
                        false
                    } else {
                        true
                    }
                });
            }
            for removed_run_id in removed
                .into_iter()
                .map(|item| item.run_id)
                .chain(removed_blocked)
            {
                self.emit(SchedulerEventKind::PendingReplaced {
                    job_id,
                    removed_run_id,
                    replacement_run_id: pending.run_id,
                });
            }
        }

        let global_full = self.ready.len() >= self.config.global_max_pending;
        let job_full = self.ready.count_job(job_id) >= job_limit;
        if global_full || job_full {
            tracing::debug!(
                job_id = %job_id,
                version,
                run_id = %pending.run_id,
                global_full,
                job_full,
                capacity = ?capacity,
                "agent scheduler pending capacity reached"
            );
            match capacity {
                CapacityPolicy::SkipNewest => {
                    self.emit(SchedulerEventKind::TriggerSkipped {
                        job_id,
                        version,
                        scheduled_at: pending.scheduled_at,
                        reason: "pending capacity reached".to_owned(),
                    });
                    return Admission::Skipped;
                }
                CapacityPolicy::ReplaceOldestTrigger => {
                    if let Some(old) = self.ready.remove_oldest_trigger(job_id) {
                        self.emit(SchedulerEventKind::PendingReplaced {
                            job_id,
                            removed_run_id: old.run_id,
                            replacement_run_id: pending.run_id,
                        });
                    } else {
                        self.block(pending);
                        return Admission::Blocked;
                    }
                }
                CapacityPolicy::Backpressure => {
                    self.block(pending);
                    return Admission::Blocked;
                }
            }
        }

        let run_id = pending.run_id;
        self.ready.insert(pending);
        let pending_count = self.ready.count_job(job_id);
        self.emit(SchedulerEventKind::ExecutionQueued {
            job_id,
            version,
            run_id,
            pending_count,
        });
        tracing::debug!(
            job_id = %job_id,
            version,
            run_id = %run_id,
            pending_count,
            "agent scheduler execution queued"
        );
        Admission::Accepted
    }

    /// 把容量不足的实例暂存到 Job.blocked，并发布 Backpressure 事件。
    fn block(&mut self, pending: PendingExecution) {
        let job_id = pending.job_id;
        let version = pending.version;
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.blocked.push_back(pending);
        }
        tracing::debug!(job_id = %job_id, version, "agent scheduler applied backpressure");
        self.emit(SchedulerEventKind::BackpressureApplied { job_id, version });
    }

    /// 在 ReadyQueue 释放容量后恢复 blocked 实例，并按原计划时间恢复周期相位。
    ///
    /// 恢复时重新检查 Job 版本，确保更新、停用或删除前积压的实例不会重新进入队列。
    pub(super) fn restore_blocked(&mut self) {
        if self.ready.len() >= self.config.global_max_pending {
            return;
        }
        let job_ids = self.jobs.keys().copied().collect::<Vec<_>>();
        for job_id in job_ids {
            let mut resume_from = None;
            let job_limit = self.jobs[&job_id]
                .max_pending
                .unwrap_or(self.config.default_job_max_pending);
            while self.ready.len() < self.config.global_max_pending
                && self.ready.count_job(job_id) < job_limit
            {
                let pending = self
                    .jobs
                    .get_mut(&job_id)
                    .and_then(|job| job.blocked.pop_front());
                let Some(pending) = pending else { break };
                if self.jobs[&job_id].version == pending.version {
                    if matches!(pending.kind, PendingKind::Normal | PendingKind::RunNow) {
                        resume_from = Some(pending.scheduled_at);
                    }
                    let version = pending.version;
                    let run_id = pending.run_id;
                    self.ready.insert(pending);
                    self.emit(SchedulerEventKind::ExecutionQueued {
                        job_id,
                        version,
                        run_id,
                        pending_count: self.ready.count_job(job_id),
                    });
                    tracing::debug!(
                        job_id = %job_id,
                        version,
                        run_id = %run_id,
                        "agent scheduler restored blocked execution"
                    );
                }
            }
            if self.jobs[&job_id].blocked.is_empty()
                && self.jobs[&job_id].next_run_at.is_none()
                && matches!(self.jobs[&job_id].state, JobState::Enabled)
                && !matches!(self.jobs[&job_id].trigger.schedule, Schedule::Once { .. })
                && let Some(resume_from) = resume_from
                && let Ok(Some(next)) =
                    next_after(&self.jobs[&job_id].trigger.schedule, resume_from)
            {
                self.schedule_normal(job_id, next, TimerKind::Normal);
            }
        }
    }
}
