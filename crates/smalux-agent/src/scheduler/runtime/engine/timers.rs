//! 正常 Trigger 与 Retry Timer 的写入、反向索引清理和墙上时间重锚定。

use super::super::timing::to_instant;
use super::*;

impl SchedulerActor {
    /// 替换该 Job 的唯一正常 Timer，并同步更新 `next_run_at` 快照。
    ///
    /// 一个 Job 同时最多保留一个正常 Timer；替换前先从 DelayQueue 删除旧 Key，
    /// 防止 Trigger 更新后旧计划仍然到期。
    pub(super) fn schedule_normal(
        &mut self,
        job_id: JobId,
        run_at: DateTime<Utc>,
        kind: TimerKind,
    ) {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if let Some(key) = job.normal_timer_key.take() {
            let _ = self.timers.try_remove(&key);
        }
        let version = job.version;
        tracing::debug!(
            job_id = %job_id,
            version,
            run_at = %run_at,
            timer_kind = ?kind,
            "agent scheduler normal timer scheduled"
        );
        let key = self.timers.insert_at(
            TimerEntry {
                job_id,
                version,
                run_at,
                kind,
            },
            to_instant(run_at),
        );
        job.normal_timer_key = Some(key);
        job.next_run_at = Some(run_at);
        self.emit(SchedulerEventKind::TriggerScheduled {
            job_id,
            version,
            run_at,
        });
    }

    /// 为同一 RunId 安排下一次 Retry，并维护可清理、可重锚定的反向索引。
    pub(super) fn schedule_retry(
        &mut self,
        job_id: JobId,
        version: u64,
        run_id: RunId,
        scheduled_at: DateTime<Utc>,
        attempt: u32,
        run_at: DateTime<Utc>,
    ) {
        if let Some(previous) = self.retry_timers.remove(&run_id) {
            let _ = self.timers.try_remove(&previous.key);
        }
        tracing::debug!(
            job_id = %job_id,
            version,
            run_id = %run_id,
            attempt,
            run_at = %run_at,
            "agent scheduler retry timer scheduled"
        );
        let key = self.timers.insert_at(
            TimerEntry {
                job_id,
                version,
                run_at,
                kind: TimerKind::Retry {
                    run_id,
                    scheduled_at,
                    attempt,
                },
            },
            to_instant(run_at),
        );
        self.retry_timers.insert(
            run_id,
            RetryTimer {
                job_id,
                key,
                run_at,
            },
        );
        self.emit(SchedulerEventKind::RetryScheduled {
            job_id,
            version,
            run_id,
            attempt,
            run_at,
        });
    }

    /// 删除指定 Job 的全部 Retry Timer 和 RunId 反向索引。
    pub(super) fn clear_retry_timers(&mut self, job_id: JobId) {
        let run_ids = self
            .retry_timers
            .iter()
            .filter_map(|(run_id, timer)| (timer.job_id == job_id).then_some(*run_id))
            .collect::<Vec<_>>();
        for run_id in run_ids {
            if let Some(timer) = self.retry_timers.remove(&run_id) {
                let _ = self.timers.try_remove(&timer.key);
            }
        }
    }

    /// 按保存的 UTC 时间重置 DelayQueue deadline，跟随系统墙上时间调整。
    ///
    /// DelayQueue 使用单调时钟；定期重锚定可以在系统墙上时间被管理员或 NTP 调整后，
    /// 仍让 Cron 和指定 UTC 时间的 Once Trigger 接近预期时间执行。
    pub(super) fn reanchor_timers(&mut self) {
        for job in self.jobs.values() {
            if let (Some(key), Some(run_at)) = (&job.normal_timer_key, job.next_run_at) {
                self.timers.reset_at(key, to_instant(run_at));
            }
        }
        for timer in self.retry_timers.values() {
            self.timers.reset_at(&timer.key, to_instant(timer.run_at));
        }
    }
}
