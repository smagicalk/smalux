//! Trigger 校验、misfire 处理和调度时间换算。

use crate::scheduler::*;
use chrono::{DateTime, TimeDelta, Utc};
use croner::Cron;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::Instant;

/// 计算 Job 最终使用的合并和容量策略。
///
/// Once 与 CatchUp 默认采用 KeepAll + Backpressure，普通周期默认采用 KeepLatest + ReplaceOldestTrigger。
pub(super) fn effective_queue_policies(
    trigger: &Trigger,
    options: &JobOptions,
) -> (TriggerCoalescing, CapacityPolicy) {
    let lossless = matches!(
        trigger,
        Trigger {
            schedule: Schedule::Once { .. },
            ..
        } | Trigger {
            misfire: MisfirePolicy::CatchUp { .. },
            ..
        }
    );
    let coalescing = options.coalescing.unwrap_or(if lossless {
        TriggerCoalescing::KeepAll
    } else {
        TriggerCoalescing::KeepLatest
    });
    let capacity = options.capacity.unwrap_or(if lossless {
        CapacityPolicy::Backpressure
    } else {
        CapacityPolicy::ReplaceOldestTrigger
    });
    (coalescing, capacity)
}

/// 校验 Scheduler 的容量和安全限制均大于零。
pub(super) fn validate_config(config: &SchedulerConfig) -> Result<(), SchedulerError> {
    if config.global_max_pending == 0
        || config.default_job_max_pending == 0
        || config.max_jobs == 0
        || config.event_channel_capacity == 0
        || config.command_channel_capacity == 0
        || config.minimum_interval.is_zero()
        || config.minimum_retry_delay.is_zero()
        || config.maximum_catch_up == 0
        || config.maximum_retry_attempts == 0
    {
        return Err(SchedulerError::Actor(
            "scheduler capacities and safety limits must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

/// 联合校验 Trigger、JobOptions 与 SchedulerConfig。
///
/// CatchUp 必须使用无损队列策略；重试次数和间隔不能绕过全局安全上限。
pub(super) fn validate_trigger(
    trigger: &Trigger,
    options: &JobOptions,
    config: &SchedulerConfig,
) -> Result<(), SchedulerError> {
    if options.max_pending == Some(0) {
        return Err(SchedulerError::InvalidTrigger(
            "job pending capacity must be greater than zero".to_owned(),
        ));
    }
    match &trigger.schedule {
        Schedule::Interval { every, .. } if *every < config.minimum_interval => {
            return Err(SchedulerError::InvalidTrigger(format!(
                "interval must be at least {:?}",
                config.minimum_interval
            )));
        }
        Schedule::Cron { expression, .. } => {
            Cron::from_str(expression)
                .map_err(|error| SchedulerError::InvalidTrigger(error.to_string()))?;
        }
        _ => {}
    }
    if let MisfirePolicy::CatchUp { max_runs } = trigger.misfire {
        if max_runs.get() > config.maximum_catch_up {
            return Err(SchedulerError::InvalidTrigger(format!(
                "catch-up count exceeds {}",
                config.maximum_catch_up
            )));
        }
        let (coalescing, capacity) = effective_queue_policies(trigger, options);
        if coalescing != TriggerCoalescing::KeepAll || capacity != CapacityPolicy::Backpressure {
            return Err(SchedulerError::InvalidTrigger(
                "catch-up requires keep-all coalescing and backpressure capacity".to_owned(),
            ));
        }
    }
    if let ExecutionRetryPolicy::Exponential {
        max_attempts,
        initial_delay,
        max_delay,
        ..
    } = &options.retry
        && (max_attempts.get() > config.maximum_retry_attempts
            || *initial_delay < config.minimum_retry_delay
            || *max_delay < *initial_delay)
    {
        return Err(SchedulerError::InvalidRetry(
            "retry attempts or delays violate scheduler limits".to_owned(),
        ));
    }
    Ok(())
}

/// 计算 Job 注册或重新启用后的首次 UTC 计划时间。
///
/// 过去的 Once 时间会收敛到 now，Cron 返回严格晚于 now 的下一次。
pub(super) fn first_run_at(
    trigger: &Trigger,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, SchedulerError> {
    match &trigger.schedule {
        Schedule::Once { at } => Ok((*at).max(now)),
        Schedule::Interval { start_at, .. } => Ok(start_at.unwrap_or(now)),
        Schedule::Cron { .. } => next_after(&trigger.schedule, now)?.ok_or_else(|| {
            SchedulerError::InvalidTrigger("cron has no future occurrence".to_owned())
        }),
    }
}

/// 一次 Timer 到期后应生成的执行时间以及下一次正常计划。
pub(super) struct DueSchedule {
    /// 本批应生成 PendingExecution 的原计划时间。
    pub(super) occurrences: Vec<DateTime<Utc>>,
    /// 下一次应写回 DelayQueue 的正常 Trigger 时间。
    pub(super) next: Option<DateTime<Utc>>,
}

/// 根据 misfire 策略计算本批执行和下一周期。
///
/// CatchUp 只迭代配置允许的次数；跨越大量历史周期时使用 O(1) 计算跳到未来。
pub(super) fn due_occurrences(
    trigger: &Trigger,
    scheduled_at: DateTime<Utc>,
    now: DateTime<Utc>,
    maximum_catch_up: u32,
) -> Result<DueSchedule, SchedulerError> {
    if matches!(trigger.schedule, Schedule::Once { .. }) {
        return Ok(DueSchedule {
            occurrences: vec![scheduled_at],
            next: None,
        });
    }
    let mut next = next_after(&trigger.schedule, scheduled_at)?;
    let missed_additional = next.is_some_and(|value| value <= now);
    if !missed_additional {
        return Ok(DueSchedule {
            occurrences: vec![scheduled_at],
            next,
        });
    }

    match trigger.misfire {
        MisfirePolicy::Skip => {
            let next = next_future_occurrence(&trigger.schedule, scheduled_at, now)?;
            Ok(DueSchedule {
                occurrences: Vec::new(),
                next,
            })
        }
        MisfirePolicy::FireOnce => {
            let next = next_future_occurrence(&trigger.schedule, scheduled_at, now)?;
            Ok(DueSchedule {
                occurrences: vec![now],
                next,
            })
        }
        MisfirePolicy::CatchUp { max_runs } => {
            let limit = max_runs.get().min(maximum_catch_up) as usize;
            let mut occurrences = vec![scheduled_at];
            while occurrences.len() < limit && next.is_some_and(|value| value <= now) {
                let value = next.unwrap();
                occurrences.push(value);
                next = next_after(&trigger.schedule, value)?;
            }
            if next.is_some_and(|value| value <= now) {
                next = next_future_occurrence(&trigger.schedule, scheduled_at, now)?;
            }
            Ok(DueSchedule { occurrences, next })
        }
    }
}

/// 计算严格位于 after 之后的下一次计划时间。
///
/// Once 没有下一次，Interval 保持固定相位，Cron 使用配置时区解析。
pub(super) fn next_after(
    schedule: &Schedule,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    match schedule {
        Schedule::Once { .. } => Ok(None),
        Schedule::Interval { every, .. } => {
            let delta = TimeDelta::from_std(*every)
                .map_err(|error| SchedulerError::InvalidTrigger(error.to_string()))?;
            after.checked_add_signed(delta).map(Some).ok_or_else(|| {
                SchedulerError::InvalidTrigger("next interval occurrence overflowed".to_owned())
            })
        }
        Schedule::Cron {
            expression,
            timezone,
        } => {
            let cron = Cron::from_str(expression)
                .map_err(|error| SchedulerError::InvalidTrigger(error.to_string()))?;
            let local = after.with_timezone(timezone);
            let next = cron
                .find_next_occurrence(&local, false)
                .map_err(|error| SchedulerError::InvalidTrigger(error.to_string()))?;
            Ok(Some(next.with_timezone(&Utc)))
        }
    }
}

/// 直接计算严格晚于 now 的下一次时间，避免逐个遍历长期积压。
///
/// Interval 使用纳秒整数除法保持 anchor 相位，Cron 可以直接从 now 查询。
fn next_future_occurrence(
    schedule: &Schedule,
    anchor: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, SchedulerError> {
    match schedule {
        Schedule::Once { .. } => Ok(None),
        Schedule::Cron { .. } => next_after(schedule, now),
        Schedule::Interval { every, .. } => {
            let interval_nanos = i128::try_from(every.as_nanos())
                .map_err(|_| SchedulerError::InvalidTrigger("interval is too large".to_owned()))?;
            if interval_nanos == 0 {
                return Err(SchedulerError::InvalidTrigger(
                    "interval must be greater than zero".to_owned(),
                ));
            }
            let anchor_nanos = datetime_nanos(anchor);
            let now_nanos = datetime_nanos(now);
            let elapsed_nanos = now_nanos.saturating_sub(anchor_nanos).max(0);
            let steps = elapsed_nanos / interval_nanos + 1;
            let target_nanos = anchor_nanos
                .checked_add(steps.checked_mul(interval_nanos).ok_or_else(|| {
                    SchedulerError::InvalidTrigger(
                        "future interval occurrence overflowed".to_owned(),
                    )
                })?)
                .ok_or_else(|| {
                    SchedulerError::InvalidTrigger(
                        "future interval occurrence overflowed".to_owned(),
                    )
                })?;
            datetime_from_nanos(target_nanos).map(Some)
        }
    }
}

/// 把 UTC DateTime 转成 i128 纳秒时间戳，用于大跨度 Interval 相位计算。
pub(super) fn datetime_nanos(value: DateTime<Utc>) -> i128 {
    i128::from(value.timestamp()) * 1_000_000_000 + i128::from(value.timestamp_subsec_nanos())
}

/// 把 i128 纳秒时间戳安全转换回 UTC DateTime。
fn datetime_from_nanos(value: i128) -> Result<DateTime<Utc>, SchedulerError> {
    let seconds = value.div_euclid(1_000_000_000);
    let nanoseconds = value.rem_euclid(1_000_000_000) as u32;
    let seconds = i64::try_from(seconds).map_err(|_| {
        SchedulerError::InvalidTrigger("future interval occurrence overflowed".to_owned())
    })?;
    DateTime::from_timestamp(seconds, nanoseconds).ok_or_else(|| {
        SchedulerError::InvalidTrigger("future interval occurrence is out of range".to_owned())
    })
}

/// 把 UTC 墙上时间换算为 Tokio 单调时钟 deadline。
///
/// 过去时间立即到期，超出平台 Instant 范围的时间饱和到最远可表示 deadline。
pub(super) fn to_instant(run_at: DateTime<Utc>) -> Instant {
    let delay = (run_at - Utc::now()).to_std().unwrap_or(Duration::ZERO);
    let now = Instant::now();
    now.checked_add(delay)
        .unwrap_or_else(|| latest_supported_instant(now, delay))
}

/// 通过二分查找对极端远期时间做饱和转换，避免不可信时间戳触发 Instant 加法 panic。
fn latest_supported_instant(now: Instant, requested: Duration) -> Instant {
    let mut lower = 0_u64;
    let mut upper = requested.as_secs();
    while lower < upper {
        let middle = lower + (upper - lower).div_ceil(2);
        if now.checked_add(Duration::from_secs(middle)).is_some() {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    now.checked_add(Duration::from_secs(lower)).unwrap_or(now)
}
