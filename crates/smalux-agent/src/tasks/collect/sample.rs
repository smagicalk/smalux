//! 采集任务统一输出模型。

use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// 带实际采样时间与间隔的强类型采集结果。
#[derive(Debug, Clone, Serialize)]
pub struct MetricSample<T> {
    /// 本次采样开始时的 Unix 毫秒时间戳。
    pub sampled_at_ms: u64,
    /// 与同一 Task 上次采样开始时间的间隔；首次为 `None`。
    pub sample_interval_ms: Option<u64>,
    /// 具体指标快照。
    pub snapshot: T,
}

impl<T> MetricSample<T> {
    pub(crate) fn new(sampled_at_ms: u64, sample_interval_ms: Option<u64>, snapshot: T) -> Self {
        Self {
            sampled_at_ms,
            sample_interval_ms,
            snapshot,
        }
    }
}

pub(crate) fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(crate) fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::MetricSample;

    #[test]
    fn metric_sample_preserves_sampling_metadata_and_snapshot() {
        let collected = MetricSample::new(123, Some(50), "cpu");

        assert_eq!(collected.sampled_at_ms, 123);
        assert_eq!(collected.sample_interval_ms, Some(50));
        assert_eq!(collected.snapshot, "cpu");
    }
}
