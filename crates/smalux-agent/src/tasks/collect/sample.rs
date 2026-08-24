//! 采集任务统一输出模型。

use serde::Serialize;
use smalux_protocol::agent::v1::{SampleMetadata, TaskResult, task_result};
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

    /// 把强类型快照及其采样元数据统一封装成协议层 `TaskResult`。
    ///
    /// 调用方只需声明快照对应的 protobuf oneof 变体，避免每个 Task
    /// 重复拷贝 `SampleMetadata` 字段。
    pub(crate) fn into_task_result(
        self,
        into_result: impl FnOnce(T) -> task_result::Result,
    ) -> TaskResult {
        TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: self.sampled_at_ms,
                sample_interval_ms: self.sample_interval_ms,
            }),
            result: Some(into_result(self.snapshot)),
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
    use smalux_protocol::agent::v1::task_result;

    use super::MetricSample;

    #[test]
    fn metric_sample_preserves_sampling_metadata_and_snapshot() {
        let collected = MetricSample::new(123, Some(50), "cpu");

        assert_eq!(collected.sampled_at_ms, 123);
        assert_eq!(collected.sample_interval_ms, Some(50));
        assert_eq!(collected.snapshot, "cpu");
    }

    #[test]
    fn metric_sample_builds_task_result_without_losing_metadata() {
        let result = MetricSample::new(123, Some(50), Default::default())
            .into_task_result(task_result::Result::Cpu);

        let sample = result.sample.expect("sample metadata");
        assert_eq!(sample.sampled_at_ms, 123);
        assert_eq!(sample.sample_interval_ms, Some(50));
        assert!(matches!(result.result, Some(task_result::Result::Cpu(_))));
    }
}
