//! 磁盘容量与 IO 周期采集任务。

use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::io::{DiskIoCollector, DiskIoSnapshot},
};

use super::{DiskSelection, MetricSample, blocking::CollectState, selection::filter_disk};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// 磁盘 IO Task 的采集配置。
pub struct DiskIoTaskConfig {
    /// 按设备名称或挂载点筛选输出；默认选择全部磁盘。
    pub disks: DiskSelection,
}

/// 独立维护磁盘 IO 增量基线的调度任务。
pub struct DiskIoTask {
    state: CollectState<DiskIoCollector>,
    config: DiskIoTaskConfig,
}

impl DiskIoTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.disk_io.v1";

    /// 使用默认配置创建拥有独立 IO 增量基线的 Task。
    pub fn new() -> Self {
        Self::with_config(DiskIoTaskConfig::default())
    }

    /// 使用显式磁盘筛选配置创建 Task。
    pub fn with_config(config: DiskIoTaskConfig) -> Self {
        Self {
            state: CollectState::new(DiskIoCollector::new()),
            config,
        }
    }

    /// 返回当前生效的磁盘筛选配置。
    pub fn config(&self) -> &DiskIoTaskConfig {
        &self.config
    }
}

impl Default for DiskIoTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ValueTask for DiskIoTask {
    type Output = MetricSample<DiskIoSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        let mut output = self
            .state
            .collect(context, DiskIoCollector::collect)
            .await?;
        filter_disk(&mut output.snapshot, &self.config.disks);
        Ok(output)
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn cancellation_mode(&self) -> crate::scheduler::TaskCancellationMode {
        crate::scheduler::TaskCancellationMode::NonCancellable
    }
}

#[cfg(test)]
mod tests {
    use crate::{scheduler::ValueTask, tasks::collect::context};

    use super::{DiskIoTask, DiskIoTaskConfig};
    use crate::tasks::collect::DiskSelection;

    #[tokio::test]
    async fn disk_io_task_keeps_its_own_warmup_state() {
        let task = DiskIoTask::new();

        let first = task.run(context()).await.unwrap();
        let second = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), DiskIoTask::KIND);
        assert!(!first.snapshot.warmed_up);
        assert!(second.snapshot.warmed_up);
        assert!(second.sample_interval_ms.is_some());
    }

    #[tokio::test]
    async fn configured_disk_task_returns_empty_zero_snapshot_when_nothing_matches() {
        let task = DiskIoTask::with_config(DiskIoTaskConfig {
            disks: DiskSelection {
                include_names: vec!["smalux-missing-disk".to_owned()],
                ..DiskSelection::default()
            },
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.config().disks.include_names.len(), 1);
        assert!(output.snapshot.devices.is_empty());
        assert_eq!(output.snapshot.read_bytes, 0);
        assert_eq!(output.snapshot.written_bytes, 0);
    }
}
