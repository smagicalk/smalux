//! 磁盘容量与 IO 周期采集任务。

use async_trait::async_trait;
pub use smalux_protocol::agent::v1::DiskIoTaskConfig;
use smalux_protocol::agent::v1::{TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::io::DiskIoCollector,
};

use super::{blocking::BlockingCollectorState, selection::filter_disk};

/// 独立维护磁盘 IO 增量基线的调度任务。
pub struct DiskIoTask {
    state: BlockingCollectorState<DiskIoCollector>,
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
            state: BlockingCollectorState::new(DiskIoCollector::new()),
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
impl ReportingTask for DiskIoTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let mut output = self
            .state
            .collect(context, DiskIoCollector::collect)
            .await?;
        if let Some(selection) = self.config.disks.as_ref() {
            filter_disk(&mut output.snapshot, selection);
        }
        Ok(output.into_task_result(task_result::Result::DiskIo))
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
    use crate::{scheduler::ReportingTask, tasks::collect::context};

    use super::{DiskIoTask, DiskIoTaskConfig};
    use crate::tasks::collect::DiskSelection;

    #[tokio::test]
    async fn disk_io_task_keeps_its_own_warmup_state() {
        let task = DiskIoTask::new();

        let first = task.run(context()).await.unwrap();
        let second = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), DiskIoTask::KIND);
        let Some(smalux_protocol::agent::v1::task_result::Result::DiskIo(first)) = first.result
        else {
            panic!("disk task must return TaskResult.disk_io");
        };
        let Some(smalux_protocol::agent::v1::task_result::Result::DiskIo(second_snapshot)) =
            second.result
        else {
            panic!("disk task must return TaskResult.disk_io");
        };
        assert!(!first.warmed_up);
        assert!(second_snapshot.warmed_up);
        assert!(
            second
                .sample
                .as_ref()
                .expect("sample metadata is required")
                .sample_interval_ms
                .is_some()
        );
    }

    #[tokio::test]
    async fn configured_disk_task_returns_empty_zero_snapshot_when_nothing_matches() {
        let task = DiskIoTask::with_config(DiskIoTaskConfig {
            disks: Some(DiskSelection {
                include_names: vec!["smalux-missing-disk".to_owned()],
                ..DiskSelection::default()
            }),
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(
            task.config()
                .disks
                .as_ref()
                .expect("selection is configured")
                .include_names
                .len(),
            1
        );
        let Some(smalux_protocol::agent::v1::task_result::Result::DiskIo(snapshot)) = output.result
        else {
            panic!("disk task must return TaskResult.disk_io");
        };
        assert!(snapshot.devices.is_empty());
        assert_eq!(snapshot.read_bytes, 0);
        assert_eq!(snapshot.written_bytes, 0);
    }
}
