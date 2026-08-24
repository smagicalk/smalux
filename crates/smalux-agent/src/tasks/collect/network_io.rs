//! 网络 IO 周期采集任务。

use async_trait::async_trait;
pub use smalux_protocol::agent::v1::NetworkIoTaskConfig;
use smalux_protocol::agent::v1::{TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::io::NetworkIoCollector,
};

use super::{blocking::BlockingCollectorState, selection::filter_network};

/// 独立维护网络流量增量基线的调度任务。
pub struct NetworkIoTask {
    state: BlockingCollectorState<NetworkIoCollector>,
    config: NetworkIoTaskConfig,
}

impl NetworkIoTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.network_io.v1";

    /// 使用默认配置创建拥有独立流量增量基线的 Task。
    pub fn new() -> Self {
        Self::with_config(NetworkIoTaskConfig::default())
    }

    /// 使用显式网卡筛选配置创建 Task。
    pub fn with_config(config: NetworkIoTaskConfig) -> Self {
        Self {
            state: BlockingCollectorState::new(NetworkIoCollector::new()),
            config,
        }
    }

    /// 返回当前生效的网卡筛选配置。
    pub fn config(&self) -> &NetworkIoTaskConfig {
        &self.config
    }
}

impl Default for NetworkIoTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReportingTask for NetworkIoTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let mut output = self
            .state
            .collect(context, NetworkIoCollector::collect)
            .await?;
        if let Some(selection) = self.config.interfaces.as_ref() {
            filter_network(&mut output.snapshot, selection);
        }
        Ok(output.into_task_result(task_result::Result::NetworkIo))
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

    use super::{NetworkIoTask, NetworkIoTaskConfig};
    use crate::tasks::collect::InterfaceSelection;

    #[tokio::test]
    async fn network_io_task_keeps_its_own_warmup_state() {
        let task = NetworkIoTask::new();

        let first = task.run(context()).await.unwrap();
        let second = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), NetworkIoTask::KIND);
        let Some(smalux_protocol::agent::v1::task_result::Result::NetworkIo(first)) = first.result
        else {
            panic!("network task must return TaskResult.network_io");
        };
        let Some(smalux_protocol::agent::v1::task_result::Result::NetworkIo(second)) =
            second.result
        else {
            panic!("network task must return TaskResult.network_io");
        };
        assert!(!first.warmed_up);
        assert!(second.warmed_up);
    }

    #[tokio::test]
    async fn configured_network_task_returns_empty_zero_snapshot_when_nothing_matches() {
        let task = NetworkIoTask::with_config(NetworkIoTaskConfig {
            interfaces: Some(InterfaceSelection {
                include: vec!["smalux-missing-interface".to_owned()],
                exclude: Vec::new(),
            }),
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(
            task.config()
                .interfaces
                .as_ref()
                .expect("selection is configured")
                .include
                .len(),
            1
        );
        let Some(smalux_protocol::agent::v1::task_result::Result::NetworkIo(snapshot)) =
            output.result
        else {
            panic!("network task must return TaskResult.network_io");
        };
        assert!(snapshot.interfaces.is_empty());
        assert_eq!(snapshot.received_bytes, 0);
        assert_eq!(snapshot.transmitted_bytes, 0);
    }
}
