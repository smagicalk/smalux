//! 网络 IO 周期采集任务。

use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::io::{NetworkIoCollector, NetworkIoSnapshot},
};

use super::{InterfaceSelection, MetricSample, blocking::CollectState, selection::filter_network};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// 网络 IO Task 的采集配置。
pub struct NetworkIoTaskConfig {
    /// 按完整名称筛选网卡；默认选择全部网卡。
    pub interfaces: InterfaceSelection,
}

/// 独立维护网络流量增量基线的调度任务。
pub struct NetworkIoTask {
    state: CollectState<NetworkIoCollector>,
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
            state: CollectState::new(NetworkIoCollector::new()),
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
impl ValueTask for NetworkIoTask {
    type Output = MetricSample<NetworkIoSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        let mut output = self
            .state
            .collect(context, NetworkIoCollector::collect)
            .await?;
        filter_network(&mut output.snapshot, &self.config.interfaces);
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

    use super::{NetworkIoTask, NetworkIoTaskConfig};
    use crate::tasks::collect::InterfaceSelection;

    #[tokio::test]
    async fn network_io_task_keeps_its_own_warmup_state() {
        let task = NetworkIoTask::new();

        let first = task.run(context()).await.unwrap();
        let second = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), NetworkIoTask::KIND);
        assert!(!first.snapshot.warmed_up);
        assert!(second.snapshot.warmed_up);
    }

    #[tokio::test]
    async fn configured_network_task_returns_empty_zero_snapshot_when_nothing_matches() {
        let task = NetworkIoTask::with_config(NetworkIoTaskConfig {
            interfaces: InterfaceSelection {
                include: vec!["smalux-missing-interface".to_owned()],
                exclude: Vec::new(),
            },
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.config().interfaces.include.len(), 1);
        assert!(output.snapshot.interfaces.is_empty());
        assert_eq!(output.snapshot.received_bytes, 0);
        assert_eq!(output.snapshot.transmitted_bytes, 0);
    }
}
