//! 本地接口地址周期采集任务。

use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::ip::{IpSnapshot, LocalIpCollector},
};

use super::{InterfaceSelection, MetricSample, blocking::CollectState, selection::filter_local_ip};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// 本地 IP Task 的采集配置。
pub struct LocalIpTaskConfig {
    /// 按完整名称筛选本地地址来源网卡；默认选择全部网卡。
    pub interfaces: InterfaceSelection,
}

/// 使用独立接口发现状态采集本地 IP 的调度任务。
pub struct LocalIpTask {
    state: CollectState<LocalIpCollector>,
    config: LocalIpTaskConfig,
}

impl LocalIpTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.local_ip.v1";

    /// 使用默认配置创建拥有独立接口发现状态的 Task。
    pub fn new() -> Self {
        Self::with_config(LocalIpTaskConfig::default())
    }

    /// 使用显式网卡筛选配置创建 Task。
    pub fn with_config(config: LocalIpTaskConfig) -> Self {
        Self {
            state: CollectState::new(LocalIpCollector::new()),
            config,
        }
    }

    /// 返回当前生效的本地接口筛选配置。
    pub fn config(&self) -> &LocalIpTaskConfig {
        &self.config
    }
}

impl Default for LocalIpTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ValueTask for LocalIpTask {
    type Output = MetricSample<IpSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        let mut output = self
            .state
            .collect(context, LocalIpCollector::collect)
            .await?;
        filter_local_ip(&mut output.snapshot, &self.config.interfaces);
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
    use crate::{
        scheduler::ValueTask,
        tasks::collect::{PublicIpState, context},
    };

    use super::{LocalIpTask, LocalIpTaskConfig};
    use crate::tasks::collect::InterfaceSelection;

    #[tokio::test]
    async fn local_ip_task_does_not_request_public_addresses() {
        let task = LocalIpTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), LocalIpTask::KIND);
        assert!(matches!(
            output.snapshot.public_ipv4,
            PublicIpState::NotRequested
        ));
        assert!(matches!(
            output.snapshot.public_ipv6,
            PublicIpState::NotRequested
        ));
    }

    #[tokio::test]
    async fn configured_local_ip_task_returns_empty_addresses_when_nothing_matches() {
        let task = LocalIpTask::with_config(LocalIpTaskConfig {
            interfaces: InterfaceSelection {
                include: vec!["smalux-missing-interface".to_owned()],
                exclude: Vec::new(),
            },
        });

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.config().interfaces.include.len(), 1);
        assert!(output.snapshot.local.is_empty());
    }
}
