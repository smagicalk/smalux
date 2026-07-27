//! 本地接口地址周期采集任务。

use async_trait::async_trait;
pub use smalux_protocol::agent::v1::LocalIpTaskConfig;
use smalux_protocol::agent::v1::{SampleMetadata, TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::ip::LocalIpCollector,
};

use super::{blocking::CollectState, selection::filter_local_ip};

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
impl ReportingTask for LocalIpTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let mut output = self
            .state
            .collect(context, LocalIpCollector::collect)
            .await?;
        if let Some(selection) = self.config.interfaces.as_ref() {
            filter_local_ip(&mut output.snapshot, selection);
        }
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: output.sampled_at_ms,
                sample_interval_ms: output.sample_interval_ms,
            }),
            result: Some(task_result::Result::LocalIp(output.snapshot)),
        })
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
        scheduler::ReportingTask,
        tasks::collect::{PublicIpStatus, context},
    };

    use super::{LocalIpTask, LocalIpTaskConfig};
    use crate::tasks::collect::InterfaceSelection;

    #[tokio::test]
    async fn local_ip_task_does_not_request_public_addresses() {
        let task = LocalIpTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), LocalIpTask::KIND);
        let Some(smalux_protocol::agent::v1::task_result::Result::LocalIp(snapshot)) =
            output.result
        else {
            panic!("local IP task must return TaskResult.local_ip");
        };
        assert_eq!(
            snapshot.public_ipv4.expect("IPv4 state").status,
            PublicIpStatus::NotRequested as i32
        );
        assert_eq!(
            snapshot.public_ipv6.expect("IPv6 state").status,
            PublicIpStatus::NotRequested as i32
        );
    }

    #[tokio::test]
    async fn configured_local_ip_task_returns_empty_addresses_when_nothing_matches() {
        let task = LocalIpTask::with_config(LocalIpTaskConfig {
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
        let Some(smalux_protocol::agent::v1::task_result::Result::LocalIp(snapshot)) =
            output.result
        else {
            panic!("local IP task must return TaskResult.local_ip");
        };
        assert!(snapshot.local.is_empty());
    }
}
