//! 主机身份周期采集任务。

use async_trait::async_trait;
use smalux_protocol::agent::v1::{SampleMetadata, TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::host,
};

use super::blocking::CollectState;

/// 采集变化频率较低的主机身份信息。
pub struct HostTask {
    state: CollectState<()>,
}

impl HostTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.host.v1";

    /// 创建无状态主机信息采集 Task。
    pub fn new() -> Self {
        Self {
            state: CollectState::new(()),
        }
    }
}

impl Default for HostTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReportingTask for HostTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let output = self.state.collect(context, |_| host::collect()).await?;
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: output.sampled_at_ms,
                sample_interval_ms: output.sample_interval_ms,
            }),
            result: Some(task_result::Result::Host(output.snapshot)),
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
    use crate::{scheduler::ReportingTask, tasks::collect::context};

    use super::HostTask;

    #[tokio::test]
    async fn host_task_returns_stable_identity_fields() {
        let task = HostTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), HostTask::KIND);
        let Some(smalux_protocol::agent::v1::task_result::Result::Host(snapshot)) = output.result
        else {
            panic!("Host task must return TaskResult.host");
        };
        assert!(!snapshot.hostname.is_empty());
        assert!(!snapshot.architecture.is_empty());
    }
}
