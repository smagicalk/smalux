//! 主机身份周期采集任务。

use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::host::{self, HostSnapshot},
};

use super::{MetricSample, blocking::CollectState};

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
impl ValueTask for HostTask {
    type Output = MetricSample<HostSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        self.state.collect(context, |_| host::collect()).await
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

    use super::HostTask;

    #[tokio::test]
    async fn host_task_returns_stable_identity_fields() {
        let task = HostTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), HostTask::KIND);
        assert!(!output.snapshot.hostname.is_empty());
        assert!(!output.snapshot.architecture.is_empty());
    }
}
