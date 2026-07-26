//! 系统平均负载周期采集任务。

use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::load::{self, LoadSnapshot},
};

use super::{MetricSample, blocking::CollectState};

/// 采集平台平均负载的调度任务。
pub struct LoadTask {
    state: CollectState<()>,
}

impl LoadTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.load.v1";

    /// 创建无状态系统负载采集 Task。
    pub fn new() -> Self {
        Self {
            state: CollectState::new(()),
        }
    }
}

impl Default for LoadTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ValueTask for LoadTask {
    type Output = MetricSample<LoadSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        self.state.collect(context, |_| load::collect()).await
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

    use super::LoadTask;

    #[tokio::test]
    async fn load_task_returns_finite_values() {
        let task = LoadTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), LoadTask::KIND);
        assert!(output.snapshot.one.is_finite());
        assert!(output.snapshot.five.is_finite());
        assert!(output.snapshot.fifteen.is_finite());
    }
}
