//! 系统平均负载周期采集任务。

use async_trait::async_trait;
use smalux_protocol::agent::v1::{TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::load,
};

use super::blocking::BlockingCollectorState;

/// 采集平台平均负载的调度任务。
pub struct LoadTask {
    state: BlockingCollectorState<()>,
}

impl LoadTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.load.v1";

    /// 创建无状态系统负载采集 Task。
    pub fn new() -> Self {
        Self {
            state: BlockingCollectorState::new(()),
        }
    }
}

impl Default for LoadTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReportingTask for LoadTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let output = self.state.collect(context, |_| load::collect()).await?;
        Ok(output.into_task_result(task_result::Result::Load))
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

    use super::LoadTask;

    #[tokio::test]
    async fn load_task_returns_finite_values() {
        let task = LoadTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), LoadTask::KIND);
        let Some(smalux_protocol::agent::v1::task_result::Result::Load(snapshot)) = output.result
        else {
            panic!("Load task must return TaskResult.load");
        };
        assert!(snapshot.one.is_finite());
        assert!(snapshot.five.is_finite());
        assert!(snapshot.fifteen.is_finite());
    }
}
