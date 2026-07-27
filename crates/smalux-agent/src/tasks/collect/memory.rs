//! 内存周期采集任务。

use async_trait::async_trait;
use smalux_protocol::agent::v1::{SampleMetadata, TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::system::SystemCollector,
};

use super::blocking::CollectState;

/// 独立刷新内存与交换空间的调度任务。
pub struct MemoryTask {
    state: CollectState<SystemCollector>,
}

impl MemoryTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.memory.v1";

    /// 创建拥有独立 sysinfo 刷新状态的内存采集 Task。
    pub fn new() -> Self {
        Self {
            state: CollectState::new(SystemCollector::new()),
        }
    }
}

impl Default for MemoryTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReportingTask for MemoryTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let output = self
            .state
            .collect(context, SystemCollector::collect_memory)
            .await?;
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: output.sampled_at_ms,
                sample_interval_ms: output.sample_interval_ms,
            }),
            result: Some(task_result::Result::Memory(output.snapshot)),
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

    use super::MemoryTask;

    #[tokio::test]
    async fn memory_task_returns_capacity_snapshot() {
        let task = MemoryTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), MemoryTask::KIND);
        assert!(
            output
                .sample
                .as_ref()
                .is_some_and(|sample| sample.sampled_at_ms > 0)
        );
        let Some(smalux_protocol::agent::v1::task_result::Result::Memory(snapshot)) = output.result
        else {
            panic!("Memory task must return TaskResult.memory");
        };
        assert!(snapshot.total_bytes >= snapshot.used_bytes);
    }
}
