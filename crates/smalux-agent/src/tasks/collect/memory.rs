//! 内存周期采集任务。

use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::{memory::MemorySnapshot, system::SystemCollector},
};

use super::{MetricSample, blocking::CollectState};

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
impl ValueTask for MemoryTask {
    type Output = MetricSample<MemorySnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        self.state
            .collect(context, SystemCollector::collect_memory)
            .await
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

    use super::MemoryTask;

    #[tokio::test]
    async fn memory_task_returns_capacity_snapshot() {
        let task = MemoryTask::new();
        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), MemoryTask::KIND);
        assert!(output.sampled_at_ms > 0);
        assert!(output.snapshot.total_bytes >= output.snapshot.used_bytes);
    }
}
