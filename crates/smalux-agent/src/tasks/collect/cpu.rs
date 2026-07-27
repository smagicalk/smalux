//! CPU 周期采集任务。

use async_trait::async_trait;
use smalux_protocol::agent::v1::{SampleMetadata, TaskResult, task_result};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::system::SystemCollector,
};

use super::blocking::CollectState;

/// 独立维护 CPU 采样基线的调度任务。
pub struct CpuTask {
    state: CollectState<SystemCollector>,
}

impl CpuTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.cpu.v1";

    /// 创建拥有独立 CPU 采样基线的 Task。
    pub fn new() -> Self {
        Self {
            state: CollectState::new(SystemCollector::new()),
        }
    }
}

impl Default for CpuTask {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReportingTask for CpuTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let output = self
            .state
            .collect(context, SystemCollector::collect_cpu)
            .await?;
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: output.sampled_at_ms,
                sample_interval_ms: output.sample_interval_ms,
            }),
            result: Some(task_result::Result::Cpu(output.snapshot)),
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
    use std::{sync::Arc, time::Duration};

    use chrono::Utc;
    use tokio::sync::mpsc;

    use crate::scheduler::{
        JobOptions, ReportingTask, SchedulerConfig, SchedulerError, SchedulerRuntime, Trigger,
    };

    use super::CpuTask;
    use crate::tasks::collect::context;

    #[tokio::test]
    async fn cpu_task_returns_timestamped_snapshot() {
        let task = CpuTask::new();

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), CpuTask::KIND);
        let sample = output.sample.as_ref().expect("sample metadata is required");
        assert!(sample.sampled_at_ms > 0);
        assert_eq!(sample.sample_interval_ms, None);
        let Some(smalux_protocol::agent::v1::task_result::Result::Cpu(snapshot)) = output.result
        else {
            panic!("CPU task must return TaskResult.cpu");
        };
        assert_eq!(snapshot.logical_cpu_count as usize, snapshot.cpus.len());
    }

    #[tokio::test]
    async fn scheduler_channel_adapter_delivers_cpu_task_output() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let (sender, mut receiver) = mpsc::channel(1);

        scheduler
            .add_reporting_channel(
                Trigger::once(Utc::now()).with_timeout(None),
                Arc::new(CpuTask::new()),
                sender,
                JobOptions::default(),
            )
            .await
            .unwrap();

        let output = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let Some(smalux_protocol::agent::v1::task_result::Result::Cpu(snapshot)) = output.result
        else {
            panic!("CPU task must return TaskResult.cpu");
        };
        assert_eq!(snapshot.logical_cpu_count as usize, snapshot.cpus.len());

        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn scheduler_rejects_timeout_for_blocking_cpu_collection() {
        let runtime = SchedulerRuntime::start(SchedulerConfig::default()).unwrap();
        let scheduler = runtime.scheduler();
        let (sender, _receiver) = mpsc::channel(1);

        let error = scheduler
            .add_reporting_channel(
                Trigger::once(Utc::now()).with_timeout(Some(Duration::from_secs(1))),
                Arc::new(CpuTask::new()),
                sender,
                JobOptions::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, SchedulerError::InvalidTrigger(_)));

        runtime.shutdown().await.unwrap();
    }
}
