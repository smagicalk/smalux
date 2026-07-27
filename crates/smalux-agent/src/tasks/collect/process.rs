//! 本机进程数量与分级列表周期采集任务。

use std::num::NonZeroUsize;

use anyhow::anyhow;
use async_trait::async_trait;
pub use smalux_protocol::agent::v1::ProcessTaskConfig;
use smalux_protocol::agent::v1::{
    CollectionMode, ProcessDetails, ProcessEntry, ProcessRanking, ProcessSelection,
    ProcessSnapshot, ProcessState, ProcessStateCount, SampleMetadata, TaskResult, task_result,
};

use crate::{
    scheduler::{ReportingTask, TaskContext, TaskError},
    tasks::collect::collectors::process::{
        ProcessCollector, ProcessConfigError, ProcessSnapshot as CollectedProcessSnapshot,
        ProcessState as CollectedProcessState,
    },
};

use super::blocking::CollectState;

/// 使用独立 sysinfo 状态采集进程数量和有限列表的调度任务。
pub struct ProcessTask {
    state: CollectState<ProcessCollector>,
    config: ProcessTaskConfig,
    mode: CollectionMode,
    selection: ProcessSelection,
    ranking: ProcessRanking,
    max_entries: Option<NonZeroUsize>,
}

impl ProcessTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.process.v1";

    /// 使用指定详细档位、PID 排名和其他默认配置创建 Task。
    pub fn new(mode: CollectionMode) -> Self {
        Self::try_with_config(ProcessTaskConfig {
            mode: mode as i32,
            selection: Some(ProcessSelection::default()),
            ranking: ProcessRanking::Pid as i32,
            max_entries: None,
        })
        .expect("built-in process task config must be valid")
    }

    /// 校验配置并创建 Process Task。
    pub fn try_with_config(config: ProcessTaskConfig) -> Result<Self, ProcessConfigError> {
        let mode = CollectionMode::try_from(config.mode)
            .ok()
            .filter(|mode| *mode != CollectionMode::Unspecified)
            .ok_or(ProcessConfigError::InvalidMode)?;
        let selection = config.selection.clone().unwrap_or_default();
        let ranking = ProcessRanking::try_from(config.ranking)
            .ok()
            .filter(|ranking| *ranking != ProcessRanking::Unspecified)
            .ok_or(ProcessConfigError::InvalidRanking)?;
        let max_entries = config
            .max_entries
            .map(|value| {
                NonZeroUsize::new(value as usize).ok_or(ProcessConfigError::InvalidMaxEntries)
            })
            .transpose()?;
        ProcessCollector::validate(mode, &selection, ranking)?;
        Ok(Self {
            state: CollectState::new(ProcessCollector::new()),
            config,
            mode,
            selection,
            ranking,
            max_entries,
        })
    }

    /// 返回当前生效的查询配置。
    pub fn config(&self) -> &ProcessTaskConfig {
        &self.config
    }
}

impl Default for ProcessTask {
    fn default() -> Self {
        Self::new(CollectionMode::Summary)
    }
}

#[async_trait]
impl ReportingTask for ProcessTask {
    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let mode = self.mode;
        let selection = self.selection.clone();
        let ranking = self.ranking;
        let max_entries = self.max_entries;
        let output = self
            .state
            .try_collect(context, move |collector| {
                collector
                    .collect(mode, &selection, ranking, max_entries)
                    .map_err(|error| anyhow!(error))
            })
            .await?;
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: output.sampled_at_ms,
                sample_interval_ms: output.sample_interval_ms,
            }),
            result: Some(task_result::Result::Process(into_proto_snapshot(
                output.snapshot,
            ))),
        })
    }

    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn cancellation_mode(&self) -> crate::scheduler::TaskCancellationMode {
        crate::scheduler::TaskCancellationMode::NonCancellable
    }
}

pub(super) fn into_proto_snapshot(snapshot: CollectedProcessSnapshot) -> ProcessSnapshot {
    ProcessSnapshot {
        mode: snapshot.mode as i32,
        total_processes: snapshot.total_processes.try_into().unwrap_or(u32::MAX),
        matched_processes: snapshot.matched_processes.try_into().unwrap_or(u32::MAX),
        states: snapshot
            .states
            .into_iter()
            .map(|state| ProcessStateCount {
                state: into_proto_state(state.state) as i32,
                count: state.count.try_into().unwrap_or(u32::MAX),
            })
            .collect(),
        entries: snapshot
            .entries
            .into_iter()
            .map(|entry| ProcessEntry {
                pid: entry.pid,
                parent_pid: entry.parent_pid,
                name: entry.name,
                state: into_proto_state(entry.state) as i32,
                started_at_seconds: entry.started_at_seconds,
                details: entry.details.map(|details| ProcessDetails {
                    cpu_usage_percent: details.cpu_usage_percent,
                    memory_bytes: details.memory_bytes,
                    virtual_memory_bytes: details.virtual_memory_bytes,
                    read_bytes: details.read_bytes,
                    written_bytes: details.written_bytes,
                    total_read_bytes: details.total_read_bytes,
                    total_written_bytes: details.total_written_bytes,
                    executable: details.executable,
                    command: details.command,
                }),
            })
            .collect(),
        truncated: snapshot.truncated,
        cpu_warmed_up: snapshot.cpu_warmed_up,
    }
}

const fn into_proto_state(state: CollectedProcessState) -> ProcessState {
    match state {
        CollectedProcessState::Idle => ProcessState::Idle,
        CollectedProcessState::Running => ProcessState::Running,
        CollectedProcessState::Sleeping => ProcessState::Sleeping,
        CollectedProcessState::Stopped => ProcessState::Stopped,
        CollectedProcessState::Zombie => ProcessState::Zombie,
        CollectedProcessState::Tracing => ProcessState::Tracing,
        CollectedProcessState::Dead => ProcessState::Dead,
        CollectedProcessState::Wakekill => ProcessState::Wakekill,
        CollectedProcessState::Waking => ProcessState::Waking,
        CollectedProcessState::Parked => ProcessState::Parked,
        CollectedProcessState::LockBlocked => ProcessState::LockBlocked,
        CollectedProcessState::UninterruptibleDiskSleep => ProcessState::UninterruptibleDiskSleep,
        CollectedProcessState::Suspended => ProcessState::Suspended,
        CollectedProcessState::Unknown => ProcessState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use smalux_protocol::agent::v1::task_result;

    use crate::{
        scheduler::ReportingTask,
        tasks::collect::{CollectionMode, ProcessRanking, ProcessSelection, context},
    };

    use super::{ProcessTask, ProcessTaskConfig};

    #[tokio::test]
    async fn process_task_preserves_config_and_returns_the_selected_process() {
        let current_pid = std::process::id();
        let task = ProcessTask::try_with_config(ProcessTaskConfig {
            mode: CollectionMode::Detailed as i32,
            selection: Some(ProcessSelection {
                include_pids: vec![current_pid],
                ..ProcessSelection::default()
            }),
            ranking: ProcessRanking::Pid as i32,
            max_entries: None,
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), ProcessTask::KIND);
        assert_eq!(task.config().mode, CollectionMode::Detailed as i32);
        let Some(task_result::Result::Process(snapshot)) = output.result else {
            panic!("process task must return TaskResult.process");
        };
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].pid, current_pid);
        assert!(snapshot.entries[0].details.is_some());
    }

    #[test]
    fn process_task_rejects_expensive_ranking_in_basic_mode() {
        let error = ProcessTask::try_with_config(ProcessTaskConfig {
            mode: CollectionMode::Basic as i32,
            selection: Some(ProcessSelection::default()),
            ranking: ProcessRanking::Memory as i32,
            max_entries: None,
        })
        .err()
        .expect("the invalid config should be rejected");

        assert_eq!(
            error,
            crate::tasks::collect::ProcessConfigError::RankingRequiresDetailed
        );
    }
}
