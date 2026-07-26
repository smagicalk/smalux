//! 本机进程数量与分级列表周期采集任务。

use std::num::NonZeroUsize;

use anyhow::anyhow;
use async_trait::async_trait;

use crate::{
    scheduler::{TaskContext, TaskError, ValueTask},
    tasks::collect::collectors::process::{
        ProcessCollector, ProcessConfigError, ProcessRanking, ProcessSelection, ProcessSnapshot,
    },
};

use super::{CollectionMode, MetricSample, blocking::CollectState};

/// Process Task 的分级查询、筛选与 TopN 配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessTaskConfig {
    /// 汇总、轻量列表或资源与命令明细。
    pub mode: CollectionMode,
    /// 进程 PID 和名称筛选。
    pub selection: ProcessSelection,
    /// Detailed 列表的排序方式。
    pub ranking: ProcessRanking,
    /// Basic/Detailed 返回列表上限；`None` 使用对应档位默认值。
    pub max_entries: Option<NonZeroUsize>,
}

impl Default for ProcessTaskConfig {
    fn default() -> Self {
        Self {
            mode: CollectionMode::Summary,
            selection: ProcessSelection::default(),
            ranking: ProcessRanking::Pid,
            max_entries: None,
        }
    }
}

/// 使用独立 sysinfo 状态采集进程数量和有限列表的调度任务。
pub struct ProcessTask {
    state: CollectState<ProcessCollector>,
    config: ProcessTaskConfig,
}

impl ProcessTask {
    /// 用于 Job 快照和诊断的稳定任务名称。
    pub const KIND: &'static str = "smalux.collect.process.v1";

    /// 使用指定详细档位、PID 排名和其他默认配置创建 Task。
    pub fn new(mode: CollectionMode) -> Self {
        Self {
            state: CollectState::new(ProcessCollector::new()),
            config: ProcessTaskConfig {
                mode,
                ..ProcessTaskConfig::default()
            },
        }
    }

    /// 校验配置并创建 Process Task。
    pub fn try_with_config(config: ProcessTaskConfig) -> Result<Self, ProcessConfigError> {
        ProcessCollector::validate(config.mode, &config.selection, config.ranking)?;
        Ok(Self {
            state: CollectState::new(ProcessCollector::new()),
            config,
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
impl ValueTask for ProcessTask {
    type Output = MetricSample<ProcessSnapshot>;

    async fn run(&self, context: TaskContext) -> Result<Self::Output, TaskError> {
        let config = self.config.clone();
        self.state
            .try_collect(context, move |collector| {
                collector
                    .collect(
                        config.mode,
                        &config.selection,
                        config.ranking,
                        config.max_entries,
                    )
                    .map_err(|error| anyhow!(error))
            })
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
    use crate::{
        scheduler::ValueTask,
        tasks::collect::{CollectionMode, ProcessRanking, ProcessSelection, context},
    };

    use super::{ProcessTask, ProcessTaskConfig};

    #[tokio::test]
    async fn process_task_preserves_config_and_returns_the_selected_process() {
        let current_pid = std::process::id();
        let task = ProcessTask::try_with_config(ProcessTaskConfig {
            mode: CollectionMode::Detailed,
            selection: ProcessSelection {
                include_pids: vec![current_pid],
                ..ProcessSelection::default()
            },
            ranking: ProcessRanking::Pid,
            max_entries: None,
        })
        .unwrap();

        let output = task.run(context()).await.unwrap();

        assert_eq!(task.kind(), ProcessTask::KIND);
        assert_eq!(task.config().mode, CollectionMode::Detailed);
        assert_eq!(output.snapshot.entries.len(), 1);
        assert_eq!(output.snapshot.entries[0].pid, current_pid);
        assert!(output.snapshot.entries[0].details.is_some());
    }

    #[test]
    fn process_task_rejects_expensive_ranking_in_basic_mode() {
        let error = ProcessTask::try_with_config(ProcessTaskConfig {
            mode: CollectionMode::Basic,
            selection: ProcessSelection::default(),
            ranking: ProcessRanking::Memory,
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
