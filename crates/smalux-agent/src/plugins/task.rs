//! 把 Plus Worker 输出适配为 Agent Scheduler 的标准 ReportingTask。

use std::sync::Arc;

use async_trait::async_trait;
use smalux_protocol::agent::v1::{
    PluginMetric, PluginTaskConfig, PluginTaskResult, SampleMetadata, TaskResult, task_result,
};

use crate::scheduler::{ReportingTask, TaskContext, TaskError};

use super::{PluginManager, manager::PluginExecutionRequest};

/// 单个远程 Job 对 Plus Worker 的不可变引用。
pub struct PluginReportingTask {
    manager: Arc<PluginManager>,
    config: PluginTaskConfig,
}

impl PluginReportingTask {
    pub fn new(manager: Arc<PluginManager>, config: PluginTaskConfig) -> Self {
        Self { manager, config }
    }
}

#[async_trait]
impl ReportingTask for PluginReportingTask {
    fn kind(&self) -> &str {
        &self.config.task_kind
    }

    async fn run(&self, context: TaskContext) -> Result<TaskResult, TaskError> {
        let output = self
            .manager
            .execute(PluginExecutionRequest {
                plugin_id: self.config.plugin_id.clone(),
                plugin_version: self.config.plugin_version.clone(),
                run_id: context.run_id,
                task_kind: self.config.task_kind.clone(),
                schema_version: self.config.schema_version,
                config: self.config.task_config.clone(),
                cancellation: context.cancellation,
            })
            .await
            .map_err(|error| TaskError::Transient(anyhow::anyhow!(error)))?;
        Ok(TaskResult {
            sample: Some(SampleMetadata {
                sampled_at_ms: context.started_at.timestamp_millis().max(0) as u64,
                sample_interval_ms: None,
            }),
            result: Some(task_result::Result::Plugin(PluginTaskResult {
                plugin_id: self.config.plugin_id.clone(),
                plugin_version: self.config.plugin_version.clone(),
                task_kind: self.config.task_kind.clone(),
                schema_version: self.config.schema_version,
                summary: output.summary,
                metrics: output
                    .metrics
                    .into_iter()
                    .map(|(name, value)| PluginMetric { name, value })
                    .collect(),
                payload: output.payload,
            })),
        })
    }
}
