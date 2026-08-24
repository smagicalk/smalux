//! Plus Task 的开发接口。

use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Agent 注入给 Plus Worker 的只读运行环境信息。
#[derive(Clone, Debug, Default)]
pub struct AgentContext {
    pub agent_version: String,
    pub operating_system: String,
    pub architecture: String,
    pub agent_id: Option<String>,
    pub data_dir: String,
    pub config_dir: String,
    pub plugin_dir: String,
    pub plugin_data_dir: String,
}

/// Plus Task 的运行上下文。
#[derive(Clone, Debug)]
pub struct PlusTaskContext {
    pub request_id: String,
    pub run_id: Vec<u8>,
    pub deadline: Option<Duration>,
    pub cancellation: CancellationToken,
    pub agent: AgentContext,
}

/// Plus Task 的通用输出。
#[derive(Clone, Debug, Default)]
pub struct PlusTaskOutput {
    pub summary: String,
    pub metrics: Vec<(String, f64)>,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum PlusTaskError {
    #[error("invalid task configuration: {0}")]
    InvalidConfig(String),
    #[error("task timed out")]
    Timeout,
    #[error("task was cancelled: {0}")]
    Cancelled(String),
    #[error("task failed: {0}")]
    Failed(String),
}

#[async_trait]
pub trait PlusTask: Send + Sync + 'static {
    fn kind(&self) -> &'static str;

    /// Worker 在接受 Execute 前调用一次，用共享运行配置建立插件内部状态。
    ///
    /// 默认实现保持旧插件兼容。初始化失败会拒绝整个 Worker，Agent 不会向其下发任务。
    async fn initialize(
        &self,
        _agent: &AgentContext,
        _runtime_config: &[u8],
    ) -> Result<(), PlusTaskError> {
        Ok(())
    }

    /// Worker 收到 Shutdown 后调用，供插件在 Agent 强制回收前完成轻量收尾。
    ///
    /// 长时间或不可取消的工作不应放在这里；Agent 仍会在关闭期限到达后终止子进程。
    async fn shutdown(&self, _reason: &str) -> Result<(), PlusTaskError> {
        Ok(())
    }

    async fn execute(
        &self,
        context: PlusTaskContext,
        config: &[u8],
    ) -> Result<PlusTaskOutput, PlusTaskError>;
}
