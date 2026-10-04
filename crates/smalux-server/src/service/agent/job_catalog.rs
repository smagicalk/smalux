//! Server 业务层向已认证 Agent 提供远程 Job 目录的边界。

use smalux_protocol::agent::v1::{
    AgentCapabilitySnapshot, AgentJobPolicySnapshot, AgentPluginInventory, PluginRuntimeSnapshot,
    ReplaceAllJobs,
};
use std::sync::Arc;

use crate::database::ServerDatabase;

/// 按 Agent 身份和其本地策略读取权威 Job 目录。
#[tonic::async_trait]
pub trait AgentJobCatalogProvider: Send + Sync {
    /// 读取该 Agent 当前应执行的权威 Job 快照。
    ///
    /// `policy` 是 Agent 主动上报的本地黑名单快照，Provider 应在返回目录前应用它。
    /// `Ok(None)` 表示当前没有目录更新，不能解释为删除全部 Job。
    async fn load_catalog(
        &self,
        agent_id: &str,
        policy: &AgentJobPolicySnapshot,
        capability: &AgentCapabilitySnapshot,
        inventory: &AgentPluginInventory,
    ) -> anyhow::Result<Option<ReplaceAllJobs>>;
}

/// 当前第一阶段 Provider：完成协议流程，但尚不绑定具体 Job 数据源。
pub struct EmptyAgentJobCatalogProvider;

#[tonic::async_trait]
impl AgentJobCatalogProvider for EmptyAgentJobCatalogProvider {
    async fn load_catalog(
        &self,
        _agent_id: &str,
        _policy: &AgentJobPolicySnapshot,
        _capability: &AgentCapabilitySnapshot,
        _inventory: &AgentPluginInventory,
    ) -> anyhow::Result<Option<ReplaceAllJobs>> {
        Ok(None)
    }
}

/// 读取 Server 数据库中每个 Agent 的权威完整 Job 目录。
pub struct DatabaseAgentJobCatalogProvider {
    database: Arc<ServerDatabase>,
}

impl DatabaseAgentJobCatalogProvider {
    pub fn new(database: Arc<ServerDatabase>) -> Self {
        Self { database }
    }
}

#[tonic::async_trait]
impl AgentJobCatalogProvider for DatabaseAgentJobCatalogProvider {
    async fn load_catalog(
        &self,
        agent_id: &str,
        policy: &AgentJobPolicySnapshot,
        capability: &AgentCapabilitySnapshot,
        inventory: &AgentPluginInventory,
    ) -> anyhow::Result<Option<ReplaceAllJobs>> {
        Ok(self
            .database
            .load_agent_job_catalog_for_session(agent_id, policy, capability, inventory)
            .await?
            .map(|record| record.catalog))
    }
}

/// 按 Agent 已安装插件清单生成当前会话的 Worker 运行时快照。
///
/// 该边界与 Job catalog 分开：前者只决定已经安装的 Worker 如何初始化，后者才决定
/// 哪些 Job 可以运行。第一版不持久化配置或 Secret，具体控制面可在后续替换 Provider。
#[tonic::async_trait]
pub trait AgentPluginRuntimeProvider: Send + Sync {
    /// 返回当前会话应该应用的完整插件运行时快照。
    async fn load_runtime(
        &self,
        agent_id: &str,
        inventory: &AgentPluginInventory,
    ) -> anyhow::Result<PluginRuntimeSnapshot>;
}

/// 第一阶段默认 Provider：确认同步顺序，但不激活任何手工安装插件。
pub struct EmptyAgentPluginRuntimeProvider;

#[tonic::async_trait]
impl AgentPluginRuntimeProvider for EmptyAgentPluginRuntimeProvider {
    async fn load_runtime(
        &self,
        _agent_id: &str,
        _inventory: &AgentPluginInventory,
    ) -> anyhow::Result<PluginRuntimeSnapshot> {
        Ok(PluginRuntimeSnapshot {
            revision: 1,
            plugins: Vec::new(),
        })
    }
}

/// 从数据库加载与当前 Agent inventory 匹配的 Plus Worker 运行时快照。
pub struct DatabaseAgentPluginRuntimeProvider {
    database: Arc<ServerDatabase>,
}

impl DatabaseAgentPluginRuntimeProvider {
    pub fn new(database: Arc<ServerDatabase>) -> Self {
        Self { database }
    }
}

#[tonic::async_trait]
impl AgentPluginRuntimeProvider for DatabaseAgentPluginRuntimeProvider {
    async fn load_runtime(
        &self,
        agent_id: &str,
        inventory: &AgentPluginInventory,
    ) -> anyhow::Result<PluginRuntimeSnapshot> {
        Ok(self
            .database
            .load_agent_plugin_runtime(agent_id, inventory)
            .await?)
    }
}
