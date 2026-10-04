//! Agent 远程 Job 控制面的写入门面。
//!
//! 管理入口不直接操作 ORM 或 Session；目录成功提交后由本模块通知在线连接执行完整对账。

use std::sync::Arc;

use smalux_protocol::agent::v1::JobDefinition;

use crate::database::{AgentJobCatalogRecord, ServerDatabase, StoredPluginRuntime};

use super::session_registry::SessionRegistry;

#[derive(Clone)]
pub struct AgentControlPlane {
    database: Arc<ServerDatabase>,
    sessions: SessionRegistry,
    /// 串行化当前 Server 进程内的目录和 runtime 写入，避免“读取旧版本后同时覆盖”。
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AgentControlPlane {
    pub(crate) fn new(database: Arc<ServerDatabase>, sessions: SessionRegistry) -> Self {
        Self {
            database,
            sessions,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// 事务替换该 Agent 的权威目录，并通知其当前在线会话重新读取完整快照。
    pub async fn replace_catalog(
        &self,
        agent_id: &str,
        jobs: Vec<JobDefinition>,
    ) -> anyhow::Result<AgentJobCatalogRecord> {
        self.replace_catalog_if_revision(agent_id, jobs, None).await
    }

    /// 按可选的期望目录版本替换 Agent Job；带版本时是严格 CAS 写入。
    pub async fn replace_catalog_if_revision(
        &self,
        agent_id: &str,
        jobs: Vec<JobDefinition>,
        expected_revision: Option<u64>,
    ) -> anyhow::Result<AgentJobCatalogRecord> {
        let _write_guard = self.write_lock.lock().await;
        let record = self
            .database
            .replace_agent_job_catalog_if_revision(agent_id, jobs, expected_revision)
            .await?;
        let notified = self.sessions.notify_catalog_changed(agent_id).await;
        tracing::info!(
            agent_id,
            catalog_revision = record.catalog.catalog_revision,
            jobs = record.catalog.jobs.len(),
            online_sessions = notified,
            "Agent Job catalog replaced"
        );
        Ok(record)
    }

    /// 替换 Agent 的 Worker runtime 期望配置，并通知在线会话重新走 runtime ACK 门控。
    pub async fn replace_plugin_runtime(
        &self,
        agent_id: &str,
        runtimes: Vec<StoredPluginRuntime>,
    ) -> anyhow::Result<u64> {
        self.replace_plugin_runtime_if_revision(agent_id, runtimes, None)
            .await
    }

    /// 按可选的期望 runtime 版本替换 Agent Plugin runtime；带版本时是严格 CAS 写入。
    pub async fn replace_plugin_runtime_if_revision(
        &self,
        agent_id: &str,
        runtimes: Vec<StoredPluginRuntime>,
        expected_revision: Option<u64>,
    ) -> anyhow::Result<u64> {
        let _write_guard = self.write_lock.lock().await;
        let revision = self
            .database
            .replace_agent_plugin_runtime_if_revision(agent_id, runtimes, expected_revision)
            .await?;
        let notified = self.sessions.notify_runtime_changed(agent_id).await;
        tracing::info!(
            agent_id,
            runtime_revision = revision,
            online_sessions = notified,
            "Agent plugin runtime replaced"
        );
        Ok(revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig, database::entity::agent,
        service::agent::session_registry::SessionRegistry,
    };
    use sea_orm::{ActiveValue::Set, EntityTrait};
    use smalux_protocol::agent::v1::{JobDefinition, TaskDefinition, task_definition};
    use uuid::Uuid;

    #[tokio::test]
    async fn catalog_commit_notifies_authenticated_session() {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .unwrap(),
        );
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![1; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
        let sessions = SessionRegistry::default();
        sessions.register(1).await;
        sessions.mark_authenticated(1, "agent-a", "ik").await;
        let mut changes = sessions.subscribe_catalog_changes(1).await.unwrap();
        let control = AgentControlPlane::new(database, sessions);
        control
            .replace_catalog(
                "agent-a",
                vec![JobDefinition {
                    job_id: Uuid::new_v4().as_bytes().to_vec(),
                    revision: 1,
                    enabled: true,
                    task: Some(TaskDefinition {
                        task: Some(task_definition::Task::Cpu(Default::default())),
                    }),
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        changes.changed().await.unwrap();
    }
}
