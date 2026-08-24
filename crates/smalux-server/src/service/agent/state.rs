//! Agent 领域共享状态。
//!
//! 该状态与 Axum 的顶层 `AppState` 解耦：gRPC service 没有 Axum `State` extractor，
//! 因此由 `AgentTransportService` 直接持有这个状态。Noise 密钥环和注册中心在
//! Server 启动时创建一次，所有 Agent 会话共享它们。

use std::{collections::HashMap, sync::Arc};

use crate::config::RuntimeConfig;
use crate::database::ServerDatabase;
use smalux_protocol::agent::v1::{
    PluginPauseNotice, PluginRuntimeSnapshot, ReplaceAllJobs, task_definition,
};
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::agent_registry::AgentRegistry;
use super::job_catalog::{
    AgentJobCatalogProvider, AgentPluginRuntimeProvider, EmptyAgentJobCatalogProvider,
    EmptyAgentPluginRuntimeProvider,
};
use super::keyring_manager::ServerKeyRingManager;
use super::plugin_schema_registry::PluginSchemaRegistry;
use super::session_registry::SessionRegistry;

/// Agent 协议服务使用的共享依赖。
#[derive(Clone)]
pub struct AgentState {
    /// 脱敏数据库后端标签，仅用于会话诊断日志。
    pub(crate) database_backend: &'static str,
    /// Server Noise 密钥环管理器；所有握手和轮换都通过它取得一致句柄。
    pub(crate) keyring_manager: Arc<ServerKeyRingManager>,
    /// Agent 注册中心；负责 Token、注册事务、Agent 激活、授权和吊销查询。
    pub(crate) agent_registry: Arc<AgentRegistry>,
    /// 已认证后按本地策略读取该 Agent 的权威远程 Job 目录。
    pub(crate) job_catalog: Arc<dyn AgentJobCatalogProvider>,
    /// 根据 Agent inventory 生成当前会话有效的 Plus Worker 运行时快照。
    pub(crate) plugin_runtime: Arc<dyn AgentPluginRuntimeProvider>,
    /// Agent 上报的 Plus 参数 Schema 内容寻址注册表。
    pub(crate) plugin_schemas: Arc<PluginSchemaRegistry>,
    /// Agent 上报 Plus Worker 熔断状态；暂停只影响同一 Agent 的对应插件。
    pub(crate) plugin_pauses: Arc<PluginPauseRegistry>,
    /// 限制所有 Agent gRPC 流同时占用的服务资源。
    pub(crate) session_slots: Arc<Semaphore>,
    /// 限制同时执行注册业务提交的会话数量。
    pub(crate) registration_slots: Arc<Semaphore>,
    /// 当前进程中的实时 Session 目录，供本地管理接口查询和定向取消。
    pub(crate) sessions: SessionRegistry,
    /// Tonic 单条 protobuf 消息的大小上限。
    pub(crate) max_grpc_message_bytes: usize,
    /// Server 关闭时通知握手、注册和业务循环退出。
    pub(crate) shutdown: CancellationToken,
}

impl AgentState {
    /// 使用共享数据库和密钥环管理器创建 Agent 状态。
    pub(crate) fn new(
        database: Arc<ServerDatabase>,
        keyring_manager: Arc<ServerKeyRingManager>,
        runtime_config: &RuntimeConfig,
        shutdown: CancellationToken,
    ) -> Self {
        tracing::info!(
            backend = database.backend_label(),
            "creating Agent shared state"
        );
        let database_backend = database.backend_label();
        let agent_registry = Arc::new(AgentRegistry::new(Arc::clone(&database)));
        let plugin_schemas = Arc::new(PluginSchemaRegistry::new(Arc::clone(&database)));
        Self {
            database_backend,
            keyring_manager,
            agent_registry,
            job_catalog: Arc::new(EmptyAgentJobCatalogProvider),
            plugin_runtime: Arc::new(EmptyAgentPluginRuntimeProvider),
            plugin_schemas,
            plugin_pauses: Arc::new(PluginPauseRegistry::default()),
            session_slots: Arc::new(Semaphore::new(runtime_config.max_agent_sessions)),
            registration_slots: Arc::new(Semaphore::new(runtime_config.max_registration_sessions)),
            sessions: SessionRegistry::default(),
            max_grpc_message_bytes: runtime_config.max_grpc_message_bytes,
            shutdown,
        }
    }

    /// 替换默认空 Provider；供后续数据库或控制面 Job 目录实现装配使用。
    pub fn with_job_catalog_provider(mut self, provider: Arc<dyn AgentJobCatalogProvider>) -> Self {
        self.job_catalog = provider;
        self
    }

    /// 替换默认空插件运行时 Provider；供未来控制面按 Agent inventory 选择配置。
    pub fn with_plugin_runtime_provider(
        mut self,
        provider: Arc<dyn AgentPluginRuntimeProvider>,
    ) -> Self {
        self.plugin_runtime = provider;
        self
    }

    /// 尝试占用一个 Agent 会话槽位；槽位随 permit 生命周期自动释放。
    pub(crate) fn try_acquire_session(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        Arc::clone(&self.session_slots).try_acquire_owned()
    }

    /// 尝试占用一个注册业务槽位；握手完成后才进入此限制。
    pub(crate) fn try_acquire_registration(
        &self,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, tokio::sync::TryAcquireError> {
        Arc::clone(&self.registration_slots).try_acquire_owned()
    }
}

/// Server 进程内的 Agent/插件暂停目录。
///
/// 当前只保存运行态；新的运行时 revision 可以清除旧暂停。后续接入管理数据库时，
/// 该结构可直接替换为 repository，不改变会话层过滤接口。
#[derive(Clone, Default)]
pub struct PluginPauseRegistry {
    entries: Arc<RwLock<HashMap<PluginPauseKey, PluginPauseNotice>>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PluginPauseKey {
    agent_id: String,
    plugin_id: String,
    plugin_version: String,
}

impl PluginPauseRegistry {
    pub async fn record(&self, agent_id: &str, notice: PluginPauseNotice) -> anyhow::Result<()> {
        if agent_id.is_empty()
            || notice.plugin_id.is_empty()
            || notice.plugin_version.is_empty()
            || notice.runtime_revision == 0
            || notice.failure_count == 0
        {
            anyhow::bail!("invalid Plus plugin pause notice");
        }
        let key = PluginPauseKey {
            agent_id: agent_id.to_owned(),
            plugin_id: notice.plugin_id.clone(),
            plugin_version: notice.plugin_version.clone(),
        };
        self.entries.write().await.insert(key, notice);
        Ok(())
    }

    pub async fn acknowledge_new_runtime(&self, agent_id: &str, snapshot: &PluginRuntimeSnapshot) {
        let mut entries = self.entries.write().await;
        for plugin in &snapshot.plugins {
            let key = PluginPauseKey {
                agent_id: agent_id.to_owned(),
                plugin_id: plugin.plugin_id.clone(),
                plugin_version: plugin.version.clone(),
            };
            if entries
                .get(&key)
                .is_some_and(|notice| snapshot.revision > notice.runtime_revision)
            {
                entries.remove(&key);
            }
        }
    }

    pub async fn is_paused(&self, agent_id: &str, plugin_id: &str, plugin_version: &str) -> bool {
        self.entries.read().await.contains_key(&PluginPauseKey {
            agent_id: agent_id.to_owned(),
            plugin_id: plugin_id.to_owned(),
            plugin_version: plugin_version.to_owned(),
        })
    }

    /// 从 Server 当前目录中移除已暂停插件的 Job；非插件 Job 保持不变。
    pub async fn filter_catalog(
        &self,
        agent_id: &str,
        mut catalog: ReplaceAllJobs,
    ) -> ReplaceAllJobs {
        let entries = self.entries.read().await;
        catalog.jobs.retain(|job| {
            let Some(task_definition) = job.task.as_ref() else {
                return true;
            };
            let Some(task_definition::Task::Plugin(plugin)) = task_definition.task.as_ref() else {
                return true;
            };
            !entries.contains_key(&PluginPauseKey {
                agent_id: agent_id.to_owned(),
                plugin_id: plugin.plugin_id.clone(),
                plugin_version: plugin.plugin_version.clone(),
            })
        });
        catalog
    }
}

#[cfg(test)]
mod tests {
    use super::PluginPauseRegistry;
    use smalux_protocol::agent::v1::{
        JobDefinition, PluginPauseNotice, PluginRuntimeConfig, PluginRuntimeSnapshot,
        ReplaceAllJobs, TaskDefinition, task_definition,
    };

    fn plugin_job() -> JobDefinition {
        JobDefinition {
            job_id: vec![1; 16],
            revision: 1,
            enabled: true,
            trigger: None,
            options: None,
            task: Some(TaskDefinition {
                task: Some(task_definition::Task::Plugin(
                    smalux_protocol::agent::v1::PluginTaskConfig {
                        plugin_id: "smalux.plus.echo".to_owned(),
                        plugin_version: "1.0.0".to_owned(),
                        task_kind: "smalux.echo.v1".to_owned(),
                        schema_version: 1,
                        task_config: Vec::new(),
                    },
                )),
            }),
        }
    }

    #[tokio::test]
    async fn paused_plugin_is_filtered_only_for_the_matching_agent() {
        let registry = PluginPauseRegistry::default();
        registry
            .record(
                "agent-a",
                PluginPauseNotice {
                    plugin_id: "smalux.plus.echo".to_owned(),
                    plugin_version: "1.0.0".to_owned(),
                    runtime_revision: 4,
                    failure_count: 3,
                    failure_window_started_at_ms: 1,
                    paused_at_ms: 2,
                    last_exit_reason: "exit 1".to_owned(),
                    last_error: String::new(),
                },
            )
            .await
            .unwrap();
        let catalog = ReplaceAllJobs {
            catalog_revision: 9,
            jobs: vec![plugin_job()],
        };
        assert!(
            registry
                .filter_catalog("agent-a", catalog.clone())
                .await
                .jobs
                .is_empty()
        );
        assert_eq!(
            registry.filter_catalog("agent-b", catalog).await.jobs.len(),
            1
        );
    }

    #[tokio::test]
    async fn newer_runtime_snapshot_clears_agent_plugin_pause() {
        let registry = PluginPauseRegistry::default();
        registry
            .record(
                "agent-a",
                PluginPauseNotice {
                    plugin_id: "smalux.plus.echo".to_owned(),
                    plugin_version: "1.0.0".to_owned(),
                    runtime_revision: 4,
                    failure_count: 3,
                    failure_window_started_at_ms: 1,
                    paused_at_ms: 2,
                    last_exit_reason: "exit 1".to_owned(),
                    last_error: String::new(),
                },
            )
            .await
            .unwrap();
        registry
            .acknowledge_new_runtime(
                "agent-a",
                &PluginRuntimeSnapshot {
                    revision: 5,
                    plugins: vec![PluginRuntimeConfig {
                        plugin_id: "smalux.plus.echo".to_owned(),
                        version: "1.0.0".to_owned(),
                        schema_version: 1,
                        config: Vec::new(),
                        requested_concurrency: 1,
                    }],
                },
            )
            .await;
        assert!(
            !registry
                .is_paused("agent-a", "smalux.plus.echo", "1.0.0")
                .await
        );
    }
}
