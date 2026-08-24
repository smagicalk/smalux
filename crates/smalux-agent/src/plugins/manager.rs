//! Plus Worker 的配置、生命周期、崩溃恢复和本地暂停状态。

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::future::join_all;
use smalux_plus_core::{AgentContext, protocol::AgentContextMessage};
use smalux_protocol::agent::v1::{PluginPauseNotice, PluginRuntimeConfig, PluginRuntimeSnapshot};

use super::worker::WorkerStartOptions;
use super::{
    InstalledPlugin, PluginCatalog, PluginWorkerClient, PluginWorkerError, WorkerTaskOutput,
};

type PluginKey = (String, String);

#[derive(Clone, Debug)]
pub struct PluginRuntimeLimits {
    pub max_workers: usize,
    pub max_concurrency: u32,
    pub task_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub startup_timeout: Duration,
    pub restart_max_attempts: usize,
    pub restart_window: Duration,
}

#[derive(Clone, Debug)]
pub struct PluginStatusSnapshot {
    pub plugin_id: String,
    pub version: String,
    pub installed: bool,
    pub active: bool,
    pub worker_pid: Option<u32>,
    pub config_revision: Option<u64>,
    pub concurrency: Option<u32>,
    pub state: String,
    pub failure_count: usize,
    pub failure_window_started_at_ms: Option<u64>,
    pub paused_at_ms: Option<u64>,
    pub last_exit_reason: Option<String>,
    pub last_error: Option<String>,
    pub server_pause_acknowledged: bool,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerState {
    Running,
    Restarting,
    Paused,
}

impl WorkerState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Restarting => "restarting",
            Self::Paused => "paused",
        }
    }
}

/// 当前插件的期望配置和可选 Worker 进程。
#[derive(Clone)]
struct ManagedWorker {
    worker: Option<Arc<PluginWorkerClient>>,
    config: Vec<u8>,
    requested_concurrency: u32,
    effective_concurrency: u32,
    config_revision: u64,
    state: WorkerState,
    failures: VecDeque<Instant>,
    failure_window_started: Option<Instant>,
    failure_window_started_at_ms: Option<u64>,
    next_restart: Option<Instant>,
    paused_at_ms: Option<u64>,
    last_exit_reason: Option<String>,
    last_error: Option<String>,
    server_pause_acknowledged: bool,
    generation: u64,
}

/// Agent 交给一个已激活 Worker 的完整单次执行请求。
pub(super) struct PluginExecutionRequest {
    pub plugin_id: String,
    pub plugin_version: String,
    pub run_id: uuid::Uuid,
    pub task_kind: String,
    pub schema_version: u32,
    pub config: Vec<u8>,
    pub cancellation: tokio_util::sync::CancellationToken,
}

/// 已安装插件与当前会话 Plus Worker 的唯一入口。
pub struct PluginManager {
    catalog: PluginCatalog,
    agent_context: RwLock<AgentContext>,
    limits: PluginRuntimeLimits,
    workers: RwLock<HashMap<PluginKey, ManagedWorker>>,
    pause_notices: tokio::sync::Mutex<HashMap<PluginKey, PluginPauseNotice>>,
    pause_notice_sent: tokio::sync::Mutex<HashSet<PluginKey>>,
    next_generation: AtomicU64,
}

impl PluginManager {
    pub fn new(
        catalog: PluginCatalog,
        agent_context: AgentContext,
        limits: PluginRuntimeLimits,
    ) -> Self {
        Self {
            catalog,
            agent_context: RwLock::new(agent_context),
            limits,
            workers: RwLock::new(HashMap::new()),
            pause_notices: tokio::sync::Mutex::new(HashMap::new()),
            pause_notice_sent: tokio::sync::Mutex::new(HashSet::new()),
            next_generation: AtomicU64::new(1),
        }
    }

    /// 没有本地插件的 Manager，供旧的内置 TaskFactory 构造路径保持兼容。
    pub fn empty() -> Self {
        Self::new(
            PluginCatalog::default(),
            AgentContext::default(),
            PluginRuntimeLimits {
                max_workers: 16,
                max_concurrency: 4,
                task_timeout: Duration::from_secs(300),
                shutdown_timeout: Duration::from_secs(5),
                startup_timeout: Duration::from_secs(10),
                restart_max_attempts: 3,
                restart_window: Duration::from_secs(600),
            },
        )
    }

    pub fn catalog(&self) -> &PluginCatalog {
        &self.catalog
    }

    /// 在首次认证完成后补充 Agent ID；其余上下文保持启动时的只读值。
    pub fn set_agent_id(&self, agent_id: Option<String>) {
        self.agent_context
            .write()
            .expect("Plugin Worker context lock poisoned")
            .agent_id = agent_id;
    }

    /// 返回本地 IPC 使用的插件生命周期快照。
    pub async fn status(&self) -> Vec<PluginStatusSnapshot> {
        let active = self
            .workers
            .read()
            .expect("Plugin Worker lock poisoned")
            .clone();
        let mut result = Vec::new();
        for plugin in self.catalog.plugins() {
            let key = (
                plugin.manifest.plugin_id.clone(),
                plugin.manifest.version.to_string(),
            );
            let worker = active.get(&key);
            let worker_pid = match worker.and_then(|worker| worker.worker.as_ref()) {
                Some(worker) => worker.pid().await,
                None => None,
            };
            result.push(PluginStatusSnapshot {
                plugin_id: key.0,
                version: key.1,
                installed: true,
                active: worker.is_some_and(|worker| worker.state == WorkerState::Running),
                worker_pid,
                config_revision: worker.map(|worker| worker.config_revision),
                concurrency: worker.map(|worker| worker.effective_concurrency),
                state: worker
                    .map(|worker| worker.state.as_str().to_owned())
                    .unwrap_or_else(|| "inactive".to_owned()),
                failure_count: worker.map(|worker| worker.failures.len()).unwrap_or(0),
                failure_window_started_at_ms: worker
                    .and_then(|worker| worker.failure_window_started_at_ms),
                paused_at_ms: worker.and_then(|worker| worker.paused_at_ms),
                last_exit_reason: worker.and_then(|worker| worker.last_exit_reason.clone()),
                last_error: worker.and_then(|worker| worker.last_error.clone()),
                server_pause_acknowledged: worker
                    .map(|worker| worker.server_pause_acknowledged)
                    .unwrap_or(false),
                generation: worker.map(|worker| worker.generation).unwrap_or(0),
            });
        }
        result
    }

    /// 检查 Worker 子进程并执行一次后台恢复；由 Agent 主事件循环周期性调用。
    pub async fn poll_health(&self) {
        let entries = self
            .workers
            .read()
            .expect("Plugin Worker lock poisoned")
            .clone();
        for (key, entry) in entries {
            if let Some(worker) = entry.worker {
                match worker.try_wait().await {
                    Ok(Some(status)) => {
                        let reason = match status.code() {
                            Some(code) => format!("exit code {code}"),
                            None => "terminated by signal".to_owned(),
                        };
                        worker.shutdown().await;
                        self.record_failure(&key, entry.generation, reason, None)
                            .await;
                    }
                    Ok(None) if worker.reader_closed() => {
                        worker.shutdown().await;
                        self.record_failure(
                            &key,
                            entry.generation,
                            "Worker protocol reader closed".to_owned(),
                            Some("Worker stdout closed before Shutdown".to_owned()),
                        )
                        .await;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(plugin_id = %key.0, version = %key.1, %error, "failed to inspect Plus Worker");
                    }
                }
            } else if entry.state == WorkerState::Restarting
                && entry
                    .next_restart
                    .is_some_and(|time| time <= Instant::now())
            {
                self.restart(&key, entry.generation).await;
            }
        }
    }

    /// 判断某个插件是否已经被本地熔断暂停。
    pub fn is_paused(&self, plugin_id: &str, version: &str) -> bool {
        self.workers
            .read()
            .expect("Plugin Worker lock poisoned")
            .get(&(plugin_id.to_owned(), version.to_owned()))
            .is_some_and(|worker| worker.state == WorkerState::Paused)
    }

    /// 返回尚未得到 Server 确认的暂停通知；重连时可以重复发送。
    pub async fn pending_pause_notices(&self) -> Vec<PluginPauseNotice> {
        let notices = self.pause_notices.lock().await;
        let sent = self.pause_notice_sent.lock().await;
        notices
            .iter()
            .filter(|(key, _)| !sent.contains(*key))
            .map(|(_, notice)| notice.clone())
            .collect()
    }

    pub async fn mark_pause_notices_sent(&self, notices: &[PluginPauseNotice]) {
        let mut sent = self.pause_notice_sent.lock().await;
        for notice in notices {
            sent.insert((notice.plugin_id.clone(), notice.plugin_version.clone()));
        }
    }

    /// 新会话建立后允许重新投递尚未得到 ACK 的暂停通知。
    pub async fn reset_pause_notice_delivery(&self) {
        self.pause_notice_sent.lock().await.clear();
    }

    /// Server 确认暂停后，清理对应的待发送通知。
    pub async fn acknowledge_pause(&self, ack: &smalux_protocol::agent::v1::PluginPauseAck) {
        if !ack.accepted {
            return;
        }
        let key = (ack.plugin_id.clone(), ack.plugin_version.clone());
        self.pause_notices.lock().await.remove(&key);
        self.pause_notice_sent.lock().await.remove(&key);
        if let Some(worker) = self
            .workers
            .write()
            .expect("Plugin Worker lock poisoned")
            .get_mut(&key)
            && worker.config_revision == ack.runtime_revision
        {
            worker.server_pause_acknowledged = true;
        }
    }

    /// 启动快照中的 Worker；全部成功才替换当前活跃映射。
    pub async fn apply_snapshot(&self, snapshot: &PluginRuntimeSnapshot) -> Result<(), String> {
        let mut workers = HashMap::new();
        if snapshot.plugins.len() > self.limits.max_workers {
            return Err(format!(
                "Plus Worker count exceeds local limit {}",
                self.limits.max_workers
            ));
        }
        let current = self
            .workers
            .read()
            .expect("Plugin Worker lock poisoned")
            .clone();
        let mut started: Vec<Arc<PluginWorkerClient>> = Vec::new();
        for config in &snapshot.plugins {
            let key = (config.plugin_id.clone(), config.version.clone());
            if workers.contains_key(&key) {
                return Err(format!(
                    "duplicate Plus runtime configuration for {} {}",
                    key.0, key.1
                ));
            }
            let concurrency = config
                .requested_concurrency
                .max(1)
                .min(self.limits.max_concurrency);
            if let Some(existing) = current.get(&key)
                && existing.state == WorkerState::Running
                && existing.worker.is_some()
                && existing.config == config.config
                && existing.requested_concurrency == concurrency
            {
                workers.insert(
                    key,
                    ManagedWorker {
                        worker: existing.worker.clone(),
                        config: existing.config.clone(),
                        requested_concurrency: concurrency,
                        effective_concurrency: existing.effective_concurrency,
                        config_revision: snapshot.revision,
                        state: WorkerState::Running,
                        failures: VecDeque::new(),
                        failure_window_started: None,
                        failure_window_started_at_ms: None,
                        next_restart: None,
                        paused_at_ms: None,
                        last_exit_reason: None,
                        last_error: None,
                        server_pause_acknowledged: false,
                        generation: existing.generation,
                    },
                );
                continue;
            }
            let worker = match self
                .start_worker(config, snapshot.revision, concurrency)
                .await
            {
                Ok(worker) => Arc::new(worker),
                Err(error) => {
                    for worker in started {
                        worker.shutdown().await;
                    }
                    return Err(error.to_string());
                }
            };
            let effective_concurrency = worker.effective_concurrency();
            started.push(Arc::clone(&worker));
            let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
            workers.insert(
                key,
                ManagedWorker {
                    worker: Some(worker),
                    config: config.config.clone(),
                    requested_concurrency: concurrency,
                    effective_concurrency,
                    config_revision: snapshot.revision,
                    state: WorkerState::Running,
                    failures: VecDeque::new(),
                    failure_window_started: None,
                    failure_window_started_at_ms: None,
                    next_restart: None,
                    paused_at_ms: None,
                    last_exit_reason: None,
                    last_error: None,
                    server_pause_acknowledged: false,
                    generation,
                },
            );
        }
        let old = {
            let mut active = self.workers.write().expect("Plugin Worker lock poisoned");
            std::mem::replace(&mut *active, workers)
        };
        let replaced = {
            let active = self.workers.read().expect("Plugin Worker lock poisoned");
            old.into_iter()
                .filter_map(|(key, old_worker)| {
                    let reused = active
                        .get(&key)
                        .and_then(|worker| worker.worker.as_ref())
                        .zip(old_worker.worker.as_ref())
                        .is_some_and(|(current, old)| Arc::ptr_eq(current, old));
                    (!reused).then_some(old_worker.worker).flatten()
                })
                .collect::<Vec<_>>()
        };
        join_all(replaced.into_iter().map(|worker| async move {
            worker.shutdown().await;
        }))
        .await;
        // 更高 revision 是 Server 的权威恢复动作，所有旧暂停通知均已失效。
        self.pause_notices.lock().await.clear();
        self.pause_notice_sent.lock().await.clear();
        Ok(())
    }

    async fn start_worker(
        &self,
        config: &PluginRuntimeConfig,
        revision: u64,
        concurrency: u32,
    ) -> Result<PluginWorkerClient, PluginWorkerError> {
        let plugin = self
            .find_installed(config)
            .map_err(PluginWorkerError::TaskFailed)?;
        PluginWorkerClient::start(
            plugin,
            WorkerStartOptions {
                config_revision: revision,
                runtime_config: config.config.clone(),
                max_concurrency: concurrency,
                agent_context: self.agent_context_for_plugin(&config.plugin_id, &config.version),
                task_timeout: self.limits.task_timeout,
                startup_timeout: self.limits.startup_timeout,
                shutdown_timeout: self.limits.shutdown_timeout,
            },
        )
        .await
    }

    async fn restart(&self, key: &PluginKey, generation: u64) {
        let (config, revision, requested_concurrency) = {
            let mut workers = self.workers.write().expect("Plugin Worker lock poisoned");
            let Some(worker) = workers.get_mut(key) else {
                return;
            };
            if worker.generation != generation || worker.state != WorkerState::Restarting {
                return;
            }
            worker.next_restart = None;
            (
                PluginRuntimeConfig {
                    plugin_id: key.0.clone(),
                    version: key.1.clone(),
                    schema_version: 0,
                    config: worker.config.clone(),
                    requested_concurrency: worker.requested_concurrency,
                },
                worker.config_revision,
                worker.requested_concurrency,
            )
        };
        match self
            .start_worker(&config, revision, requested_concurrency)
            .await
        {
            Ok(worker) => {
                let effective_concurrency = worker.effective_concurrency();
                let replacement = Arc::new(worker);
                let install = {
                    let mut workers = self.workers.write().expect("Plugin Worker lock poisoned");
                    workers.get_mut(key).is_some_and(|current| {
                        if current.generation == generation
                            && current.state == WorkerState::Restarting
                        {
                            current.worker = Some(Arc::clone(&replacement));
                            current.state = WorkerState::Running;
                            current.last_error = None;
                            current.effective_concurrency = effective_concurrency;
                            true
                        } else {
                            false
                        }
                    })
                };
                if install {
                    tracing::info!(plugin_id = %key.0, version = %key.1, generation, "Plus Worker recovered");
                } else {
                    replacement.shutdown().await;
                }
            }
            Err(error) => {
                self.record_failure(
                    key,
                    generation,
                    "restart failed".to_owned(),
                    Some(error.to_string()),
                )
                .await
            }
        }
    }

    async fn record_failure(
        &self,
        key: &PluginKey,
        generation: u64,
        reason: String,
        error: Option<String>,
    ) {
        let notice = {
            let mut workers = self.workers.write().expect("Plugin Worker lock poisoned");
            let Some(worker) = workers.get_mut(key) else {
                return;
            };
            if worker.generation != generation {
                tracing::debug!(plugin_id = %key.0, version = %key.1, generation, current_generation = worker.generation, "ignored stale Plus Worker failure");
                return;
            }
            worker.worker = None;
            let now = Instant::now();
            worker
                .failures
                .retain(|failure| now.duration_since(*failure) <= self.limits.restart_window);
            if worker.failures.is_empty() {
                worker.failure_window_started = None;
                worker.failure_window_started_at_ms = None;
            }
            if worker.failure_window_started.is_none() {
                worker.failure_window_started = Some(now);
                worker.failure_window_started_at_ms = Some(unix_millis());
            }
            worker.failures.push_back(now);
            worker.last_exit_reason = Some(reason.clone());
            worker.last_error = error.clone();
            if worker.failures.len() >= self.limits.restart_max_attempts.max(1) {
                worker.state = WorkerState::Paused;
                worker.paused_at_ms = Some(unix_millis());
                worker.next_restart = None;
                tracing::warn!(plugin_id = %key.0, version = %key.1, failures = worker.failures.len(), "Plus Worker paused after repeated failures");
                Some(PluginPauseNotice {
                    plugin_id: key.0.clone(),
                    plugin_version: key.1.clone(),
                    runtime_revision: worker.config_revision,
                    failure_count: worker.failures.len() as u32,
                    failure_window_started_at_ms: worker
                        .failure_window_started_at_ms
                        .unwrap_or_else(unix_millis),
                    paused_at_ms: worker.paused_at_ms.unwrap_or_else(unix_millis),
                    last_exit_reason: worker.last_exit_reason.clone().unwrap_or_default(),
                    last_error: worker.last_error.clone().unwrap_or_default(),
                })
            } else {
                worker.state = WorkerState::Restarting;
                let attempt = worker.failures.len().saturating_sub(1).min(10);
                worker.next_restart = Some(now + Duration::from_secs(1u64 << attempt));
                tracing::warn!(plugin_id = %key.0, version = %key.1, failures = worker.failures.len(), "Plus Worker restart scheduled");
                None
            }
        };
        if let Some(notice) = notice {
            self.pause_notices.lock().await.insert(key.clone(), notice);
            self.pause_notice_sent.lock().await.remove(key);
        }
    }

    fn agent_context_for_plugin(&self, plugin_id: &str, version: &str) -> AgentContextMessage {
        let mut context = self
            .agent_context
            .read()
            .expect("Plugin Worker context lock poisoned")
            .clone();
        context.plugin_data_dir = std::path::Path::new(&context.data_dir)
            .join("plus")
            .join(plugin_id)
            .join(version)
            .to_string_lossy()
            .into_owned();
        AgentContextMessage {
            agent_version: context.agent_version,
            operating_system: context.operating_system,
            architecture: context.architecture,
            agent_id: context.agent_id,
            data_dir: context.data_dir,
            config_dir: context.config_dir,
            plugin_dir: context.plugin_dir,
            plugin_data_dir: context.plugin_data_dir,
        }
    }

    /// 执行一个当前处于 Running 状态的插件 Task；暂停或恢复期间不自动重试。
    pub(super) async fn execute(
        &self,
        request: PluginExecutionRequest,
    ) -> Result<WorkerTaskOutput, PluginWorkerError> {
        let worker = self
            .workers
            .read()
            .expect("Plugin Worker lock poisoned")
            .get(&(request.plugin_id, request.plugin_version))
            .and_then(|worker| worker.worker.clone())
            .ok_or_else(|| {
                PluginWorkerError::TaskFailed(
                    "Plugin Worker is not active in this session".to_owned(),
                )
            })?;
        worker
            .execute(
                request.run_id,
                &request.task_kind,
                request.schema_version,
                request.config,
                request.cancellation,
            )
            .await
    }

    fn find_installed(&self, config: &PluginRuntimeConfig) -> Result<&InstalledPlugin, String> {
        self.catalog
            .plugins()
            .find(|plugin| {
                plugin.manifest.plugin_id == config.plugin_id
                    && plugin.manifest.version.to_string() == config.version
            })
            .ok_or_else(|| {
                format!(
                    "Plus plugin {} {} is not installed",
                    config.plugin_id, config.version
                )
            })
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::{ManagedWorker, PluginManager, WorkerState};
    use std::collections::VecDeque;

    fn manager_with_worker() -> (PluginManager, (String, String)) {
        let manager = PluginManager::empty();
        let key = ("smalux.plus.test".to_owned(), "1.0.0".to_owned());
        manager.workers.write().expect("worker lock").insert(
            key.clone(),
            ManagedWorker {
                worker: None,
                config: Vec::new(),
                requested_concurrency: 1,
                effective_concurrency: 1,
                config_revision: 7,
                state: WorkerState::Running,
                failures: VecDeque::new(),
                failure_window_started: None,
                failure_window_started_at_ms: None,
                next_restart: None,
                paused_at_ms: None,
                last_exit_reason: None,
                last_error: None,
                server_pause_acknowledged: false,
                generation: 7,
            },
        );
        (manager, key)
    }

    #[tokio::test]
    async fn repeated_worker_failures_pause_and_queue_one_notice() {
        let (manager, key) = manager_with_worker();
        for attempt in 1..=2 {
            manager
                .record_failure(&key, 7, format!("exit {attempt}"), None)
                .await;
            assert_eq!(
                manager
                    .workers
                    .read()
                    .expect("worker lock")
                    .get(&key)
                    .unwrap()
                    .state,
                WorkerState::Restarting
            );
        }

        manager
            .record_failure(&key, 7, "exit 3".to_owned(), None)
            .await;
        let worker = manager
            .workers
            .read()
            .expect("worker lock")
            .get(&key)
            .unwrap()
            .clone();
        assert_eq!(worker.state, WorkerState::Paused);
        assert_eq!(worker.failures.len(), 3);
        let notices = manager.pending_pause_notices().await;
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].runtime_revision, 7);
    }

    #[tokio::test]
    async fn pause_notice_is_removed_only_after_accepted_ack() {
        let (manager, key) = manager_with_worker();
        manager
            .record_failure(&key, 7, "exit".to_owned(), None)
            .await;
        manager
            .record_failure(&key, 7, "exit".to_owned(), None)
            .await;
        manager
            .record_failure(&key, 7, "exit".to_owned(), None)
            .await;
        let notice = manager.pending_pause_notices().await.pop().unwrap();
        manager
            .acknowledge_pause(&smalux_protocol::agent::v1::PluginPauseAck {
                plugin_id: notice.plugin_id.clone(),
                plugin_version: notice.plugin_version.clone(),
                runtime_revision: notice.runtime_revision,
                accepted: false,
                error: Some("retry".to_owned()),
            })
            .await;
        assert_eq!(manager.pending_pause_notices().await.len(), 1);
        manager
            .acknowledge_pause(&smalux_protocol::agent::v1::PluginPauseAck {
                plugin_id: notice.plugin_id.clone(),
                plugin_version: notice.plugin_version.clone(),
                runtime_revision: notice.runtime_revision,
                accepted: true,
                error: None,
            })
            .await;
        assert!(manager.pending_pause_notices().await.is_empty());
    }

    #[tokio::test]
    async fn stale_worker_generation_failure_is_ignored() {
        let (manager, key) = manager_with_worker();
        manager
            .record_failure(&key, 6, "stale exit".to_owned(), None)
            .await;
        let worker = manager
            .workers
            .read()
            .expect("worker lock")
            .get(&key)
            .unwrap()
            .clone();
        assert_eq!(worker.state, WorkerState::Running);
        assert!(worker.failures.is_empty());
    }
}
