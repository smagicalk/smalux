//! Smalux Agent 进程入口。

mod cli;
mod commands;
mod outbox;

use std::{
    future::Future,
    io::{IsTerminal, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

use smalux_agent::client::{
    AgentStateStore, FileAgentStateStore, SmaluxClient, SmaluxClientEvent, SmaluxClientHandle,
};
use smalux_agent::management::JobResultBufferStats;
use smalux_agent::management::{EffectiveConfigSnapshot, ManagementState};
use smalux_agent::plugins::{
    PluginCatalog, PluginManager, PluginRuntimeLimits, PluginRuntimeState, RuntimeSnapshotResult,
};
use smalux_agent::remote_jobs::{self, RemoteJobPolicyManager};
use smalux_agent::scheduler::{CallbackError, SchedulerRuntime, TaskReportSink};
use smalux_plus_core::AgentContext;
use smalux_protocol::{
    agent::v1::{
        AgentCapabilitySync, AgentPluginSync, agent_capability_sync, agent_job_policy_sync,
        agent_plugin_sync,
    },
    tonic_transport::SessionEvent,
};
use tokio_util::sync::CancellationToken;

const JOB_RESULT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const OUTBOX_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// 初始化日志，然后启动认证连接、远程 Job 控制器和本地 Scheduler。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = cli::parse();
    let control_endpoint = cli.control_endpoint;
    let cli::CliCommand::Run(args) = cli.command else {
        return commands::execute(control_endpoint, cli.command).await;
    };
    // Agent 的业务模块只产生 tracing 事件；由进程入口安装公共控制台和滚动文件层。
    smalux_core::logs::init_tracing();
    tracing::info!("smalux agent process started");
    let configuration = (*args).resolve(control_endpoint)?;
    if let Err(error) = run_agent(configuration).await {
        tracing::error!(error = %error, "smalux agent stopped with an error");
        return Err(error);
    }
    tracing::info!("smalux agent process stopped");
    Ok(())
}

/// 组装 Agent 的长期状态、连接层和调度层，并负责进程级优雅关闭。
async fn run_agent(configuration: cli::RunConfiguration) -> anyhow::Result<()> {
    let process_shutdown = CancellationToken::new();
    let signal_task = spawn_shutdown_signal_listener(process_shutdown.clone());
    let state_store = Arc::new(FileAgentStateStore::new(&configuration.state_file));
    let policy = Arc::new(load_job_policy(&configuration.policy_file).await?);
    let data_dir = configuration
        .state_file
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let plugins = Arc::new(PluginManager::new(
        PluginCatalog::discover(&configuration.plugin_directory)?,
        AgentContext {
            agent_version: env!("CARGO_PKG_VERSION").to_owned(),
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
            agent_id: None,
            data_dir: data_dir.to_string_lossy().into_owned(),
            config_dir: data_dir.join("agent").to_string_lossy().into_owned(),
            plugin_dir: configuration
                .plugin_directory
                .to_string_lossy()
                .into_owned(),
            plugin_data_dir: String::new(),
        },
        PluginRuntimeLimits {
            max_workers: configuration.plugin_max_workers,
            max_concurrency: configuration.plugin_max_concurrency as u32,
            task_timeout: configuration.plugin_task_timeout,
            shutdown_timeout: configuration.plugin_shutdown_timeout,
            startup_timeout: configuration.plugin_startup_timeout,
            restart_max_attempts: configuration.plugin_restart_max_attempts,
            restart_window: configuration.plugin_restart_window,
        },
    ));
    tracing::info!(
        directory = %plugins.catalog().root().display(),
        installed_plugins = plugins.catalog().plugins().count(),
        "discovered local Plus plugins"
    );
    let shared_store: Arc<dyn AgentStateStore> = state_store;
    let mut client = SmaluxClient::new(configuration.client.clone(), Arc::clone(&shared_store));
    let job_result_stats = Arc::new(JobResultBufferStats::default());
    let management = Arc::new(ManagementState::new(
        Arc::clone(&shared_store),
        client.subscribe_connection_status(),
        EffectiveConfigSnapshot {
            server_endpoint: configuration.client.endpoint.clone(),
            grpc_prefix: configuration.client.grpc_prefix.clone(),
            registration_token: if configuration.client.registration_token.is_some() {
                "<redacted>".to_owned()
            } else {
                "not_set".to_owned()
            },
            state_file: configuration.state_file.clone(),
            control_endpoint: configuration.control_endpoint.clone(),
            policy_file: configuration.policy_file.clone(),
            handshake_timeout_ms: duration_millis(configuration.client.handshake_timeout),
            heartbeat_interval_ms: duration_millis(configuration.client.heartbeat.interval),
            heartbeat_timeout_ms: duration_millis(configuration.client.heartbeat.timeout),
            reconnect_initial_delay_ms: duration_millis(
                configuration.client.reconnect.initial_delay,
            ),
            reconnect_max_delay_ms: duration_millis(configuration.client.reconnect.max_delay),
            task_report_buffer_capacity: configuration.task_report_buffer_capacity,
            job_result_buffer_capacity: configuration.job_result_buffer_capacity,
            shutdown_drain_timeout_ms: duration_millis(configuration.shutdown_drain_timeout),
            offline_job_timeout_ms: duration_millis(configuration.offline_job_timeout),
            scheduler_global_concurrency: configuration.scheduler.global_concurrency.get(),
            scheduler_global_max_pending: configuration.scheduler.global_max_pending,
            scheduler_default_job_concurrency: configuration
                .scheduler
                .default_job_concurrency
                .get(),
            scheduler_default_job_max_pending: configuration.scheduler.default_job_max_pending,
            scheduler_max_jobs: configuration.scheduler.max_jobs,
            scheduler_shutdown_timeout_ms: duration_millis(
                configuration.scheduler.shutdown_timeout,
            ),
            plugin_max_workers: configuration.plugin_max_workers,
            plugin_max_concurrency: configuration.plugin_max_concurrency,
            plugin_task_timeout_ms: duration_millis(configuration.plugin_task_timeout),
            plugin_shutdown_timeout_ms: duration_millis(configuration.plugin_shutdown_timeout),
            plugin_startup_timeout_ms: duration_millis(configuration.plugin_startup_timeout),
            plugin_restart_max_attempts: configuration.plugin_restart_max_attempts,
            plugin_restart_window_ms: duration_millis(configuration.plugin_restart_window),
        },
        Arc::clone(&policy),
        Arc::clone(&plugins),
        Arc::clone(&job_result_stats),
    ));
    let control_shutdown = CancellationToken::new();
    let mut control_task = tokio::spawn(smalux_agent::management::run_server(
        configuration.control_endpoint,
        Arc::clone(&management),
        control_shutdown.clone(),
    ));

    // connect 会按本地状态自动选择首次 XXpsk3 注册或已注册 IK 认证。
    let connection_result = tokio::select! {
        result = client.connect() => result,
        result = &mut control_task => {
            signal_task.abort();
            let result = result
                .map_err(|error| anyhow::anyhow!("Agent control task failed: {error}"))?
                .map_err(|error| anyhow::anyhow!("Agent control endpoint failed: {error:#}"));
            return result;
        }
        () = process_shutdown.cancelled() => {
            tracing::info!("Agent shutdown signal received during initial connection");
            control_shutdown.cancel();
            let _ = control_task.await;
            signal_task.abort();
            return Ok(());
        }
    };
    if let Err(error) = connection_result {
        control_shutdown.cancel();
        let _ = control_task.await;
        signal_task.abort();
        return Err(error.into());
    }
    let client_handle = match client.handle() {
        Ok(handle) => handle,
        Err(error) => {
            control_shutdown.cancel();
            let _ = control_task.await;
            signal_task.abort();
            return Err(error.into());
        }
    };
    if let Some(state) = shared_store.load().await? {
        plugins.set_agent_id(state.agent_id().map(str::to_owned));
    }

    // Scheduler 与网络连接的生命周期相互独立。链路临时断开时，Job 仍按原计划运行，
    // 结果出口会返回临时错误；监督器完成 IK 重连后同一个句柄可继续发送新结果。
    let scheduler_runtime = match SchedulerRuntime::start(configuration.scheduler.clone()) {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = client.disconnect().await;
            control_shutdown.cancel();
            let _ = control_task.await;
            signal_task.abort();
            return Err(error.into());
        }
    };
    let report_handle = client_handle.clone();
    let pending_task_reports = Arc::new(tokio::sync::Mutex::new(outbox::TaskReportOutbox::new(
        configuration.task_report_buffer_capacity,
    )?));
    let pending_job_results = tokio::sync::Mutex::new(outbox::JobResultOutbox::new(
        configuration.job_result_buffer_capacity,
        Arc::clone(&job_result_stats),
    )?);
    let report_outbox = Arc::clone(&pending_task_reports);
    let report_sink: Arc<dyn TaskReportSink> = Arc::new(move |report| {
        let report_handle = report_handle.clone();
        let report_outbox = Arc::clone(&report_outbox);
        async move {
            report_outbox
                .lock()
                .await
                .submit(&report_handle, report)
                .await
                .map_err(CallbackError::Transient)
        }
    });
    let remote_jobs = Arc::new(
        remote_jobs::RemoteJobController::with_policy_manager_and_plugins(
            scheduler_runtime.scheduler(),
            report_sink,
            policy,
            Arc::clone(&plugins),
        ),
    );
    management
        .attach_runtime(
            scheduler_runtime.scheduler(),
            Arc::clone(&remote_jobs),
            client_handle.clone(),
        )
        .await;

    let run_result = run_event_loop(
        &mut client,
        EventLoopContext {
            client_handle: &client_handle,
            remote_jobs: &remote_jobs,
            management: &management,
            pending_task_reports: &pending_task_reports,
            pending_job_results: &pending_job_results,
            plugins: &plugins,
            plugin_runtime: &mut PluginRuntimeState::default(),
            process_shutdown: &process_shutdown,
            offline_job_timeout: configuration.offline_job_timeout,
        },
    )
    .await;

    // 先停止 Scheduler，保证没有新 TaskReport 进入 Client，再关闭协议会话。
    let scheduler_result = scheduler_runtime.shutdown().await;
    drain_outboxes(
        &client_handle,
        &pending_job_results,
        &pending_task_reports,
        configuration.shutdown_drain_timeout,
    )
    .await;
    let disconnect_result = client.disconnect().await;
    control_shutdown.cancel();
    let control_result = control_task
        .await
        .map_err(|error| anyhow::anyhow!("Agent control task failed: {error}"))?;
    signal_task.abort();
    let _ = signal_task.await;
    run_result?;
    scheduler_result?;
    disconnect_result?;
    control_result?;
    Ok(())
}

/// 单消费者事件循环：接收 Server Job 命令并把执行结果发回原会话。
struct EventLoopContext<'a> {
    client_handle: &'a SmaluxClientHandle,
    remote_jobs: &'a Arc<remote_jobs::RemoteJobController>,
    management: &'a ManagementState,
    pending_task_reports: &'a tokio::sync::Mutex<outbox::TaskReportOutbox>,
    pending_job_results: &'a tokio::sync::Mutex<outbox::JobResultOutbox>,
    plugins: &'a Arc<PluginManager>,
    plugin_runtime: &'a mut PluginRuntimeState,
    process_shutdown: &'a CancellationToken,
    offline_job_timeout: Duration,
}

async fn run_event_loop(
    client: &mut SmaluxClient,
    context: EventLoopContext<'_>,
) -> anyhow::Result<()> {
    // Job 命令可能恰好在链路断开时完成；内存队列避免该瞬时错误终止整个 Agent。
    // 跨进程可靠投递仍需要后续持久化 outbox，此处不改变现有磁盘格式。
    let mut result_retry = tokio::time::interval(JOB_RESULT_RETRY_INTERVAL);
    result_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    result_retry.tick().await;
    let mut offline_job_expiry = OfflineJobExpiry::default();
    loop {
        tokio::select! {
            () = context.process_shutdown.cancelled() => {
                tracing::info!("Agent shutdown signal received");
                return Ok(());
            }
            event = client.next_event() => {
                let Some(event) = event? else {
                    anyhow::bail!("Agent Client event channel closed unexpectedly");
                };
                match event {
                    SmaluxClientEvent::Connected { mode } => {
                        offline_job_expiry.cancel().await;
                        context.plugins.reset_pause_notice_delivery().await;
                        context.management.record_connected(mode).await;
                        tracing::info!(?mode, "Agent authenticated session is ready");
                        context.client_handle
                            .send_agent_job_policy(context.remote_jobs.policy().await.to_protocol_message())
                            .await?;
                        context.client_handle
                            .send_agent_capability(agent_capability_message())
                            .await?;
                        context.client_handle
                            .send_agent_plugin(agent_plugin_inventory_message(context.plugins.catalog()))
                            .await?;
                        send_pending_plugin_pauses(
                            context.client_handle,
                            context.remote_jobs,
                            context.plugins,
                        )
                        .await?;
                        context.pending_job_results.lock().await.flush(context.client_handle).await?;
                        context.pending_task_reports.lock().await.flush(context.client_handle).await?;
                    }
                    SmaluxClientEvent::Disconnected { reason, retry_in } => {
                        context.plugins.reset_pause_notice_delivery().await;
                        context.management.record_disconnected(reason.clone()).await;
                        tracing::warn!(%reason, ?retry_in, "Agent session disconnected; IK reconnect scheduled");
                        let remote_jobs = Arc::clone(context.remote_jobs);
                        let offline_job_timeout = context.offline_job_timeout;
                        offline_job_expiry.start(offline_job_timeout, async move {
                            tracing::warn!(
                                timeout_ms = duration_millis(offline_job_timeout),
                                "Agent offline Job deadline expired; clearing remote Jobs until Server resynchronizes"
                            );
                            if let Err(error) = remote_jobs.clear().await {
                                tracing::error!(%error, "failed to clear remote Jobs after offline timeout");
                            }
                        });
                    }
                    SmaluxClientEvent::Fatal(error) => return Err(error.into()),
                    SmaluxClientEvent::Session(SessionEvent::JobCommand(command)) => {
                        let result = context.remote_jobs.apply_command(command).await;
                        context.pending_job_results.lock().await.submit(context.client_handle, result).await?;
                    }
                    SmaluxClientEvent::Session(SessionEvent::AgentJobPolicy(message)) => {
                        match message.body {
                            Some(agent_job_policy_sync::Body::Query(_)) => {
                                context.client_handle
                                    .send_agent_job_policy(context.remote_jobs.policy().await.to_protocol_message())
                                    .await?;
                            }
                            Some(agent_job_policy_sync::Body::Acknowledgement(ack)) => {
                                context.management.record_policy_acknowledgement(ack.revision).await;
                            }
                            _ => tracing::warn!("Agent received an invalid Job policy message from Server"),
                        }
                    }
                    SmaluxClientEvent::Session(SessionEvent::AgentCapability(message)) => {
                        match message.body {
                            Some(agent_capability_sync::Body::Query(_)) => {
                                context.client_handle
                                    .send_agent_capability(agent_capability_message())
                                    .await?;
                            }
                            _ => tracing::warn!("Agent received an invalid capability message from Server"),
                        }
                    }
                    SmaluxClientEvent::Session(SessionEvent::AgentPlugin(message)) => {
                        match message.body {
                            Some(agent_plugin_sync::Body::Query(_)) => {
                                context.client_handle
                                    .send_agent_plugin(agent_plugin_inventory_message(context.plugins.catalog()))
                                    .await?;
                            }
                            Some(agent_plugin_sync::Body::Snapshot(snapshot)) => {
                                let acknowledgement = match context.plugin_runtime.validate_snapshot(&snapshot) {
                                    Ok(RuntimeSnapshotResult::Applied) => match context.plugins.apply_snapshot(&snapshot).await {
                                        Ok(()) => {
                                            for plugin in &snapshot.plugins {
                                                context.remote_jobs.resume_plugin(&plugin.plugin_id, &plugin.version).await;
                                            }
                                            context.plugin_runtime.confirm_snapshot(&snapshot)
                                        },
                                        Err(error) => context.plugin_runtime.reject(snapshot.revision, error),
                                    },
                                    Ok(RuntimeSnapshotResult::IgnoredStale) => {
                                        if snapshot.revision
                                            == context.plugin_runtime.applied_revision()
                                        {
                                            smalux_protocol::agent::v1::AgentPluginAck {
                                                revision: snapshot.revision,
                                                accepted: true,
                                                error: None,
                                            }
                                        } else {
                                            context.plugin_runtime.reject(
                                                snapshot.revision,
                                                "stale Plus plugin runtime snapshot",
                                            )
                                        }
                                    }
                                    Err(error) => context.plugin_runtime.reject(snapshot.revision, error),
                                };
                                context.client_handle
                                    .send_agent_plugin(AgentPluginSync {
                                        body: Some(agent_plugin_sync::Body::Acknowledgement(acknowledgement)),
                                    })
                                    .await?;
                            }
                            Some(agent_plugin_sync::Body::SchemaQuery(query)) => {
                                for schema_hash in query.schema_hashes {
                                    let Some(schema_bundle) = context.plugins.catalog().schema_bytes(&schema_hash) else {
                                        return Err(anyhow::anyhow!(
                                            "Server requested a Plus schema that is not present in the current inventory"
                                        ));
                                    };
                                    context.client_handle
                                        .send_agent_plugin(AgentPluginSync {
                                            body: Some(agent_plugin_sync::Body::SchemaResponse(
                                                smalux_protocol::agent::v1::PluginSchemaResponse {
                                                    schema_hash,
                                                    schema_bundle: schema_bundle.to_vec(),
                                                },
                                            )),
                                        })
                                        .await?;
                                }
                            }
                            Some(agent_plugin_sync::Body::PauseAcknowledgement(ack)) => {
                                context.plugins.acknowledge_pause(&ack).await;
                            }
                            Some(agent_plugin_sync::Body::PauseNotice(_)) => {
                                tracing::warn!("Agent received an unexpected Plus pause notice from Server");
                            }
                            _ => tracing::warn!("Agent received an invalid plugin message from Server"),
                        }
                    }
                    SmaluxClientEvent::Session(SessionEvent::Diagnostic(message)) => {
                        tracing::debug!(?message, "Agent received application message");
                    }
                    SmaluxClientEvent::Session(SessionEvent::KeyRotation(message)) => {
                        tracing::debug!(?message, "Agent received a key rotation message handled by the Client supervisor");
                    }
                    SmaluxClientEvent::Session(other) => {
                        tracing::warn!(?other, "Agent received a session message unexpected for its role");
                    }
                }
            }
            _ = result_retry.tick() => {
                context.plugins.poll_health().await;
                send_pending_plugin_pauses(
                    context.client_handle,
                    context.remote_jobs,
                    context.plugins,
                )
                .await?;
                let mut results = context.pending_job_results.lock().await;
                if !results.is_empty() {
                    results.flush(context.client_handle).await?;
                }
                drop(results);
                let mut reports = context.pending_task_reports.lock().await;
                if !reports.is_empty() {
                    reports.flush(context.client_handle).await?;
                }
            }
        }
    }
}

async fn send_pending_plugin_pauses(
    client_handle: &SmaluxClientHandle,
    remote_jobs: &remote_jobs::RemoteJobController,
    plugins: &PluginManager,
) -> anyhow::Result<()> {
    let notices = plugins.pending_pause_notices().await;
    for notice in &notices {
        remote_jobs
            .pause_plugin(&notice.plugin_id, &notice.plugin_version)
            .await?;
        let result = client_handle
            .send_agent_plugin(AgentPluginSync {
                body: Some(agent_plugin_sync::Body::PauseNotice(notice.clone())),
            })
            .await;
        if let Err(error) = result {
            if matches!(
                error,
                smalux_agent::client::SmaluxClientError::TemporarilyUnavailable
            ) {
                return Ok(());
            }
            return Err(error.into());
        }
    }
    plugins.mark_pause_notices_sent(&notices).await;
    Ok(())
}

/// 管理从首次断线开始计算的单个远程 Job 过期计时器。
///
/// 重复断线通知不会延长期限；连接恢复时必须调用 [`cancel`](Self::cancel) 并等待任务退出，
/// 从而保证旧计时器不会在新会话已经开始同步后清空刚收到的 Job。
#[derive(Default)]
struct OfflineJobExpiry {
    active: Option<(CancellationToken, tokio::task::JoinHandle<()>)>,
}

impl OfflineJobExpiry {
    fn start<F>(&mut self, timeout: Duration, on_expire: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.active.is_some() {
            return;
        }
        let cancellation = CancellationToken::new();
        let wait_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                () = wait_cancellation.cancelled() => {}
                () = tokio::time::sleep(timeout) => on_expire.await,
            }
        });
        self.active = Some((cancellation, task));
    }

    async fn cancel(&mut self) {
        let Some((cancellation, task)) = self.active.take() else {
            return;
        };
        cancellation.cancel();
        let _ = task.await;
    }
}

impl Drop for OfflineJobExpiry {
    fn drop(&mut self) {
        if let Some((cancellation, task)) = self.active.take() {
            cancellation.cancel();
            task.abort();
        }
    }
}

fn agent_capability_message() -> AgentCapabilitySync {
    AgentCapabilitySync {
        body: Some(agent_capability_sync::Body::Snapshot(
            smalux_agent::tasks::agent_capability_snapshot(),
        )),
    }
}

/// 把已校验的本地 Manifest 转成仅含公开能力的会话清单。
fn agent_plugin_inventory_message(plugins: &PluginCatalog) -> AgentPluginSync {
    AgentPluginSync {
        body: Some(agent_plugin_sync::Body::Inventory(plugins.inventory())),
    }
}

/// 在一个总超时内补发退出前仍留在内存中的结果；超时只记录丢失数量，不阻塞退出。
async fn drain_outboxes(
    client: &SmaluxClientHandle,
    job_results: &tokio::sync::Mutex<outbox::JobResultOutbox>,
    task_reports: &tokio::sync::Mutex<outbox::TaskReportOutbox>,
    timeout: Duration,
) {
    let drain = async {
        loop {
            let mut results = job_results.lock().await;
            results.flush(client).await?;
            let results_empty = results.is_empty();
            drop(results);

            let mut reports = task_reports.lock().await;
            reports.flush(client).await?;
            let reports_empty = reports.is_empty();
            drop(reports);

            if results_empty && reports_empty {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(OUTBOX_RETRY_INTERVAL).await;
        }
    };

    match tokio::time::timeout(timeout, drain).await {
        Ok(Ok(())) => tracing::debug!("Agent shutdown outboxes drained"),
        Ok(Err(error)) => tracing::warn!(%error, "failed to drain Agent shutdown outboxes"),
        Err(_) => {
            let pending_job_results = job_results.lock().await.pending_len();
            let dropped_job_results = job_results.lock().await.dropped_count();
            let reports = task_reports.lock().await;
            tracing::warn!(
                pending_job_results,
                dropped_job_results,
                pending_task_reports = reports.pending_len(),
                dropped_task_reports = reports.dropped_count(),
                timeout_ms = duration_millis(timeout),
                "Agent shutdown outbox drain timed out"
            );
        }
    }
}

fn spawn_shutdown_signal_listener(shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match wait_for_shutdown_signal().await {
            Ok(signal) => tracing::info!(signal, "Agent process shutdown requested"),
            Err(error) => tracing::error!(%error, "failed to listen for Agent shutdown signals"),
        }
        shutdown.cancel();
    })
}

/// 等待平台关闭信号。Unix 同时支持 Ctrl+C(SIGINT) 和服务管理器使用的 SIGTERM。
async fn wait_for_shutdown_signal() -> std::io::Result<&'static str> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                Ok("ctrl_c")
            }
            _ = terminate.recv() => Ok("sigterm"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("ctrl_c")
    }
}

fn duration_millis(value: Duration) -> u64 {
    value.as_millis().try_into().unwrap_or(u64::MAX)
}

/// 加载本地策略；交互终端可明确确认备份并重置，服务环境则 fail-fast。
async fn load_job_policy(path: &Path) -> anyhow::Result<RemoteJobPolicyManager> {
    match RemoteJobPolicyManager::load_file(path).await {
        Ok(manager) => Ok(manager),
        Err(error) if std::io::stdin().is_terminal() => {
            eprintln!(
                "failed to load Agent Job policy {}: {error:#}",
                path.display()
            );
            eprint!("back up the invalid file and reset the policy? [y/N] ");
            std::io::stderr().flush()?;
            let mut answer = String::new();
            std::io::stdin().read_line(&mut answer)?;
            anyhow::ensure!(
                matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes"),
                "Agent Job policy recovery was declined"
            );
            let backup = RemoteJobPolicyManager::repair_file(path).await?;
            tracing::warn!(?backup, policy_file = %path.display(), "reset invalid Agent Job policy");
            RemoteJobPolicyManager::load_file(path).await
        }
        Err(error) => Err(anyhow::anyhow!(
            "failed to load Agent Job policy {}: {error:#}; run `smalux-agent jobs policy repair --reset` while the Agent is stopped",
            path.display()
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::Mutex;

    use super::OfflineJobExpiry;

    #[tokio::test]
    async fn offline_job_expiry_runs_after_the_configured_deadline() {
        let expired = Arc::new(Mutex::new(false));
        let marker = Arc::clone(&expired);
        let mut expiry = OfflineJobExpiry::default();

        expiry.start(Duration::from_millis(1), async move {
            *marker.lock().await = true;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(*expired.lock().await);
    }

    #[tokio::test]
    async fn reconnect_cancels_offline_job_expiry_before_it_can_run() {
        let expired = Arc::new(Mutex::new(false));
        let marker = Arc::clone(&expired);
        let mut expiry = OfflineJobExpiry::default();

        expiry.start(Duration::from_millis(20), async move {
            *marker.lock().await = true;
        });
        expiry.cancel().await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        assert!(!*expired.lock().await);
    }
}
