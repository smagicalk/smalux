//! Smalux Agent 进程入口。

mod cli;
mod commands;
mod outbox;

use chrono::{DateTime, Utc};
use std::{
    future::Future,
    io::{IsTerminal, Write},
    path::Path,
    sync::Arc,
    time::Duration,
};

use smalux_agent::client::{
    AgentStateStore, FileAgentStateStore, SmaluxClient, SmaluxClientError, SmaluxClientEvent,
    SmaluxClientHandle,
};
use smalux_agent::management::{EffectiveConfigSnapshot, ManagementState};
use smalux_agent::management::{JobEventBufferStats, JobResultBufferStats};
use smalux_agent::plugins::{
    PluginCatalog, PluginManager, PluginRuntimeLimits, PluginRuntimeState, RuntimeSnapshotResult,
};
use smalux_agent::remote_jobs::{self, RemoteJobPolicyManager};
use smalux_agent::scheduler::{
    CallbackError, SchedulerEvent, SchedulerEventKind, SchedulerRuntime, TaskReportSink,
};
use smalux_plus_core::AgentContext;
use smalux_protocol::{
    agent::v1::{
        AgentCapabilitySync, AgentPluginSync, AgentReconcileSummary, JobEvent, JobEventKind,
        agent_capability_sync, agent_job_policy_sync, agent_plugin_sync,
    },
    tonic_transport::SessionEvent,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
    // 该 ID 只标识当前 Agent 进程实例，不写入磁盘；重连复用，重启后重新生成。
    let process_instance_id = *Uuid::new_v4().as_bytes();
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
    let job_event_stats = Arc::new(JobEventBufferStats::default());
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
        Arc::clone(&job_event_stats),
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
    let pending_job_events = Arc::new(tokio::sync::Mutex::new(outbox::JobEventOutbox::new(
        configuration.task_report_buffer_capacity,
        Arc::clone(&job_event_stats),
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
    let mut scheduler_events = scheduler_runtime.scheduler().subscribe_events();
    let mut job_event_emitter = JobEventEmitter::new(process_instance_id);
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
            pending_job_events: &pending_job_events,
            pending_job_results: &pending_job_results,
            plugins: &plugins,
            scheduler_events: &mut scheduler_events,
            job_event_emitter: &mut job_event_emitter,
            plugin_runtime: &mut PluginRuntimeState::default(),
            process_instance_id,
            process_shutdown: &process_shutdown,
            offline_job_timeout: configuration.offline_job_timeout,
        },
    )
    .await;

    // 先停止 Scheduler，保证没有新 TaskReport 进入 Client，再关闭协议会话。
    let scheduler_result = scheduler_runtime.shutdown().await;
    // Scheduler 已经停止接收新执行后，再回收 Plus Worker，避免退出时遗留插件子进程。
    plugins.shutdown_all().await;
    drain_outboxes(
        &client_handle,
        &pending_job_results,
        &pending_task_reports,
        &pending_job_events,
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
    pending_job_events: &'a tokio::sync::Mutex<outbox::JobEventOutbox>,
    pending_job_results: &'a tokio::sync::Mutex<outbox::JobResultOutbox>,
    plugins: &'a Arc<PluginManager>,
    scheduler_events: &'a mut tokio::sync::broadcast::Receiver<Arc<SchedulerEvent>>,
    /// 为实际发送到 Server 的事件分配 Agent 进程内连续序号。
    job_event_emitter: &'a mut JobEventEmitter,
    plugin_runtime: &'a mut PluginRuntimeState,
    /// 当前 Agent 进程实例 UUID，用于重连摘要和后续事件去重。
    process_instance_id: [u8; 16],
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
            scheduler_event = context.scheduler_events.recv() => {
                match scheduler_event {
                    Ok(event) => {
                        if let Some(event) = context.job_event_emitter.emit(&event) {
                            context.pending_job_events.lock().await.submit(context.client_handle, event).await?;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        tracing::warn!(count, "Agent Job event subscriber lagged; skipped local events");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::warn!("Agent Scheduler event stream closed");
                    }
                }
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
                        // 先发送对账摘要，让 Server 在收到 capability/inventory 后即可决定是否
                        // 需要下发完整 runtime 和 Job catalog，避免重连时重复传输相同快照。
                        let (catalog_revision, catalog_digest) =
                            context.remote_jobs.catalog_reconcile_state().await;
                        let (runtime_revision, runtime_digest) =
                            context.plugin_runtime.reconcile_state();
                        send_or_defer(
                            context.client_handle.send_reconcile_summary(AgentReconcileSummary {
                                instance_id: context.process_instance_id.to_vec(),
                                catalog_revision,
                                catalog_digest,
                                runtime_revision,
                                runtime_digest,
                            }).await,
                            "Agent reconcile summary",
                        ).await?;
                        send_or_defer(
                            context.client_handle
                                .send_agent_job_policy(context.remote_jobs.policy().await.to_protocol_message())
                                .await,
                            "Agent Job policy",
                        )
                        .await?;
                        send_or_defer(
                            context
                                .client_handle
                                .send_agent_capability(agent_capability_message())
                                .await,
                            "Agent capability",
                        )
                        .await?;
                        send_or_defer(
                            context
                                .client_handle
                                .send_agent_plugin(agent_plugin_inventory_message(context.plugins.catalog()))
                                .await,
                            "Agent plugin inventory",
                        )
                        .await?;
                        send_pending_plugin_pauses(
                            context.client_handle,
                            context.remote_jobs,
                            context.plugins,
                        )
                        .await?;
                        context.pending_job_results.lock().await.flush(context.client_handle).await?;
                        context.pending_task_reports.lock().await.flush(context.client_handle).await?;
                        context.pending_job_events.lock().await.flush(context.client_handle).await?;
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
                            if let Err(error) = remote_jobs.clear_remote_jobs().await {
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
                                send_or_defer(
                                    context
                                        .client_handle
                                        .send_agent_job_policy(context.remote_jobs.policy().await.to_protocol_message())
                                        .await,
                                    "Agent Job policy response",
                                )
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
                                send_or_defer(
                                    context
                                        .client_handle
                                        .send_agent_capability(agent_capability_message())
                                        .await,
                                    "Agent capability response",
                                )
                                .await?;
                            }
                            _ => tracing::warn!("Agent received an invalid capability message from Server"),
                        }
                    }
                    SmaluxClientEvent::Session(SessionEvent::AgentPlugin(message)) => {
                        match message.body {
                            Some(agent_plugin_sync::Body::Query(_)) => {
                                send_or_defer(
                                    context
                                        .client_handle
                                        .send_agent_plugin(agent_plugin_inventory_message(context.plugins.catalog()))
                                        .await,
                                    "Agent plugin inventory response",
                                )
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
                                send_or_defer(context.client_handle
                                    .send_agent_plugin(AgentPluginSync {
                                        body: Some(agent_plugin_sync::Body::Acknowledgement(acknowledgement)),
                                    })
                                    .await, "Agent plugin runtime acknowledgement").await?;
                            }
                            Some(agent_plugin_sync::Body::SchemaQuery(query)) => {
                                for schema_hash in query.schema_hashes {
                                    let Some(schema_bundle) = context.plugins.catalog().schema_bytes(&schema_hash) else {
                                        return Err(anyhow::anyhow!(
                                            "Server requested a Plus schema that is not present in the current inventory"
                                        ));
                                    };
                                    send_or_defer(context.client_handle
                                        .send_agent_plugin(AgentPluginSync {
                                            body: Some(agent_plugin_sync::Body::SchemaResponse(
                                                smalux_protocol::agent::v1::PluginSchemaResponse {
                                                    schema_hash,
                                                    schema_bundle: schema_bundle.to_vec(),
                                                },
                                            )),
                                        })
                                        .await, "Agent plugin schema response").await?;
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
                drop(reports);
                let mut events = context.pending_job_events.lock().await;
                if !events.is_empty() {
                    events.flush(context.client_handle).await?;
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
            if error.is_retryable() {
                return Ok(());
            }
            return Err(error.into());
        }
    }
    plugins.mark_pause_notices_sent(&notices).await;
    Ok(())
}

/// 连接短暂断开时，控制消息交给监督器在重连后重新同步；认证或协议错误仍终止事件循环。
async fn send_or_defer(
    result: Result<(), SmaluxClientError>,
    label: &'static str,
) -> anyhow::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if error.is_retryable() => {
            tracing::debug!(message = label, error = %error, "deferred Agent control message until reconnect");
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
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

/// 为当前 Agent 进程发出的 JobEvent 分配实例 ID 和连续序号。
///
/// Scheduler 自身会为所有内部事件编号，但 Agent 只上报其中的诊断事件，直接复用
/// Scheduler 序号会因为被过滤的成功事件产生“假缺口”。这个发射器只给实际发出的
/// 事件编号，Server 才能区分真正丢失的事件和正常过滤。
struct JobEventEmitter {
    instance_id: [u8; 16],
    next_sequence: u64,
}

impl JobEventEmitter {
    /// 创建从序号 1 开始的新进程事件流。
    fn new(instance_id: [u8; 16]) -> Self {
        Self {
            instance_id,
            next_sequence: 0,
        }
    }

    /// 过滤并包装一条 Scheduler 事件；不需要上报的成功事件返回 `None`。
    fn emit(&mut self, event: &SchedulerEvent) -> Option<JobEvent> {
        let mut value = scheduler_event_message(event)?;
        self.next_sequence = self.next_sequence.saturating_add(1);
        value.sequence = self.next_sequence;
        value.instance_id = self.instance_id.to_vec();
        Some(value)
    }
}

/// 只把 Server 需要诊断的异常或状态变化映射为协议事件；成功采样由 TaskReport 表达。
fn scheduler_event_message(event: &SchedulerEvent) -> Option<JobEvent> {
    let mut value = JobEvent {
        sequence: event.sequence,
        emitted_at: Some(timestamp(event.emitted_at)),
        ..Default::default()
    };
    match &event.kind {
        SchedulerEventKind::ExecutionFailed {
            job_id,
            version,
            run_id,
            attempt,
            error,
            will_retry,
        } => {
            value.kind = JobEventKind::ExecutionFailed as i32;
            value.job_id = job_id.as_bytes().to_vec();
            value.revision = *version;
            value.run_id = run_id.as_bytes().to_vec();
            value.attempt = *attempt;
            value.message = error.clone();
            value.will_retry = *will_retry;
        }
        SchedulerEventKind::ExecutionTimedOut {
            job_id,
            version,
            run_id,
            attempt,
        } => {
            value.kind = JobEventKind::ExecutionTimedOut as i32;
            value.job_id = job_id.as_bytes().to_vec();
            value.revision = *version;
            value.run_id = run_id.as_bytes().to_vec();
            value.attempt = *attempt;
        }
        SchedulerEventKind::ExecutionPanicked {
            job_id,
            version,
            run_id,
            attempt,
            message,
        } => {
            value.kind = JobEventKind::ExecutionPanicked as i32;
            value.job_id = job_id.as_bytes().to_vec();
            value.revision = *version;
            value.run_id = run_id.as_bytes().to_vec();
            value.attempt = *attempt;
            value.message = message.clone();
        }
        SchedulerEventKind::ExecutionCancelled {
            job_id,
            version,
            run_id,
        } => {
            value.kind = JobEventKind::ExecutionCancelled as i32;
            value.job_id = job_id.as_bytes().to_vec();
            value.revision = *version;
            value.run_id = run_id.as_bytes().to_vec();
        }
        SchedulerEventKind::RetryScheduled {
            job_id,
            version,
            run_id,
            attempt,
            run_at,
        } => {
            value.kind = JobEventKind::RetryScheduled as i32;
            value.job_id = job_id.as_bytes().to_vec();
            value.revision = *version;
            value.run_id = run_id.as_bytes().to_vec();
            value.attempt = *attempt;
            value.run_at = Some(timestamp(*run_at));
        }
        SchedulerEventKind::JobDisabled {
            job_id,
            version,
            reason,
        } => {
            value.kind = JobEventKind::JobDisabled as i32;
            value.job_id = job_id.as_bytes().to_vec();
            value.revision = *version;
            value.message = reason.clone();
        }
        SchedulerEventKind::CallbackFailed {
            job_id,
            version,
            run_id,
            error,
        } => {
            value.kind = JobEventKind::ReportDeliveryFailed as i32;
            value.job_id = job_id.as_bytes().to_vec();
            value.revision = *version;
            value.run_id = run_id.as_bytes().to_vec();
            value.message = error.clone();
        }
        _ => return None,
    }
    Some(value)
}

fn timestamp(value: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
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
    job_events: &tokio::sync::Mutex<outbox::JobEventOutbox>,
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

            let mut events = job_events.lock().await;
            events.flush(client).await?;
            let events_empty = events.is_empty();
            drop(events);

            if results_empty && reports_empty && events_empty {
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
            let events = job_events.lock().await;
            tracing::warn!(
                pending_job_results,
                dropped_job_results,
                pending_task_reports = reports.pending_len(),
                dropped_task_reports = reports.dropped_count(),
                pending_job_events = events.pending_len(),
                dropped_job_events = events.dropped_count(),
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

    use chrono::Utc;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::{JobEventEmitter, OfflineJobExpiry, scheduler_event_message};

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

    #[test]
    fn scheduler_failure_event_becomes_job_event_without_success_payload() {
        let event = smalux_agent::scheduler::SchedulerEvent {
            sequence: 7,
            emitted_at: Utc::now(),
            kind: smalux_agent::scheduler::SchedulerEventKind::ExecutionFailed {
                job_id: Uuid::new_v4(),
                version: 3,
                run_id: Uuid::new_v4(),
                attempt: 2,
                error: "timeout".to_owned(),
                will_retry: true,
            },
        };
        let message = scheduler_event_message(&event).expect("failure should be reported");
        assert_eq!(message.sequence, 7);
        assert_eq!(message.revision, 3);
        assert_eq!(message.attempt, 2);
        assert!(message.will_retry);
    }

    #[test]
    fn scheduler_success_event_is_not_duplicated_as_job_event() {
        let event = smalux_agent::scheduler::SchedulerEvent {
            sequence: 8,
            emitted_at: Utc::now(),
            kind: smalux_agent::scheduler::SchedulerEventKind::ExecutionSucceeded {
                job_id: Uuid::new_v4(),
                version: 1,
                run_id: Uuid::new_v4(),
                attempt: 1,
                duration_ms: 4,
            },
        };
        assert!(scheduler_event_message(&event).is_none());
    }

    #[test]
    fn job_event_emitter_assigns_contiguous_sequences_and_instance_id() {
        let mut emitter = JobEventEmitter::new([4; 16]);
        let first = smalux_agent::scheduler::SchedulerEvent {
            sequence: 100,
            emitted_at: Utc::now(),
            kind: smalux_agent::scheduler::SchedulerEventKind::ExecutionFailed {
                job_id: Uuid::new_v4(),
                version: 1,
                run_id: Uuid::new_v4(),
                attempt: 1,
                error: "one".to_owned(),
                will_retry: false,
            },
        };
        let second = smalux_agent::scheduler::SchedulerEvent {
            sequence: 900,
            ..first.clone()
        };
        let first = emitter.emit(&first).unwrap();
        let second = emitter.emit(&second).unwrap();
        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert_eq!(first.instance_id, vec![4; 16]);
        assert_eq!(second.instance_id, vec![4; 16]);
    }
}
