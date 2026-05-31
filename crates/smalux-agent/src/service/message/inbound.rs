//! Server 入站命令调度。
//!
//! 协议 listener 只负责把 server 消息转换为内部命令，本模块负责校验和执行业务动作。

use super::outbound::{
    ControlAckEnvelope, ControlErrorEnvelope, OutboundEvent, OutboundSender, OutboundSequence,
};
use crate::config::ConfigManager;
use crate::config::manager::{validate_process_sampling_options, validate_socket_sampling_options};
use crate::config::model::{AgentConfigPatch, ExportConfig};
use crate::service::collector::{CollectorCommand, CollectorCommandSender};
use crate::service::options::DiagnosticOptions;
use crate::service::reporter::{ReporterCommand, ReporterCommandSender};
use crate::service::{
    probe::{RemoteProbeManager, RemoteProbeRunRequest},
    shell::{RemoteShellManager, RemoteShellOpenRequest},
    task::{RemoteTaskManager, RemoteTaskRunRequest},
};
use smalux_core::model::info::MetricLevel;
use smalux_protocol::{Ack, ProtocolError};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::watch;

/// server 入站命令队列容量。
const INBOUND_COMMAND_QUEUE_CAPACITY: usize = 128;

/// 入站命令发送端。
pub(crate) type InboundCommandSender = mpsc::Sender<InboundCommandEnvelope>;
/// 入站命令接收端。
pub(crate) type InboundCommandReceiver = mpsc::Receiver<InboundCommandEnvelope>;

/// 创建 server 入站命令队列。
pub(crate) fn inbound_command_channel() -> (InboundCommandSender, InboundCommandReceiver) {
    mpsc::channel(INBOUND_COMMAND_QUEUE_CAPACITY)
}

/// 协议无关的 server 入站命令。
///
/// 这里刻意不出现 WebSocket、Komari 或 JSON frame 的概念。协议 listener 负责把外部消息
/// 翻译成这些内部命令，调度器只按统一语义处理，后续新增 gRPC 或其它兼容格式时不用改
/// 远程 shell/task/probe 的执行逻辑。
#[derive(Debug)]
pub(crate) enum InboundCommand {
    /// 服务配置增量更新。
    ConfigPatch {
        /// 需要应用的配置 patch。
        patch: AgentConfigPatch,
    },
    /// 立即采集一次进程信息。
    CollectProcessesOnce {
        /// 本次采集级别；缺省使用当前配置。
        level: Option<MetricLevel>,
        /// 本次返回条数上限；缺省使用当前配置。
        limit: Option<usize>,
    },
    /// 立即采集一次 Socket 信息。
    CollectSocketsOnce {
        /// 本次采集级别；缺省使用当前配置。
        level: Option<MetricLevel>,
        /// 本次返回条数上限；缺省使用当前配置。
        limit: Option<usize>,
    },
    /// 打开远程 shell 会话。
    RemoteShellOpen {
        /// 远程 shell 打开请求。
        request: RemoteShellOpenRequest,
        /// 某些兼容协议需要独立的 shell stream 导出配置。
        stream_export_config: Option<ExportConfig>,
    },
    /// 执行远程非交互任务。
    RemoteTaskRun {
        /// 远程任务请求。
        request: RemoteTaskRunRequest,
    },
    /// 执行远程网络探测。
    RemoteProbeRun {
        /// 远程探测请求。
        request: RemoteProbeRunRequest,
    },
    /// 请求尽快发送完整 snapshot。
    SnapshotRequest {
        /// 请求原因，便于日志定位。
        reason: Option<String>,
    },
}

impl InboundCommand {
    /// 返回稳定命令名称。
    fn name(&self) -> &'static str {
        match self {
            Self::ConfigPatch { .. } => "config_patch",
            Self::CollectProcessesOnce { .. } => "collect_processes_once",
            Self::CollectSocketsOnce { .. } => "collect_sockets_once",
            Self::RemoteShellOpen { .. } => "remote_shell_open",
            Self::RemoteTaskRun { .. } => "remote_task_run",
            Self::RemoteProbeRun { .. } => "remote_probe_run",
            Self::SnapshotRequest { .. } => "snapshot_request",
        }
    }
}

/// server 控制消息元数据。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ServerCommandMeta {
    /// server 侧消息序号。
    pub(crate) sequence: u64,
}

/// 带响应元数据的入站命令。
#[derive(Debug)]
pub(crate) struct InboundCommandEnvelope {
    /// 控制命令。
    pub(crate) command: InboundCommand,
    /// 可选响应元数据；legacy raw 消息和第三方兼容消息通常为空。
    pub(crate) meta: Option<ServerCommandMeta>,
}

impl InboundCommandEnvelope {
    /// 创建不需要 ack/error 的入站命令。
    pub(crate) fn without_response(command: InboundCommand) -> Self {
        Self {
            command,
            meta: None,
        }
    }

    /// 创建需要 ack/error 的入站命令。
    pub(crate) fn with_response(command: InboundCommand, sequence: u64) -> Self {
        Self {
            command,
            meta: Some(ServerCommandMeta { sequence }),
        }
    }
}

/// 入站命令调度器。
#[derive(Debug, Clone)]
pub(crate) struct ControlDispatcher {
    /// 动态配置管理器。
    config_manager: ConfigManager,
    /// 远程 shell 会话管理器。
    remote_shell: RemoteShellManager,
    /// 远程非交互任务管理器。
    remote_task: RemoteTaskManager,
    /// 远程网络探测管理器。
    remote_probe: RemoteProbeManager,
    /// 采集控制命令发送端。
    collector_commands: CollectorCommandSender,
    /// reporter 控制命令发送端。
    reporter_commands: ReporterCommandSender,
    /// 出站事件发送端，用于控制命令 ack/error。
    outbound_tx: OutboundSender,
    /// 全局出站序号。
    sequence: OutboundSequence,
    /// 诊断采集静态权限。
    diagnostics: DiagnosticOptions,
}

impl ControlDispatcher {
    /// 创建入站命令调度器。
    pub(crate) fn new(
        config_manager: ConfigManager,
        remote_shell: RemoteShellManager,
        remote_task: RemoteTaskManager,
        remote_probe: RemoteProbeManager,
        collector_commands: CollectorCommandSender,
        reporter_commands: ReporterCommandSender,
        outbound_tx: OutboundSender,
        sequence: OutboundSequence,
        diagnostics: DiagnosticOptions,
    ) -> Self {
        Self {
            config_manager,
            remote_shell,
            remote_task,
            remote_probe,
            collector_commands,
            reporter_commands,
            outbound_tx,
            sequence,
            diagnostics,
        }
    }

    /// 执行单条入站命令。
    pub(crate) fn dispatch(&self, envelope: InboundCommandEnvelope) -> anyhow::Result<()> {
        let command_name = envelope.command.name();
        let meta = envelope.meta;
        let result = self.dispatch_command(envelope.command);
        // 只有带 server sequence 的 Smalux ServerFrame 才回 ack/error。
        // legacy text 和第三方兼容消息没有可关联的 sequence，失败只在本地日志体现。
        if let Some(meta) = meta {
            self.queue_control_response(meta, command_name, result.as_ref().err());
        }
        result
    }

    /// 执行不带响应处理的单条命令。
    fn dispatch_command(&self, command: InboundCommand) -> anyhow::Result<()> {
        match command {
            InboundCommand::ConfigPatch { patch } => self.apply_config_patch(patch),
            InboundCommand::CollectProcessesOnce { level, limit } => {
                self.collect_processes_once(level, limit)
            }
            InboundCommand::CollectSocketsOnce { level, limit } => {
                self.collect_sockets_once(level, limit)
            }
            InboundCommand::RemoteShellOpen {
                request,
                stream_export_config,
            } => self.open_remote_shell(request, stream_export_config),
            InboundCommand::RemoteTaskRun { request } => self.remote_task.start(request),
            InboundCommand::RemoteProbeRun { request } => self.remote_probe.start(request),
            InboundCommand::SnapshotRequest { reason } => self.request_snapshot(reason),
        }
    }

    /// 应用 server 配置 patch。
    fn apply_config_patch(&self, patch: AgentConfigPatch) -> anyhow::Result<()> {
        self.ensure_config_patch_allowed(&patch)?;
        let updated = self.config_manager.apply_patch(patch)?;
        tracing::info!(
            core_interval_ms = updated.core.interval.as_millis(),
            disk_interval_ms = updated.disk.interval.as_millis(),
            network_interval_ms = updated.network.interval.as_millis(),
            report_interval_ms = updated.report.interval.as_millis(),
            public_ip_refresh_interval_ms = updated.public_ip.refresh_interval.as_millis(),
            remote_probe_enabled = updated.remote_probe.enabled,
            remote_probe_timeout_ms = updated.remote_probe.timeout.as_millis(),
            remote_probe_global_min_interval_ms =
                updated.remote_probe.global_min_interval.as_millis(),
            remote_probe_target_min_interval_ms =
                updated.remote_probe.target_min_interval.as_millis(),
            export_reconnect_interval_ms = updated.export.reconnect_interval.as_millis(),
            "server config patch applied"
        );
        Ok(())
    }

    /// 投递一次性进程采集命令。
    fn collect_processes_once(
        &self,
        level: Option<MetricLevel>,
        limit: Option<usize>,
    ) -> anyhow::Result<()> {
        let command = self.resolve_process_command(level, limit)?;
        self.send_collector_command(command)?;
        let CollectorCommand::SampleProcessesOnce { level, limit } = command else {
            unreachable!("resolved process command must be process command")
        };
        tracing::info!(level = ?level, limit, "one-shot process collection requested");
        Ok(())
    }

    /// 投递一次性 Socket 采集命令。
    fn collect_sockets_once(
        &self,
        level: Option<MetricLevel>,
        limit: Option<usize>,
    ) -> anyhow::Result<()> {
        let command = self.resolve_socket_command(level, limit)?;
        self.send_collector_command(command)?;
        let CollectorCommand::SampleSocketsOnce { level, limit } = command else {
            unreachable!("resolved socket command must be socket command")
        };
        tracing::info!(level = ?level, limit, "one-shot socket collection requested");
        Ok(())
    }

    /// 打开远程 shell。
    fn open_remote_shell(
        &self,
        request: RemoteShellOpenRequest,
        stream_export_config: Option<ExportConfig>,
    ) -> anyhow::Result<()> {
        let current_config = self.config_manager.current();
        let session_id = request.session_id.clone();
        let export_config = stream_export_config.unwrap_or_else(|| current_config.export.clone());

        self.remote_shell
            .open(request, &export_config, &current_config.remote_shell)?;
        tracing::info!(session_id = %session_id, "remote shell open accepted");
        Ok(())
    }

    /// 请求 reporter 尽快生成完整 snapshot。
    fn request_snapshot(&self, reason: Option<String>) -> anyhow::Result<()> {
        let current = self.config_manager.current();
        if !current.report.enabled {
            anyhow::bail!("reporting is disabled");
        }

        self.reporter_commands
            .try_send(ReporterCommand::ForceSnapshot {
                reason: reason.clone(),
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => anyhow::anyhow!("reporter command queue is full"),
                TrySendError::Closed(_) => anyhow::anyhow!("reporter command channel is closed"),
            })?;
        tracing::info!(reason, "snapshot request accepted");
        Ok(())
    }

    /// 根据命令执行结果投递 ack 或 error。
    fn queue_control_response(
        &self,
        meta: ServerCommandMeta,
        command_name: &'static str,
        error: Option<&anyhow::Error>,
    ) {
        let config = self.config_manager.current();
        let sequence = self.sequence.next();
        // ack/error 也走出站事件队列，而不是在控制线程里直接发 WebSocket。
        // 这样重连、pending 重投和 adapter 不支持时的跳过逻辑都由 export_supervisor 统一处理。
        let event = match error {
            Some(error) => OutboundEvent::ControlError(ControlErrorEnvelope::new(
                config.agent_id,
                sequence,
                ProtocolError {
                    sequence: Some(meta.sequence),
                    code: format!("{command_name}_failed"),
                    message: error.to_string(),
                },
            )),
            None => OutboundEvent::ControlAck(ControlAckEnvelope::new(
                config.agent_id,
                sequence,
                Ack {
                    sequence: meta.sequence,
                },
            )),
        };

        if let Err(error) = self.outbound_tx.try_send(event) {
            tracing::warn!(
                error = %control_response_queue_error(error),
                server_sequence = meta.sequence,
                "control response dropped"
            );
        }
    }

    /// 检查 server patch 是否试图打开未授权的 details 采集。
    fn ensure_config_patch_allowed(&self, patch: &AgentConfigPatch) -> anyhow::Result<()> {
        let current = self.config_manager.current();
        let mut next = current.clone();
        patch.apply_to(&mut next);

        if details_becomes_enabled(
            current.processes.enabled,
            current.processes.level,
            next.processes.enabled,
            next.processes.level,
        ) && !self.diagnostics.allow_process_details
        {
            anyhow::bail!("process details collection is not allowed by startup options");
        }
        if details_becomes_enabled(
            current.sockets.enabled,
            current.sockets.level,
            next.sockets.enabled,
            next.sockets.level,
        ) && !self.diagnostics.allow_socket_details
        {
            anyhow::bail!("socket details collection is not allowed by startup options");
        }

        Ok(())
    }

    /// 解析一次性进程采集请求。
    fn resolve_process_command(
        &self,
        level: Option<MetricLevel>,
        limit: Option<usize>,
    ) -> anyhow::Result<CollectorCommand> {
        let current = self.config_manager.current();
        let level = level.unwrap_or(current.processes.level);
        let limit = limit.unwrap_or(current.processes.limit);
        validate_process_sampling_options(level, limit, None)?;

        if matches!(level, MetricLevel::Details) && !self.diagnostics.allow_process_details {
            anyhow::bail!("process details collection is not allowed by startup options");
        }

        Ok(CollectorCommand::SampleProcessesOnce { level, limit })
    }

    /// 解析一次性 Socket 采集请求。
    fn resolve_socket_command(
        &self,
        level: Option<MetricLevel>,
        limit: Option<usize>,
    ) -> anyhow::Result<CollectorCommand> {
        let current = self.config_manager.current();
        let level = level.unwrap_or(current.sockets.level);
        let limit = limit.unwrap_or(current.sockets.limit);
        validate_socket_sampling_options(level, limit, None)?;

        if matches!(level, MetricLevel::Details) && !self.diagnostics.allow_socket_details {
            anyhow::bail!("socket details collection is not allowed by startup options");
        }

        Ok(CollectorCommand::SampleSocketsOnce { level, limit })
    }

    /// 投递采集控制命令；队列满时直接返回错误，避免 server 堆积诊断请求。
    fn send_collector_command(&self, command: CollectorCommand) -> anyhow::Result<()> {
        self.collector_commands
            .try_send(command)
            .map_err(|error| match error {
                TrySendError::Full(_) => anyhow::anyhow!("collector command queue is full"),
                TrySendError::Closed(_) => anyhow::anyhow!("collector command channel is closed"),
            })
    }
}

/// 格式化控制响应入队失败原因。
fn control_response_queue_error(error: TrySendError<OutboundEvent>) -> String {
    match error {
        TrySendError::Full(_) => "outbound event queue is full".to_string(),
        TrySendError::Closed(_) => "outbound event queue is closed".to_string(),
    }
}

/// 入站命令循环。
pub(crate) async fn inbound_command_loop(
    mut commands: InboundCommandReceiver,
    dispatcher: ControlDispatcher,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    tracing::debug!(
        queue_capacity = INBOUND_COMMAND_QUEUE_CAPACITY,
        "inbound command loop started"
    );

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    tracing::warn!("inbound command channel closed; stopping dispatcher");
                    break;
                };

                if let Err(err) = dispatcher.dispatch(command) {
                    tracing::error!(error = ?err, "inbound command dispatch failed");
                }
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    tracing::debug!("inbound command loop stopped");
}

/// 判断 patch 后是否新打开了 details 采集。
fn details_becomes_enabled(
    current_enabled: bool,
    current_level: MetricLevel,
    next_enabled: bool,
    next_level: MetricLevel,
) -> bool {
    next_enabled
        && matches!(next_level, MetricLevel::Details)
        && !(current_enabled && matches!(current_level, MetricLevel::Details))
}
