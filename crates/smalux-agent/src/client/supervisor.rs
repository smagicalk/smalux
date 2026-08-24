//! 已认证 Session 的启动、心跳验证和断线重连监督器。

use std::sync::Arc;

use smalux_protocol::{
    agent::v1::{
        AgentCapabilitySync, AgentJobPolicySync, AgentPluginSync, JobCommandResult,
        KeyRotationMessage, TaskReport, key_rotation_message,
    },
    noise::{KeyId, NoiseError, NoisePublicKey, RotationId},
    tonic_transport::{HeartbeatStats, RunningSession, SessionEvent, TransportError},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, sleep_until},
};
use tracing::{debug, info, warn};

use super::{
    AgentStateStore, AuthenticationMode, ClientConnectionStatus, SmaluxClientConfig,
    SmaluxClientError, SmaluxClientEvent,
    connection::{ActiveConnection, ConnectionCoordinator, stop_running_session},
    state::PersistedAgentState,
};

/// 外层 Client 到监督器的稳定命令；Session 重建不会改变这个 channel。
pub(super) enum SupervisorCommand {
    SendTaskReport {
        report: TaskReport,
        completed: oneshot::Sender<Result<(), SmaluxClientError>>,
    },
    SendJobCommandResult {
        result: JobCommandResult,
        completed: oneshot::Sender<Result<(), SmaluxClientError>>,
    },
    SendAgentJobPolicy {
        message: AgentJobPolicySync,
        completed: oneshot::Sender<Result<(), SmaluxClientError>>,
    },
    SendAgentCapability {
        message: AgentCapabilitySync,
        completed: oneshot::Sender<Result<(), SmaluxClientError>>,
    },
    SendAgentPlugin {
        message: AgentPluginSync,
        completed: oneshot::Sender<Result<(), SmaluxClientError>>,
    },
    HeartbeatStats {
        completed: oneshot::Sender<Result<HeartbeatStats, SmaluxClientError>>,
    },
    Shutdown {
        completed: oneshot::Sender<()>,
    },
}

/// 运行活动 Session，遇到临时链路错误时持续用 IK 重连。
pub(super) async fn run(
    config: SmaluxClientConfig,
    mut active: ActiveConnection,
    store: Arc<dyn AgentStateStore>,
    mut commands: mpsc::Receiver<SupervisorCommand>,
    events: mpsc::Sender<SmaluxClientEvent>,
    status: watch::Sender<ClientConnectionStatus>,
) {
    let connection = ConnectionCoordinator::new(&config, store.as_ref());
    announce_connected(&events, &status, active.mode).await;
    loop {
        // 缓存事件和实时事件在这里汇合；只有没有缓存时才等待新命令或网络帧。
        let event = match active.buffered_events.pop_front() {
            Some(event) => Some(Ok(event)),
            None => tokio::select! {
                command = commands.recv() => {
                    match command {
                        Some(command) => {
                            if !handle_connected_command(command, &active.running).await {
                                stop_running_session(active.running).await;
                                status.send_replace(ClientConnectionStatus::Disconnected);
                                return;
                            }
                        }
                        None => {
                            stop_running_session(active.running).await;
                            return;
                        }
                    }
                    continue;
                }
                event = active.running.events.recv() => event,
            },
        };

        let disconnect_error = match event {
            Some(Ok(event)) => {
                match process_session_event(event, &mut active, store.as_ref()).await {
                    Ok(Some(event)) => {
                        if !forward_event_or_command(
                            SmaluxClientEvent::Session(event),
                            &active.running,
                            &mut commands,
                            &events,
                        )
                        .await
                        {
                            stop_running_session(active.running).await;
                            status.send_replace(ClientConnectionStatus::Disconnected);
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        stop_running_session(active.running).await;
                        report_fatal(&events, &status, error);
                        return;
                    }
                }
                continue;
            }
            Some(Err(error)) => error,
            None => TransportError::Closed,
        };
        stop_running_session(active.running).await;
        match reconnect(
            &config,
            &connection,
            active.state,
            disconnect_error,
            &mut commands,
            &events,
            &status,
        )
        .await
        {
            Ok(reconnected) => active = reconnected,
            Err(SmaluxClientError::ShutdownRequested) => {
                status.send_replace(ClientConnectionStatus::Disconnected);
                return;
            }
            Err(error) => {
                report_fatal(&events, &status, error);
                return;
            }
        }
    }
}

fn report_fatal(
    events: &mpsc::Sender<SmaluxClientEvent>,
    status: &watch::Sender<ClientConnectionStatus>,
    error: SmaluxClientError,
) {
    status.send_replace(ClientConnectionStatus::Fatal);
    let _ = events.try_send(SmaluxClientEvent::Fatal(error));
}

/// 消费由 Client 负责的安全控制消息；普通业务事件继续交给上层单消费者。
async fn process_session_event(
    event: SessionEvent,
    active: &mut ActiveConnection,
    store: &dyn AgentStateStore,
) -> Result<Option<SessionEvent>, SmaluxClientError> {
    let SessionEvent::KeyRotation(message) = event else {
        return Ok(Some(event));
    };
    let announcement = match message.body {
        Some(key_rotation_message::Body::ServerAnnouncement(announcement)) => announcement,
        body => return Ok(Some(SessionEvent::KeyRotation(KeyRotationMessage { body }))),
    };

    let public_key = NoisePublicKey::from_bytes(&announcement.new_public_key)?;
    let announced_key_id = KeyId::from_bytes(&announcement.new_key_id)?;
    if public_key.key_id() != announced_key_id {
        return Err(NoiseError::UnknownKeyId.into());
    }
    let rotation_id = RotationId::from_bytes(&announcement.rotation_id)?;
    let updated = active
        .state
        .with_server_public_key(public_key)
        .map_err(SmaluxClientError::state_store)?;

    // 必须先持久化再 ACK；进程在这两步之间退出时，重连仍保留新旧两把 Server key。
    store
        .save(&updated)
        .await
        .map_err(SmaluxClientError::state_store)?;
    active.state = updated;
    if let Err(error) = active
        .running
        .handle
        .acknowledge_server_key(rotation_id, announced_key_id)
        .await
    {
        // 新 key 已安全落盘；发送通道关闭时 Driver 随后会报告断线并使用新旧候选重连。
        warn!(
            error = %error,
            ?announced_key_id,
            "saved rotated Server public key but could not send acknowledgement"
        );
        return Ok(None);
    }
    info!(
        ?announced_key_id,
        "saved and acknowledged a rotated Server public key"
    );
    Ok(None)
}

/// 等待事件队列容量时仍继续处理控制命令，避免队列满后 Shutdown 永久等待。
async fn forward_event_or_command(
    event: SmaluxClientEvent,
    running: &RunningSession,
    commands: &mut mpsc::Receiver<SupervisorCommand>,
    events: &mpsc::Sender<SmaluxClientEvent>,
) -> bool {
    loop {
        tokio::select! {
            permit = events.reserve() => {
                let Ok(permit) = permit else {
                    return false;
                };
                permit.send(event);
                return true;
            }
            command = commands.recv() => {
                match command {
                    Some(command) => {
                        if !handle_connected_command(command, running).await {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
        }
    }
}

async fn reconnect(
    config: &SmaluxClientConfig,
    connection: &ConnectionCoordinator<'_>,
    state: PersistedAgentState,
    initial_error: TransportError,
    commands: &mut mpsc::Receiver<SupervisorCommand>,
    events: &mpsc::Sender<SmaluxClientEvent>,
    status: &watch::Sender<ClientConnectionStatus>,
) -> Result<ActiveConnection, SmaluxClientError> {
    let initial_error = SmaluxClientError::Transport(initial_error);
    if !initial_error.is_retryable() {
        return Err(initial_error);
    }
    status.send_replace(ClientConnectionStatus::Reconnecting);
    let mut delay = config.reconnect.initial_delay;
    let mut reason = initial_error.to_string();
    if events
        .send(SmaluxClientEvent::Disconnected {
            reason: reason.clone(),
            retry_in: delay,
        })
        .await
        .is_err()
    {
        return Err(SmaluxClientError::ShutdownRequested);
    }
    loop {
        let deadline = Instant::now() + delay;
        loop {
            tokio::select! {
                _ = sleep_until(deadline) => break,
                command = commands.recv() => {
                    match command {
                        Some(SupervisorCommand::Shutdown { completed }) => {
                            let _ = completed.send(());
                            status.send_replace(ClientConnectionStatus::Disconnected);
                            return Err(SmaluxClientError::ShutdownRequested);
                        }
                        Some(command) => reject_disconnected_command(command),
                        None => return Err(SmaluxClientError::ShutdownRequested),
                    }
                }
            }
        }

        match connection.connect_registered(&state).await {
            Ok(reconnected) => {
                announce_connected(events, status, AuthenticationMode::ReconnectIk).await;
                return Ok(reconnected);
            }
            Err(error) if error.is_retryable() => {
                reason = error.to_string();
                delay = delay.saturating_mul(2).min(config.reconnect.max_delay);
                debug!(%reason, ?delay, "Agent IK reconnect attempt failed; retrying");
            }
            Err(error) => return Err(error),
        }
    }
}

async fn announce_connected(
    events: &mpsc::Sender<SmaluxClientEvent>,
    status: &watch::Sender<ClientConnectionStatus>,
    mode: AuthenticationMode,
) {
    status.send_replace(ClientConnectionStatus::Connected);
    if events
        .send(SmaluxClientEvent::Connected { mode })
        .await
        .is_err()
    {
        warn!(?mode, "Agent status event queue is closed");
    }
    info!(?mode, "Agent Client connection is authenticated");
}

/// 返回 `false` 表示收到 Shutdown，调用方应结束监督器。
async fn handle_connected_command(command: SupervisorCommand, running: &RunningSession) -> bool {
    match command {
        SupervisorCommand::SendTaskReport { report, completed } => {
            let _ = completed.send(
                running
                    .handle
                    .send_task_report(report)
                    .await
                    .map_err(SmaluxClientError::Transport),
            );
            true
        }
        SupervisorCommand::SendJobCommandResult { result, completed } => {
            let _ = completed.send(
                running
                    .handle
                    .send_job_command_result(result)
                    .await
                    .map_err(SmaluxClientError::Transport),
            );
            true
        }
        SupervisorCommand::SendAgentJobPolicy { message, completed } => {
            let _ = completed.send(
                running
                    .handle
                    .send_agent_job_policy(message)
                    .await
                    .map_err(SmaluxClientError::Transport),
            );
            true
        }
        SupervisorCommand::SendAgentCapability { message, completed } => {
            let _ = completed.send(
                running
                    .handle
                    .send_agent_capability(message)
                    .await
                    .map_err(SmaluxClientError::Transport),
            );
            true
        }
        SupervisorCommand::SendAgentPlugin { message, completed } => {
            let _ = completed.send(
                running
                    .handle
                    .send_agent_plugin(message)
                    .await
                    .map_err(SmaluxClientError::Transport),
            );
            true
        }
        SupervisorCommand::HeartbeatStats { completed } => {
            let _ = completed.send(
                running
                    .handle
                    .heartbeat_stats()
                    .await
                    .map_err(SmaluxClientError::Transport),
            );
            true
        }
        SupervisorCommand::Shutdown { completed } => {
            let _ = running.handle.shutdown().await;
            let _ = completed.send(());
            false
        }
    }
}

fn reject_disconnected_command(command: SupervisorCommand) {
    match command {
        SupervisorCommand::SendTaskReport { completed, .. }
        | SupervisorCommand::SendJobCommandResult { completed, .. }
        | SupervisorCommand::SendAgentJobPolicy { completed, .. }
        | SupervisorCommand::SendAgentCapability { completed, .. }
        | SupervisorCommand::SendAgentPlugin { completed, .. } => {
            let _ = completed.send(Err(SmaluxClientError::TemporarilyUnavailable));
        }
        SupervisorCommand::HeartbeatStats { completed } => {
            let _ = completed.send(Err(SmaluxClientError::TemporarilyUnavailable));
        }
        SupervisorCommand::Shutdown { completed } => {
            let _ = completed.send(());
        }
    }
}

impl SmaluxClientError {
    pub(super) fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(
                TransportError::Transport(_)
                | TransportError::Timeout(_)
                | TransportError::Closed
                | TransportError::HeartbeatTimeout,
            ) => true,
            Self::Transport(TransportError::Status(status)) => matches!(
                status.code(),
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::ResourceExhausted
                    | tonic::Code::Aborted
                    | tonic::Code::Internal
                    | tonic::Code::Unavailable
            ),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use smalux_protocol::tonic_transport::TransportError;
    use tokio::sync::oneshot;
    use tonic::{Code, Status};

    use super::{SmaluxClientError, SupervisorCommand, reject_disconnected_command};

    #[test]
    fn retryable_errors_are_limited_to_transient_transport_failures() {
        for error in [
            SmaluxClientError::Transport(TransportError::Closed),
            SmaluxClientError::Transport(TransportError::HeartbeatTimeout),
            SmaluxClientError::Transport(TransportError::Timeout("handshake")),
            SmaluxClientError::Transport(TransportError::Status(Status::new(
                Code::Unavailable,
                "temporary",
            ))),
        ] {
            assert!(error.is_retryable(), "{error} should be retryable");
        }

        for error in [
            SmaluxClientError::RegistrationTokenRequired,
            SmaluxClientError::Transport(TransportError::Protocol("invalid frame".to_owned())),
            SmaluxClientError::Transport(TransportError::Status(Status::new(
                Code::Unauthenticated,
                "invalid identity",
            ))),
        ] {
            assert!(!error.is_retryable(), "{error} must fail fast");
        }
    }

    #[tokio::test]
    async fn disconnected_commands_return_temporary_failure_but_shutdown_succeeds() {
        let (completed, result) = oneshot::channel();
        reject_disconnected_command(SupervisorCommand::HeartbeatStats { completed });
        assert!(matches!(
            result.await.unwrap(),
            Err(SmaluxClientError::TemporarilyUnavailable)
        ));

        let (completed, result) = oneshot::channel();
        reject_disconnected_command(SupervisorCommand::Shutdown { completed });
        result.await.expect("shutdown acknowledgement");
    }
}
