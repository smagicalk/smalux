//! Agent 对外使用的统一 Client 门面。
//!
//! 调用方只配置 Server、可选注册 Token 和状态存储，然后调用 [`SmaluxClient::connect`]。
//! Client 会根据持久化阶段自动选择 XXpsk3 或 IK，并在认证成功后启动 SessionDriver，
//! 因此心跳、Pong、rekey 和临时网络故障重连都不依赖业务代码持续轮询底层流。

use std::{sync::Arc, time::Duration};

use smalux_protocol::{
    agent::v1::{
        AgentCapabilitySync, AgentJobPolicySync, AgentPluginSync, HealthResponse, JobCommandResult,
        TaskReport,
    },
    noise::NoiseError,
    tonic_transport::{HeartbeatStats, SessionEvent, TransportError},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

mod config;
mod connection;
mod state;
mod supervisor;

pub use config::{ReconnectPolicy, RegistrationToken, SmaluxClientConfig};
pub use state::{AgentStateStore, FileAgentStateStore, PersistedAgentState, RegistrationStage};
use supervisor::SupervisorCommand;

/// 本次连接采用的认证方式。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationMode {
    RegistrationXxPsk3,
    ReconnectIk,
}

/// Client 当前连接生命周期状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Fatal,
}

/// 对业务层公开的连接和协议事件。
#[derive(Debug)]
pub enum SmaluxClientEvent {
    Connected { mode: AuthenticationMode },
    Disconnected { reason: String, retry_in: Duration },
    Session(SessionEvent),
    Fatal(SmaluxClientError),
}

/// Agent Client 稳定错误分类。
#[derive(Debug, thiserror::Error)]
pub enum SmaluxClientError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Noise(#[from] NoiseError),
    #[error("Agent state store failed: {0}")]
    StateStore(#[source] anyhow::Error),
    #[error("registration Token is required because this Agent is not registered")]
    RegistrationTokenRequired,
    #[error("registration Token is invalid: {0}")]
    InvalidRegistrationToken(String),
    #[error("Server returned registration data inconsistent with the saved pending state")]
    InconsistentPendingRegistration,
    #[error("Smalux Client is already connected")]
    AlreadyConnected,
    #[error("Smalux Client is not connected")]
    NotConnected,
    #[error("Smalux Client is reconnecting; retry the operation later")]
    TemporarilyUnavailable,
    #[error("Smalux Client supervisor stopped")]
    SupervisorStopped,
    #[error("Smalux Client shutdown was requested")]
    ShutdownRequested,
}

/// 可跨异步任务克隆的业务发送句柄。
///
/// 句柄只持有监督器命令通道，不持有事件接收端和生命周期任务。因此 Scheduler 的
/// `TaskReportSink` 可以长期保存它，而 `SmaluxClient` 仍然保持事件的单消费者语义。
#[derive(Clone)]
pub struct SmaluxClientHandle {
    commands: mpsc::Sender<SupervisorCommand>,
}

impl SmaluxClientHandle {
    /// 通过当前已认证会话上报一次 Task 结果。
    pub async fn send_task_report(&self, report: TaskReport) -> Result<(), SmaluxClientError> {
        self.request(|completed| SupervisorCommand::SendTaskReport { report, completed })
            .await
    }

    /// 返回 Server 对上一条 Job 命令的结构化执行结果。
    pub async fn send_job_command_result(
        &self,
        result_value: JobCommandResult,
    ) -> Result<(), SmaluxClientError> {
        self.request(|completed| SupervisorCommand::SendJobCommandResult {
            result: result_value,
            completed,
        })
        .await
    }

    /// 通过当前认证会话同步 Agent 拥有的本地 Job 策略。
    pub async fn send_agent_job_policy(
        &self,
        message: AgentJobPolicySync,
    ) -> Result<(), SmaluxClientError> {
        self.request(|completed| SupervisorCommand::SendAgentJobPolicy { message, completed })
            .await
    }

    /// 通过当前认证会话发送 Agent 能力查询或完整快照。
    pub async fn send_agent_capability(
        &self,
        message: AgentCapabilitySync,
    ) -> Result<(), SmaluxClientError> {
        self.request(|completed| SupervisorCommand::SendAgentCapability { message, completed })
            .await
    }

    /// 通过当前认证会话发送 Plus 插件查询、清单、运行时快照或确认。
    pub async fn send_agent_plugin(
        &self,
        message: AgentPluginSync,
    ) -> Result<(), SmaluxClientError> {
        self.request(|completed| SupervisorCommand::SendAgentPlugin { message, completed })
            .await
    }

    /// 查询后台 Driver 维护的发送、接收和最近 RTT 统计。
    pub async fn heartbeat_stats(&self) -> Result<HeartbeatStats, SmaluxClientError> {
        self.request(|completed| SupervisorCommand::HeartbeatStats { completed })
            .await
    }

    /// 统一一次“向监督器发送命令并等待结果”的 request/response Adapter。
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, SmaluxClientError>>) -> SupervisorCommand,
    ) -> Result<T, SmaluxClientError> {
        let (completed, result) = oneshot::channel();
        self.commands
            .send(command(completed))
            .await
            .map_err(|_| SmaluxClientError::SupervisorStopped)?;
        result
            .await
            .map_err(|_| SmaluxClientError::SupervisorStopped)?
    }
}

impl SmaluxClientError {
    fn state_store(error: anyhow::Error) -> Self {
        Self::StateStore(error)
    }
}

/// 自动认证、后台维护和稳定业务收发的外层 Client。
pub struct SmaluxClient {
    config: SmaluxClientConfig,
    state_store: Arc<dyn AgentStateStore>,
    commands: Option<mpsc::Sender<SupervisorCommand>>,
    events: Option<mpsc::Receiver<SmaluxClientEvent>>,
    status: watch::Receiver<ClientConnectionStatus>,
    status_sender: watch::Sender<ClientConnectionStatus>,
    supervisor: Option<JoinHandle<()>>,
}

impl SmaluxClient {
    pub fn new(config: SmaluxClientConfig, state_store: Arc<dyn AgentStateStore>) -> Self {
        let (status_sender, status) = watch::channel(ClientConnectionStatus::Disconnected);
        Self {
            config,
            state_store,
            commands: None,
            events: None,
            status,
            status_sender,
            supervisor: None,
        }
    }

    /// 自动加载本地状态并选择 XXpsk3 或 IK；成功返回时后台 Driver 已经运行。
    pub async fn connect(&mut self) -> Result<(), SmaluxClientError> {
        if self.supervisor.is_some() {
            return Err(SmaluxClientError::AlreadyConnected);
        }
        self.status_sender
            .send_replace(ClientConnectionStatus::Connecting);
        let mut delay = self.config.reconnect.initial_delay;
        let active = loop {
            match connection::ConnectionCoordinator::new(&self.config, self.state_store.as_ref())
                .establish_initial()
                .await
            {
                Ok(active) => break active,
                Err(error) if error.is_retryable() => {
                    self.status_sender
                        .send_replace(ClientConnectionStatus::Reconnecting);
                    tracing::warn!(
                        error = %error,
                        ?delay,
                        "initial Agent connection failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2).min(self.config.reconnect.max_delay);
                }
                Err(error) => {
                    self.status_sender
                        .send_replace(ClientConnectionStatus::Disconnected);
                    return Err(error);
                }
            }
        };
        let (command_sender, commands) = mpsc::channel(self.config.driver.command_capacity.max(1));
        let (event_sender, events) = mpsc::channel(self.config.driver.event_capacity.max(1));
        let task = tokio::spawn(supervisor::run(
            self.config.clone(),
            active,
            Arc::clone(&self.state_store),
            commands,
            event_sender,
            self.status_sender.clone(),
        ));
        self.commands = Some(command_sender);
        self.events = Some(events);
        self.supervisor = Some(task);
        Ok(())
    }

    /// 未认证健康检查，不改变当前会话和注册状态。
    pub async fn health_check(&self) -> Result<HealthResponse, SmaluxClientError> {
        connection::ConnectionCoordinator::new(&self.config, self.state_store.as_ref())
            .health_check()
            .await
    }

    pub fn connection_status(&self) -> ClientConnectionStatus {
        *self.status.borrow()
    }

    /// 订阅连接生命周期状态，供本地管理接口只读观察。
    pub fn subscribe_connection_status(&self) -> watch::Receiver<ClientConnectionStatus> {
        self.status.clone()
    }

    /// 获取稳定业务发送句柄；会话断线重建不会使该句柄失效。
    pub fn handle(&self) -> Result<SmaluxClientHandle, SmaluxClientError> {
        Ok(SmaluxClientHandle {
            commands: self.command_sender()?.clone(),
        })
    }

    pub async fn send_task_report(&self, report: TaskReport) -> Result<(), SmaluxClientError> {
        self.handle()?.send_task_report(report).await
    }

    pub async fn send_job_command_result(
        &self,
        result_value: JobCommandResult,
    ) -> Result<(), SmaluxClientError> {
        self.handle()?.send_job_command_result(result_value).await
    }

    pub async fn heartbeat_stats(&self) -> Result<HeartbeatStats, SmaluxClientError> {
        self.handle()?.heartbeat_stats().await
    }

    pub async fn next_event(&mut self) -> Result<Option<SmaluxClientEvent>, SmaluxClientError> {
        let events = self
            .events
            .as_mut()
            .ok_or(SmaluxClientError::NotConnected)?;
        Ok(events.recv().await)
    }

    pub async fn disconnect(&mut self) -> Result<(), SmaluxClientError> {
        let Some(commands) = self.commands.take() else {
            return Ok(());
        };
        let (completed, result) = oneshot::channel();
        if commands
            .send(SupervisorCommand::Shutdown { completed })
            .await
            .is_ok()
        {
            let _ = result.await;
        }
        if let Some(task) = self.supervisor.take() {
            task.await
                .map_err(|_| SmaluxClientError::SupervisorStopped)?;
        }
        self.events = None;
        self.status_sender
            .send_replace(ClientConnectionStatus::Disconnected);
        Ok(())
    }

    /// 显式清除本地注册状态；不会自动撤销 Server 端 Agent。
    pub async fn forget_registration(&mut self) -> Result<(), SmaluxClientError> {
        self.disconnect().await?;
        self.state_store
            .clear()
            .await
            .map_err(SmaluxClientError::state_store)
    }

    fn command_sender(&self) -> Result<&mpsc::Sender<SupervisorCommand>, SmaluxClientError> {
        self.commands
            .as_ref()
            .ok_or(SmaluxClientError::NotConnected)
    }
}

impl Drop for SmaluxClient {
    fn drop(&mut self) {
        if let Some(task) = self.supervisor.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{FileAgentStateStore, SmaluxClient, SmaluxClientConfig, SmaluxClientError};

    #[tokio::test]
    async fn disconnected_client_rejects_business_send_without_network_access() {
        let config = SmaluxClientConfig::new("http://127.0.0.1:12345").unwrap();
        let store = Arc::new(FileAgentStateStore::new(
            std::env::temp_dir().join("unused-smalux-client-state.json"),
        ));
        let client = SmaluxClient::new(config, store);

        let error = client
            .send_task_report(Default::default())
            .await
            .unwrap_err();
        assert!(matches!(error, SmaluxClientError::NotConnected));
    }
}
