//! 可选的会话 Driver，把长流收发、维护定时器和业务事件统一放在一个 Tokio task 中。

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, info, warn};

use crate::{
    agent::v1::{
        AgentCapabilitySync, AgentJobPolicySync, AgentKeyRotationAccepted, AgentPluginSync,
        DiagnosticMessage, JobCommand, JobCommandResult, KeyRotationMessage, SecureMessage,
        ServerKeyAcknowledgement, TaskReport, key_rotation_message, secure_message,
    },
    noise::{AgentRotationPrepared, KeyId, RotationId, ServerRotationPrepared},
};

use super::{HeartbeatStats, SessionEvent, TonicNoiseSession, TransportError};

/// Driver 的有界队列容量配置。
#[derive(Clone, Copy, Debug)]
pub struct SessionDriverConfig {
    /// 业务发送请求队列容量；队列满时发送方异步等待，形成反压。
    pub command_capacity: usize,
    /// 已解密业务事件队列容量；消费过慢时 Driver 暂停继续读取。
    pub event_capacity: usize,
}

impl Default for SessionDriverConfig {
    fn default() -> Self {
        Self {
            command_capacity: 32,
            event_capacity: 128,
        }
    }
}

/// Driver 内部命令；每次发送都通过 oneshot 返回实际写入结果。
pub(crate) enum DriverCommand {
    Send {
        message: SecureMessage,
        completed: oneshot::Sender<Result<(), TransportError>>,
    },
    Shutdown {
        completed: oneshot::Sender<()>,
    },
    Ping {
        nonce: u64,
        completed: oneshot::Sender<Result<(), TransportError>>,
    },
    RequestRekey {
        completed: oneshot::Sender<Result<u64, TransportError>>,
    },
    RequireRekey {
        completed: oneshot::Sender<Result<u64, TransportError>>,
    },
    HeartbeatStats {
        completed: oneshot::Sender<Result<HeartbeatStats, TransportError>>,
    },
}

/// 可克隆的会话发送句柄；真正的加密和 nonce 推进始终在 Driver task 内串行完成。
#[derive(Clone)]
pub struct SessionHandle {
    commands: mpsc::Sender<DriverCommand>,
}

impl SessionHandle {
    /// 向 Driver 投递一个需要响应的命令，并统一处理命令队列或响应端关闭。
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, TransportError>>) -> DriverCommand,
    ) -> Result<T, TransportError> {
        let (completed, result) = oneshot::channel();
        self.commands.send(command(completed)).await.map_err(|_| {
            warn!("Noise session Driver command channel is closed");
            TransportError::Closed
        })?;
        result.await.map_err(|_| TransportError::Closed)?
    }

    /// 发送原始加密业务 envelope，供高级或尚未封装的消息类型使用。
    pub async fn send(&self, message: SecureMessage) -> Result<(), TransportError> {
        self.request(|completed| DriverCommand::Send { message, completed })
            .await
    }

    /// 发送示例或链路诊断请求/响应。
    pub async fn send_diagnostic(&self, message: DiagnosticMessage) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::Diagnostic(message)),
        })
        .await
    }

    /// 发送强类型 Task 上报。
    pub async fn send_task_report(&self, report: TaskReport) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::TaskReport(Box::new(report))),
        })
        .await
    }

    /// 发送 Server Job 控制命令。
    pub async fn send_job_command(&self, command: JobCommand) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::JobCommand(command)),
        })
        .await
    }

    /// 发送 Agent Job 控制结果。
    pub async fn send_job_command_result(
        &self,
        result: JobCommandResult,
    ) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::JobCommandResult(Box::new(result))),
        })
        .await
    }

    /// 发送 Agent Job 策略查询、完整快照或确认。
    pub async fn send_agent_job_policy(
        &self,
        message: AgentJobPolicySync,
    ) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::AgentJobPolicy(message)),
        })
        .await
    }

    /// 发送 Agent 能力查询或完整快照。
    pub async fn send_agent_capability(
        &self,
        message: AgentCapabilitySync,
    ) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::AgentCapability(message)),
        })
        .await
    }

    /// 发送 Plus 插件查询、清单、运行时快照或确认。
    pub async fn send_agent_plugin(&self, message: AgentPluginSync) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::AgentPlugin(message)),
        })
        .await
    }

    /// 手动发送加密 Ping；自动 Driver 通常会按 HeartbeatPolicy 自行发送。
    pub async fn ping(&self, nonce: u64) -> Result<(), TransportError> {
        debug!(
            nonce,
            "queueing manual heartbeat ping in Noise session Driver"
        );
        self.request(|completed| DriverCommand::Ping { nonce, completed })
            .await
    }

    /// Agent/initiator 立即发起当前连接的同步 rekey，并返回新 generation。
    pub async fn request_rekey(&self) -> Result<u64, TransportError> {
        info!("queueing initiator Noise rekey in session Driver");
        self.request(|completed| DriverCommand::RequestRekey { completed })
            .await
    }

    /// Server/responder 通知 Agent 发起同步 rekey。
    pub async fn require_rekey(&self) -> Result<u64, TransportError> {
        info!("queueing responder Noise rekey requirement in session Driver");
        self.request(|completed| DriverCommand::RequireRekey { completed })
            .await
    }

    /// 查询 Driver 当前维护的心跳统计和最近一次 RTT 样本。
    pub async fn heartbeat_stats(&self) -> Result<HeartbeatStats, TransportError> {
        self.request(|completed| DriverCommand::HeartbeatStats { completed })
            .await
    }

    /// Agent 发送已经持久化 pending 私钥的静态公钥轮换请求。
    pub async fn request_agent_key_rotation(
        &self,
        prepared: &AgentRotationPrepared,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::AgentRequest(
            prepared.request.clone(),
        )))
        .await
    }

    /// Server 确认已经保存 Agent pending 公钥。
    pub async fn accept_agent_key_rotation(
        &self,
        rotation_id: RotationId,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::AgentAccepted(
            AgentKeyRotationAccepted {
                rotation_id: rotation_id.as_bytes().to_vec(),
            },
        )))
        .await
    }

    /// Server 公告已经持久化的下一把静态公钥。
    pub async fn announce_server_key(
        &self,
        prepared: &ServerRotationPrepared,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::ServerAnnouncement(
            prepared.announcement.clone(),
        )))
        .await
    }

    /// Agent 确认已经校验并保存 Server pending 公钥。
    pub async fn acknowledge_server_key(
        &self,
        rotation_id: RotationId,
        key_id: KeyId,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::ServerAcknowledgement(
            ServerKeyAcknowledgement {
                rotation_id: rotation_id.as_bytes().to_vec(),
                key_id: key_id.as_bytes().to_vec(),
            },
        )))
        .await
    }

    /// 请求 Driver 正常结束；已经进入 Tonic channel 的帧不会被撤回。
    pub async fn shutdown(&self) -> Result<(), TransportError> {
        info!("requesting Noise session Driver shutdown");
        let (completed, result) = oneshot::channel();
        if self
            .commands
            .send(DriverCommand::Shutdown { completed })
            .await
            .is_err()
        {
            // Driver 已经因为远端关闭或错误退出时，shutdown 仍然是幂等成功。
            return Ok(());
        }
        let _ = result.await;
        Ok(())
    }
}

/// 单消费者的强类型业务事件接收器。
pub struct SessionEventReceiver {
    events: mpsc::Receiver<Result<SessionEvent, TransportError>>,
}

impl SessionEventReceiver {
    /// 等待下一条业务事件或终止错误；返回 `None` 表示 Driver 已经结束且队列已排空。
    pub async fn recv(&mut self) -> Option<Result<SessionEvent, TransportError>> {
        self.events.recv().await
    }
}

/// 已启动 Driver 的三个所有权句柄。
pub struct RunningSession {
    /// 可克隆给多个采集任务的发送句柄。
    pub handle: SessionHandle,
    /// 由一个业务调度循环消费的事件接收器。
    pub events: SessionEventReceiver,
    /// Driver task；等待它可确认网络循环已经完全退出。
    pub task: JoinHandle<()>,
}

/// 尚未启动的会话 Driver。
pub struct SessionDriver {
    session: TonicNoiseSession,
    commands: mpsc::Receiver<DriverCommand>,
    events: mpsc::Sender<Result<SessionEvent, TransportError>>,
}

impl SessionDriver {
    /// 创建 Driver 及其外部句柄，但不自动启动 Tokio task。
    pub fn new(
        session: TonicNoiseSession,
        config: SessionDriverConfig,
    ) -> (Self, SessionHandle, SessionEventReceiver) {
        info!(
            command_capacity = config.command_capacity.max(1),
            event_capacity = config.event_capacity.max(1),
            "created Noise session Driver"
        );
        let (command_sender, commands) = mpsc::channel(config.command_capacity.max(1));
        let (events, event_receiver) = mpsc::channel(config.event_capacity.max(1));
        (
            Self {
                session,
                commands,
                events,
            },
            SessionHandle {
                commands: command_sender,
            },
            SessionEventReceiver {
                events: event_receiver,
            },
        )
    }

    /// 在当前 task 中运行到主动关闭、远端关闭或发生错误。
    pub async fn run(self) {
        info!("running Noise session Driver task");
        self.session.run_driver(self.commands, self.events).await;
        info!("Noise session Driver task stopped");
    }

    /// 创建并立即启动 Driver，适合不需要自定义 task 生命周期的调用方。
    pub fn spawn(session: TonicNoiseSession, config: SessionDriverConfig) -> RunningSession {
        info!("spawning Noise session Driver task");
        let (driver, handle, events) = Self::new(session, config);
        let task = tokio::spawn(driver.run());
        RunningSession {
            handle,
            events,
            task,
        }
    }
}

/// 构造 Driver 发送的长期静态密钥轮换消息。
fn rotation(body: key_rotation_message::Body) -> SecureMessage {
    SecureMessage {
        body: Some(secure_message::Body::KeyRotation(KeyRotationMessage {
            body: Some(body),
        })),
    }
}
