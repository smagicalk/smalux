//! 已建立 Tonic + Noise 长流的业务收发、心跳、rekey 和换钥控制消息。
//!
//! `TonicNoiseSession` 把请求 sender、响应 stream 和 `SecureSession` 绑定在同一可变对象中，
//! 强制所有 nonce 相关操作串行执行。

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::{
    agent::v1::{
        AgentKeyRotationAccepted, JobCommand, JobCommandResult, KeyRotationMessage, Messages, Ping,
        Pong, ProtocolFrame, RegistrationMessage, RekeyAck, RekeyRequest, RekeyRequired,
        SecureMessage, ServerKeyAcknowledgement, SessionControl, TaskReport, key_rotation_message,
        secure_message, session_control,
    },
    noise::{AgentRotationPrepared, NoiseError, RotationId, SecureSession, ServerRotationPrepared},
};

use super::TransportError;
use super::driver::DriverCommand;

#[derive(Clone, Copy, Debug)]
/// 长流存活检测策略。
pub struct HeartbeatPolicy {
    /// 没有入站帧达到该时长时，`receive()` 主动发送加密 Ping。
    pub interval: Duration,
    /// 距离上次成功接收超过该时长时判定会话失联。
    pub timeout: Duration,
}

impl Default for HeartbeatPolicy {
    /// 默认每 30 秒探测一次，90 秒无入站消息判定超时。
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(90),
        }
    }
}

#[derive(Clone, Copy, Debug)]
/// 当前连接的对称 cipher key 更新策略。
pub struct RekeyPolicy {
    /// 单个 generation 最长存活时间。
    pub max_age: Duration,
    /// 单个 generation 最多处理的双向加密帧数。
    pub max_frames: u64,
    /// `true` 时 initiator 会在 `receive()` 中自动发起 rekey。
    pub automatic: bool,
}

/// 当前会话维护动作是否已经到期。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaintenanceStatus {
    /// 当前连接已经超过最大无入站时长，继续使用前应关闭并重连。
    pub heartbeat_expired: bool,
    /// 当前端近期没有发送数据，可以发送加密 Ping。
    pub ping_due: bool,
    /// initiator 已达到自动 rekey 的时间或帧数阈值。
    pub rekey_due: bool,
}

/// 一次 [`TonicNoiseSession::perform_maintenance`] 实际完成的动作。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceResult {
    /// 本次调用是否发送了 Ping。
    pub ping_sent: bool,
    /// 本次调用完成的新 generation；未执行 rekey 时为 `None`。
    pub rekeyed_generation: Option<u64>,
}

/// 已解密并完成控制帧过滤后的强类型会话事件。
#[derive(Debug)]
pub enum SessionEvent {
    /// 首次注册状态消息。
    Registration(RegistrationMessage),
    /// 通用请求或响应消息。
    Messages(Messages),
    /// 长期静态身份密钥轮换消息。
    KeyRotation(KeyRotationMessage),
    /// Server 下发的 Job 控制命令。
    JobCommand(JobCommand),
    /// Agent 返回的 Job 控制结果。
    JobCommandResult(Box<JobCommandResult>),
    /// Agent 上报的强类型 Task 执行结果。
    TaskReport(Box<TaskReport>),
}

impl TryFrom<SecureMessage> for SessionEvent {
    type Error = TransportError;

    /// 把底层 Protobuf envelope 分类为业务事件；控制帧不允许从该入口泄漏。
    fn try_from(message: SecureMessage) -> Result<Self, Self::Error> {
        match message.body {
            Some(secure_message::Body::RegistrationMessage(value)) => Ok(Self::Registration(value)),
            Some(secure_message::Body::Messages(value)) => Ok(Self::Messages(value)),
            Some(secure_message::Body::KeyRotation(value)) => Ok(Self::KeyRotation(value)),
            Some(secure_message::Body::JobCommand(value)) => Ok(Self::JobCommand(value)),
            Some(secure_message::Body::JobCommandResult(value)) => {
                Ok(Self::JobCommandResult(value))
            }
            Some(secure_message::Body::TaskReport(value)) => Ok(Self::TaskReport(value)),
            Some(secure_message::Body::Error(error)) => {
                let code = crate::agent::v1::SecureErrorCode::try_from(error.code)
                    .unwrap_or(crate::agent::v1::SecureErrorCode::Unspecified);
                Err(TransportError::RemoteSecure(code, error.message))
            }
            Some(secure_message::Body::SessionControl(_)) => Err(TransportError::Protocol(
                "session control escaped the transport state machine".to_owned(),
            )),
            None => Err(TransportError::Protocol(
                "SecureMessage body is required".to_owned(),
            )),
        }
    }
}

impl Default for RekeyPolicy {
    /// 默认 1 小时或 `2^20` 帧触发，且开启自动 rekey。
    fn default() -> Self {
        Self {
            max_age: Duration::from_secs(60 * 60),
            max_frames: 1 << 20,
            automatic: true,
        }
    }
}

/// Client 和 Server 的 Tonic 发送 channel 具有不同 item 类型，用枚举统一封装。
enum Outbound {
    /// Client 请求流直接发送 `ProtocolFrame`。
    Client(mpsc::Sender<ProtocolFrame>),
    /// Server 响应流需要发送 `Result<ProtocolFrame, Status>`。
    Server(mpsc::Sender<Result<ProtocolFrame, Status>>),
}

/// 已建立的 Tonic + Noise 会话。所有方法使用 `&mut self`，保证 nonce 严格串行。
pub struct TonicNoiseSession {
    /// 当前端对应的有界响应/请求 channel。
    outbound: Outbound,
    /// 对端发送的 gRPC 帧流。
    inbound: Streaming<ProtocolFrame>,
    /// 持有双向 AEAD key 和 nonce 的底层 Noise 会话。
    secure: SecureSession,
    /// Agent/Client 为 initiator；Server 为 responder。
    initiator: bool,
    /// 当前 generation 建立时间，用于 max_age 判断。
    established_at: Instant,
    /// 最近一次成功发送加密帧的时间。
    last_sent: Instant,
    /// 最近一次成功解密入站帧的时间。
    last_received: Instant,
    /// 自动 Ping 使用的本地递增关联值。
    next_ping_nonce: u64,
    /// 当前心跳参数。
    heartbeat: HeartbeatPolicy,
    /// 当前 rekey 参数。
    rekey: RekeyPolicy,
    /// rekey 等待 ACK 时提前到达的业务消息，完成换钥后按原顺序返回。
    buffered_messages: VecDeque<SecureMessage>,
}

impl TonicNoiseSession {
    /// 把 Client 请求 sender、Server 响应 stream 和 Noise 会话组合为 initiator 会话。
    pub(crate) fn client(
        outbound: mpsc::Sender<ProtocolFrame>,
        inbound: Streaming<ProtocolFrame>,
        secure: SecureSession,
    ) -> Self {
        Self::new(Outbound::Client(outbound), inbound, secure, true)
    }

    pub(crate) fn server(
        outbound: mpsc::Sender<Result<ProtocolFrame, Status>>,
        inbound: Streaming<ProtocolFrame>,
        secure: SecureSession,
    ) -> Self {
        Self::new(Outbound::Server(outbound), inbound, secure, false)
    }

    /// 初始化两端共用的时间基准、心跳策略和 rekey 策略。
    fn new(
        outbound: Outbound,
        inbound: Streaming<ProtocolFrame>,
        secure: SecureSession,
        initiator: bool,
    ) -> Self {
        let now = Instant::now();
        Self {
            outbound,
            inbound,
            secure,
            initiator,
            established_at: now,
            last_sent: now,
            last_received: now,
            next_ping_nonce: 1,
            heartbeat: HeartbeatPolicy::default(),
            rekey: RekeyPolicy::default(),
            buffered_messages: VecDeque::new(),
        }
    }

    /// 替换当前心跳策略；下一次 `receive()` 立即使用新值。
    pub fn set_heartbeat_policy(&mut self, policy: HeartbeatPolicy) {
        self.heartbeat = policy;
    }

    /// 替换自动 rekey 策略；不会立即切换密钥，下一次 `receive()` 才会评估。
    pub fn set_rekey_policy(&mut self, policy: RekeyPolicy) {
        self.rekey = policy;
    }

    /// 返回当前心跳策略副本，便于状态展示或诊断。
    pub fn heartbeat_policy(&self) -> HeartbeatPolicy {
        self.heartbeat
    }

    /// 最近没有成功发送达到 `interval` 时返回 `true`。
    pub fn should_ping(&self) -> bool {
        self.last_sent.elapsed() >= self.heartbeat.interval
    }

    /// 最近没有成功接收达到 `timeout` 时返回 `true`。
    pub fn heartbeat_expired(&self) -> bool {
        self.last_received.elapsed() >= self.heartbeat.timeout
    }

    /// 判断当前 generation 是否达到自动 rekey 的时长或帧数阈值。
    pub fn should_rekey(&self) -> bool {
        self.rekey.automatic
            && (self.established_at.elapsed() >= self.rekey.max_age
                || self.secure.encrypted_frames() >= self.rekey.max_frames)
    }

    /// 返回当前维护状态，不发送网络消息也不改变 cipher state。
    pub fn maintenance_status(&self) -> MaintenanceStatus {
        MaintenanceStatus {
            heartbeat_expired: self.heartbeat_expired(),
            ping_due: self.should_ping(),
            rekey_due: self.initiator && self.should_rekey(),
        }
    }

    /// 执行一次已经到期的心跳或自动 rekey；调用方可在自己的 `select!` 循环中定期调用。
    pub async fn perform_maintenance(&mut self) -> Result<MaintenanceResult, TransportError> {
        let status = self.maintenance_status();
        if status.heartbeat_expired {
            return Err(TransportError::HeartbeatTimeout);
        }
        let mut result = MaintenanceResult::default();
        if status.rekey_due {
            result.rekeyed_generation = Some(self.request_rekey().await?);
        }
        if status.ping_due {
            let nonce = self.next_ping_nonce;
            self.next_ping_nonce = self.next_ping_nonce.wrapping_add(1);
            self.ping(nonce).await?;
            result.ping_sent = true;
        }
        Ok(result)
    }

    /// 编码并加密一条业务或控制消息，然后写入当前端的 Tonic channel。
    ///
    /// `&mut self` 保证同一会话不会并发复用 sending nonce。
    pub async fn send(&mut self, message: SecureMessage) -> Result<(), TransportError> {
        // 先加密再异步发送；加密成功时 nonce 已推进，因此发送失败后会话应关闭而不是重试同帧。
        let frame = self.secure.encrypt(&message)?;
        self.send_frame(frame).await?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// 发送一条强类型 Task 上报。
    pub async fn send_task_report(&mut self, report: TaskReport) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::TaskReport(Box::new(report))),
        })
        .await
    }

    /// 发送一条 Server Job 控制命令。
    pub async fn send_job_command(&mut self, command: JobCommand) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::JobCommand(command)),
        })
        .await
    }

    /// 发送一条 Agent Job 控制结果。
    pub async fn send_job_command_result(
        &mut self,
        result: JobCommandResult,
    ) -> Result<(), TransportError> {
        self.send(SecureMessage {
            body: Some(secure_message::Body::JobCommandResult(Box::new(result))),
        })
        .await
    }

    /// 接收下一条强类型业务事件；心跳和 rekey 控制帧仍在内部处理。
    pub async fn receive_event(&mut self) -> Result<Option<SessionEvent>, TransportError> {
        self.receive()
            .await?
            .map(SessionEvent::try_from)
            .transpose()
    }

    /// 接收下一条业务消息；Ping/Pong 和 responder rekey 在内部处理。
    ///
    /// 控制消息不会返回给普通业务循环；方法会持续读取，直到得到业务消息、流关闭或错误。
    pub async fn receive(&mut self) -> Result<Option<SecureMessage>, TransportError> {
        loop {
            if let Some(message) = self.buffered_messages.pop_front() {
                return Ok(Some(message));
            }
            // 只有 initiator 发起同步 rekey，防止双方同时切 key 造成方向失步。
            if self.initiator && self.should_rekey() {
                self.request_rekey().await?;
                // request_rekey 可能在 Ack 前收到并缓存业务帧，回到循环顶部优先交付它们。
                continue;
            }
            if self.heartbeat_expired() {
                return Err(TransportError::HeartbeatTimeout);
            }
            let frame =
                match tokio::time::timeout(self.heartbeat.interval, self.inbound.message()).await {
                    Ok(result) => result?,
                    Err(_) => {
                        let nonce = self.next_ping_nonce;
                        self.next_ping_nonce = self.next_ping_nonce.wrapping_add(1);
                        self.ping(nonce).await?;
                        continue;
                    }
                };
            let Some(frame) = frame else {
                return Ok(None);
            };
            let message = self.secure.decrypt(frame)?;
            self.last_received = Instant::now();
            match message.body {
                Some(secure_message::Body::SessionControl(control)) => {
                    match self.handle_control(control).await {
                        Err(TransportError::RekeyRequired) if self.initiator => {
                            self.request_rekey().await?;
                        }
                        result => result?,
                    }
                }
                _ => return Ok(Some(message)),
            }
        }
    }

    /// 手动发送加密 Ping；通常由 `receive()` 的空闲超时分支自动调用。
    pub async fn ping(&mut self, nonce: u64) -> Result<(), TransportError> {
        self.send(control(session_control::Body::Ping(Ping { nonce })))
            .await
    }

    /// Server 通知 Agent 发起同步 rekey，但本方法本身不切换任何 cipher key。
    ///
    /// 只有 responder 可以调用；返回值是期望的下一 generation。
    pub async fn require_rekey(&mut self) -> Result<u64, TransportError> {
        if self.initiator {
            return Err(TransportError::Protocol(
                "only the Noise responder may require rekey".to_owned(),
            ));
        }
        let generation = self.secure.generation().saturating_add(1);
        self.send(control(session_control::Body::RekeyRequired(
            RekeyRequired {
                next_generation: generation,
            },
        )))
        .await?;
        Ok(generation)
    }

    /// Agent 发送已准备好的长期静态公钥轮换请求。
    ///
    /// 调用前必须先持久化 `AgentKeySet::snapshot()` 中的 pending 私钥。
    pub async fn request_agent_key_rotation(
        &mut self,
        prepared: &AgentRotationPrepared,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::AgentRequest(
            prepared.request.clone(),
        )))
        .await
    }

    /// Server 确认已把 Agent pending 公钥写入授权存储。
    pub async fn accept_agent_key_rotation(
        &mut self,
        rotation_id: RotationId,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::AgentAccepted(
            AgentKeyRotationAccepted {
                rotation_id: rotation_id.as_bytes().to_vec(),
            },
        )))
        .await
    }

    /// Server 发送已准备好的下一把长期静态公钥公告。
    ///
    /// 调用前必须先持久化含 next 私钥的 `ServerKeyRingSnapshot`。
    pub async fn announce_server_key(
        &mut self,
        prepared: &ServerRotationPrepared,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::ServerAnnouncement(
            prepared.announcement.clone(),
        )))
        .await
    }

    /// Agent 确认已经校验并持久化 Server pending 公钥。
    pub async fn acknowledge_server_key(
        &mut self,
        rotation_id: RotationId,
        key_id: crate::noise::KeyId,
    ) -> Result<(), TransportError> {
        self.send(rotation(key_rotation_message::Body::ServerAcknowledgement(
            ServerKeyAcknowledgement {
                rotation_id: rotation_id.as_bytes().to_vec(),
                key_id: key_id.as_bytes().to_vec(),
            },
        )))
        .await
    }

    /// Agent/initiator 发起同步 rekey。Server 通过 receive() 自动响应。
    ///
    /// 方法会一直读取到匹配 generation 的 Ack；等待期间出现普通业务消息视为协议顺序错误。
    /// Agent 发起当前连接的对称 cipher rekey，并等待 Server 的加密 Ack。
    ///
    /// Ack 仍由旧 key 加密；验证成功后双方才按固定方向顺序切换密钥。
    pub async fn request_rekey(&mut self) -> Result<u64, TransportError> {
        // responder 只能调用 require_rekey，不能直接进入 initiator 状态机。
        if !self.initiator {
            return Err(TransportError::Protocol(
                "only the Noise initiator may request rekey".to_owned(),
            ));
        }
        let generation = self.secure.generation().saturating_add(1);
        self.send(control(session_control::Body::RekeyRequest(RekeyRequest {
            generation,
        })))
        .await?;
        loop {
            let frame = self
                .inbound
                .message()
                .await?
                .ok_or(TransportError::Closed)?;
            let message = self.secure.decrypt(frame)?;
            match message.body {
                Some(secure_message::Body::SessionControl(SessionControl {
                    body: Some(session_control::Body::RekeyAck(ack)),
                })) if ack.generation == generation => {
                    self.secure.rekey_incoming();
                    self.secure.rekey_outgoing();
                    self.secure.finish_rekey(generation);
                    self.established_at = Instant::now();
                    self.last_received = Instant::now();
                    return Ok(generation);
                }
                Some(secure_message::Body::SessionControl(control)) => match control.body {
                    Some(session_control::Body::RekeyRequired(required))
                        if required.next_generation == generation => {}
                    _ => self.handle_control(control).await?,
                },
                _ => self.buffered_messages.push_back(message),
            }
        }
    }

    /// 返回双方已经完成同步 rekey 的 generation 编号。
    pub fn generation(&self) -> u64 {
        self.secure.generation()
    }

    /// 返回当前 generation 内已成功加密和解密的帧数总和。
    pub fn encrypted_frames(&self) -> u64 {
        self.secure.encrypted_frames()
    }

    /// 处理一条已经解密的 `SessionControl`。
    async fn handle_control(&mut self, control: SessionControl) -> Result<(), TransportError> {
        match control.body {
            Some(session_control::Body::Ping(ping)) => {
                // Pong 原样返回关联 nonce，让发送方确认收到的是对应探测响应。
                self.send(control_message(session_control::Body::Pong(Pong {
                    nonce: ping.nonce,
                })))
                .await
            }
            Some(session_control::Body::Pong(_)) => Ok(()),
            Some(session_control::Body::RekeyRequest(request)) if !self.initiator => {
                // responder 只接受严格的下一代，拒绝重复、跳代和回放。
                if request.generation != self.secure.generation().saturating_add(1) {
                    return Err(TransportError::Protocol(
                        "unexpected rekey generation".to_owned(),
                    ));
                }
                self.secure.rekey_incoming();
                self.send(control_message(session_control::Body::RekeyAck(RekeyAck {
                    generation: request.generation,
                })))
                .await?;
                self.secure.rekey_outgoing();
                self.secure.finish_rekey(request.generation);
                self.established_at = Instant::now();
                Ok(())
            }
            Some(session_control::Body::RekeyRequired(_)) if self.initiator => {
                // 把控制权交给 Agent 主循环，由它显式调用 request_rekey。
                Err(TransportError::RekeyRequired)
            }
            Some(session_control::Body::RekeyAck(_)) => Err(TransportError::Protocol(
                "unexpected rekey acknowledgement".to_owned(),
            )),
            _ => Err(TransportError::Protocol(
                "invalid session control message".to_owned(),
            )),
        }
    }

    /// 运行可选 Driver 的单所有者事件循环。
    pub(crate) async fn run_driver(
        mut self,
        mut commands: mpsc::Receiver<DriverCommand>,
        events: mpsc::Sender<Result<SessionEvent, TransportError>>,
    ) {
        // interval 的第一次 tick 会立即完成，先消费它，避免 Driver 启动后无条件发送 Ping。
        let tick_period = self.heartbeat.interval.max(Duration::from_millis(1));
        let mut maintenance = tokio::time::interval(tick_period);
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        maintenance.tick().await;

        loop {
            tokio::select! {
                // 入站优先，尽快处理 Ping、rekey 等控制帧，降低对端等待时间。
                biased;
                inbound = self.inbound.message() => {
                    let frame = match inbound {
                        Ok(Some(frame)) => frame,
                        Ok(None) => {
                            let _ = events.send(Err(TransportError::Closed)).await;
                            return;
                        }
                        Err(error) => {
                            let _ = events.send(Err(TransportError::Status(error))).await;
                            return;
                        }
                    };
                    match self.process_driver_frame(frame).await {
                        Ok(Some(event)) => {
                            if events.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let _ = events.send(Err(error)).await;
                            return;
                        }
                    }
                    if self.flush_buffered_events(&events).await.is_err() {
                        return;
                    }
                }
                command = commands.recv() => {
                    match command {
                        Some(DriverCommand::Send { message, completed }) => {
                            match self.send(message).await {
                                Ok(()) => {
                                    let _ = completed.send(Ok(()));
                                }
                                Err(error) => {
                                    let _ = completed.send(Err(error));
                                    return;
                                }
                            }
                        }
                        Some(DriverCommand::Shutdown { completed }) => {
                            let _ = completed.send(());
                            return;
                        }
                        Some(DriverCommand::Ping { nonce, completed }) => {
                            let failed = match self.ping(nonce).await {
                                Ok(()) => {
                                    let _ = completed.send(Ok(()));
                                    false
                                }
                                Err(error) => {
                                    let _ = completed.send(Err(error));
                                    true
                                }
                            };
                            if failed {
                                return;
                            }
                        }
                        Some(DriverCommand::RequestRekey { completed }) => {
                            let failed = match self.request_rekey().await {
                                Ok(generation) => {
                                    let _ = completed.send(Ok(generation));
                                    false
                                }
                                Err(error) => {
                                    let _ = completed.send(Err(error));
                                    true
                                }
                            };
                            if failed {
                                return;
                            }
                            if self.flush_buffered_events(&events).await.is_err() {
                                return;
                            }
                        }
                        Some(DriverCommand::RequireRekey { completed }) => {
                            let failed = match self.require_rekey().await {
                                Ok(generation) => {
                                    let _ = completed.send(Ok(generation));
                                    false
                                }
                                Err(error) => {
                                    let _ = completed.send(Err(error));
                                    true
                                }
                            };
                            if failed {
                                return;
                            }
                        }
                        None => return,
                    }
                }
                _ = maintenance.tick() => {
                    if let Err(error) = self.perform_maintenance().await {
                        let _ = events.send(Err(error)).await;
                        return;
                    }
                    if self.flush_buffered_events(&events).await.is_err() {
                        return;
                    }
                }
            }
        }
    }

    /// 解密 Driver 收到的一帧，并在返回业务事件前完成所有内部控制动作。
    async fn process_driver_frame(
        &mut self,
        frame: ProtocolFrame,
    ) -> Result<Option<SessionEvent>, TransportError> {
        let message = self.secure.decrypt(frame)?;
        self.last_received = Instant::now();
        match message.body {
            Some(secure_message::Body::SessionControl(control)) => {
                match self.handle_control(control).await {
                    Err(TransportError::RekeyRequired) if self.initiator => {
                        self.request_rekey().await?;
                    }
                    result => result?,
                }
                Ok(None)
            }
            _ => SessionEvent::try_from(message).map(Some),
        }
    }

    /// 把 rekey 等待期间缓存的业务消息按原始接收顺序交给 Driver 事件队列。
    async fn flush_buffered_events(
        &mut self,
        events: &mpsc::Sender<Result<SessionEvent, TransportError>>,
    ) -> Result<(), ()> {
        while let Some(message) = self.buffered_messages.pop_front() {
            let event = SessionEvent::try_from(message);
            events.send(event).await.map_err(|_| ())?;
        }
        Ok(())
    }

    async fn send_frame(&self, frame: ProtocolFrame) -> Result<(), TransportError> {
        match &self.outbound {
            Outbound::Client(sender) => {
                sender.send(frame).await.map_err(|_| TransportError::Closed)
            }
            Outbound::Server(sender) => sender
                .send(Ok(frame))
                .await
                .map_err(|_| TransportError::Closed),
        }
    }
}

/// 构造一条包含 `SessionControl` 的加密业务消息。
fn control(body: session_control::Body) -> SecureMessage {
    control_message(body)
}

fn control_message(body: session_control::Body) -> SecureMessage {
    SecureMessage {
        body: Some(secure_message::Body::SessionControl(SessionControl {
            body: Some(body),
        })),
    }
}

fn rotation(body: key_rotation_message::Body) -> SecureMessage {
    SecureMessage {
        body: Some(secure_message::Body::KeyRotation(KeyRotationMessage {
            body: Some(body),
        })),
    }
}

impl From<NoiseError> for TransportError {
    /// 允许加解密和控制状态机使用 `?` 把核心错误提升为传输层错误。
    fn from(error: NoiseError) -> Self {
        Self::Noise(error)
    }
}
