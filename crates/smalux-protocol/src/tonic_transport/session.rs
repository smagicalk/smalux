//! 已建立 Tonic + Noise 长流的业务收发、心跳、rekey 和换钥控制消息。
//!
//! `TonicNoiseSession` 把请求 sender、响应 stream 和 `SecureSession` 绑定在同一可变对象中，
//! 强制所有 nonce 相关操作串行执行。

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::{
    agent::v1::{
        AgentKeyRotationAccepted, KeyRotationMessage, Ping, Pong, ProtocolFrame, RekeyAck,
        RekeyRequest, RekeyRequired, SecureMessage, ServerKeyAcknowledgement, SessionControl,
        key_rotation_message, secure_message, session_control,
    },
    noise::{AgentRotationPrepared, NoiseError, RotationId, SecureSession, ServerRotationPrepared},
};

use super::TransportError;

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

    /// 接收下一条业务消息；Ping/Pong 和 responder rekey 在内部处理。
    ///
    /// 控制消息不会返回给普通业务循环；方法会持续读取，直到得到业务消息、流关闭或错误。
    pub async fn receive(&mut self) -> Result<Option<SecureMessage>, TransportError> {
        loop {
            // 只有 initiator 发起同步 rekey，防止双方同时切 key 造成方向失步。
            if self.initiator && self.should_rekey() {
                self.request_rekey().await?;
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
                    self.handle_control(control).await?;
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
                Some(secure_message::Body::SessionControl(control)) => {
                    self.handle_control(control).await?;
                }
                _ => {
                    return Err(TransportError::Protocol(
                        "business message arrived while rekey was pending".to_owned(),
                    ));
                }
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
