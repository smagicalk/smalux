//! Tonic + Noise 会话的无状态策略、统计快照和业务事件分类。

use std::time::Duration;

use tracing::warn;

use crate::agent::v1::{
    AgentCapabilitySync, AgentJobPolicySync, AgentPluginSync, DiagnosticMessage, JobCommand,
    JobCommandResult, KeyRotationMessage, RegistrationMessage, SecureMessage, TaskReport,
    secure_message,
};

use super::super::TransportError;

/// 长流存活检测策略。
#[derive(Clone, Copy, Debug)]
pub struct HeartbeatPolicy {
    /// 没有入站帧达到该时长时，`receive()` 主动发送加密 Ping。
    pub interval: Duration,
    /// 距离上次成功接收超过该时长时判定会话失联。
    pub timeout: Duration,
}

/// 一次成功匹配的 Ping/Pong 样本。
///
/// `rtt` 使用发送端和接收端同一进程内的单调时钟计算，不受两台机器系统时钟偏差影响。
/// Unix 微秒字段只用于日志、诊断和跨机器对照，不应被用来计算单向延迟。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatSample {
    /// Ping/Pong 的关联值。
    pub nonce: u64,
    /// 本地从发送 Ping 到收到匹配 Pong 的往返时间。
    pub rtt: Duration,
    /// Ping 发送方记录的 Unix 微秒时间。
    pub sent_at_unix_micros: u64,
    /// 对端收到 Ping 时记录的 Unix 微秒时间。
    pub responder_received_at_unix_micros: u64,
    /// 对端发送 Pong 前记录的 Unix 微秒时间。
    pub responder_sent_at_unix_micros: u64,
    /// 本地收到 Pong 时记录的 Unix 微秒时间。
    pub received_at_unix_micros: u64,
}

/// 当前会话累计的链路心跳统计。
///
/// 统计只在收到与本地 pending nonce 匹配的 Pong 后更新。`lost_count` 在心跳超时关闭
/// 会话时记录尚未收到响应的探测数量；业务消息本身不会清零连续心跳失败次数。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeartbeatStats {
    /// 已成功写入 Tonic outbound channel 的 Ping 数量。
    pub sent_count: u64,
    /// 收到并匹配 nonce 的 Pong 数量。
    pub received_count: u64,
    /// 会话因心跳超时而放弃的 pending Ping 数量。
    pub lost_count: u64,
    /// 最近一次成功 Pong 后清零的连续失败次数。
    pub consecutive_failures: u64,
    /// 最近一次成功探测的样本。
    pub last_sample: Option<HeartbeatSample>,
    /// 已成功探测样本中的最小 RTT。
    pub min_rtt: Option<Duration>,
    /// 已成功探测样本中的最大 RTT。
    pub max_rtt: Option<Duration>,
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

/// 当前连接的对称 cipher key 更新策略。
#[derive(Clone, Copy, Debug)]
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
    /// 对端近期没有发送入站数据，可以发送加密 Ping 验证反向链路。
    pub ping_due: bool,
    /// initiator 已达到自动 rekey 的时间或帧数阈值。
    pub rekey_due: bool,
}

/// 一次 `TonicNoiseSession::perform_maintenance` 实际完成的动作。
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
    /// 示例和链路诊断请求或响应消息。
    Diagnostic(DiagnosticMessage),
    /// 长期静态身份密钥轮换消息。
    KeyRotation(KeyRotationMessage),
    /// Server 下发的 Job 控制命令。
    JobCommand(JobCommand),
    /// Agent 返回的 Job 控制结果。
    JobCommandResult(Box<JobCommandResult>),
    /// Agent 上报的强类型 Task 执行结果。
    TaskReport(Box<TaskReport>),
    /// Agent 本地远程 Job 策略的查询、快照或确认。
    AgentJobPolicy(AgentJobPolicySync),
    /// Agent 可执行 Task 与 Probe 协议的查询或完整快照。
    AgentCapability(AgentCapabilitySync),
    /// Plus 插件清单、运行时快照或应用确认。
    AgentPlugin(AgentPluginSync),
}

impl TryFrom<SecureMessage> for SessionEvent {
    type Error = TransportError;

    /// 把底层 Protobuf envelope 分类为业务事件；控制帧不允许从该入口泄漏。
    fn try_from(message: SecureMessage) -> Result<Self, Self::Error> {
        match message.body {
            Some(secure_message::Body::RegistrationMessage(value)) => Ok(Self::Registration(value)),
            Some(secure_message::Body::Diagnostic(value)) => Ok(Self::Diagnostic(value)),
            Some(secure_message::Body::KeyRotation(value)) => Ok(Self::KeyRotation(value)),
            Some(secure_message::Body::JobCommand(value)) => Ok(Self::JobCommand(value)),
            Some(secure_message::Body::JobCommandResult(value)) => {
                Ok(Self::JobCommandResult(value))
            }
            Some(secure_message::Body::TaskReport(value)) => Ok(Self::TaskReport(value)),
            Some(secure_message::Body::AgentJobPolicy(value)) => Ok(Self::AgentJobPolicy(value)),
            Some(secure_message::Body::AgentCapability(value)) => Ok(Self::AgentCapability(value)),
            Some(secure_message::Body::AgentPlugin(value)) => Ok(Self::AgentPlugin(value)),
            Some(secure_message::Body::Error(error)) => {
                let code = crate::agent::v1::SecureErrorCode::try_from(error.code)
                    .unwrap_or(crate::agent::v1::SecureErrorCode::Unspecified);
                warn!(?code, "received encrypted remote secure error");
                Err(TransportError::RemoteSecure(code, error.message))
            }
            Some(secure_message::Body::SessionControl(_)) => {
                warn!("session control escaped the transport state machine");
                Err(TransportError::Protocol(
                    "session control escaped the transport state machine".to_owned(),
                ))
            }
            None => {
                warn!("received SecureMessage without a body");
                Err(TransportError::Protocol(
                    "SecureMessage body is required".to_owned(),
                ))
            }
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

#[cfg(test)]
mod tests {
    use crate::agent::v1::{
        AgentCapabilityQuery, AgentCapabilitySync, SecureMessage, agent_capability_sync,
        secure_message,
    };

    use super::SessionEvent;

    #[test]
    fn capability_envelope_is_exposed_as_a_typed_session_event() {
        let message = AgentCapabilitySync {
            body: Some(agent_capability_sync::Body::Query(AgentCapabilityQuery {})),
        };
        let event = SessionEvent::try_from(SecureMessage {
            body: Some(secure_message::Body::AgentCapability(message)),
        })
        .unwrap();

        assert!(matches!(event, SessionEvent::AgentCapability(_)));
    }
}
