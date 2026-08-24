//! Agent 首次注册阶段。
//!
//! `XXpsk3` 完成后，密码学身份已经成立，但数据库中的 Agent 仍然处于未激活状态。
//! 本模块负责把协议消息和注册中心的持久化步骤按固定顺序串起来：
//!
//! ```text
//! 接收 RegistrationRequest
//!   -> prepare_registration（创建或恢复 pending 事务）
//!   -> 发送 RegistrationPrepared
//!   -> 接收并校验 RegistrationCommit
//!   -> commit_registration（激活 Agent、消费 Token）
//!   -> 发送 RegistrationCommitted
//! ```
//!
//! 任意业务拒绝都会发送一个粗粒度的加密 `SecureError`，然后返回 `Ok(None)`，表示当前
//! 会话已经被正常拒绝；数据库或底层传输错误则继续以 `TransportError` 返回给上层日志。

use std::time::Duration;

use smalux_protocol::{
    agent::v1::{SecureError, SecureErrorCode},
    tonic_transport::{ServerRegistration, TonicNoiseSession, TransportError},
};

use crate::service::agent::agent_registry::PendingRegistration;

use super::super::{AgentTransportService, map_registration_prepare_error};

/// Agent 注册 commit 等待上限。
///
/// 该超时只覆盖“Server 已发送 `RegistrationPrepared` 后等待 Agent 确认”的阶段，
/// 不会限制注册完成后的长期业务流。
const REGISTRATION_COMMIT_TIMEOUT: Duration = Duration::from_secs(10);

impl AgentTransportService {
    /// 执行一次完整的 XXpsk3 注册阶段。
    ///
    /// 返回 `Some((agent_id, session))` 表示数据库已经激活 Agent，可以进入业务循环；
    /// 返回 `None` 表示拒绝消息已经成功发送，当前连接应结束但不应被记录为 Server 内部故障。
    pub(super) async fn handle_registration_session(
        &self,
        session_id: u64,
        mut registration: ServerRegistration,
    ) -> Result<Option<(String, TonicNoiseSession)>, TransportError> {
        // 注册会访问 Token、注册事务和 Agent 表，使用独立 semaphore 限制数据库压力。
        // permit 一直持有到本方法返回，包含等待 Agent commit 的网络阶段。
        let Ok(_registration_permit) = self.state.try_acquire_registration() else {
            tracing::warn!(session_id, "Agent registration capacity reached");
            return send_registration_rejection(
                registration,
                SecureError {
                    code: SecureErrorCode::ResourceExhausted as i32,
                    message: "registration capacity is temporarily exhausted".to_owned(),
                },
            )
            .await;
        };

        tracing::info!(
            session_id,
            "Agent XXpsk3 handshake completed; entering registration"
        );
        let pending = match self
            .receive_and_prepare_registration(session_id, &mut registration)
            .await
        {
            Ok(pending) => pending,
            Err(error) => return send_registration_rejection(registration, error).await,
        };

        // 只有 pending 已经落库后才发送 Prepared；Agent 收到该消息后才可以持久化本地
        // 身份材料并发送 Commit，因此不会出现 Server 已通知但没有对应事务的情况。
        tracing::debug!(
            session_id,
            registration_id = ?pending.registration_id,
            agent_id_len = pending.agent_id.len(),
            "sending encrypted registration preparation"
        );
        registration
            .send_registration_prepared(pending.registration_id, pending.agent_id.clone())
            .await?;

        if let Err(error) = self
            .receive_and_commit_registration(session_id, &mut registration, &pending)
            .await
        {
            return send_registration_rejection(registration, error).await;
        }

        // 数据库 commit 成功后才发送最终确认；该方法消费 registration，并返回可以继续
        // 处理策略、Job 和上报消息的长期会话对象。
        let session = registration
            .send_registration_committed(pending.registration_id)
            .await?;
        tracing::info!(
            session_id,
            agent_id_len = pending.agent_id.len(),
            "Agent registration committed"
        );
        Ok(Some((pending.agent_id, session)))
    }

    /// 读取密文注册资料，并在数据库中创建或恢复 pending 事务。
    async fn receive_and_prepare_registration(
        &self,
        session_id: u64,
        registration: &mut ServerRegistration,
    ) -> Result<PendingRegistration, SecureError> {
        // 消息结构校验发生在数据库写入前，避免无效请求创建半成品事务。
        let request = registration
            .receive_registration_request()
            .await
            .map_err(|error| {
                tracing::warn!(session_id, error = %error, "Agent registration request is invalid");
                SecureError {
                    code: SecureErrorCode::InvalidMessage as i32,
                    message: "registration request is invalid".to_owned(),
                }
            })?;
        let peer_public_key = registration.peer_public_key();
        self.state
            .agent_registry
            .prepare_registration(
                registration.registration_token_id(),
                &request.token,
                peer_public_key,
            )
            .await
            .map_err(|error| {
                let (code, message) = map_registration_prepare_error(&error);
                tracing::warn!(
                    session_id,
                    error = %error,
                    "Agent registration preparation was rejected"
                );
                SecureError {
                    code: code as i32,
                    message: message.to_owned(),
                }
            })
    }

    /// 等待 Agent 保存本地身份，然后原子激活 Agent 并消费 Token。
    async fn receive_and_commit_registration(
        &self,
        session_id: u64,
        registration: &mut ServerRegistration,
        pending: &PendingRegistration,
    ) -> Result<(), SecureError> {
        registration
            .receive_registration_commit(pending.registration_id, REGISTRATION_COMMIT_TIMEOUT)
            .await
            .map_err(|error| {
                tracing::warn!(session_id, error = %error, "Agent did not complete registration commit");
                SecureError {
                    code: SecureErrorCode::InvalidMessage as i32,
                    message: "registration commit was not completed".to_owned(),
                }
            })?;
        self.state
            .agent_registry
            .commit_registration(pending)
            .await
            .map_err(|error| {
                tracing::warn!(session_id, error = %error, "Agent registration commit failed");
                SecureError {
                    code: SecureErrorCode::Internal as i32,
                    message: "registration service is unavailable".to_owned(),
                }
            })
    }
}

/// 发送注册阶段的加密拒绝消息，并把“已正常拒绝”转换为 `Ok(None)`。
///
/// `ServerRegistration::send_rejection` 会消费会话对象，因此调用方不能在发送后继续尝试
/// 读取或发送注册消息；这种所有权约束可以防止拒绝后误进入下一阶段。
async fn send_registration_rejection(
    registration: ServerRegistration,
    error: SecureError,
) -> Result<Option<(String, TonicNoiseSession)>, TransportError> {
    registration.send_rejection(error).await?;
    Ok(None)
}
