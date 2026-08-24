//! Agent 后续 IK 会话的业务授权阶段。
//!
//! IK 只证明对端拥有对应的 Noise 静态私钥，不等于数据库业务授权。这里根据认证出的
//! Agent 公钥查询注册中心，并区分“已吊销”“未知/未激活”和“数据库故障”三类结果。
//! 对端只收到安全的协议错误文本，详细原因只进入 Server 日志。

use smalux_protocol::{
    agent::v1::{SecureError, SecureErrorCode},
    tonic_transport::{ServerAuthentication, TonicNoiseSession, TransportError},
};

use crate::service::agent::agent_registry::AgentAuthorizationError;

use super::super::AgentTransportService;

impl AgentTransportService {
    /// 校验 IK 会话对应的 Agent 授权，并返回可以进入业务循环的会话。
    ///
    /// `Some` 表示授权成功；`None` 表示拒绝消息已发送，当前连接应正常结束。只有
    /// 真正的传输错误才通过 `Err` 返回给外层会话任务。
    pub(super) async fn handle_authorization_session(
        &self,
        session_id: u64,
        authentication: ServerAuthentication,
    ) -> Result<Option<(String, TonicNoiseSession)>, TransportError> {
        tracing::info!(
            session_id,
            "Agent IK handshake completed; entering authorization"
        );
        let peer_public_key = authentication.peer_public_key();

        // 授权查询只使用握手认证后的公钥，不信任 Agent 自己在业务消息中声明的 ID。
        let authorized = match self
            .state
            .agent_registry
            .authorize_agent(peer_public_key)
            .await
        {
            Ok(authorized) => authorized,
            Err(AgentAuthorizationError::Revoked) => {
                tracing::warn!(session_id, "Agent is revoked");
                return send_authorization_rejection(
                    authentication,
                    SecureError {
                        code: SecureErrorCode::AgentNotAuthorized as i32,
                        message: "agent is not authorized".to_owned(),
                    },
                )
                .await;
            }
            Err(AgentAuthorizationError::Unauthorized) => {
                tracing::warn!(session_id, "Agent authorization was rejected");
                return send_authorization_rejection(
                    authentication,
                    SecureError {
                        code: SecureErrorCode::AgentNotAuthorized as i32,
                        message: "agent is not authorized".to_owned(),
                    },
                )
                .await;
            }
            Err(AgentAuthorizationError::Database(error)) => {
                tracing::warn!(
                    session_id,
                    error = %error,
                    "Agent authorization lookup failed"
                );
                return send_authorization_rejection(
                    authentication,
                    SecureError {
                        code: SecureErrorCode::Internal as i32,
                        message: "authorization service is unavailable".to_owned(),
                    },
                )
                .await;
            }
        };

        tracing::info!(
            session_id,
            agent_key_id = ?authorized.public_key.key_id(),
            agent_id_len = authorized.agent_id.len(),
            "Agent authorization succeeded"
        );
        Ok(Some((authorized.agent_id, authentication.authorize())))
    }
}

/// 发送 IK 授权失败的加密错误，并结束当前待授权会话。
async fn send_authorization_rejection(
    authentication: ServerAuthentication,
    error: SecureError,
) -> Result<Option<(String, TonicNoiseSession)>, TransportError> {
    authentication.send_rejection(error).await?;
    Ok(None)
}
