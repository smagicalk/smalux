//! Noise 握手完成后的 Agent 注册、授权与加密业务会话策略。

use std::time::Duration;

use smalux_protocol::{
    agent::v1::{
        EchoResponse, Messages, MessagesResponse, SecureError, SecureErrorCode, SecureMessage,
        messages, messages_request, messages_response, secure_message,
    },
    tonic_transport::{IncomingSession, TonicNoiseSession, TransportError},
};

use crate::service::agent::agent_registrar::AgentAuthorizationError;

use super::{AgentServer, registration_prepare_error};

impl AgentServer {
    /// 处理已经完成 Noise 握手的会话。
    ///
    /// XXpsk3 必须先完成四阶段注册状态机；IK 必须先检查吊销状态和业务授权。只有
    /// 这些检查成功后，才把会话交给加密业务循环。注册资料错误会映射为对应的
    /// 加密协议错误；数据库故障只返回 `Internal`，不会泄露存储细节。
    pub(super) async fn handle_established_session(
        &self,
        session_id: u64,
        incoming: IncomingSession,
    ) -> Result<(), TransportError> {
        let (agent_id, mut session) = match incoming {
            IncomingSession::Registration(mut registration) => {
                let registration_permit = match self.state.try_acquire_registration() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!(session_id, "Agent registration capacity reached");
                        registration
                            .reject(SecureError {
                                code: SecureErrorCode::ResourceExhausted as i32,
                                message: "registration capacity is temporarily exhausted"
                                    .to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };
                let _registration_permit = registration_permit;
                tracing::info!(
                    session_id,
                    "Agent XXpsk3 handshake completed; entering registration"
                );
                let peer_public_key = registration.peer_public_key();
                let request = match registration.receive_request().await {
                    Ok(request) => request,
                    Err(error) => {
                        tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent registration request is invalid"
                        );
                        registration
                            .reject(SecureError {
                                code: SecureErrorCode::InvalidMessage as i32,
                                message: "registration request is invalid".to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };

                let pending = match self
                    .state
                    .agent_registrar
                    .prepare_registration(
                        registration.registration_token_id(),
                        &request.token,
                        peer_public_key,
                        &request.agent_name,
                    )
                    .await
                {
                    Ok(pending) => pending,
                    Err(error) => {
                        let (code, message) = registration_prepare_error(&error);
                        tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent registration preparation was rejected"
                        );
                        registration
                            .reject(SecureError {
                                code: code as i32,
                                message: message.to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };

                tracing::debug!(
                    session_id,
                    registration_id = ?pending.registration_id,
                    agent_id_len = pending.agent_id.len(),
                    "sending encrypted registration preparation"
                );
                registration
                    .prepare(pending.registration_id, pending.agent_id.clone())
                    .await?;
                if let Err(error) = registration
                    .wait_for_commit(pending.registration_id, Duration::from_secs(10))
                    .await
                {
                    tracing::warn!(
                        session_id,
                        error = %error,
                        "Agent did not complete registration commit"
                    );
                    registration
                        .reject(SecureError {
                            code: SecureErrorCode::InvalidMessage as i32,
                            message: "registration commit was not completed".to_owned(),
                        })
                        .await?;
                    return Ok(());
                }

                if let Err(error) = self
                    .state
                    .agent_registrar
                    .commit_registration(&pending)
                    .await
                {
                    tracing::warn!(
                        session_id,
                        error = %error,
                        "Agent registration commit failed"
                    );
                    registration
                        .reject(SecureError {
                            code: SecureErrorCode::Internal as i32,
                            message: "registration service is unavailable".to_owned(),
                        })
                        .await?;
                    return Ok(());
                }

                let session = registration.complete(pending.registration_id).await?;
                tracing::info!(
                    session_id,
                    agent_id_len = pending.agent_id.len(),
                    "Agent registration committed"
                );
                (pending.agent_id, session)
            }
            IncomingSession::Authentication(authentication) => {
                tracing::info!(
                    session_id,
                    "Agent IK handshake completed; entering authorization"
                );
                let peer_public_key = authentication.peer_public_key();
                let authorized = match self
                    .state
                    .agent_registrar
                    .authorize_agent(peer_public_key)
                    .await
                {
                    Ok(authorized) => authorized,
                    Err(AgentAuthorizationError::Revoked) => {
                        tracing::warn!(session_id, "Agent is revoked");
                        authentication
                            .reject(SecureError {
                                code: SecureErrorCode::AgentNotAuthorized as i32,
                                message: "agent is not authorized".to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                    Err(AgentAuthorizationError::Unauthorized) => {
                        tracing::warn!(session_id, "Agent authorization was rejected");
                        authentication
                            .reject(SecureError {
                                code: SecureErrorCode::AgentNotAuthorized as i32,
                                message: "agent is not authorized".to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                    Err(AgentAuthorizationError::Database(error)) => {
                        tracing::warn!(
                            session_id,
                            error = %error,
                            "Agent authorization lookup failed"
                        );
                        authentication
                            .reject(SecureError {
                                code: SecureErrorCode::Internal as i32,
                                message: "authorization service is unavailable".to_owned(),
                            })
                            .await?;
                        return Ok(());
                    }
                };
                tracing::info!(
                    session_id,
                    agent_key_id = ?authorized.public_key.key_id(),
                    agent_id_len = authorized.agent_id.len(),
                    "Agent authorization succeeded"
                );
                (authorized.agent_id, authentication.authorize())
            }
        };

        self.handle_business_messages(session_id, &agent_id, &mut session)
            .await
    }

    /// 处理 Noise transport mode 内的业务消息。
    ///
    /// `TonicNoiseSession::receive` 会自动处理心跳和对称密钥 rekey；这里仅负责业务
    /// `MessagesRequest` 的回显。未来接入 Job/Task 时，应在这个边界分派到对应服务。
    async fn handle_business_messages(
        &self,
        session_id: u64,
        agent_id: &str,
        session: &mut TonicNoiseSession,
    ) -> Result<(), TransportError> {
        while let Some(message) = session.receive().await? {
            let Some(secure_message::Body::Messages(Messages {
                body: Some(messages::Body::Request(request)),
            })) = message.body
            else {
                tracing::warn!(
                    session_id,
                    agent_id_len = agent_id.len(),
                    "Agent sent an unexpected encrypted business message"
                );
                session
                    .send(SecureMessage {
                        body: Some(secure_message::Body::Error(SecureError {
                            code: SecureErrorCode::InvalidMessage as i32,
                            message: "business session requires MessagesRequest".to_owned(),
                        })),
                    })
                    .await?;
                continue;
            };

            tracing::debug!(
                session_id,
                agent_id_len = agent_id.len(),
                sequence = request.sequence,
                "received encrypted Agent business request"
            );
            let response_payload = request.payload.map(|payload| match payload {
                messages_request::Payload::BytesPayload(value) => {
                    messages_response::Payload::BytesPayload(value)
                }
                messages_request::Payload::StringPayload(value) => {
                    messages_response::Payload::StringPayload(value)
                }
                messages_request::Payload::EchoRequest(value) => {
                    messages_response::Payload::EchoResponse(EchoResponse {
                        payload: value.payload,
                    })
                }
            });
            session
                .send(SecureMessage {
                    body: Some(secure_message::Body::Messages(Messages {
                        body: Some(messages::Body::Response(MessagesResponse {
                            acknowledged_sequence: request.sequence,
                            payload: response_payload,
                        })),
                    })),
                })
                .await?;
            tracing::trace!(
                session_id,
                sequence = request.sequence,
                "sent encrypted Agent business response"
            );
        }
        tracing::info!(session_id, "Agent encrypted session disconnected");
        Ok(())
    }
}
