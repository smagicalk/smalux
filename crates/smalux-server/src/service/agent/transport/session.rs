//! Noise 握手完成后的 Agent 会话编排。
//!
//! 这个文件只负责根据握手结果选择后续阶段，并把成功建立的会话交给业务消息循环。
//! 具体实现按协议阶段拆到相邻模块：
//!
//! - [`registration`]：首次 `XXpsk3` 注册、prepare/commit 和 Token 消费前后的消息交互；
//! - [`authorization`]：后续 `IK` 会话的 Agent 授权与吊销处理；
//! - [`business`]：授权完成后同步本地策略并处理 Job 业务消息。
//!
//! 这样可以让每个模块只关注一种协议阶段，同时保留 `AgentTransportService` 作为唯一的会话入口。

mod authorization;
mod business;
mod registration;

use std::sync::Arc;

use smalux_protocol::{
    agent::v1::{ProtocolFrame, protocol_frame},
    noise::ServerKeyRing,
    tonic_transport::{IncomingSession, ServerSessionAcceptor, TransportError},
};
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_util::sync::CancellationToken;
use tonic::{Status, Streaming};

use super::AgentTransportService;

impl AgentTransportService {
    /// 拥有一条 Agent gRPC 流从 Noise 握手到业务结束的完整生命周期。
    ///
    /// `open_session` 把 Tonic transport 资源移交给本方法后即可立即返回响应流；permit
    /// 则一直保留到 worker 退出，从而让会话容量统计覆盖握手、注册、授权和业务阶段。
    pub(super) async fn run_session_worker(
        self,
        session_id: u64,
        inbound: Streaming<ProtocolFrame>,
        sender: mpsc::Sender<Result<ProtocolFrame, Status>>,
        keyring: Arc<ServerKeyRing>,
        session_permit: OwnedSemaphorePermit,
        session_cancellation: CancellationToken,
    ) {
        let _session_permit = session_permit;
        tracing::debug!(session_id, "Agent gRPC session worker started");

        // ServerSessionAcceptor 负责读取首帧、完成 XXpsk3/IK Noise responder 握手，
        // 并把注册 Token ID 交给注册中心解析 PSK。PSK 本身绝不写入日志。
        let registry = Arc::clone(&self.state.agent_registry);
        let shutdown = self.state.shutdown.clone();
        let acceptor = ServerSessionAcceptor::default();
        let incoming = tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!(session_id, "Agent gRPC session cancelled during handshake");
                return;
            }
            _ = session_cancellation.cancelled() => {
                tracing::info!(session_id, "Agent gRPC session disconnected by management request");
                return;
            }
            incoming = acceptor
                .accept_incoming_with_psk_resolver(
                    inbound,
                    sender.clone(),
                    keyring.as_ref(),
                    move |token_id| async move {
                        registry
                            .resolve_registration_psk(&token_id)
                            .await
                            .map_err(|error| TransportError::Protocol(error.to_string()))
                    },
                ) => incoming,
        };

        // Noise 尚未建立时只能返回粗粒度 ProtocolError；不能把 Token、PSK 或 snow
        // 的内部错误直接回显给 Client。握手成功后则只发送加密 SecureError。
        let incoming = match incoming {
            Ok(incoming) => incoming,
            Err(error) => {
                tracing::warn!(
                    session_id,
                    error = %error,
                    "Agent Noise handshake failed"
                );
                if sender
                    .send(Ok(ProtocolFrame {
                        body: Some(protocol_frame::Body::ProtocolError(error.protocol_error())),
                    }))
                    .await
                    .is_err()
                {
                    tracing::debug!(
                        session_id,
                        "Agent client already closed; handshake error could not be delivered"
                    );
                }
                return;
            }
        };

        let result = tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!(session_id, "Agent gRPC session cancelled");
                return;
            }
            _ = session_cancellation.cancelled() => {
                tracing::info!(session_id, "Agent gRPC session disconnected by management request");
                return;
            }
            result = self.handle_established_session(session_id, incoming) => result,
        };
        match result {
            Ok(()) => tracing::info!(session_id, "Agent gRPC session completed"),
            Err(error) => tracing::warn!(
                session_id,
                error = %error,
                "Agent encrypted session aborted"
            ),
        }
    }

    /// 处理已经完成 Noise 握手、但尚未进入业务循环的会话。
    ///
    /// `ServerSessionAcceptor` 只负责密码学握手和消息解密，不能替业务层决定 Agent
    /// 是否已注册或是否被吊销。因此这里先按 `IncomingSession` 的阶段分支：
    ///
    /// - `Registration` 进入四阶段 XXpsk3 注册；
    /// - `Authentication` 查询 IK 对应的 Agent 授权状态；
    /// - 阶段失败时，子模块已经发送安全的加密拒绝消息，本方法正常结束当前会话；
    /// - 阶段成功时，统一进入 `business::run_business_session`。
    pub(super) async fn handle_established_session(
        &self,
        session_id: u64,
        incoming: IncomingSession,
    ) -> Result<(), TransportError> {
        let authentication_mode = match &incoming {
            IncomingSession::Registration(_) => "xxpsk3",
            IncomingSession::Authentication(_) => "ik",
        };
        let established = match incoming {
            IncomingSession::Registration(registration) => {
                self.state.sessions.mark_registering(session_id).await;
                self.handle_registration_session(session_id, registration)
                    .await?
            }
            IncomingSession::Authentication(authentication) => {
                self.handle_authorization_session(session_id, authentication)
                    .await?
            }
        };

        // `None` 表示对端已经收到加密拒绝消息；这不是 Server 内部错误，不能再进入
        // 业务循环，也不需要把同一个拒绝重复发送一次。
        let Some((agent_id, mut session)) = established else {
            tracing::debug!(
                session_id,
                "Agent session ended before business authorization"
            );
            return Ok(());
        };

        self.state
            .sessions
            .mark_authenticated(session_id, &agent_id, authentication_mode)
            .await;
        // 吊销可能与 IK 授权并发发生。登记 Session 后再按稳定 Agent ID 复查一次，
        // 保证管理端的“持久化吊销 + 取消活动连接”不会漏掉刚授权但尚未登记的连接。
        if !self
            .state
            .agent_registry
            .is_agent_active(&agent_id)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?
        {
            tracing::warn!(session_id, %agent_id, "Agent was revoked before business session start");
            return Ok(());
        }

        // 注册和 IK 授权最终都收敛到同一个长期 Noise transport 会话，因此业务层不需要
        // 关心 Agent 是刚注册还是断线重连，只需要使用稳定的 agent_id 处理消息即可。
        self.run_business_session(session_id, &agent_id, &mut session)
            .await
    }
}
