//! Agent 注册、状态恢复和已注册连接建立流程。
//!
//! 外层 `SmaluxClient` 只需要调用 [`ConnectionCoordinator::establish_initial`]；本 module
//! 根据持久化阶段选择 XXpsk3 或 IK，并保证身份材料在协议确认前后按正确顺序落盘。

use std::{collections::VecDeque, time::Duration};

use smalux_protocol::{
    agent::v1::{HealthResponse, SecureErrorCode},
    noise::NoiseIdentity,
    tonic_transport::{
        AgentProtocolClient, RunningSession, SessionDriver, SessionEvent, TonicNoiseSession,
        TransportError, parse_registration_credential,
    },
};
use tokio::time::{sleep, timeout};

use super::{
    AgentStateStore, AuthenticationMode, SmaluxClientConfig, SmaluxClientError,
    state::{PersistedAgentState, RegistrationStage},
};

/// 已通过 XX commit 或 IK + Ping/Pong 验证的活动连接。
pub(super) struct ActiveConnection {
    pub(super) state: PersistedAgentState,
    pub(super) running: RunningSession,
    pub(super) mode: AuthenticationMode,
    pub(super) buffered_events: VecDeque<SessionEvent>,
}

/// 持久化阶段决定的首次连接动作。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InitialConnectionAction {
    Register,
    RecoverPending,
    ConnectRegistered,
}

/// 将配置、状态存储和底层协议 Client 组合成完整的连接建立 module。
pub(super) struct ConnectionCoordinator<'a> {
    config: &'a SmaluxClientConfig,
    store: &'a dyn AgentStateStore,
    protocol: AgentProtocolClient,
}

impl<'a> ConnectionCoordinator<'a> {
    pub(super) fn new(config: &'a SmaluxClientConfig, store: &'a dyn AgentStateStore) -> Self {
        let mut protocol = AgentProtocolClient::new(config.endpoint.clone());
        protocol.set_handshake_timeout(config.handshake_timeout);
        if let Some(prefix) = &config.grpc_prefix {
            protocol.set_grpc_prefix(prefix.clone());
        }
        Self {
            config,
            store,
            protocol,
        }
    }

    pub(super) async fn health_check(&self) -> Result<HealthResponse, SmaluxClientError> {
        self.protocol.health_check().await.map_err(Into::into)
    }

    /// 加载或创建本地身份，并按持久化阶段选择注册、恢复或已注册连接。
    pub(super) async fn establish_initial(&self) -> Result<ActiveConnection, SmaluxClientError> {
        let state = match self
            .store
            .load()
            .await
            .map_err(SmaluxClientError::state_store)?
        {
            Some(state) => state,
            None => {
                let state = PersistedAgentState::identity_prepared(NoiseIdentity::generate()?);
                self.store
                    .save(&state)
                    .await
                    .map_err(SmaluxClientError::state_store)?;
                state
            }
        };

        match initial_connection_action(state.stage()) {
            InitialConnectionAction::Register => self.register(&state).await,
            InitialConnectionAction::RecoverPending => {
                // Server 可能已 commit，而 Agent 在保存 Registered 前退出。先尝试 IK；
                // 只有明确未授权时才再次使用 Token 恢复 XX 注册事务。
                match self.connect_registered(&state).await {
                    Ok(mut connection) => {
                        let registered = PersistedAgentState::registered_from_pending(&state)
                            .map_err(SmaluxClientError::state_store)?;
                        self.store
                            .save(&registered)
                            .await
                            .map_err(SmaluxClientError::state_store)?;
                        connection.state = registered;
                        Ok(connection)
                    }
                    Err(error) if is_agent_not_authorized(&error) => self.register(&state).await,
                    Err(error) => Err(error),
                }
            }
            InitialConnectionAction::ConnectRegistered => self.connect_registered(&state).await,
        }
    }

    /// 使用已保存 Agent 身份和 Server 公钥候选执行 IK。
    pub(super) async fn connect_registered(
        &self,
        state: &PersistedAgentState,
    ) -> Result<ActiveConnection, SmaluxClientError> {
        let session = self
            .protocol
            .connect_with_candidates(state.identity(), state.server_public_keys())
            .await?;
        let (running, buffered_events) = self.verify_authorized_session(session).await?;
        Ok(ActiveConnection {
            state: state.clone(),
            running,
            mode: AuthenticationMode::ReconnectIk,
            buffered_events,
        })
    }

    /// 执行 XX 注册，并严格按 pending、commit、registered 的顺序持久化状态。
    async fn register(
        &self,
        state: &PersistedAgentState,
    ) -> Result<ActiveConnection, SmaluxClientError> {
        let token = self
            .config
            .registration_token
            .as_ref()
            .ok_or(SmaluxClientError::RegistrationTokenRequired)?;
        let (token_id, psk) = parse_registration_credential(token.expose())
            .map_err(|error| SmaluxClientError::InvalidRegistrationToken(error.to_string()))?;
        let pending = self
            .protocol
            .prepare_registration_with_token_id(
                state.identity().clone(),
                &psk,
                token_id.to_owned(),
                token.expose().to_owned(),
            )
            .await
            .map_err(SmaluxClientError::from)?;
        let pending_state = PersistedAgentState::registration_pending(
            pending.agent_id.clone(),
            pending.agent_identity.clone(),
            pending.server_public_key,
            pending.registration_id,
        );
        if state.stage() == RegistrationStage::RegistrationPending {
            verify_same_pending(state, &pending_state)?;
        }
        self.store
            .save(&pending_state)
            .await
            .map_err(SmaluxClientError::state_store)?;

        let registration = pending
            .send_registration_commit_and_receive_committed()
            .await?;
        let registered = PersistedAgentState::registered_from_pending(&pending_state)
            .map_err(SmaluxClientError::state_store)?;
        self.store
            .save(&registered)
            .await
            .map_err(SmaluxClientError::state_store)?;
        let running = self.start_driver(registration.session);
        Ok(ActiveConnection {
            state: registered,
            running,
            mode: AuthenticationMode::RegistrationXxPsk3,
            buffered_events: VecDeque::new(),
        })
    }

    fn start_driver(&self, mut session: TonicNoiseSession) -> RunningSession {
        session.set_heartbeat_policy(self.config.heartbeat);
        session.set_rekey_policy(self.config.rekey);
        SessionDriver::spawn(session, self.config.driver)
    }

    /// IK 握手完成后用加密 Ping/Pong 确认 Server 已通过数据库业务授权。
    async fn verify_authorized_session(
        &self,
        session: TonicNoiseSession,
    ) -> Result<(RunningSession, VecDeque<SessionEvent>), SmaluxClientError> {
        let mut running = self.start_driver(session);
        let mut buffered = VecDeque::new();
        running.handle.ping(0).await?;
        let verification = async {
            loop {
                tokio::select! {
                    event = running.events.recv() => {
                        match event {
                            Some(Ok(event)) => buffered.push_back(event),
                            Some(Err(error)) => return Err(SmaluxClientError::Transport(error)),
                            None => return Err(SmaluxClientError::Transport(TransportError::Closed)),
                        }
                    }
                    _ = sleep(Duration::from_millis(10)) => {
                        if running.handle.heartbeat_stats().await?.received_count > 0 {
                            return Ok(());
                        }
                    }
                }
            }
        };
        match timeout(self.config.handshake_timeout, verification).await {
            Ok(Ok(())) => Ok((running, buffered)),
            Ok(Err(error)) => {
                stop_running_session(running).await;
                Err(error)
            }
            Err(_) => {
                stop_running_session(running).await;
                Err(SmaluxClientError::Transport(TransportError::Timeout(
                    "verifying Agent authorization",
                )))
            }
        }
    }
}

fn initial_connection_action(stage: RegistrationStage) -> InitialConnectionAction {
    match stage {
        RegistrationStage::IdentityPrepared => InitialConnectionAction::Register,
        RegistrationStage::RegistrationPending => InitialConnectionAction::RecoverPending,
        RegistrationStage::Registered => InitialConnectionAction::ConnectRegistered,
    }
}

fn verify_same_pending(
    previous: &PersistedAgentState,
    current: &PersistedAgentState,
) -> Result<(), SmaluxClientError> {
    let same = previous.agent_id() == current.agent_id()
        && previous.identity().public_key() == current.identity().public_key()
        && previous.server_public_keys() == current.server_public_keys()
        && previous.registration_id() == current.registration_id();
    if !same {
        return Err(SmaluxClientError::InconsistentPendingRegistration);
    }
    Ok(())
}

fn is_agent_not_authorized(error: &SmaluxClientError) -> bool {
    matches!(
        error,
        SmaluxClientError::Transport(TransportError::RemoteSecure(
            SecureErrorCode::AgentNotAuthorized,
            _
        ))
    )
}

pub(super) async fn stop_running_session(running: RunningSession) {
    let _ = running.handle.shutdown().await;
    if let Err(error) = running.task.await {
        tracing::debug!(error = %error, "Agent SessionDriver task join failed during shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::{InitialConnectionAction, initial_connection_action};
    use crate::client::RegistrationStage;

    #[test]
    fn registration_stage_selects_the_expected_noise_handshake() {
        assert_eq!(
            initial_connection_action(RegistrationStage::IdentityPrepared),
            InitialConnectionAction::Register
        );
        assert_eq!(
            initial_connection_action(RegistrationStage::RegistrationPending),
            InitialConnectionAction::RecoverPending
        );
        assert_eq!(
            initial_connection_action(RegistrationStage::Registered),
            InitialConnectionAction::ConnectRegistered
        );
    }
}
