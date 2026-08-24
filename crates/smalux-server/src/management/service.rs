//! Server 本地管理业务服务。
//!
//! 本模块只处理管理请求和领域服务调用；协议 DTO 位于 protocol 子模块，本地传输位于 ipc 子模块。

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use smalux_protocol::tonic_transport::{
    validate_agent_display_name, validate_registration_token_id,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::RuntimeConfig,
    database::{AgentRecord, RegistrationTokenRecord, RevokeTokenOutcome, ServerDatabase},
    service::agent::{
        agent_registry::AgentRegistry,
        keyring_manager::ServerKeyRingManager,
        session_registry::{SessionRegistry, SessionSnapshot},
    },
};

use super::protocol::*;

/// CLI 与未来 Web 管理接口共用的业务门面。
///
/// 它组合数据库长期状态、Session 实时状态和密钥环状态；传输层不得绕过这里直接操作
/// ORM。实例随 `AppState` 存活，所有字段均为进程内共享依赖或不可变配置快照。
pub(crate) struct AdminService {
    /// 用于计算进程内 uptime，不依赖可回拨的系统时钟。
    started_at: Instant,
    /// Agent/Token 持久化管理查询入口。
    database: Arc<ServerDatabase>,
    /// 注册 Token 的安全生成入口。
    registry: Arc<AgentRegistry>,
    /// Server Noise 密钥环及 revision 读取入口。
    keyring: Arc<ServerKeyRingManager>,
    /// 当前进程 Session 的查询和取消入口。
    sessions: SessionRegistry,
    /// 启动时已验证的非敏感运行配置快照。
    runtime_config: RuntimeConfig,
    /// 与 HTTP/gRPC Server 共用的全局关闭令牌。
    shutdown: CancellationToken,
}

impl AdminService {
    /// 由应用启动层注入全部依赖；构造过程不执行 I/O。
    pub(crate) fn new(
        database: Arc<ServerDatabase>,
        registry: Arc<AgentRegistry>,
        keyring: Arc<ServerKeyRingManager>,
        sessions: SessionRegistry,
        runtime_config: RuntimeConfig,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            started_at: Instant::now(),
            database,
            registry,
            keyring,
            sessions,
            runtime_config,
            shutdown,
        }
    }

    /// 执行单个管理请求，并把内部错误收敛成不会泄漏细节的协议响应。
    pub(crate) async fn handle(&self, request: ControlRequest) -> ControlResponse {
        match self.handle_result(request).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(error = %error.message, code = %error.code, "Server management request failed");
                ControlResponse::Error {
                    code: error.code,
                    message: error.message,
                }
            }
        }
    }

    /// 管理操作的实际分派；每个分支负责服务端二次校验和领域调用。
    async fn handle_result(&self, request: ControlRequest) -> Result<ControlResponse, AdminError> {
        match request {
            ControlRequest::Status => Ok(ControlResponse::Status(self.status().await?)),
            ControlRequest::EffectiveConfig => {
                Ok(ControlResponse::EffectiveConfig(EffectiveConfig {
                    listen_address: self.runtime_config.address.clone(),
                    listen_port: self.runtime_config.port,
                    database_backend: self.database.backend_label().to_owned(),
                    database_url: "<redacted>".to_owned(),
                    max_agent_sessions: self.runtime_config.max_agent_sessions,
                    max_registration_sessions: self.runtime_config.max_registration_sessions,
                    max_grpc_message_bytes: self.runtime_config.max_grpc_message_bytes,
                }))
            }
            ControlRequest::CreateRegistrationToken {
                agent_name,
                valid_for_seconds,
            } => {
                if valid_for_seconds == Some(0) {
                    return Err(AdminError::invalid(
                        "registration Token validity must be greater than zero",
                    ));
                }
                let token = self
                    .registry
                    .create_registration_token(
                        agent_name,
                        valid_for_seconds.map(Duration::from_secs),
                    )
                    .await
                    .map_err(AdminError::internal)?;
                Ok(ControlResponse::RegistrationTokenCreated(IssuedToken {
                    token_id: token.token_id().to_owned(),
                    credential: token.expose_credential().to_owned(),
                    expires_at_unix_micros: token.expires_at_unix_micros(),
                }))
            }
            ControlRequest::ListRegistrationTokens {
                status,
                agent_name,
                limit,
                after,
            } => {
                validate_page_limit(limit)?;
                validate_filter(
                    status.as_deref(),
                    &["active", "used", "revoked", "expired"],
                    "Token status",
                )?;
                let values = self
                    .database
                    .list_registration_tokens(
                        status.as_deref(),
                        agent_name.as_deref(),
                        limit.into(),
                        after.as_deref(),
                    )
                    .await
                    .map_err(AdminError::internal)?;
                Ok(ControlResponse::RegistrationTokens(
                    values.into_iter().map(token_view).collect(),
                ))
            }
            ControlRequest::GetRegistrationToken { token_id } => {
                validate_token_id(&token_id)?;
                Ok(ControlResponse::RegistrationToken(
                    self.database
                        .find_registration_token(&token_id)
                        .await
                        .map_err(AdminError::internal)?
                        .map(token_view),
                ))
            }
            ControlRequest::RevokeRegistrationToken { token_id } => {
                validate_token_id(&token_id)?;
                match self
                    .database
                    .revoke_registration_token(&token_id)
                    .await
                    .map_err(AdminError::internal)?
                {
                    RevokeTokenOutcome::Revoked | RevokeTokenOutcome::AlreadyRevoked => {
                        let token = self
                            .database
                            .find_registration_token(&token_id)
                            .await
                            .map_err(AdminError::internal)?
                            .ok_or_else(|| {
                                AdminError::not_found("registration Token was not found")
                            })?;
                        Ok(ControlResponse::RegistrationTokenRevoked(token_view(token)))
                    }
                    RevokeTokenOutcome::AlreadyUsed => Err(AdminError::conflict(
                        "registration Token has already been used",
                    )),
                    RevokeTokenOutcome::NotFound => {
                        Err(AdminError::not_found("registration Token was not found"))
                    }
                }
            }
            ControlRequest::ListAgents {
                status,
                name,
                online,
                limit,
                after,
            } => {
                validate_page_limit(limit)?;
                validate_filter(status.as_deref(), &["active", "revoked"], "Agent status")?;
                let sessions = self.sessions.list().await;
                let online_agent_ids = sessions
                    .iter()
                    .filter_map(|session| session.agent_id.clone())
                    .collect::<Vec<_>>();
                let agents = self
                    .database
                    .list_agents(
                        status.as_deref(),
                        name.as_deref(),
                        online.map(|value| (online_agent_ids.as_slice(), value)),
                        limit.into(),
                        after.as_deref(),
                    )
                    .await
                    .map_err(AdminError::internal)?
                    .into_iter()
                    .map(|agent| agent_view(agent, &sessions))
                    .collect::<Vec<_>>();
                Ok(ControlResponse::Agents(agents))
            }
            ControlRequest::GetAgent { agent_id } => {
                let sessions = self.sessions.list().await;
                Ok(ControlResponse::Agent(
                    self.database
                        .find_agent(&agent_id)
                        .await
                        .map_err(AdminError::internal)?
                        .map(|agent| agent_view(agent, &sessions)),
                ))
            }
            ControlRequest::RenameAgent { agent_id, name } => {
                validate_agent_display_name(&name)
                    .map_err(|_| AdminError::invalid("Agent name is invalid"))?;
                let sessions = self.sessions.list().await;
                let agent = self
                    .database
                    .rename_agent(&agent_id, &name)
                    .await
                    .map_err(AdminError::internal)?
                    .ok_or_else(|| AdminError::not_found("Agent was not found"))?;
                Ok(ControlResponse::AgentUpdated(agent_view(agent, &sessions)))
            }
            ControlRequest::RevokeAgent { agent_id } => {
                let agent = self
                    .database
                    .revoke_agent(&agent_id)
                    .await
                    .map_err(AdminError::internal)?
                    .ok_or_else(|| AdminError::not_found("Agent was not found"))?;
                let disconnected_sessions = self.sessions.disconnect_agent(&agent_id).await;
                Ok(ControlResponse::AgentRevoked {
                    agent: agent_view(agent, &[]),
                    disconnected_sessions,
                })
            }
            ControlRequest::ListSessions { agent_id, state } => {
                validate_filter(
                    state.as_deref(),
                    &["handshaking", "registering", "authenticated"],
                    "Session state",
                )?;
                let mut sessions = self.sessions.list().await;
                sessions.retain(|session| {
                    agent_id
                        .as_deref()
                        .is_none_or(|value| session.agent_id.as_deref() == Some(value))
                        && state
                            .as_deref()
                            .is_none_or(|value| session.stage.as_str() == value)
                });
                Ok(ControlResponse::Sessions(
                    sessions.into_iter().map(session_view).collect(),
                ))
            }
            ControlRequest::GetSession { session_id } => Ok(ControlResponse::Session(
                self.sessions.get(session_id).await.map(session_view),
            )),
            ControlRequest::DisconnectSession { session_id } => {
                if !self.sessions.disconnect(session_id).await {
                    return Err(AdminError::not_found("Session was not found"));
                }
                Ok(ControlResponse::SessionDisconnected { session_id })
            }
            ControlRequest::KeyringStatus => {
                Ok(ControlResponse::KeyringStatus(self.keyring_status()?))
            }
            ControlRequest::Shutdown => {
                self.shutdown.cancel();
                Ok(ControlResponse::ShutdownAccepted)
            }
        }
    }

    /// 合并数据库计数、实时 Session 和内存密钥环生成状态快照。
    async fn status(&self) -> Result<ServerStatus, AdminError> {
        let counts = self
            .database
            .management_counts()
            .await
            .map_err(AdminError::internal)?;
        Ok(ServerStatus {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            uptime_ms: self.started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
            database_backend: self.database.backend_label().to_owned(),
            active_sessions: self.sessions.len().await,
            max_agent_sessions: self.runtime_config.max_agent_sessions,
            max_registration_sessions: self.runtime_config.max_registration_sessions,
            active_agents: counts.active_agents,
            revoked_agents: counts.revoked_agents,
            active_tokens: counts.active_tokens,
            used_tokens: counts.used_tokens,
            revoked_tokens: counts.revoked_tokens,
            expired_tokens: counts.expired_tokens,
            keyring_revision: self.keyring.revision().map_err(AdminError::internal)?,
            active_server_keys: self
                .keyring
                .active_key_count()
                .map_err(AdminError::internal)?,
            shutting_down: self.shutdown.is_cancelled(),
        })
    }

    /// 只提取 key ID 和轮换元数据；公钥原文及私钥永远不进入 DTO。
    fn keyring_status(&self) -> Result<KeyringStatus, AdminError> {
        let keyring = self
            .keyring
            .current_keyring()
            .map_err(AdminError::internal)?;
        let snapshot = keyring.snapshot();
        Ok(KeyringStatus {
            revision: self.keyring.revision().map_err(AdminError::internal)?,
            current_key_id: hex(snapshot.current.key_id().as_bytes()),
            next_key_id: snapshot
                .next
                .map(|identity| hex(identity.key_id().as_bytes())),
            previous_key_id: snapshot
                .previous
                .map(|identity| hex(identity.key_id().as_bytes())),
            rotation_id: snapshot.rotation_id.map(|id| hex(id.as_bytes())),
        })
    }
}

/// 管理边界内部错误；`message` 必须适合直接返回本地调用方。
struct AdminError {
    code: String,
    message: String,
}

impl AdminError {
    /// 构造输入校验错误。
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_argument".to_owned(),
            message: message.into(),
        }
    }
    /// 构造资源不存在错误。
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: "not_found".to_owned(),
            message: message.into(),
        }
    }
    /// 构造与当前持久化状态冲突的错误。
    fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: "conflict".to_owned(),
            message: message.into(),
        }
    }
    /// 记录完整内部原因，但只向调用方返回固定脱敏文本。
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error = %error, "Server management operation failed");
        Self {
            code: "internal".to_owned(),
            message: "Server management operation failed".to_owned(),
        }
    }
}

/// 将不含 PSK 的数据库 Token 记录映射为 IPC DTO。
fn token_view(record: RegistrationTokenRecord) -> RegistrationTokenView {
    RegistrationTokenView {
        token_id: record.token_id,
        agent_name: record.display_name,
        status: record.status,
        created_at_unix_micros: record.created_at,
        updated_at_unix_micros: record.updated_at,
        expires_at_unix_micros: record.expires_at,
        used_at_unix_micros: record.used_at,
    }
}

/// 合并持久化 Agent 记录和当前进程 Session，派生实时在线状态。
fn agent_view(record: AgentRecord, sessions: &[SessionSnapshot]) -> AgentView {
    let online = sessions
        .iter()
        .any(|session| session.agent_id.as_deref() == Some(&record.agent_id));
    AgentView {
        agent_id: record.agent_id,
        name: record.name,
        status: record.status,
        online,
        created_at_unix_micros: record.created_at,
        updated_at_unix_micros: record.updated_at,
        revoked_at_unix_micros: record.revoked_at,
    }
}

/// 将内部 Session 快照映射为稳定字符串状态的 IPC DTO。
fn session_view(snapshot: SessionSnapshot) -> SessionView {
    SessionView {
        session_id: snapshot.session_id,
        agent_id: snapshot.agent_id,
        authentication_mode: snapshot.authentication_mode,
        state: snapshot.stage.as_str().to_owned(),
        connected_at_unix_micros: snapshot.connected_at_unix_micros,
        last_activity_at_unix_micros: snapshot.last_activity_at_unix_micros,
    }
}

/// 将固定长度二进制 ID 编码为小写十六进制，不引入密钥材料格式化依赖。
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

/// 在 IPC 边界再次限制页大小，不能依赖 CLI 已经执行过校验。
fn validate_page_limit(limit: u32) -> Result<(), AdminError> {
    if !(1..=500).contains(&limit) {
        return Err(AdminError::invalid("page limit must be between 1 and 500"));
    }
    Ok(())
}

/// 校验字符串枚举过滤器，拒绝绕过 Clap 构造的未知值。
fn validate_filter(value: Option<&str>, allowed: &[&str], label: &str) -> Result<(), AdminError> {
    if value.is_some_and(|value| !allowed.contains(&value)) {
        return Err(AdminError::invalid(format!("{label} filter is invalid")));
    }
    Ok(())
}

/// 使用协议层规则验证公开 Token ID，不在错误中回显不可信输入。
fn validate_token_id(token_id: &str) -> Result<(), AdminError> {
    validate_registration_token_id(token_id)
        .map_err(|_| AdminError::invalid("registration Token ID is invalid"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sea_orm::{ActiveModelTrait, Set};

    use super::{
        AdminService, ControlRequest, ControlResponse, validate_filter, validate_page_limit,
    };
    use crate::{
        config::{DatabaseConfig, RuntimeConfig},
        database::{ServerDatabase, entity::agent},
        service::agent::{
            agent_registry::AgentRegistry, keyring_manager::ServerKeyRingManager,
            session_registry::SessionRegistry,
        },
    };
    use tokio_util::sync::CancellationToken;

    #[test]
    fn ipc_filters_and_page_limits_are_validated_independently_of_clap() {
        assert!(validate_page_limit(0).is_err());
        assert!(validate_page_limit(501).is_err());
        assert!(validate_page_limit(50).is_ok());
        assert!(validate_filter(Some("unknown"), &["active"], "status").is_err());
        assert!(validate_filter(Some("active"), &["active"], "status").is_ok());
    }

    async fn service() -> (AdminService, Arc<ServerDatabase>, SessionRegistry) {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .unwrap(),
        );
        let registry = Arc::new(AgentRegistry::new(Arc::clone(&database)));
        let keyring = Arc::new(
            ServerKeyRingManager::load_or_create(Arc::clone(&database))
                .await
                .unwrap(),
        );
        let sessions = SessionRegistry::default();
        (
            AdminService::new(
                Arc::clone(&database),
                registry,
                keyring,
                sessions.clone(),
                RuntimeConfig {
                    address: "127.0.0.1".to_owned(),
                    port: 12345,
                    max_agent_sessions: 256,
                    max_registration_sessions: 32,
                    max_grpc_message_bytes: 1024 * 1024,
                },
                CancellationToken::new(),
            ),
            database,
            sessions,
        )
    }

    #[tokio::test]
    async fn issued_credential_is_returned_once_but_token_queries_only_return_metadata() {
        let (service, _, _) = service().await;
        let issued = service
            .handle(ControlRequest::CreateRegistrationToken {
                agent_name: Some("node-one".to_owned()),
                valid_for_seconds: Some(1_800),
            })
            .await;
        let ControlResponse::RegistrationTokenCreated(issued) = issued else {
            panic!("issued registration Token")
        };
        assert!(issued.credential.starts_with(&issued.token_id));

        let listed = service
            .handle(ControlRequest::ListRegistrationTokens {
                status: Some("active".to_owned()),
                agent_name: None,
                limit: 50,
                after: None,
            })
            .await;
        let json = serde_json::to_string(&listed).unwrap();
        assert!(!json.contains(&issued.credential));
        assert!(!json.contains("psk"));
        assert!(json.contains(&issued.token_id));
    }

    #[tokio::test]
    async fn revoking_an_agent_persists_revocation_and_cancels_its_session() {
        let (service, database, sessions) = service().await;
        let now = 1_700_000_000_000_000_i64;
        agent::ActiveModel {
            agent_id: Set("agent-one".to_owned()),
            name: Set("node-one".to_owned()),
            public_key: Set(vec![3; 32]),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        }
        .insert(database.connection())
        .await
        .unwrap();
        let cancellation = sessions.register(9).await;
        sessions.mark_authenticated(9, "agent-one", "ik").await;

        let response = service
            .handle(ControlRequest::RevokeAgent {
                agent_id: "agent-one".to_owned(),
            })
            .await;
        let ControlResponse::AgentRevoked {
            agent,
            disconnected_sessions,
        } = response
        else {
            panic!("revoked Agent response")
        };
        assert_eq!(agent.status, "revoked");
        assert_eq!(disconnected_sessions, 1);
        assert!(cancellation.is_cancelled());
        assert!(!database.is_agent_active("agent-one").await.unwrap());
    }
}
