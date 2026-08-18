use std::sync::Arc;

use crate::database::{
    DatabaseError, PendingAgentRegistration, PersistedAgentAuthorization,
    PrepareAgentRegistrationError, ServerDatabase,
};
use smalux_protocol::{noise::NoisePublicKey, tonic_transport::validate_registration_token_id};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// prepare 阶段可安全映射到协议错误码的失败类型。
///
/// 名称格式属于协议输入规则，数据库的 Token/身份冲突和持久化错误由底层错误映射而来。
#[derive(Debug, thiserror::Error)]
pub(crate) enum PrepareRegistrationError {
    #[error("registration token is invalid")]
    InvalidToken,
    #[error("registration token is already used or bound")]
    TokenAlreadyUsed,
    #[error("Agent identity is already registered")]
    AgentAlreadyRegistered,
    #[error("Agent name is invalid")]
    InvalidAgentName,
    #[error("registration database operation failed")]
    Database(#[from] DatabaseError),
    #[error("registration state is invalid")]
    Internal(#[source] anyhow::Error),
}

impl From<PrepareAgentRegistrationError> for PrepareRegistrationError {
    fn from(error: PrepareAgentRegistrationError) -> Self {
        match error {
            PrepareAgentRegistrationError::InvalidToken => Self::InvalidToken,
            PrepareAgentRegistrationError::TokenAlreadyUsed => Self::TokenAlreadyUsed,
            PrepareAgentRegistrationError::AgentAlreadyRegistered => Self::AgentAlreadyRegistered,
            PrepareAgentRegistrationError::Database(error) => Self::Database(error),
            PrepareAgentRegistrationError::Internal(error) => Self::Internal(error),
        }
    }
}
/// XXpsk3 `RegistrationPrepared` 阶段需要暂存的注册事务。
///
/// 该类型由数据库层创建，服务层只把它附着到 Noise 会话并在 commit 时回传。
pub(crate) type PendingRegistration = PendingAgentRegistration;

/// IK 握手通过后供业务层使用的授权结果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedAgent {
    pub(crate) agent_id: String,
    pub(crate) public_key: NoisePublicKey,
}

/// IK 身份不能进入业务会话的结构化原因。
#[derive(Debug, thiserror::Error)]
pub(crate) enum AgentAuthorizationError {
    /// 数据库中明确记录该 Agent 已被吊销。
    #[error("Agent is revoked")]
    Revoked,
    /// 公钥未知、状态损坏或 Agent 尚未激活。
    #[error("Agent is not authorized")]
    Unauthorized,
    /// 授权快照查询失败。
    #[error("Agent authorization database operation failed")]
    Database(#[from] DatabaseError),
}

pub(crate) struct AgentRegistrar {
    /// 注册事务最终由该数据库连接池持久化；所有查询方法均保留异步数据库边界。
    database: Arc<ServerDatabase>,
}

pub(crate) const REGISTRATION_CLEANUP_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

impl AgentRegistrar {
    pub fn new(database: Arc<ServerDatabase>) -> Self {
        tracing::debug!(
            backend = database.backend_label(),
            "creating database-backed Agent registrar"
        );
        Self { database }
    }

    /// 删除已经过期且尚未提交的注册尝试，释放公钥和 Token 的唯一索引。
    pub(crate) async fn cleanup_expired_registrations(&self) -> anyhow::Result<u64> {
        let deleted = self.database.cleanup_expired_agent_registrations().await?;
        if deleted > 0 {
            tracing::info!(deleted, "cleaned expired Agent registration attempts");
        }
        Ok(deleted)
    }

    /// 启动可取消的注册尝试清理任务。
    pub(crate) fn start_cleanup_task(
        self: &Arc<Self>,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let registrar = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REGISTRATION_CLEANUP_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::debug!("Agent registration cleanup task cancelled");
                        return;
                    }
                    _ = ticker.tick() => {
                        if let Err(error) = registrar.cleanup_expired_registrations().await {
                            tracing::warn!(error = %error, "failed to clean expired Agent registrations");
                        }
                    }
                }
            }
        })
    }

    /// 根据公开 Token ID 解析一条仍可用于首次注册的 32 字节 PSK。
    ///
    /// 未知、过期、已消费和已吊销 Token 统一返回无效错误，避免握手前泄露 Token
    /// 生命周期细节。PSK 只作为返回值进入 Noise，不写入日志。
    pub(crate) async fn resolve_registration_psk(
        &self,
        token_id: &str,
    ) -> anyhow::Result<[u8; 32]> {
        validate_registration_token_id(token_id)
            .map_err(|_| anyhow::anyhow!("registration token is invalid"))?;
        let psk = self
            .database
            .load_active_registration_psk(token_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("registration token is invalid"))?;
        tracing::debug!(
            token_id_len = token_id.len(),
            database_backend = self.database.backend_label(),
            "resolved active registration PSK"
        );
        Ok(psk)
    }

    /// 校验握手 Token、Agent 公钥和展示名称，并原子持久化 pending 注册。
    ///
    /// 同一 Token 与公钥的重试返回相同事务 ID；名称只是允许重复的展示元数据，
    /// 不参与身份判断。prepare 阶段只创建注册尝试，commit 才会创建 `active` Agent。
    pub(crate) async fn prepare_registration(
        &self,
        token_id: &str,
        registration_token: &str,
        agent_public_key: NoisePublicKey,
        agent_name: &str,
    ) -> Result<PendingRegistration, PrepareRegistrationError> {
        validate_agent_name(agent_name)?;
        let (request_token_id, request_psk) = parse_registration_token(registration_token)?;
        if request_token_id != token_id {
            return Err(PrepareRegistrationError::InvalidToken);
        }
        let pending = self
            .database
            .prepare_agent_registration(
                token_id,
                &request_psk,
                agent_public_key.as_bytes(),
                agent_name,
            )
            .await?;

        tracing::info!(
            registration_id = %uuid::Uuid::from_bytes(pending.registration_id),
            agent_id = %pending.agent_id,
            agent_key_id = ?agent_public_key.key_id(),
            "prepared database-backed Agent registration"
        );
        Ok(pending)
    }

    /// 原子提交注册：激活 Agent、完成注册事务并一次性消费 Token。
    ///
    /// 重复提交同一事务保持成功；事务 ID、Agent ID、Token 状态或过期时间不匹配时
    /// 整体回滚，不能留下“Agent 已激活但 Token 未消费”的半完成状态。
    pub(crate) async fn commit_registration(
        &self,
        pending: &PendingRegistration,
    ) -> anyhow::Result<()> {
        self.database.commit_agent_registration(pending).await?;

        tracing::info!(
            registration_id = %uuid::Uuid::from_bytes(pending.registration_id),
            agent_id = %pending.agent_id,
            "committed database-backed Agent registration"
        );
        Ok(())
    }

    /// 使用 IK 握手认证出的长期公钥查询 Agent 授权记录。
    ///
    /// 只有状态为 `active` 且没有吊销时间的 Agent 才能进入业务会话。
    /// 未登记、仍在注册、已吊销等情况统一返回未授权，避免暴露记录状态。
    pub(crate) async fn authorize_agent(
        &self,
        agent_public_key: NoisePublicKey,
    ) -> Result<AuthorizedAgent, AgentAuthorizationError> {
        let authorization = self
            .database
            .find_agent_authorization(agent_public_key.as_bytes())
            .await?;
        let agent_id = match authorization {
            PersistedAgentAuthorization::Authorized(agent_id) => agent_id,
            PersistedAgentAuthorization::Revoked => {
                return Err(AgentAuthorizationError::Revoked);
            }
            PersistedAgentAuthorization::Unauthorized => {
                return Err(AgentAuthorizationError::Unauthorized);
            }
        };
        tracing::debug!(
            agent_id = %agent_id,
            agent_key_id = ?agent_public_key.key_id(),
            "authorized Agent by its Noise identity"
        );
        Ok(AuthorizedAgent {
            agent_id,
            public_key: agent_public_key,
        })
    }
}

/// Agent 名称沿用 Example 的保守字符集，避免后续进入路径、日志或标签时需要二次清洗。
fn validate_agent_name(agent_name: &str) -> Result<(), PrepareRegistrationError> {
    if agent_name.is_empty()
        || agent_name.len() > 64
        || !agent_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(PrepareRegistrationError::InvalidAgentName);
    }
    Ok(())
}

/// 解析 `公开 Token ID.64 位十六进制 PSK`，不在错误中包含原始凭据。
fn parse_registration_token(token: &str) -> Result<(String, [u8; 32]), PrepareRegistrationError> {
    let (token_id, secret) = token
        .split_once('.')
        .ok_or(PrepareRegistrationError::InvalidToken)?;
    validate_registration_token_id(token_id).map_err(|_| PrepareRegistrationError::InvalidToken)?;
    if secret.len() != 64 {
        return Err(PrepareRegistrationError::InvalidToken);
    }

    let mut psk = [0_u8; 32];
    for (index, chunk) in secret.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        psk[index] = (high << 4) | low;
    }
    Ok((token_id.to_owned(), psk))
}

/// 把单个 ASCII 十六进制字符转换为半字节。
fn hex_nibble(value: u8) -> Result<u8, PrepareRegistrationError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(PrepareRegistrationError::InvalidToken),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sea_orm::{ActiveModelTrait, EntityTrait, Set};

    use crate::database::{
        DatabaseConfig, ServerDatabase,
        entity::{agent, agent_registration, registration_token},
    };
    use smalux_protocol::noise::NoisePublicKey;

    use super::{AgentAuthorizationError, AgentRegistrar};

    async fn registrar_with_token() -> AgentRegistrar {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .expect("database should connect"),
        );
        registration_token::ActiveModel {
            token_id: Set("token-test".to_owned()),
            psk: Set(vec![7; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            used_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("registration token should insert");
        AgentRegistrar::new(database)
    }

    fn registration_token(token_id: &str, byte: u8) -> String {
        let secret = std::iter::repeat_n(format!("{byte:02x}"), 32).collect::<String>();
        format!("{token_id}.{secret}")
    }

    #[tokio::test]
    async fn valid_registration_token_resolves_its_psk() {
        let registrar = registrar_with_token().await;

        assert_eq!(
            registrar
                .resolve_registration_psk("token-test")
                .await
                .expect("active token should resolve"),
            [7; 32]
        );
    }

    #[tokio::test]
    async fn consumed_revoked_expired_and_malformed_tokens_are_rejected() {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .expect("database should connect"),
        );
        for (token_id, status, expires_at, used_at, psk) in [
            ("token-used", "used", None, Some(2), vec![7; 32]),
            ("token-revoked", "revoked", None, None, vec![7; 32]),
            ("token-expired", "active", Some(1), None, vec![7; 32]),
            ("token-malformed", "active", None, None, vec![7; 31]),
        ] {
            registration_token::ActiveModel {
                token_id: Set(token_id.to_owned()),
                psk: Set(psk),
                status: Set(status.to_owned()),
                created_at: Set(1),
                updated_at: Set(1),
                expires_at: Set(expires_at),
                used_at: Set(used_at),
            }
            .insert(database.connection())
            .await
            .expect("test token should insert");
        }
        let registrar = AgentRegistrar::new(database);

        for token_id in [
            "token-used",
            "token-revoked",
            "token-expired",
            "token-malformed",
            "token-missing",
        ] {
            assert!(
                registrar.resolve_registration_psk(token_id).await.is_err(),
                "{token_id} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn identical_prepare_retry_returns_the_same_registration() {
        let registrar = registrar_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[9; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);

        let first = registrar
            .prepare_registration("token-test", &token, public_key, "agent-one")
            .await
            .expect("first prepare should succeed");
        let retry = registrar
            .prepare_registration("token-test", &token, public_key, "agent-one")
            .await
            .expect("identical prepare should be idempotent");

        assert_eq!(retry, first);
        assert!(!first.agent_id.is_empty());
    }

    #[tokio::test]
    async fn expired_prepare_can_register_again_with_the_same_public_key() {
        let registrar = registrar_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[16; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);
        let first = registrar
            .prepare_registration("token-test", &token, public_key, "agent-expired")
            .await
            .expect("first prepare should succeed");

        let model = agent_registration::Entity::find_by_id(
            uuid::Uuid::from_bytes(first.registration_id).to_string(),
        )
        .one(registrar.database.connection())
        .await
        .expect("registration lookup should work")
        .expect("registration should exist");
        let mut expired: agent_registration::ActiveModel = model.into();
        expired.expires_at = Set(Some(1));
        expired
            .update(registrar.database.connection())
            .await
            .expect("registration should be expired");
        assert_eq!(
            registrar
                .cleanup_expired_registrations()
                .await
                .expect("expired registration cleanup should work"),
            1
        );

        let second = registrar
            .prepare_registration("token-test", &token, public_key, "agent-retry")
            .await
            .expect("expired registration should be replaceable");
        assert_ne!(second.registration_id, first.registration_id);
        assert_ne!(second.agent_id, first.agent_id);
        assert!(
            agent::Entity::find_by_id(&second.agent_id)
                .one(registrar.database.connection())
                .await
                .expect("prepared Agent lookup should work")
                .is_none()
        );
    }

    #[tokio::test]
    async fn prepare_rejects_mismatched_token_credentials_and_identity_bindings() {
        let registrar = registrar_with_token().await;
        let first_key =
            NoisePublicKey::from_bytes(&[12; 32]).expect("test public key should be valid");
        let other_key =
            NoisePublicKey::from_bytes(&[13; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);

        assert!(
            registrar
                .prepare_registration("different-id", &token, first_key, "agent-four")
                .await
                .is_err(),
            "encrypted Token ID must match the ID that selected the handshake PSK"
        );
        assert!(
            registrar
                .prepare_registration(
                    "token-test",
                    &registration_token("token-test", 8),
                    first_key,
                    "agent-four",
                )
                .await
                .is_err(),
            "full Token must carry the PSK stored for its public ID"
        );

        registrar
            .prepare_registration("token-test", &token, first_key, "agent-four")
            .await
            .expect("first binding should succeed");
        assert!(
            registrar
                .prepare_registration("token-test", &token, other_key, "agent-five")
                .await
                .is_err(),
            "one Token must not bind a second Agent"
        );
        assert!(
            registrar
                .prepare_registration("token-test", &token, first_key, "renamed-agent")
                .await
                .is_ok(),
            "changing a display name must not change Agent identity"
        );
    }

    #[tokio::test]
    async fn independent_agents_may_share_the_same_display_name() {
        let registrar = registrar_with_token().await;
        registration_token::ActiveModel {
            token_id: Set("token-two".to_owned()),
            psk: Set(vec![8; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            used_at: Set(None),
        }
        .insert(registrar.database.connection())
        .await
        .expect("second registration token should insert");

        let first = registrar
            .prepare_registration(
                "token-test",
                &registration_token("token-test", 7),
                NoisePublicKey::from_bytes(&[14; 32]).expect("first key should be valid"),
                "shared-name",
            )
            .await
            .expect("first Agent should prepare");
        let second = registrar
            .prepare_registration(
                "token-two",
                &registration_token("token-two", 8),
                NoisePublicKey::from_bytes(&[15; 32]).expect("second key should be valid"),
                "shared-name",
            )
            .await
            .expect("same display name should not identify an Agent");

        assert_ne!(first.agent_id, second.agent_id);
    }

    #[tokio::test]
    async fn commit_consumes_the_token_and_is_idempotent() {
        let registrar = registrar_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[10; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);
        let pending = registrar
            .prepare_registration("token-test", &token, public_key, "agent-two")
            .await
            .expect("prepare should succeed");
        assert!(
            agent::Entity::find_by_id(&pending.agent_id)
                .one(registrar.database.connection())
                .await
                .expect("prepared Agent lookup should work")
                .is_none(),
            "prepare must not create an active Agent record"
        );
        assert!(
            registrar
                .resolve_registration_psk("token-test")
                .await
                .is_ok(),
            "prepare must not consume the token"
        );

        registrar
            .commit_registration(&pending)
            .await
            .expect("commit should succeed");
        let committed_agent = agent::Entity::find_by_id(&pending.agent_id)
            .one(registrar.database.connection())
            .await
            .expect("committed Agent lookup should work")
            .expect("commit should create the Agent record");
        assert_eq!(committed_agent.status, "active");
        registrar
            .commit_registration(&pending)
            .await
            .expect("commit retry should be idempotent");
        assert!(
            registrar
                .resolve_registration_psk("token-test")
                .await
                .is_err(),
            "committed token must no longer resolve"
        );
    }

    #[tokio::test]
    async fn only_active_agent_is_authorized_and_revocation_is_reported() {
        let registrar = registrar_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[11; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);
        let pending = registrar
            .prepare_registration("token-test", &token, public_key, "agent-three")
            .await
            .expect("prepare should succeed");
        assert!(matches!(
            registrar.authorize_agent(public_key).await,
            Err(AgentAuthorizationError::Unauthorized)
        ));

        registrar
            .commit_registration(&pending)
            .await
            .expect("commit should succeed");
        let authorized = registrar
            .authorize_agent(public_key)
            .await
            .expect("active Agent should authorize");
        assert_eq!(authorized.agent_id, pending.agent_id);
        assert_eq!(authorized.public_key, public_key);

        let model = agent::Entity::find_by_id(&pending.agent_id)
            .one(registrar.database.connection())
            .await
            .expect("Agent lookup should work")
            .expect("Agent should exist");
        let mut revoked: agent::ActiveModel = model.into();
        revoked.status = Set("revoked".to_owned());
        revoked.revoked_at = Set(Some(2));
        revoked
            .update(registrar.database.connection())
            .await
            .expect("Agent should revoke");

        assert!(matches!(
            registrar.authorize_agent(public_key).await,
            Err(AgentAuthorizationError::Revoked)
        ));
    }
}
