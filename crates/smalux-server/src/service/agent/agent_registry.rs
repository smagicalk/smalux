use std::{fmt, sync::Arc, time::Duration};

use crate::database::{
    DatabaseError, PendingAgentRegistration, PersistedAgentAuthorization,
    PrepareAgentRegistrationError, ServerDatabase,
};
use secrecy::{ExposeSecret, SecretString};
use smalux_protocol::{
    noise::NoisePublicKey,
    tonic_transport::{
        parse_registration_credential, validate_agent_display_name, validate_registration_token_id,
    },
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// prepare 阶段可安全映射到协议错误码的失败类型。
///
/// Token/身份冲突和持久化错误由数据库层映射而来。
#[derive(Debug, thiserror::Error)]
pub(crate) enum PrepareRegistrationError {
    #[error("registration token is invalid")]
    InvalidToken,
    #[error("registration token is already used or bound")]
    TokenAlreadyUsed,
    #[error("Agent identity is already registered")]
    AgentAlreadyRegistered,
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

/// 新签发、只应向授权调用方展示一次的 Agent 注册凭据。
#[allow(dead_code)] // 下一步由 CLI 或受保护管理 API 消费；领域接口先保持完整。
pub(crate) struct IssuedRegistrationToken {
    token_id: String,
    credential: SecretString,
    expires_at_unix_micros: Option<i64>,
}

#[allow(dead_code)]
impl IssuedRegistrationToken {
    /// 返回握手首帧中公开携带的 Token ID。
    pub(crate) fn token_id(&self) -> &str {
        &self.token_id
    }

    /// 显式暴露完整 `token_id.psk`，仅供未来 CLI 或受保护管理 API 展示一次。
    pub(crate) fn expose_credential(&self) -> &str {
        self.credential.expose_secret()
    }

    /// 返回数据库保存的 Unix 微秒过期时间；`None` 表示永久有效。
    pub(crate) fn expires_at_unix_micros(&self) -> Option<i64> {
        self.expires_at_unix_micros
    }
}

impl fmt::Debug for IssuedRegistrationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedRegistrationToken")
            .field("token_id", &self.token_id)
            .field("credential", &"<redacted>")
            .field("expires_at_unix_micros", &self.expires_at_unix_micros)
            .finish()
    }
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

pub(crate) struct AgentRegistry {
    /// 注册事务最终由该数据库连接池持久化；所有查询方法均保留异步数据库边界。
    database: Arc<ServerDatabase>,
}

pub(crate) const REGISTRATION_CLEANUP_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(60);

impl AgentRegistry {
    pub fn new(database: Arc<ServerDatabase>) -> Self {
        tracing::debug!(
            backend = database.backend_label(),
            "creating database-backed Agent registry"
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

    /// 创建一条供首次 XXpsk3 注册使用的一次性 Token。
    ///
    /// 使用操作系统 CSPRNG 生成公开 Token ID 与 32 字节 PSK，将可选展示名称、状态和
    /// 过期时间写入 `registration_tokens` 后，向调用者返回一次
    /// `token_id.64位十六进制PSK`。未指定展示名称时，prepare 会使用生成的 Agent ID。
    /// 调用方只能把完整凭据交给授权操作者，绝不能记录完整 Token 或 PSK。
    #[allow(dead_code)]
    pub(crate) async fn create_registration_token(
        &self,
        display_name: Option<String>,
        valid_for: Option<Duration>,
    ) -> anyhow::Result<IssuedRegistrationToken> {
        if let Some(display_name) = display_name.as_deref() {
            validate_agent_display_name(display_name)
                .map_err(|_| anyhow::anyhow!("Agent display name is invalid"))?;
        }
        if valid_for.is_some_and(|duration| duration.is_zero()) {
            anyhow::bail!("registration Token validity duration must be greater than zero");
        }
        let mut id = [0_u8; 16];
        let mut psk = [0_u8; 32];
        getrandom::fill(&mut id).map_err(|error| {
            anyhow::anyhow!("failed to generate registration Token ID: {error}")
        })?;
        getrandom::fill(&mut psk)
            .map_err(|error| anyhow::anyhow!("failed to generate registration PSK: {error}"))?;
        let token_id = encode_hex(&id);
        let credential = SecretString::from(format!("{token_id}.{}", encode_hex(&psk)));
        let expires_at_unix_micros = self
            .database
            .insert_registration_token(&token_id, &psk, display_name.as_deref(), valid_for)
            .await?;
        tracing::info!(
            token_id = %token_id,
            expires = expires_at_unix_micros.is_some(),
            database_backend = self.database.backend_label(),
            "created Agent registration Token"
        );
        Ok(IssuedRegistrationToken {
            token_id,
            credential,
            expires_at_unix_micros,
        })
    }

    /// 启动可取消的注册尝试清理任务。
    pub(crate) fn start_cleanup_task(
        self: &Arc<Self>,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REGISTRATION_CLEANUP_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::debug!("Agent registration cleanup task cancelled");
                        return;
                    }
                    _ = ticker.tick() => {
                        if let Err(error) = registry.cleanup_expired_registrations().await {
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

    /// 校验握手 Token 和 Agent 公钥，并原子持久化 pending 注册。
    ///
    /// 同一 Token 与公钥的重试返回相同事务 ID。展示名称来自 Server 签发的 Token；
    /// 未指定时使用生成的 Agent ID。prepare 阶段只创建注册尝试，commit 才会创建
    /// `active` Agent。
    pub(crate) async fn prepare_registration(
        &self,
        token_id: &str,
        registration_token: &str,
        agent_public_key: NoisePublicKey,
    ) -> Result<PendingRegistration, PrepareRegistrationError> {
        let (request_token_id, request_psk) = parse_registration_credential(registration_token)
            .map_err(|_| PrepareRegistrationError::InvalidToken)?;
        if request_token_id != token_id {
            return Err(PrepareRegistrationError::InvalidToken);
        }
        let pending = self
            .database
            .prepare_agent_registration(token_id, &request_psk, agent_public_key.as_bytes())
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

    /// 按稳定 Agent ID 复查授权状态，用于关闭授权与管理吊销之间的竞态窗口。
    pub(crate) async fn is_agent_active(&self, agent_id: &str) -> anyhow::Result<bool> {
        Ok(self.database.is_agent_active(agent_id).await?)
    }
}

/// 把随机字节编码成固定长度小写十六进制，不引入额外序列化格式。
fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use sea_orm::{ActiveModelTrait, EntityTrait, PaginatorTrait, Set};

    use crate::database::{
        DatabaseConfig, ServerDatabase,
        entity::{agent, agent_registration, registration_token},
    };
    use smalux_protocol::{noise::NoisePublicKey, tonic_transport::parse_registration_credential};

    use super::{AgentAuthorizationError, AgentRegistry};

    async fn empty_registry() -> AgentRegistry {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .expect("database should connect"),
        );
        AgentRegistry::new(database)
    }

    async fn registry_with_token() -> AgentRegistry {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .expect("database should connect"),
        );
        registration_token::ActiveModel {
            token_id: Set("token-test".to_owned()),
            psk: Set(vec![7; 32]),
            display_name: Set(Some("agent-one".to_owned())),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            used_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("registration token should insert");
        AgentRegistry::new(database)
    }

    fn registration_token(token_id: &str, byte: u8) -> String {
        let secret = std::iter::repeat_n(format!("{byte:02x}"), 32).collect::<String>();
        format!("{token_id}.{secret}")
    }

    #[tokio::test]
    async fn valid_registration_token_resolves_its_psk() {
        let registry = registry_with_token().await;

        assert_eq!(
            registry
                .resolve_registration_psk("token-test")
                .await
                .expect("active token should resolve"),
            [7; 32]
        );
    }

    #[tokio::test]
    async fn generated_permanent_token_round_trips_through_the_registration_resolver() {
        let registry = empty_registry().await;

        let issued = registry
            .create_registration_token(None, None)
            .await
            .expect("registration token should be generated");
        let credential = issued.expose_credential();
        let (_, encoded_psk) = credential
            .split_once('.')
            .expect("generated credential should contain a separator");
        let (credential_id, credential_psk) = parse_registration_credential(credential)
            .expect("generated credential should use the protocol format");

        assert_eq!(credential_id, issued.token_id());
        assert_eq!(issued.token_id().len(), 32);
        assert!(
            issued
                .token_id()
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
        assert_eq!(encoded_psk.len(), 64);
        assert!(
            encoded_psk
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
        assert_eq!(issued.expires_at_unix_micros(), None);
        assert_eq!(
            registry
                .resolve_registration_psk(issued.token_id())
                .await
                .expect("persisted token should resolve"),
            credential_psk
        );
    }

    #[tokio::test]
    async fn token_display_name_is_bound_by_the_server_during_prepare() {
        let registry = empty_registry().await;
        let issued = registry
            .create_registration_token(Some("database-node".to_owned()), None)
            .await
            .expect("registration token should be generated");
        let public_key =
            NoisePublicKey::from_bytes(&[21; 32]).expect("test public key should be valid");

        let pending = registry
            .prepare_registration(issued.token_id(), issued.expose_credential(), public_key)
            .await
            .expect("registration should prepare");
        let registration = agent_registration::Entity::find_by_id(
            uuid::Uuid::from_bytes(pending.registration_id).to_string(),
        )
        .one(registry.database.connection())
        .await
        .expect("registration lookup should work")
        .expect("registration should exist");

        assert_eq!(registration.agent_name, "database-node");
    }

    #[tokio::test]
    async fn invalid_token_display_name_is_rejected_before_persistence() {
        let registry = empty_registry().await;

        let error = registry
            .create_registration_token(Some("invalid display name".to_owned()), None)
            .await
            .expect_err("invalid display name should be rejected");

        assert_eq!(error.to_string(), "Agent display name is invalid");
        assert_eq!(
            registration_token::Entity::find()
                .count(registry.database.connection())
                .await
                .expect("registration token count should work"),
            0
        );
    }

    #[tokio::test]
    async fn token_without_display_name_defaults_to_the_server_generated_agent_id() {
        let registry = empty_registry().await;
        let issued = registry
            .create_registration_token(None, None)
            .await
            .expect("registration token should be generated");
        let public_key =
            NoisePublicKey::from_bytes(&[22; 32]).expect("test public key should be valid");

        let pending = registry
            .prepare_registration(issued.token_id(), issued.expose_credential(), public_key)
            .await
            .expect("registration should prepare");
        let registration = agent_registration::Entity::find_by_id(
            uuid::Uuid::from_bytes(pending.registration_id).to_string(),
        )
        .one(registry.database.connection())
        .await
        .expect("registration lookup should work")
        .expect("registration should exist");

        assert_eq!(registration.agent_name, pending.agent_id);
    }

    #[tokio::test]
    async fn generated_temporary_token_reports_a_future_expiration_and_remains_resolvable() {
        let registry = empty_registry().await;
        let valid_for = Duration::from_secs(60);
        let before = std::time::SystemTime::now();

        let issued = registry
            .create_registration_token(None, Some(valid_for))
            .await
            .expect("temporary registration token should be generated");
        let after = std::time::SystemTime::now();
        let expires_at = issued
            .expires_at_unix_micros()
            .expect("temporary token should report its expiration");
        let earliest = before
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
            + valid_for.as_micros();
        let latest = after
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros()
            + valid_for.as_micros();

        assert!((earliest..=latest).contains(&(expires_at as u128)));
        assert!(
            registry
                .resolve_registration_psk(issued.token_id())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn token_generation_rejects_zero_validity_duration() {
        let registry = empty_registry().await;

        let error = registry
            .create_registration_token(None, Some(Duration::ZERO))
            .await
            .expect_err("zero validity must be rejected");

        assert!(error.to_string().contains("greater than zero"));
    }

    #[tokio::test]
    async fn token_generation_rejects_a_duration_that_cannot_fit_the_database_timestamp() {
        let registry = empty_registry().await;

        let error = registry
            .create_registration_token(None, Some(Duration::MAX))
            .await
            .expect_err("overflowing validity must be rejected");

        assert!(error.to_string().contains("does not fit"));
    }

    #[tokio::test]
    async fn issued_token_debug_output_redacts_the_secret_credential() {
        let registry = empty_registry().await;
        let issued = registry
            .create_registration_token(None, None)
            .await
            .expect("registration token should be generated");
        let secret = issued.expose_credential().to_owned();

        let debug = format!("{issued:?}");

        assert!(!debug.contains(&secret));
        assert!(debug.contains(issued.token_id()));
        assert!(debug.contains("<redacted>"));
    }

    #[tokio::test]
    async fn independently_generated_tokens_have_distinct_ids_and_psks() {
        let registry = empty_registry().await;

        let first = registry
            .create_registration_token(None, None)
            .await
            .expect("first registration token should be generated");
        let second = registry
            .create_registration_token(None, None)
            .await
            .expect("second registration token should be generated");

        assert_ne!(first.token_id(), second.token_id());
        assert_ne!(first.expose_credential(), second.expose_credential());
        assert!(
            registry
                .resolve_registration_psk(first.token_id())
                .await
                .is_ok()
        );
        assert!(
            registry
                .resolve_registration_psk(second.token_id())
                .await
                .is_ok()
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
                display_name: Set(None),
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
        let registry = AgentRegistry::new(database);

        for token_id in [
            "token-used",
            "token-revoked",
            "token-expired",
            "token-malformed",
            "token-missing",
        ] {
            assert!(
                registry.resolve_registration_psk(token_id).await.is_err(),
                "{token_id} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn identical_prepare_retry_returns_the_same_registration() {
        let registry = registry_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[9; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);

        let first = registry
            .prepare_registration("token-test", &token, public_key)
            .await
            .expect("first prepare should succeed");
        let retry = registry
            .prepare_registration("token-test", &token, public_key)
            .await
            .expect("identical prepare should be idempotent");

        assert_eq!(retry, first);
        assert!(!first.agent_id.is_empty());
    }

    #[tokio::test]
    async fn expired_prepare_can_register_again_with_the_same_public_key() {
        let registry = registry_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[16; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);
        let first = registry
            .prepare_registration("token-test", &token, public_key)
            .await
            .expect("first prepare should succeed");

        let model = agent_registration::Entity::find_by_id(
            uuid::Uuid::from_bytes(first.registration_id).to_string(),
        )
        .one(registry.database.connection())
        .await
        .expect("registration lookup should work")
        .expect("registration should exist");
        let mut expired: agent_registration::ActiveModel = model.into();
        expired.expires_at = Set(Some(1));
        expired
            .update(registry.database.connection())
            .await
            .expect("registration should be expired");
        assert_eq!(
            registry
                .cleanup_expired_registrations()
                .await
                .expect("expired registration cleanup should work"),
            1
        );

        let second = registry
            .prepare_registration("token-test", &token, public_key)
            .await
            .expect("expired registration should be replaceable");
        assert_ne!(second.registration_id, first.registration_id);
        assert_ne!(second.agent_id, first.agent_id);
        assert!(
            agent::Entity::find_by_id(&second.agent_id)
                .one(registry.database.connection())
                .await
                .expect("prepared Agent lookup should work")
                .is_none()
        );
    }

    #[tokio::test]
    async fn prepare_rejects_mismatched_token_credentials_and_identity_bindings() {
        let registry = registry_with_token().await;
        let first_key =
            NoisePublicKey::from_bytes(&[12; 32]).expect("test public key should be valid");
        let other_key =
            NoisePublicKey::from_bytes(&[13; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);

        assert!(
            registry
                .prepare_registration("different-id", &token, first_key)
                .await
                .is_err(),
            "encrypted Token ID must match the ID that selected the handshake PSK"
        );
        assert!(
            registry
                .prepare_registration(
                    "token-test",
                    &registration_token("token-test", 8),
                    first_key,
                )
                .await
                .is_err(),
            "full Token must carry the PSK stored for its public ID"
        );

        registry
            .prepare_registration("token-test", &token, first_key)
            .await
            .expect("first binding should succeed");
        assert!(
            registry
                .prepare_registration("token-test", &token, other_key)
                .await
                .is_err(),
            "one Token must not bind a second Agent"
        );
        assert!(
            registry
                .prepare_registration("token-test", &token, first_key)
                .await
                .is_ok(),
            "repeating an identical binding must remain idempotent"
        );
    }

    #[tokio::test]
    async fn independent_agents_may_share_the_same_display_name() {
        let registry = registry_with_token().await;
        registration_token::ActiveModel {
            token_id: Set("token-two".to_owned()),
            psk: Set(vec![8; 32]),
            display_name: Set(Some("agent-one".to_owned())),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            used_at: Set(None),
        }
        .insert(registry.database.connection())
        .await
        .expect("second registration token should insert");

        let first = registry
            .prepare_registration(
                "token-test",
                &registration_token("token-test", 7),
                NoisePublicKey::from_bytes(&[14; 32]).expect("first key should be valid"),
            )
            .await
            .expect("first Agent should prepare");
        let second = registry
            .prepare_registration(
                "token-two",
                &registration_token("token-two", 8),
                NoisePublicKey::from_bytes(&[15; 32]).expect("second key should be valid"),
            )
            .await
            .expect("same display name should not identify an Agent");

        assert_ne!(first.agent_id, second.agent_id);
    }

    #[tokio::test]
    async fn commit_consumes_the_token_and_is_idempotent() {
        let registry = registry_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[10; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);
        let pending = registry
            .prepare_registration("token-test", &token, public_key)
            .await
            .expect("prepare should succeed");
        assert!(
            agent::Entity::find_by_id(&pending.agent_id)
                .one(registry.database.connection())
                .await
                .expect("prepared Agent lookup should work")
                .is_none(),
            "prepare must not create an active Agent record"
        );
        assert!(
            registry
                .resolve_registration_psk("token-test")
                .await
                .is_ok(),
            "prepare must not consume the token"
        );

        registry
            .commit_registration(&pending)
            .await
            .expect("commit should succeed");
        let committed_agent = agent::Entity::find_by_id(&pending.agent_id)
            .one(registry.database.connection())
            .await
            .expect("committed Agent lookup should work")
            .expect("commit should create the Agent record");
        assert_eq!(committed_agent.status, "active");
        registry
            .commit_registration(&pending)
            .await
            .expect("commit retry should be idempotent");
        assert!(
            registry
                .resolve_registration_psk("token-test")
                .await
                .is_err(),
            "committed token must no longer resolve"
        );
    }

    #[tokio::test]
    async fn only_active_agent_is_authorized_and_revocation_is_reported() {
        let registry = registry_with_token().await;
        let public_key =
            NoisePublicKey::from_bytes(&[11; 32]).expect("test public key should be valid");
        let token = registration_token("token-test", 7);
        let pending = registry
            .prepare_registration("token-test", &token, public_key)
            .await
            .expect("prepare should succeed");
        assert!(matches!(
            registry.authorize_agent(public_key).await,
            Err(AgentAuthorizationError::Unauthorized)
        ));

        registry
            .commit_registration(&pending)
            .await
            .expect("commit should succeed");
        let authorized = registry
            .authorize_agent(public_key)
            .await
            .expect("active Agent should authorize");
        assert_eq!(authorized.agent_id, pending.agent_id);
        assert_eq!(authorized.public_key, public_key);

        let model = agent::Entity::find_by_id(&pending.agent_id)
            .one(registry.database.connection())
            .await
            .expect("Agent lookup should work")
            .expect("Agent should exist");
        let mut revoked: agent::ActiveModel = model.into();
        revoked.status = Set("revoked".to_owned());
        revoked.revoked_at = Set(Some(2));
        revoked
            .update(registry.database.connection())
            .await
            .expect("Agent should revoke");

        assert!(matches!(
            registry.authorize_agent(public_key).await,
            Err(AgentAuthorizationError::Revoked)
        ));
    }
}
