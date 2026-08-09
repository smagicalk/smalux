use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::database::{
    ServerDatabase,
    entity::{agent, agent_registration, registration_token},
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DbErr, EntityTrait, QueryFilter, Set, TransactionTrait,
};
use smalux_protocol::{noise::NoisePublicKey, tonic_transport::validate_registration_token_id};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// XXpsk3 `RegistrationPrepared` 阶段需要暂存的注册事务。
///
/// `registration_id` 和预分配 Agent ID 来自数据库注册事务，不能只保存在当前进程内。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingRegistration {
    pub(crate) registration_id: [u8; 16],
    pub(crate) agent_id: String,
}

/// IK 握手通过后供业务层使用的授权结果。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedAgent {
    pub(crate) agent_id: String,
    pub(crate) public_key: NoisePublicKey,
}

/// 数据库存储的稳定 Agent 生命周期值；数据库仍使用字符串以兼容三种后端。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AgentStatus {
    Active,
    Revoked,
}

impl AgentStatus {
    const ACTIVE: &'static str = "active";
    const REVOKED: &'static str = "revoked";

    fn parse(value: &str) -> Option<Self> {
        match value {
            Self::ACTIVE => Some(Self::Active),
            Self::REVOKED => Some(Self::Revoked),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Active => Self::ACTIVE,
            Self::Revoked => Self::REVOKED,
        }
    }
}

/// 数据库存储的稳定注册 Token 生命周期值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenStatus {
    Active,
    Used,
    Revoked,
}

impl TokenStatus {
    const ACTIVE: &'static str = "active";
    const USED: &'static str = "used";
    const REVOKED: &'static str = "revoked";

    fn parse(value: &str) -> Option<Self> {
        match value {
            Self::ACTIVE => Some(Self::Active),
            Self::USED => Some(Self::Used),
            Self::REVOKED => Some(Self::Revoked),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Active => Self::ACTIVE,
            Self::Used => Self::USED,
            Self::Revoked => Self::REVOKED,
        }
    }
}

/// 数据库存储的稳定注册事务状态值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RegistrationStatus {
    Prepared,
    Committed,
    Expired,
}

impl RegistrationStatus {
    const PREPARED: &'static str = "prepared";
    const COMMITTED: &'static str = "committed";
    const EXPIRED: &'static str = "expired";

    fn parse(value: &str) -> Option<Self> {
        match value {
            Self::PREPARED => Some(Self::Prepared),
            Self::COMMITTED => Some(Self::Committed),
            Self::EXPIRED => Some(Self::Expired),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => Self::PREPARED,
            Self::Committed => Self::COMMITTED,
            Self::Expired => Self::EXPIRED,
        }
    }
}

/// prepare 阶段可安全映射到协议错误码的失败类型。
///
/// 数据库和本地状态错误保留在 `Internal` 的 source 中供 Server 日志诊断，但其
/// 详细文本绝不能回传给 Agent。
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
    Database(#[from] DbErr),
    #[error("registration state is invalid")]
    Internal(#[source] anyhow::Error),
}

pub(crate) struct AgentRegistrar {
    /// 注册事务最终由该数据库连接池持久化；所有查询方法均保留异步数据库边界。
    database: Arc<ServerDatabase>,
}

/// pending 注册允许 Agent 断线重试的最长时间。
const PENDING_REGISTRATION_TTL_MICROS: i64 = 10 * 60 * 1_000_000;
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
        let now = unix_timestamp_micros()?;
        let prepared = agent_registration::Entity::delete_many()
            .filter(agent_registration::Column::Status.eq(RegistrationStatus::Prepared.as_str()))
            .filter(agent_registration::Column::ExpiresAt.lt(now))
            .filter(agent_registration::Column::AgentId.is_null())
            .exec(self.database.connection())
            .await?
            .rows_affected;
        let expired = agent_registration::Entity::delete_many()
            .filter(agent_registration::Column::Status.eq(RegistrationStatus::Expired.as_str()))
            .filter(agent_registration::Column::AgentId.is_null())
            .exec(self.database.connection())
            .await?
            .rows_affected;
        let deleted = prepared + expired;
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
        let token = registration_token::Entity::find_by_id(token_id)
            .one(self.database.connection())
            .await?
            .ok_or_else(|| anyhow::anyhow!("registration token is invalid"))?;
        let now = unix_timestamp_micros()?;
        if TokenStatus::parse(&token.status) != Some(TokenStatus::Active)
            || token.used_at.is_some()
            || token.expires_at.is_some_and(|expires_at| expires_at <= now)
        {
            anyhow::bail!("registration token is invalid");
        }
        let psk: [u8; 32] = token
            .psk
            .try_into()
            .map_err(|_| anyhow::anyhow!("registration token PSK is invalid"))?;
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

        let public_key = agent_public_key.as_bytes().to_vec();
        let now = unix_timestamp_micros().map_err(PrepareRegistrationError::Internal)?;
        let transaction = self.database.connection().begin().await?;

        let token = registration_token::Entity::find_by_id(token_id)
            .one(&transaction)
            .await?
            .ok_or(PrepareRegistrationError::InvalidToken)?;
        if TokenStatus::parse(&token.status) == Some(TokenStatus::Used) || token.used_at.is_some() {
            return Err(PrepareRegistrationError::TokenAlreadyUsed);
        }
        if TokenStatus::parse(&token.status) != Some(TokenStatus::Active)
            || token.expires_at.is_some_and(|expires_at| expires_at <= now)
            || !constant_time_eq(&token.psk, &request_psk)
        {
            return Err(PrepareRegistrationError::InvalidToken);
        }

        // 首次注册还没有 Agent 行，只能使用已认证的公钥定位注册尝试。
        // 过期尝试在同一事务中删除，从而释放公钥和 Token 的唯一约束。
        if let Some(existing_registration) = agent_registration::Entity::find()
            .filter(agent_registration::Column::AgentPublicKey.eq(public_key.clone()))
            .one(&transaction)
            .await?
        {
            if registration_is_expired(&existing_registration, now) {
                agent_registration::Entity::delete_by_id(&existing_registration.registration_id)
                    .exec(&transaction)
                    .await?;
            } else if existing_registration.token_id == token_id
                && matches!(
                    RegistrationStatus::parse(&existing_registration.status),
                    Some(RegistrationStatus::Prepared | RegistrationStatus::Committed)
                )
            {
                let registration_id = parse_registration_id(&existing_registration.registration_id)
                    .map_err(PrepareRegistrationError::Internal)?;
                let agent_id = existing_registration.reserved_agent_id.clone();
                transaction.commit().await?;
                return Ok(PendingRegistration {
                    registration_id,
                    agent_id,
                });
            } else {
                return Err(PrepareRegistrationError::AgentAlreadyRegistered);
            }
        }

        // 兼容旧数据：已有 Agent 记录即使缺少注册事务，也不能被另一个 Token 绑定。
        if agent::Entity::find()
            .filter(agent::Column::PublicKey.eq(public_key.clone()))
            .one(&transaction)
            .await?
            .is_some()
        {
            return Err(PrepareRegistrationError::AgentAlreadyRegistered);
        }

        if let Some(existing_token_registration) = agent_registration::Entity::find()
            .filter(agent_registration::Column::TokenId.eq(token_id))
            .one(&transaction)
            .await?
        {
            if registration_is_expired(&existing_token_registration, now) {
                agent_registration::Entity::delete_by_id(
                    &existing_token_registration.registration_id,
                )
                .exec(&transaction)
                .await?;
            } else {
                return Err(PrepareRegistrationError::TokenAlreadyUsed);
            }
        }
        let registration_uuid = Uuid::new_v4();
        let registration_id = registration_uuid.into_bytes();
        let agent_id = Uuid::new_v4().to_string();
        agent_registration::ActiveModel {
            registration_id: Set(registration_uuid.to_string()),
            token_id: Set(token_id.to_owned()),
            agent_id: Set(None),
            reserved_agent_id: Set(agent_id.clone()),
            agent_name: Set(agent_name.to_owned()),
            agent_public_key: Set(public_key),
            status: Set(RegistrationStatus::Prepared.as_str().to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            expires_at: Set(Some(now + PENDING_REGISTRATION_TTL_MICROS)),
            committed_at: Set(None),
        }
        .insert(&transaction)
        .await?;
        transaction.commit().await?;

        tracing::info!(
            registration_id = %registration_uuid,
            agent_id = %agent_id,
            agent_key_id = ?agent_public_key.key_id(),
            "prepared database-backed Agent registration"
        );
        Ok(PendingRegistration {
            registration_id,
            agent_id,
        })
    }

    /// 原子提交注册：激活 Agent、完成注册事务并一次性消费 Token。
    ///
    /// 重复提交同一事务保持成功；事务 ID、Agent ID、Token 状态或过期时间不匹配时
    /// 整体回滚，不能留下“Agent 已激活但 Token 未消费”的半完成状态。
    pub(crate) async fn commit_registration(
        &self,
        pending: &PendingRegistration,
    ) -> anyhow::Result<()> {
        let registration_id = Uuid::from_bytes(pending.registration_id).to_string();
        let now = unix_timestamp_micros()?;
        let transaction = self.database.connection().begin().await?;
        let registration = agent_registration::Entity::find_by_id(&registration_id)
            .one(&transaction)
            .await?
            .ok_or_else(|| anyhow::anyhow!("registration transaction is unknown"))?;
        if registration.reserved_agent_id != pending.agent_id {
            anyhow::bail!("registration transaction does not match Agent");
        }

        // committed 是终态；网络确认丢失后的重试不应再次消费 Token。
        if RegistrationStatus::parse(&registration.status) == Some(RegistrationStatus::Committed) {
            let agent_id = registration
                .agent_id
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("committed registration has no Agent ID"))?;
            let existing_agent = agent::Entity::find_by_id(agent_id)
                .one(&transaction)
                .await?
                .ok_or_else(|| anyhow::anyhow!("committed Agent record is missing"))?;
            if AgentStatus::parse(&existing_agent.status) != Some(AgentStatus::Active) {
                anyhow::bail!("committed Agent is not active");
            }
            transaction.commit().await?;
            return Ok(());
        }
        if RegistrationStatus::parse(&registration.status) != Some(RegistrationStatus::Prepared)
            || registration
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            anyhow::bail!("registration transaction is not committable");
        }

        let token = registration_token::Entity::find_by_id(&registration.token_id)
            .one(&transaction)
            .await?
            .ok_or_else(|| anyhow::anyhow!("registration token is invalid"))?;
        if TokenStatus::parse(&token.status) != Some(TokenStatus::Active)
            || token.used_at.is_some()
            || token.expires_at.is_some_and(|expires_at| expires_at <= now)
        {
            anyhow::bail!("registration token is invalid");
        }

        if agent::Entity::find_by_id(&registration.reserved_agent_id)
            .one(&transaction)
            .await?
            .is_some()
        {
            anyhow::bail!("reserved Agent ID is already in use");
        }

        agent::ActiveModel {
            agent_id: Set(registration.reserved_agent_id.clone()),
            name: Set(registration.agent_name.clone()),
            public_key: Set(registration.agent_public_key.clone()),
            status: Set(AgentStatus::Active.as_str().to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        }
        .insert(&transaction)
        .await?;

        let mut committed_registration: agent_registration::ActiveModel = registration.into();
        committed_registration.agent_id = Set(Some(pending.agent_id.clone()));
        committed_registration.status = Set(RegistrationStatus::Committed.as_str().to_owned());
        committed_registration.updated_at = Set(now);
        committed_registration.committed_at = Set(Some(now));
        committed_registration.update(&transaction).await?;

        let mut consumed_token: registration_token::ActiveModel = token.into();
        consumed_token.status = Set(TokenStatus::Used.as_str().to_owned());
        consumed_token.updated_at = Set(now);
        consumed_token.used_at = Set(Some(now));
        consumed_token.update(&transaction).await?;
        transaction.commit().await?;

        tracing::info!(
            registration_id = %registration_id,
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
    ) -> anyhow::Result<AuthorizedAgent> {
        let agent = agent::Entity::find()
            .filter(agent::Column::PublicKey.eq(agent_public_key.as_bytes().to_vec()))
            .one(self.database.connection())
            .await?
            .ok_or_else(|| anyhow::anyhow!("Agent is not authorized"))?;
        if AgentStatus::parse(&agent.status) != Some(AgentStatus::Active)
            || agent.revoked_at.is_some()
        {
            anyhow::bail!("Agent is not authorized");
        }
        tracing::debug!(
            agent_id = %agent.agent_id,
            agent_key_id = ?agent_public_key.key_id(),
            "authorized Agent by its Noise identity"
        );
        Ok(AuthorizedAgent {
            agent_id: agent.agent_id,
            public_key: agent_public_key,
        })
    }

    /// 检查 Agent 是否已经被吊销；吊销结果应优先于业务消息处理。
    ///
    /// 未登记公钥返回 `false`，因为“未知身份”和“已知但已吊销”是两个不同状态；
    /// 调用方仍必须继续调用 [`Self::authorize_agent`]，不能把 `false` 当作已授权。
    pub(crate) async fn is_revoked(
        &self,
        agent_public_key: NoisePublicKey,
    ) -> anyhow::Result<bool> {
        let agent = agent::Entity::find()
            .filter(agent::Column::PublicKey.eq(agent_public_key.as_bytes().to_vec()))
            .one(self.database.connection())
            .await?;
        let revoked = agent.as_ref().is_some_and(|agent| {
            AgentStatus::parse(&agent.status) == Some(AgentStatus::Revoked)
                || agent.revoked_at.is_some()
        });
        tracing::debug!(
            agent_key_id = ?agent_public_key.key_id(),
            agent_id = agent.as_ref().map(|agent| agent.agent_id.as_str()),
            revoked,
            "checked Agent revocation state"
        );
        Ok(revoked)
    }
}

/// 返回统一的 Unix 微秒时间戳，供 Token 和注册事务进行跨数据库比较。
fn unix_timestamp_micros() -> anyhow::Result<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(duration.as_micros().try_into()?)
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

/// 用固定循环比较数据库 PSK 和请求 PSK，避免根据首个不同字节提前返回。
fn constant_time_eq(expected: &[u8], actual: &[u8; 32]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    expected
        .iter()
        .zip(actual)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

/// 将数据库 UUID 字符串恢复为协议固定的 16 字节事务 ID。
fn parse_registration_id(value: &str) -> anyhow::Result<[u8; 16]> {
    Ok(Uuid::parse_str(value)?.into_bytes())
}

/// 判断注册尝试是否已经进入可清理状态。
fn registration_is_expired(registration: &agent_registration::Model, now: i64) -> bool {
    RegistrationStatus::parse(&registration.status) == Some(RegistrationStatus::Expired)
        || (RegistrationStatus::parse(&registration.status) == Some(RegistrationStatus::Prepared)
            && registration
                .expires_at
                .is_some_and(|expires_at| expires_at <= now))
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

    use super::AgentRegistrar;

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
        assert!(registrar.authorize_agent(public_key).await.is_err());

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
        assert!(
            !registrar
                .is_revoked(public_key)
                .await
                .expect("lookup should work")
        );

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

        assert!(
            registrar
                .is_revoked(public_key)
                .await
                .expect("lookup should work")
        );
        assert!(registrar.authorize_agent(public_key).await.is_err());
    }
}
