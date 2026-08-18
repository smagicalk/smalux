//! Agent 注册、授权和吊销状态的持久化实现。
//!
//! 本模块是 SeaORM 与注册流程之间的唯一适配器。调用方只需要给出经过协议层解析的
//! Token、公钥和展示名称；查询条件、事务、字符串状态值及跨表一致性都留在这里，
//! 防止业务模块绕过数据库不变量。

use std::time::{SystemTime, UNIX_EPOCH};

use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseTransaction, EntityTrait, QueryFilter, Set,
    TransactionTrait,
};
use uuid::Uuid;

use super::{
    DatabaseError, ServerDatabase,
    entity::{agent, agent_registration, registration_token},
};

/// XXpsk3 prepare 阶段写入数据库后返回的稳定注册标识。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingAgentRegistration {
    /// 协议中使用的固定长度注册事务 ID。
    pub(crate) registration_id: [u8; 16],
    /// prepare 阶段预分配、commit 阶段才会实际创建的 Agent ID。
    pub(crate) agent_id: String,
}

/// 单次数据库查询得到的 IK 身份授权快照。
///
/// 服务层用该结果区分吊销和普通未授权，不需要为同一公钥连续访问数据库两次。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PersistedAgentAuthorization {
    /// Agent 处于 active 状态且没有吊销时间。
    Authorized(String),
    /// Agent 状态或吊销时间明确表示已吊销。
    Revoked,
    /// 公钥不存在，或记录状态不能用于业务会话。
    Unauthorized,
}

/// prepare 事务可以安全映射到协议错误码的失败类型。
///
/// 数据库驱动和不变量错误保留为内部 source，调用方不得把详细文本发送给 Agent。
#[derive(Debug, thiserror::Error)]
pub(crate) enum PrepareAgentRegistrationError {
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

impl ServerDatabase {
    /// 删除过期且尚未 commit 的注册事务，释放 Token 与公钥的唯一索引。
    pub(crate) async fn cleanup_expired_agent_registrations(&self) -> Result<u64, DatabaseError> {
        let now = unix_timestamp_micros()?;
        let deleted = agent_registration::Entity::delete_many()
            .filter(agent_registration::Column::AgentId.is_null())
            .filter(
                Condition::any()
                    .add(agent_registration::Column::Status.eq(RegistrationStatus::EXPIRED))
                    .add(
                        Condition::all()
                            .add(
                                agent_registration::Column::Status.eq(RegistrationStatus::PREPARED),
                            )
                            .add(agent_registration::Column::ExpiresAt.lt(now)),
                    ),
            )
            .exec(self.connection())
            .await?
            .rows_affected;
        Ok(deleted)
    }

    /// 返回仍可用于首次 XXpsk3 握手的 Token PSK。
    ///
    /// 不存在、已消费、已吊销、已过期和格式错误的数据库 PSK 都统一返回 `None`，避免
    /// 握手前向调用方泄露 Token 生命周期细节。
    pub(crate) async fn load_active_registration_psk(
        &self,
        token_id: &str,
    ) -> Result<Option<[u8; 32]>, DatabaseError> {
        let Some(token) = registration_token::Entity::find_by_id(token_id)
            .one(self.connection())
            .await?
        else {
            return Ok(None);
        };
        let now = unix_timestamp_micros()?;
        if TokenStatus::parse(&token.status) != Some(TokenStatus::Active)
            || token.used_at.is_some()
            || token.expires_at.is_some_and(|expires_at| expires_at <= now)
        {
            return Ok(None);
        }
        let psk = token.psk.try_into().map_err(|_| {
            DatabaseError::InvalidAgentRegistration(
                "registration token PSK must be 32 bytes".to_owned(),
            )
        })?;
        Ok(Some(psk))
    }

    /// 校验 Token 状态并原子写入或重用 pending 注册事务。
    ///
    /// 同一 Token 与同一 Agent 公钥的重试返回已有事务；同一个 Token 不能绑定第二把
    /// 公钥。第一版没有历史 Agent 数据，因此不再保留“Agent 记录缺少注册事务”的
    /// 兼容查询。
    pub(crate) async fn prepare_agent_registration(
        &self,
        token_id: &str,
        request_psk: &[u8; 32],
        agent_public_key: &[u8; 32],
        agent_name: &str,
    ) -> Result<PendingAgentRegistration, PrepareAgentRegistrationError> {
        let public_key = agent_public_key.to_vec();
        let now = unix_timestamp_micros().map_err(PrepareAgentRegistrationError::Database)?;
        let transaction = self
            .connection()
            .begin()
            .await
            .map_err(DatabaseError::from)?;

        let token = registration_token::Entity::find_by_id(token_id)
            .one(&transaction)
            .await
            .map_err(DatabaseError::from)?
            .ok_or(PrepareAgentRegistrationError::InvalidToken)?;
        let token_status = TokenStatus::parse(&token.status);
        if token_status == Some(TokenStatus::Used) || token.used_at.is_some() {
            return Err(PrepareAgentRegistrationError::TokenAlreadyUsed);
        }
        if token_status != Some(TokenStatus::Active)
            || token.expires_at.is_some_and(|expires_at| expires_at <= now)
            || !constant_time_eq(&token.psk, request_psk)
        {
            return Err(PrepareAgentRegistrationError::InvalidToken);
        }

        // 首次注册还没有 Agent 行，因此用已认证的公钥定位可重试的注册事务。
        // 过期事务在同一 SQL 事务中删除，随后可用相同 Token 或公钥重新注册。
        if let Some(existing_registration) = agent_registration::Entity::find()
            .filter(agent_registration::Column::AgentPublicKey.eq(public_key.clone()))
            .one(&transaction)
            .await
            .map_err(DatabaseError::from)?
            && !delete_registration_if_expired(&transaction, &existing_registration, now).await?
        {
            if existing_registration.token_id == token_id
                && matches!(
                    RegistrationStatus::parse(&existing_registration.status),
                    Some(RegistrationStatus::Prepared | RegistrationStatus::Committed)
                )
            {
                let registration_id = parse_registration_id(&existing_registration.registration_id)
                    .map_err(PrepareAgentRegistrationError::Internal)?;
                let agent_id = existing_registration.reserved_agent_id.clone();
                transaction.commit().await.map_err(DatabaseError::from)?;
                return Ok(PendingAgentRegistration {
                    registration_id,
                    agent_id,
                });
            }
            return Err(PrepareAgentRegistrationError::AgentAlreadyRegistered);
        }

        if let Some(existing_token_registration) = agent_registration::Entity::find()
            .filter(agent_registration::Column::TokenId.eq(token_id))
            .one(&transaction)
            .await
            .map_err(DatabaseError::from)?
            && !delete_registration_if_expired(&transaction, &existing_token_registration, now)
                .await?
        {
            return Err(PrepareAgentRegistrationError::TokenAlreadyUsed);
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
            status: Set(RegistrationStatus::PREPARED.to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            expires_at: Set(Some(now + PENDING_REGISTRATION_TTL_MICROS)),
            committed_at: Set(None),
        }
        .insert(&transaction)
        .await
        .map_err(DatabaseError::from)?;
        transaction.commit().await.map_err(DatabaseError::from)?;

        Ok(PendingAgentRegistration {
            registration_id,
            agent_id,
        })
    }

    /// 原子创建 active Agent、提交注册事务并消费 Token。
    pub(crate) async fn commit_agent_registration(
        &self,
        pending: &PendingAgentRegistration,
    ) -> Result<(), DatabaseError> {
        let registration_id = Uuid::from_bytes(pending.registration_id).to_string();
        let now = unix_timestamp_micros()?;
        let transaction = self.connection().begin().await?;
        let registration = agent_registration::Entity::find_by_id(&registration_id)
            .one(&transaction)
            .await?
            .ok_or_else(|| {
                DatabaseError::InvalidAgentRegistration(
                    "registration transaction is unknown".to_owned(),
                )
            })?;
        if registration.reserved_agent_id != pending.agent_id {
            return Err(DatabaseError::InvalidAgentRegistration(
                "registration transaction does not match Agent".to_owned(),
            ));
        }

        if RegistrationStatus::parse(&registration.status) == Some(RegistrationStatus::Committed) {
            let agent_id = registration.agent_id.as_deref().ok_or_else(|| {
                DatabaseError::InvalidAgentRegistration(
                    "committed registration has no Agent ID".to_owned(),
                )
            })?;
            let existing_agent = agent::Entity::find_by_id(agent_id)
                .one(&transaction)
                .await?
                .ok_or_else(|| {
                    DatabaseError::InvalidAgentRegistration(
                        "committed Agent record is missing".to_owned(),
                    )
                })?;
            if AgentStatus::parse(&existing_agent.status) != Some(AgentStatus::Active) {
                return Err(DatabaseError::InvalidAgentRegistration(
                    "committed Agent is not active".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok(());
        }
        if RegistrationStatus::parse(&registration.status) != Some(RegistrationStatus::Prepared)
            || registration
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
        {
            return Err(DatabaseError::InvalidAgentRegistration(
                "registration transaction is not committable".to_owned(),
            ));
        }

        let token = registration_token::Entity::find_by_id(&registration.token_id)
            .one(&transaction)
            .await?
            .ok_or_else(|| {
                DatabaseError::InvalidAgentRegistration("registration token is invalid".to_owned())
            })?;
        if TokenStatus::parse(&token.status) != Some(TokenStatus::Active)
            || token.used_at.is_some()
            || token.expires_at.is_some_and(|expires_at| expires_at <= now)
        {
            return Err(DatabaseError::InvalidAgentRegistration(
                "registration token is invalid".to_owned(),
            ));
        }
        if agent::Entity::find_by_id(&registration.reserved_agent_id)
            .one(&transaction)
            .await?
            .is_some()
        {
            return Err(DatabaseError::InvalidAgentRegistration(
                "reserved Agent ID is already in use".to_owned(),
            ));
        }

        agent::ActiveModel {
            agent_id: Set(registration.reserved_agent_id.clone()),
            name: Set(registration.agent_name.clone()),
            public_key: Set(registration.agent_public_key.clone()),
            status: Set(AgentStatus::ACTIVE.to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        }
        .insert(&transaction)
        .await?;

        let mut committed_registration: agent_registration::ActiveModel = registration.into();
        committed_registration.agent_id = Set(Some(pending.agent_id.clone()));
        committed_registration.status = Set(RegistrationStatus::COMMITTED.to_owned());
        committed_registration.updated_at = Set(now);
        committed_registration.committed_at = Set(Some(now));
        committed_registration.update(&transaction).await?;

        let mut consumed_token: registration_token::ActiveModel = token.into();
        consumed_token.status = Set(TokenStatus::USED.to_owned());
        consumed_token.updated_at = Set(now);
        consumed_token.used_at = Set(Some(now));
        consumed_token.update(&transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// 使用一次一致查询判断 IK 身份的授权状态。
    pub(crate) async fn find_agent_authorization(
        &self,
        public_key: &[u8; 32],
    ) -> Result<PersistedAgentAuthorization, DatabaseError> {
        let Some(agent) = agent::Entity::find()
            .filter(agent::Column::PublicKey.eq(public_key.to_vec()))
            .one(self.connection())
            .await?
        else {
            return Ok(PersistedAgentAuthorization::Unauthorized);
        };

        let status = AgentStatus::parse(&agent.status);
        if status == Some(AgentStatus::Revoked) || agent.revoked_at.is_some() {
            return Ok(PersistedAgentAuthorization::Revoked);
        }
        if status == Some(AgentStatus::Active) {
            return Ok(PersistedAgentAuthorization::Authorized(agent.agent_id));
        }
        Ok(PersistedAgentAuthorization::Unauthorized)
    }
}

const PENDING_REGISTRATION_TTL_MICROS: i64 = 10 * 60 * 1_000_000;

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
}

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
}

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
}

fn unix_timestamp_micros() -> Result<i64, DatabaseError> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    duration.as_micros().try_into().map_err(|_| {
        DatabaseError::InvalidAgentRegistration("Unix timestamp does not fit in i64".to_owned())
    })
}

fn constant_time_eq(expected: &[u8], actual: &[u8; 32]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            })
            == 0
}

fn parse_registration_id(value: &str) -> anyhow::Result<[u8; 16]> {
    Ok(Uuid::parse_str(value)?.into_bytes())
}

fn registration_is_expired(registration: &agent_registration::Model, now: i64) -> bool {
    let status = RegistrationStatus::parse(&registration.status);
    status == Some(RegistrationStatus::Expired)
        || (status == Some(RegistrationStatus::Prepared)
            && registration
                .expires_at
                .is_some_and(|expires_at| expires_at <= now))
}

/// 删除一条已进入可清理状态的注册事务；返回值表示是否实际执行了删除。
async fn delete_registration_if_expired(
    transaction: &DatabaseTransaction,
    registration: &agent_registration::Model,
    now: i64,
) -> Result<bool, DatabaseError> {
    if !registration_is_expired(registration, now) {
        return Ok(false);
    }
    agent_registration::Entity::delete_by_id(&registration.registration_id)
        .exec(transaction)
        .await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use sea_orm::{ActiveModelTrait, EntityTrait, Set};

    use super::{RegistrationStatus, ServerDatabase};
    use crate::database::{
        DatabaseConfig,
        entity::{agent_registration, registration_token},
    };

    /// 创建一组 Token 和注册事务，便于精确验证清理查询的每个状态分支。
    async fn insert_registration(
        database: &ServerDatabase,
        suffix: &str,
        status: &str,
        expires_at: Option<i64>,
        public_key_byte: u8,
    ) {
        let token_id = format!("token-{suffix}");
        registration_token::ActiveModel {
            token_id: Set(token_id.clone()),
            psk: Set(vec![public_key_byte; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            used_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("test token should insert");

        agent_registration::ActiveModel {
            registration_id: Set(format!("registration-{suffix}")),
            token_id: Set(token_id),
            agent_id: Set(None),
            reserved_agent_id: Set(format!("agent-{suffix}")),
            agent_name: Set(format!("Agent {suffix}")),
            agent_public_key: Set(vec![public_key_byte; 32]),
            status: Set(status.to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(expires_at),
            committed_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("test registration should insert");
    }

    #[tokio::test]
    async fn cleanup_removes_only_expired_uncommitted_registrations() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("database should connect");
        insert_registration(
            &database,
            "prepared-expired",
            RegistrationStatus::PREPARED,
            Some(1),
            1,
        )
        .await;
        insert_registration(
            &database,
            "explicitly-expired",
            RegistrationStatus::EXPIRED,
            None,
            2,
        )
        .await;
        insert_registration(
            &database,
            "prepared-active",
            RegistrationStatus::PREPARED,
            Some(i64::MAX),
            3,
        )
        .await;

        let deleted = database
            .cleanup_expired_agent_registrations()
            .await
            .expect("cleanup should succeed");

        assert_eq!(deleted, 2);
        assert!(
            agent_registration::Entity::find_by_id("registration-prepared-expired")
                .one(database.connection())
                .await
                .expect("expired prepared lookup should succeed")
                .is_none()
        );
        assert!(
            agent_registration::Entity::find_by_id("registration-explicitly-expired")
                .one(database.connection())
                .await
                .expect("explicit expired lookup should succeed")
                .is_none()
        );
        assert!(
            agent_registration::Entity::find_by_id("registration-prepared-active")
                .one(database.connection())
                .await
                .expect("active prepared lookup should succeed")
                .is_some()
        );
    }
}
