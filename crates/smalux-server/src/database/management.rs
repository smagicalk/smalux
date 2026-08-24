//! 管理服务使用的安全查询与状态变更。
//!
//! 这些方法只返回管理界面需要的元数据，不返回注册 PSK、Agent 公钥原文或 Server
//! 私钥。CLI 和未来 HTTP 管理接口都必须经服务层调用这里，不能直接拼接 ORM 查询。

use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};

use super::{
    DatabaseError, ServerDatabase,
    agent_registration::unix_timestamp_micros,
    entity::{agent, registration_token},
};

#[derive(Clone, Debug, PartialEq, Eq)]
/// 数据库层返回给管理服务的 Token 元数据，不包含 `psk` 列。
pub(crate) struct RegistrationTokenRecord {
    /// 可公开查询的 Token ID。
    pub token_id: String,
    /// 签发时由 Server 绑定的 Agent 展示名称。
    pub display_name: Option<String>,
    /// 持久化状态或根据当前时间派生的 `expired`。
    pub status: String,
    /// 创建时间，Unix epoch 微秒。
    pub created_at: i64,
    /// 最近更新时间，Unix epoch 微秒。
    pub updated_at: i64,
    /// 过期时间，Unix epoch 微秒；`None` 表示永久有效。
    pub expires_at: Option<i64>,
    /// 成功注册时的消费时间，Unix epoch 微秒。
    pub used_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// 数据库层返回给管理服务的 Agent 元数据，不包含 Noise 公钥原文。
pub(crate) struct AgentRecord {
    /// 唯一且稳定的 Agent 身份键。
    pub agent_id: String,
    /// 允许重复和修改的展示名称。
    pub name: String,
    /// 持久化授权状态。
    pub status: String,
    /// 创建时间，Unix epoch 微秒。
    pub created_at: i64,
    /// 最近更新时间，Unix epoch 微秒。
    pub updated_at: i64,
    /// 吊销时间，Unix epoch 微秒。
    pub revoked_at: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// `status` 命令需要的数据库聚合计数。
pub(crate) struct ManagementCounts {
    /// 状态为 active 的 Agent 数。
    pub active_agents: u64,
    /// 状态为 revoked 的 Agent 数。
    pub revoked_agents: u64,
    /// active 且尚未过期的 Token 数。
    pub active_tokens: u64,
    /// 已被一次注册成功消费的 Token 数。
    pub used_tokens: u64,
    /// 被管理员吊销的 Token 数。
    pub revoked_tokens: u64,
    /// 状态仍为 active、但当前时间已超过有效期的 Token 数。
    pub expired_tokens: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Token 吊销的幂等结果，服务层据此选择成功、冲突或不存在响应。
pub(crate) enum RevokeTokenOutcome {
    /// 本次调用完成了 active -> revoked 更新。
    Revoked,
    /// 调用前已经是 revoked，仍按幂等成功处理。
    AlreadyRevoked,
    /// Token 已被注册消费，不能追溯性吊销。
    AlreadyUsed,
    /// 数据库中没有此 Token ID。
    NotFound,
}

impl ServerDatabase {
    /// 按公开字段过滤并以 Token ID 游标分页，不读取或返回 PSK。
    ///
    /// `expired` 不是持久化状态，而是由 `active + expires_at <= now` 动态派生。
    pub(crate) async fn list_registration_tokens(
        &self,
        status: Option<&str>,
        display_name: Option<&str>,
        limit: u64,
        after: Option<&str>,
    ) -> Result<Vec<RegistrationTokenRecord>, DatabaseError> {
        let now = unix_timestamp_micros()?;
        let mut query = registration_token::Entity::find();
        if let Some(after) = after {
            query = query.filter(registration_token::Column::TokenId.gt(after));
        }
        if let Some(display_name) = display_name {
            query = query.filter(registration_token::Column::DisplayName.contains(display_name));
        }
        query = match status {
            Some("expired") => query
                .filter(registration_token::Column::Status.eq("active"))
                .filter(registration_token::Column::ExpiresAt.lte(now)),
            Some("active") => query
                .filter(registration_token::Column::Status.eq("active"))
                .filter(
                    sea_orm::Condition::any()
                        .add(registration_token::Column::ExpiresAt.is_null())
                        .add(registration_token::Column::ExpiresAt.gt(now)),
                ),
            Some(status) => query.filter(registration_token::Column::Status.eq(status)),
            None => query,
        };
        let models = query
            .order_by_asc(registration_token::Column::TokenId)
            .limit(limit)
            .all(self.connection())
            .await?;
        Ok(models
            .into_iter()
            .map(|model| token_record(model, now))
            .collect())
    }

    /// 按公开 Token ID 查询管理元数据，不读取或返回 PSK。
    pub(crate) async fn find_registration_token(
        &self,
        token_id: &str,
    ) -> Result<Option<RegistrationTokenRecord>, DatabaseError> {
        let now = unix_timestamp_micros()?;
        Ok(registration_token::Entity::find_by_id(token_id)
            .one(self.connection())
            .await?
            .map(|model| token_record(model, now)))
    }

    /// 幂等吊销尚未消费的 Token。
    ///
    /// 已消费 Token 返回 [`RevokeTokenOutcome::AlreadyUsed`]，避免改变已经完成的注册历史。
    pub(crate) async fn revoke_registration_token(
        &self,
        token_id: &str,
    ) -> Result<RevokeTokenOutcome, DatabaseError> {
        let Some(model) = registration_token::Entity::find_by_id(token_id)
            .one(self.connection())
            .await?
        else {
            return Ok(RevokeTokenOutcome::NotFound);
        };
        match model.status.as_str() {
            "revoked" => return Ok(RevokeTokenOutcome::AlreadyRevoked),
            "used" => return Ok(RevokeTokenOutcome::AlreadyUsed),
            "active" => {}
            _ => {
                return Err(DatabaseError::InvalidAgentRegistration(
                    "registration token has an unknown status".to_owned(),
                ));
            }
        }
        let now = unix_timestamp_micros()?;
        let mut active: registration_token::ActiveModel = model.into();
        active.status = Set("revoked".to_owned());
        active.updated_at = Set(now);
        active.update(self.connection()).await?;
        Ok(RevokeTokenOutcome::Revoked)
    }

    /// 按持久化字段和当前在线 Agent ID 集合过滤，再执行 Agent ID 游标分页。
    ///
    /// 在线集合来自进程内 Session 目录，只用于查询条件，不写回数据库。
    pub(crate) async fn list_agents(
        &self,
        status: Option<&str>,
        name: Option<&str>,
        online_filter: Option<(&[String], bool)>,
        limit: u64,
        after: Option<&str>,
    ) -> Result<Vec<AgentRecord>, DatabaseError> {
        let mut query = agent::Entity::find();
        if let Some(status) = status {
            query = query.filter(agent::Column::Status.eq(status));
        }
        if let Some(name) = name {
            query = query.filter(agent::Column::Name.contains(name));
        }
        if let Some(after) = after {
            query = query.filter(agent::Column::AgentId.gt(after));
        }
        if let Some((online_agent_ids, true)) = online_filter {
            if online_agent_ids.is_empty() {
                return Ok(Vec::new());
            }
            query = query.filter(agent::Column::AgentId.is_in(online_agent_ids.iter().cloned()));
        } else if let Some((online_agent_ids, false)) = online_filter
            && !online_agent_ids.is_empty()
        {
            query =
                query.filter(agent::Column::AgentId.is_not_in(online_agent_ids.iter().cloned()));
        }
        Ok(query
            .order_by_asc(agent::Column::AgentId)
            .limit(limit)
            .all(self.connection())
            .await?
            .into_iter()
            .map(agent_record)
            .collect())
    }

    /// 按稳定 Agent ID 查询不含 Noise 公钥的管理记录。
    pub(crate) async fn find_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentRecord>, DatabaseError> {
        Ok(agent::Entity::find_by_id(agent_id)
            .one(self.connection())
            .await?
            .map(agent_record))
    }

    /// 判断 Agent 是否存在且仍被授权；不存在和已吊销都返回 `false`。
    pub(crate) async fn is_agent_active(&self, agent_id: &str) -> Result<bool, DatabaseError> {
        let Some(model) = agent::Entity::find_by_id(agent_id)
            .one(self.connection())
            .await?
        else {
            return Ok(false);
        };
        Ok(model.status == "active" && model.revoked_at.is_none())
    }

    /// 仅修改展示名称和更新时间，不改变 Agent ID、公钥或授权状态。
    pub(crate) async fn rename_agent(
        &self,
        agent_id: &str,
        name: &str,
    ) -> Result<Option<AgentRecord>, DatabaseError> {
        let Some(model) = agent::Entity::find_by_id(agent_id)
            .one(self.connection())
            .await?
        else {
            return Ok(None);
        };
        let mut active: agent::ActiveModel = model.into();
        active.name = Set(name.to_owned());
        active.updated_at = Set(unix_timestamp_micros()?);
        Ok(Some(agent_record(active.update(self.connection()).await?)))
    }

    /// 幂等持久化 Agent 吊销状态；断开实时 Session 由上层服务完成。
    pub(crate) async fn revoke_agent(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentRecord>, DatabaseError> {
        let Some(model) = agent::Entity::find_by_id(agent_id)
            .one(self.connection())
            .await?
        else {
            return Ok(None);
        };
        if model.status == "revoked" && model.revoked_at.is_some() {
            return Ok(Some(agent_record(model)));
        }
        let now = unix_timestamp_micros()?;
        let mut active: agent::ActiveModel = model.into();
        active.status = Set("revoked".to_owned());
        active.updated_at = Set(now);
        active.revoked_at = Set(Some(now));
        Ok(Some(agent_record(active.update(self.connection()).await?)))
    }

    /// 计算管理状态页需要的 Agent 和 Token 分类总数。
    pub(crate) async fn management_counts(&self) -> Result<ManagementCounts, DatabaseError> {
        let now = unix_timestamp_micros()?;
        let active_agents = agent::Entity::find()
            .filter(agent::Column::Status.eq("active"))
            .count(self.connection())
            .await?;
        let revoked_agents = agent::Entity::find()
            .filter(agent::Column::Status.eq("revoked"))
            .count(self.connection())
            .await?;
        let active_tokens = registration_token::Entity::find()
            .filter(registration_token::Column::Status.eq("active"))
            .filter(
                sea_orm::Condition::any()
                    .add(registration_token::Column::ExpiresAt.is_null())
                    .add(registration_token::Column::ExpiresAt.gt(now)),
            )
            .count(self.connection())
            .await?;
        let expired_tokens = registration_token::Entity::find()
            .filter(registration_token::Column::Status.eq("active"))
            .filter(registration_token::Column::ExpiresAt.lte(now))
            .count(self.connection())
            .await?;
        let used_tokens = registration_token::Entity::find()
            .filter(registration_token::Column::Status.eq("used"))
            .count(self.connection())
            .await?;
        let revoked_tokens = registration_token::Entity::find()
            .filter(registration_token::Column::Status.eq("revoked"))
            .count(self.connection())
            .await?;
        Ok(ManagementCounts {
            active_agents,
            revoked_agents,
            active_tokens,
            used_tokens,
            revoked_tokens,
            expired_tokens,
        })
    }
}

/// 将 ORM 模型映射成安全视图，并根据同一个 `now` 快照派生过期状态。
fn token_record(model: registration_token::Model, now: i64) -> RegistrationTokenRecord {
    let status = if model.status == "active"
        && model.expires_at.is_some_and(|expires_at| expires_at <= now)
    {
        "expired".to_owned()
    } else {
        model.status
    };
    RegistrationTokenRecord {
        token_id: model.token_id,
        display_name: model.display_name,
        status,
        created_at: model.created_at,
        updated_at: model.updated_at,
        expires_at: model.expires_at,
        used_at: model.used_at,
    }
}

/// 丢弃 Agent 公钥，只保留管理命令需要的字段。
fn agent_record(model: agent::Model) -> AgentRecord {
    AgentRecord {
        agent_id: model.agent_id,
        name: model.name,
        status: model.status,
        created_at: model.created_at,
        updated_at: model.updated_at,
        revoked_at: model.revoked_at,
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::{ActiveModelTrait, Set};

    use super::*;
    use crate::config::DatabaseConfig;

    async fn database() -> ServerDatabase {
        ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("in-memory Server database")
    }

    #[tokio::test]
    async fn registration_token_queries_expose_metadata_and_revoke_idempotently() {
        let database = database().await;
        database
            .insert_registration_token(
                "00112233445566778899aabbccddeeff",
                &[7; 32],
                Some("node-one"),
                Some(std::time::Duration::from_secs(60)),
            )
            .await
            .unwrap();

        let tokens = database
            .list_registration_tokens(Some("active"), None, 50, None)
            .await
            .unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].display_name.as_deref(), Some("node-one"));
        assert_eq!(tokens[0].status, "active");

        assert_eq!(
            database
                .revoke_registration_token(&tokens[0].token_id)
                .await
                .unwrap(),
            RevokeTokenOutcome::Revoked
        );
        assert_eq!(
            database
                .revoke_registration_token(&tokens[0].token_id)
                .await
                .unwrap(),
            RevokeTokenOutcome::AlreadyRevoked
        );
    }

    #[tokio::test]
    async fn duplicate_agent_names_are_managed_only_by_agent_id() {
        let database = database().await;
        let now = unix_timestamp_micros().unwrap();
        for (agent_id, public_key) in [("agent-a", vec![1; 32]), ("agent-b", vec![2; 32])] {
            agent::ActiveModel {
                agent_id: Set(agent_id.to_owned()),
                name: Set("shared-name".to_owned()),
                public_key: Set(public_key),
                status: Set("active".to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                revoked_at: Set(None),
            }
            .insert(database.connection())
            .await
            .unwrap();
        }

        let renamed = database
            .rename_agent("agent-a", "renamed")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(renamed.name, "renamed");
        let revoked = database.revoke_agent("agent-b").await.unwrap().unwrap();
        assert_eq!(revoked.status, "revoked");
        assert_eq!(
            database
                .find_agent("agent-a")
                .await
                .unwrap()
                .unwrap()
                .status,
            "active"
        );
    }
}
