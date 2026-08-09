//! 一次性 Agent 注册 Token 的持久化模型。

use sea_orm::entity::prelude::*;

/// registration_tokens 表的 SeaORM 实体。
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "registration_tokens")]
pub struct Model {
    /// Noise 首帧公开携带的 Token ID，不是秘密。
    #[sea_orm(primary_key, auto_increment = false)]
    pub token_id: String,
    /// XXpsk3 使用的 32 字节 PSK；生产环境应由应用层加密或使用密钥管理系统保护。
    pub psk: Vec<u8>,
    /// active、used 或 revoked；状态值由注册中心统一解析。
    pub status: String,
    /// 创建时间，Unix 微秒。
    pub created_at: i64,
    /// 最近一次状态变更时间，Unix 微秒。
    pub updated_at: i64,
    /// 过期时间；永久有效 Token 为空。
    pub expires_at: Option<i64>,
    /// 消费时间；未消费时为空。
    pub used_at: Option<i64>,
    /// 该 Token 产生的所有注册事务。
    #[sea_orm(has_many)]
    pub registrations: HasMany<super::agent_registration::Entity>,
}

// 保留 SeaORM 的默认 ActiveModel 行为，后续可在这里加入 Token 生命周期钩子。
impl ActiveModelBehavior for ActiveModel {}
