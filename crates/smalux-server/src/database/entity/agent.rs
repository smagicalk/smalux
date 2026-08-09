//! 已登记 Agent 的长期身份和授权状态。

use sea_orm::entity::prelude::*;

/// agents 表的 SeaORM 实体。
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agents")]
pub struct Model {
    /// Server 分配的稳定业务 ID。
    #[sea_orm(primary_key, auto_increment = false)]
    pub agent_id: String,
    /// Agent 可读展示名称；允许重复，不能用于身份判断或表间关联。
    pub name: String,
    /// Agent Noise 长期静态公钥，固定 32 字节。
    pub public_key: Vec<u8>,
    /// 当前授权状态：active 或 revoked。
    pub status: String,
    /// Unix 微秒时间戳，避免绑定某一个 SQL 方言的默认时间函数。
    pub created_at: i64,
    /// 最近一次授权状态或元数据变更时间。
    pub updated_at: i64,
    /// 吊销时间；未吊销时为空。
    pub revoked_at: Option<i64>,
    /// 该 Agent 关联的所有注册事务。
    #[sea_orm(has_many)]
    pub registrations: HasMany<super::agent_registration::Entity>,
}

// SeaORM 要求实体的 ActiveModel 实现该行为接口；后续可在这里添加保存前后的业务钩子。
impl ActiveModelBehavior for ActiveModel {}
