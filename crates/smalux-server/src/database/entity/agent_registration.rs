//! XXpsk3 注册事务的幂等状态模型。

use sea_orm::entity::prelude::*;

/// agent_registrations 表的 SeaORM 实体。
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_registrations")]
pub struct Model {
    /// Server 生成的 16 字节事务 ID，以十六进制或 UUID 字符串保存。
    #[sea_orm(primary_key, auto_increment = false)]
    pub registration_id: String,
    /// 本次注册使用的公开 Token ID。
    pub token_id: String,
    /// commit 后回填的 Agent 外键；prepare 阶段为空。
    pub agent_id: Option<String>,
    /// Server 在 prepare 阶段分配的稳定 Agent ID。
    pub reserved_agent_id: String,
    /// Client 在加密注册请求中提交的名称。
    pub agent_name: String,
    /// XXpsk3 认证得到的 Agent Noise 静态公钥，固定 32 字节。
    pub agent_public_key: Vec<u8>,
    /// prepared、committed 或 expired；状态值由注册中心统一解析。
    pub status: String,
    /// 创建时间，Unix 微秒。
    pub created_at: i64,
    /// 最近一次状态变更时间，Unix 微秒。
    pub updated_at: i64,
    /// Pending 事务的过期时间。
    pub expires_at: Option<i64>,
    /// 完成提交的时间；未完成时为空。
    pub committed_at: Option<i64>,
    /// 当前事务预分配的 Agent；prepare 阶段通过预分配 ID 关联，commit 后外键才回填。
    #[sea_orm(belongs_to, from = "reserved_agent_id", to = "agent_id")]
    pub agent: BelongsTo<super::agent::Entity>,
    /// 当前事务使用的注册 Token。
    #[sea_orm(belongs_to, from = "token_id", to = "token_id")]
    pub token: BelongsTo<super::registration_token::Entity>,
}

// 保留 SeaORM 的默认 ActiveModel 行为，后续可在这里加入注册事务钩子。
impl ActiveModelBehavior for ActiveModel {}
