//! Agent 最近一次可执行能力快照。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_capabilities")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub agent_id: String,
    pub revision: i64,
    pub agent_version: String,
    pub payload: Vec<u8>,
    pub updated_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
