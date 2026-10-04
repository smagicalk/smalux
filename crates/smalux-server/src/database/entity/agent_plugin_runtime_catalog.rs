//! 每个 Agent 的 Plus Worker 运行时快照版本。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_plugin_runtime_catalogs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub agent_id: String,
    pub revision: i64,
    pub updated_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
