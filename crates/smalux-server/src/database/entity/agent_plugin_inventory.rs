//! Agent 最近一次已安装 Plus 插件清单。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_plugin_inventories")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub agent_id: String,
    pub revision: i64,
    pub payload: Vec<u8>,
    pub updated_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
