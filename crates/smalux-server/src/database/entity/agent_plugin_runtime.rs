//! 单 Agent、单插件版本的期望 Worker 运行时配置。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_plugin_runtimes")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub runtime_key: String,
    pub agent_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub schema_hash: Vec<u8>,
    pub schema_version: i32,
    pub config: Vec<u8>,
    pub requested_concurrency: i32,
    pub updated_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
