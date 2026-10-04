//! 每个 Agent 的权威远程 Job 目录版本。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_job_catalogs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub agent_id: String,
    pub revision: i64,
    pub updated_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
