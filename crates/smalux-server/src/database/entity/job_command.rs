//! 下发 Job 命令及 Agent 返回状态的持久化实体。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "job_commands")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub command_id: String,
    pub agent_id: String,
    pub catalog_revision: i64,
    pub command_payload: Vec<u8>,
    pub result_status: Option<i32>,
    pub result_payload: Option<Vec<u8>>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
}

impl ActiveModelBehavior for ActiveModel {}
