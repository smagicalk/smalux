//! 单 Agent 远程 Job 定义的原始 Protobuf 存储。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "agent_jobs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_key: String,
    pub agent_id: String,
    pub job_id: Vec<u8>,
    pub revision: i64,
    pub enabled: bool,
    pub task_kind: String,
    pub definition: Vec<u8>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
