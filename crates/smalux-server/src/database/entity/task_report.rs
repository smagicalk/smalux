//! 成功 TaskReport 的去重持久化实体。

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "task_reports")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub report_id: String,
    pub agent_id: String,
    pub job_id: Vec<u8>,
    pub job_revision: i64,
    pub run_id: Vec<u8>,
    pub attempt: i32,
    pub scheduled_at: Option<i64>,
    pub started_at: Option<i64>,
    pub result_kind: String,
    pub payload: Vec<u8>,
    pub received_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
