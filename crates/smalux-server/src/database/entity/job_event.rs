//! Agent Scheduler 生命周期事件的追加存储实体。

use sea_orm::entity::prelude::*;

/// `job_events` 表；业务事件原始 Protobuf 同时保存在 `payload`。
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "job_events")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: String,
    pub agent_id: String,
    pub instance_id: Vec<u8>,
    pub sequence: i64,
    pub kind: i32,
    pub job_id: Vec<u8>,
    pub revision: i64,
    pub run_id: Vec<u8>,
    pub attempt: i32,
    pub emitted_at: i64,
    pub run_at: Option<i64>,
    pub duration_ms: i64,
    pub message: String,
    pub will_retry: bool,
    pub pending_count: i32,
    pub gap_detected: bool,
    pub payload: Vec<u8>,
}

impl ActiveModelBehavior for ActiveModel {}
