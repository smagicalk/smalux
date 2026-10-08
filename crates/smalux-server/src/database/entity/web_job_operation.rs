//! Durable Web Job mutation metadata, excluding Agent command payloads.

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "web_job_operations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub operation_id: String,
    pub actor_user_id: String,
    pub method: String,
    pub client_mutation_id: String,
    pub request_hash: String,
    pub agent_id: String,
    pub target_job_id: Option<String>,
    pub action_kind: String,
    pub expected_catalog_revision: i64,
    pub target_catalog_revision: i64,
    pub desired_digest: Vec<u8>,
    pub command_id: Option<String>,
    pub state: String,
    pub reason: Option<String>,
    pub initial_response_json: Vec<u8>,
    pub created_at: i64,
    pub updated_at: i64,
    pub sent_at: Option<i64>,
    pub completed_at: Option<i64>,
}

impl ActiveModelBehavior for ActiveModel {}
