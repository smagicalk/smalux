//! Durable idempotency and lifecycle records for Web Job catalog mutations.

use std::time::{SystemTime, UNIX_EPOCH};

use sea_orm::{
    ActiveValue::Set,
    ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, OnConflict},
};
use smalux_protocol::agent::v1::{JobDefinition, ReplaceAllJobs};
use smalux_protocol::reconciliation::catalog_digest;
use uuid::Uuid;

use super::{AgentJobCatalogRecord, DatabaseError, ServerDatabase, entity::web_job_operation};

pub(crate) const WEB_OPERATION_PENDING: &str = "pending";
pub(crate) const WEB_OPERATION_WAITING_AGENT: &str = "waiting_agent";
pub(crate) const WEB_OPERATION_SENT: &str = "sent";
pub(crate) const WEB_OPERATION_APPLIED: &str = "applied";
pub(crate) const WEB_OPERATION_BLOCKED: &str = "blocked";
pub(crate) const WEB_OPERATION_REJECTED: &str = "rejected";
pub(crate) const WEB_OPERATION_SUPERSEDED: &str = "superseded";
pub(crate) const WEB_OPERATION_UNKNOWN: &str = "unknown";

/// Data prepared by the Web handler before it atomically mutates a catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WebJobOperationDraft {
    pub operation_id: String,
    pub actor_user_id: String,
    /// HTTP/operation method used as part of the idempotency scope.
    pub method: String,
    pub client_mutation_id: String,
    /// Lowercase SHA-256 hex digest of the canonical request.
    pub request_hash: String,
    pub agent_id: String,
    /// Business operation name, separate from the idempotency-scope method.
    pub action: String,
    pub target_job_id: Option<String>,
    /// Sanitized initial response JSON; persisted byte-for-byte for replay.
    pub response_json: String,
}

/// Durable Web catalog mutation record. All timestamp values are Unix epoch microseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WebJobOperationRecord {
    pub operation_id: String,
    pub actor_user_id: String,
    pub method: String,
    pub client_mutation_id: String,
    pub request_hash: String,
    pub agent_id: String,
    pub action: String,
    pub expected_catalog_revision: u64,
    pub target_catalog_revision: u64,
    pub desired_digest: Vec<u8>,
    pub target_job_id: Option<String>,
    pub state: String,
    pub command_id: Option<String>,
    pub reason: Option<String>,
    pub response_json: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub sent_at: Option<i64>,
    pub completed_at: Option<i64>,
}

/// Result of an atomic Web catalog mutation; replayed requests have no new catalog write.
#[derive(Clone, Debug)]
pub(crate) struct WebJobCatalogMutationRecord {
    pub catalog: Option<AgentJobCatalogRecord>,
    pub operation: WebJobOperationRecord,
    pub replayed: bool,
}

impl ServerDatabase {
    /// Look up one idempotency key, including the request hash used to decide safe replay.
    pub(crate) async fn get_web_job_operation_by_key(
        &self,
        actor_user_id: &str,
        method: &str,
        client_mutation_id: &str,
    ) -> Result<Option<WebJobOperationRecord>, DatabaseError> {
        let model = web_job_operation::Entity::find()
            .filter(web_job_operation::Column::ActorUserId.eq(actor_user_id))
            .filter(web_job_operation::Column::Method.eq(method))
            .filter(web_job_operation::Column::ClientMutationId.eq(client_mutation_id))
            .one(self.connection())
            .await?;
        model.map(operation_record).transpose()
    }

    /// Look up an operation by its UUID string identifier.
    pub(crate) async fn get_web_job_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<WebJobOperationRecord>, DatabaseError> {
        web_job_operation::Entity::find_by_id(operation_id)
            .one(self.connection())
            .await?
            .map(operation_record)
            .transpose()
    }

    /// List recent records for an Agent, with a hard maximum of 100 rows.
    pub(crate) async fn list_recent_web_job_operations(
        &self,
        agent_id: &str,
        actor_user_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WebJobOperationRecord>, DatabaseError> {
        let mut query = web_job_operation::Entity::find()
            .filter(web_job_operation::Column::AgentId.eq(agent_id));
        if let Some(actor_user_id) = actor_user_id {
            query = query.filter(web_job_operation::Column::ActorUserId.eq(actor_user_id));
        }
        let rows = query
            .order_by_desc(web_job_operation::Column::CreatedAt)
            .order_by_desc(web_job_operation::Column::OperationId)
            .limit(limit.min(100) as u64)
            .all(self.connection())
            .await?;
        rows.into_iter().map(operation_record).collect()
    }

    /// Atomically CAS/replace the complete catalog and persist its idempotency record.
    ///
    /// A retry with the same key and request hash returns the original immutable record without
    /// applying another catalog revision. Reusing a key with a different request hash is a
    /// database conflict. Both the operation reservation and catalog transaction roll back
    /// together on validation or revision failure.
    pub(crate) async fn replace_agent_job_catalog_for_web(
        &self,
        agent_id: &str,
        jobs: Vec<JobDefinition>,
        expected_revision: u64,
        draft: WebJobOperationDraft,
    ) -> Result<WebJobCatalogMutationRecord, DatabaseError> {
        validate_operation_draft(&draft, agent_id)?;
        let expected_revision_i64 = i64::try_from(expected_revision).map_err(|_| {
            DatabaseError::InvalidJobCatalog("expected catalog revision exceeds i64".to_owned())
        })?;
        let target_revision = expected_revision_i64.checked_add(1).ok_or_else(|| {
            DatabaseError::InvalidJobCatalog("catalog revision overflow".to_owned())
        })?;
        let transaction = self.connection().begin().await?;
        let desired_catalog_revision = u64::try_from(target_revision).map_err(|_| {
            DatabaseError::InvalidJobCatalog("catalog revision is negative".to_owned())
        })?;
        let desired_digest = catalog_digest(&ReplaceAllJobs {
            catalog_revision: desired_catalog_revision,
            jobs: jobs.clone(),
        })
        .to_vec();

        if let Some(existing) = find_by_idempotency_key(
            &transaction,
            &draft.actor_user_id,
            &draft.method,
            &draft.client_mutation_id,
        )
        .await?
        {
            if existing.request_hash != draft.request_hash {
                return Err(idempotency_conflict());
            }
            transaction.commit().await?;
            return Ok(WebJobCatalogMutationRecord {
                catalog: None,
                operation: operation_record(existing)?,
                replayed: true,
            });
        }

        let now = unix_micros()?;
        let insertion = web_job_operation::Entity::insert(web_job_operation::ActiveModel {
            operation_id: Set(draft.operation_id.clone()),
            actor_user_id: Set(draft.actor_user_id.clone()),
            method: Set(draft.method.clone()),
            client_mutation_id: Set(draft.client_mutation_id.clone()),
            request_hash: Set(draft.request_hash.clone()),
            agent_id: Set(agent_id.to_owned()),
            target_job_id: Set(draft.target_job_id.clone()),
            action_kind: Set(draft.action.clone()),
            expected_catalog_revision: Set(expected_revision_i64),
            target_catalog_revision: Set(target_revision),
            desired_digest: Set(desired_digest.clone()),
            command_id: Set(None),
            state: Set(WEB_OPERATION_WAITING_AGENT.to_owned()),
            reason: Set(None),
            initial_response_json: Set(draft.response_json.as_bytes().to_vec()),
            created_at: Set(now),
            updated_at: Set(now),
            sent_at: Set(None),
            completed_at: Set(None),
        })
        .on_conflict(
            OnConflict::columns([
                web_job_operation::Column::ActorUserId,
                web_job_operation::Column::Method,
                web_job_operation::Column::ClientMutationId,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec(&transaction)
        .await;

        match insertion {
            Ok(_) => {}
            Err(sea_orm::DbErr::RecordNotInserted) => {
                let Some(existing) = find_by_idempotency_key(
                    &transaction,
                    &draft.actor_user_id,
                    &draft.method,
                    &draft.client_mutation_id,
                )
                .await?
                else {
                    return Err(DatabaseError::InvalidJobCatalog(
                        "Web operation idempotency reservation was not persisted".to_owned(),
                    ));
                };
                if existing.request_hash != draft.request_hash {
                    return Err(idempotency_conflict());
                }
                transaction.commit().await?;
                return Ok(WebJobCatalogMutationRecord {
                    catalog: None,
                    operation: operation_record(existing)?,
                    replayed: true,
                });
            }
            Err(error) => return Err(error.into()),
        }

        let catalog = ServerDatabase::replace_agent_job_catalog_in_transaction(
            &transaction,
            agent_id,
            jobs,
            Some(expected_revision),
        )
        .await?;
        let superseded_at = catalog.updated_at;

        // A committed newer full catalog makes older commands/results stale. Only live states
        // are superseded; terminal history remains immutable apart from timestamps on live rows.
        web_job_operation::Entity::update_many()
            .col_expr(
                web_job_operation::Column::State,
                Expr::value(WEB_OPERATION_SUPERSEDED),
            )
            .col_expr(
                web_job_operation::Column::UpdatedAt,
                Expr::value(superseded_at),
            )
            .filter(web_job_operation::Column::AgentId.eq(agent_id))
            .filter(web_job_operation::Column::TargetCatalogRevision.lt(target_revision))
            .filter(web_job_operation::Column::State.is_in([
                WEB_OPERATION_PENDING,
                WEB_OPERATION_WAITING_AGENT,
                WEB_OPERATION_SENT,
                WEB_OPERATION_BLOCKED,
            ]))
            .exec(&transaction)
            .await?;

        transaction.commit().await?;
        let operation = WebJobOperationRecord {
            operation_id: draft.operation_id,
            actor_user_id: draft.actor_user_id,
            method: draft.method,
            client_mutation_id: draft.client_mutation_id,
            request_hash: draft.request_hash,
            agent_id: agent_id.to_owned(),
            action: draft.action,
            expected_catalog_revision: expected_revision,
            target_catalog_revision: target_revision as u64,
            desired_digest,
            target_job_id: draft.target_job_id,
            state: WEB_OPERATION_WAITING_AGENT.to_owned(),
            command_id: None,
            reason: None,
            response_json: draft.response_json,
            created_at: now,
            updated_at: now,
            sent_at: None,
            completed_at: None,
        };
        Ok(WebJobCatalogMutationRecord {
            catalog: Some(catalog),
            operation,
            replayed: false,
        })
    }
}

async fn find_by_idempotency_key<C>(
    connection: &C,
    actor_user_id: &str,
    method: &str,
    client_mutation_id: &str,
) -> Result<Option<web_job_operation::Model>, DatabaseError>
where
    C: sea_orm::ConnectionTrait,
{
    Ok(web_job_operation::Entity::find()
        .filter(web_job_operation::Column::ActorUserId.eq(actor_user_id))
        .filter(web_job_operation::Column::Method.eq(method))
        .filter(web_job_operation::Column::ClientMutationId.eq(client_mutation_id))
        .one(connection)
        .await?)
}

fn operation_record(
    model: web_job_operation::Model,
) -> Result<WebJobOperationRecord, DatabaseError> {
    let response_json = String::from_utf8(model.initial_response_json).map_err(|error| {
        DatabaseError::InvalidJobCatalog(format!(
            "stored Web Job operation response is not UTF-8: {error}"
        ))
    })?;
    Ok(WebJobOperationRecord {
        operation_id: model.operation_id,
        actor_user_id: model.actor_user_id,
        method: model.method,
        client_mutation_id: model.client_mutation_id,
        request_hash: model.request_hash,
        agent_id: model.agent_id,
        action: model.action_kind,
        expected_catalog_revision: stored_revision(model.expected_catalog_revision)?,
        target_catalog_revision: stored_revision(model.target_catalog_revision)?,
        desired_digest: model.desired_digest,
        target_job_id: model.target_job_id,
        state: model.state,
        command_id: model.command_id,
        reason: model.reason,
        response_json,
        created_at: model.created_at,
        updated_at: model.updated_at,
        sent_at: model.sent_at,
        completed_at: model.completed_at,
    })
}

fn stored_revision(value: i64) -> Result<u64, DatabaseError> {
    u64::try_from(value).map_err(|_| {
        DatabaseError::InvalidJobCatalog("stored Web Job operation revision is negative".to_owned())
    })
}

fn validate_operation_draft(
    draft: &WebJobOperationDraft,
    agent_id: &str,
) -> Result<(), DatabaseError> {
    let invalid = |message: &str| DatabaseError::InvalidJobCatalog(message.to_owned());
    if Uuid::parse_str(&draft.operation_id).is_err() {
        return Err(invalid("Web operation id must be a UUID string"));
    }
    if draft.actor_user_id.is_empty() || draft.actor_user_id.len() > 36 {
        return Err(invalid(
            "Web operation actor user id must contain 1 to 36 bytes",
        ));
    }
    if draft.method.is_empty() || draft.method.len() > 64 {
        return Err(invalid("Web operation method must contain 1 to 64 bytes"));
    }
    if draft.client_mutation_id.is_empty() || draft.client_mutation_id.len() > 36 {
        return Err(invalid(
            "Web operation client mutation id must contain 1 to 36 bytes",
        ));
    }
    if draft.request_hash.len() != 64
        || !draft
            .request_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid(
            "Web operation request hash must be lowercase SHA-256 hex",
        ));
    }
    if draft.agent_id != agent_id || agent_id.is_empty() || agent_id.len() > 64 {
        return Err(invalid(
            "Web operation Agent id does not match catalog target",
        ));
    }
    if draft.action.is_empty() || draft.action.len() > 64 {
        return Err(invalid("Web operation action must contain 1 to 64 bytes"));
    }
    if let Some(target_job_id) = &draft.target_job_id
        && Uuid::parse_str(target_job_id).is_err()
    {
        return Err(invalid("Web operation target Job id must be a UUID string"));
    }
    serde_json::from_str::<serde_json::Value>(&draft.response_json)
        .map_err(|_error| invalid("Web operation initial response must be valid JSON"))?;
    Ok(())
}

fn idempotency_conflict() -> DatabaseError {
    DatabaseError::WebJobIdempotencyConflict
}

fn unix_micros() -> Result<i64, DatabaseError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_micros()
        .min(i64::MAX as u128) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::DatabaseConfig, database::entity::agent};
    use sea_orm::Set;

    fn draft() -> WebJobOperationDraft {
        WebJobOperationDraft {
            operation_id: Uuid::new_v4().to_string(),
            actor_user_id: Uuid::new_v4().to_string(),
            method: "POST".to_owned(),
            client_mutation_id: Uuid::new_v4().to_string(),
            request_hash: "a".repeat(64),
            agent_id: "agent-a".to_owned(),
            action: "replace_catalog".to_owned(),
            target_job_id: None,
            response_json: " { \"ok\": true } ".to_owned(),
        }
    }

    async fn insert_agent(database: &ServerDatabase) {
        let now = unix_micros().unwrap();
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![9; 32]),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
    }

    async fn memory_database() -> ServerDatabase {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        insert_agent(&database).await;
        database
    }

    #[tokio::test]
    async fn web_operation_idempotency_replays_same_hash_and_conflicts_on_changed_hash() {
        let database = memory_database().await;
        let operation = draft();
        let first = database
            .replace_agent_job_catalog_for_web("agent-a", Vec::new(), 0, operation.clone())
            .await
            .unwrap();
        assert!(!first.replayed);
        assert_eq!(first.operation.target_catalog_revision, 1);
        assert_eq!(first.operation.state, WEB_OPERATION_WAITING_AGENT);

        let replay = database
            .replace_agent_job_catalog_for_web("agent-a", Vec::new(), 0, operation.clone())
            .await
            .unwrap();
        assert!(replay.replayed);
        assert!(replay.catalog.is_none());
        assert_eq!(replay.operation.operation_id, first.operation.operation_id);
        assert_eq!(replay.operation.response_json, operation.response_json);
        assert_eq!(
            database
                .load_agent_job_catalog("agent-a", &Default::default())
                .await
                .unwrap()
                .unwrap()
                .catalog
                .catalog_revision,
            1
        );

        let mut conflicting = operation.clone();
        conflicting.request_hash = "b".repeat(64);
        let error = database
            .replace_agent_job_catalog_for_web("agent-a", Vec::new(), 1, conflicting)
            .await
            .unwrap_err();
        assert!(matches!(error, DatabaseError::WebJobIdempotencyConflict));
    }

    #[tokio::test]
    async fn web_operation_reservation_rolls_back_when_catalog_cas_conflicts() {
        let database = memory_database().await;
        database
            .replace_agent_job_catalog("agent-a", Vec::new())
            .await
            .unwrap();
        let operation = draft();
        let error = database
            .replace_agent_job_catalog_for_web("agent-a", Vec::new(), 0, operation.clone())
            .await
            .unwrap_err();
        assert!(matches!(error, DatabaseError::RevisionConflict { .. }));
        assert!(
            database
                .get_web_job_operation_by_key(
                    &operation.actor_user_id,
                    &operation.method,
                    &operation.client_mutation_id,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            database
                .load_agent_job_catalog("agent-a", &Default::default())
                .await
                .unwrap()
                .unwrap()
                .catalog
                .catalog_revision,
            1
        );
    }

    #[tokio::test]
    async fn web_operation_survives_database_reopen() {
        let directory = std::env::temp_dir().join(format!("smalux-web-job-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("operations.db");
        let mut url = url::Url::parse("sqlite:///").unwrap();
        url.set_path(
            &path
                .to_string_lossy()
                .replace('\\', "/")
                .replace('%', "%25"),
        );
        url.set_query(Some("mode=rwc"));
        let url = url.to_string();
        #[cfg(windows)]
        let url = url.replacen("sqlite:///", "sqlite:", 1);

        let database = ServerDatabase::connect(DatabaseConfig::new(url.clone()))
            .await
            .unwrap();
        insert_agent(&database).await;
        let operation = draft();
        database
            .replace_agent_job_catalog_for_web("agent-a", Vec::new(), 0, operation.clone())
            .await
            .unwrap();
        database.connection().clone().close().await.unwrap();
        drop(database);

        let reopened = ServerDatabase::connect(DatabaseConfig::new(url))
            .await
            .unwrap();
        let persisted = reopened
            .get_web_job_operation(&operation.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.response_json, operation.response_json);
        assert_eq!(persisted.request_hash, operation.request_hash);
        assert_eq!(persisted.target_catalog_revision, 1);
        reopened.connection().clone().close().await.unwrap();
        drop(reopened);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
