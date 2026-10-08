//! Server 下发 JobCommand 与 Agent 处理结果的持久化 Adapter。

use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, EntityTrait, QueryFilter, TransactionTrait,
    sea_query::Expr,
};
use smalux_protocol::agent::v1::{
    JobCommand, JobCommandErrorCode, JobCommandResult, JobCommandStatus,
    job_command as command_action,
};
use smalux_protocol::reconciliation::catalog_digest;
use uuid::Uuid;

use super::{
    DatabaseError, ServerDatabase,
    entity::{job_command as command_record, web_job_operation},
    web_job_operations::{
        WEB_OPERATION_APPLIED, WEB_OPERATION_BLOCKED, WEB_OPERATION_PENDING,
        WEB_OPERATION_REJECTED, WEB_OPERATION_SENT, WEB_OPERATION_UNKNOWN,
        WEB_OPERATION_WAITING_AGENT,
    },
};

impl ServerDatabase {
    /// 在写入加密流前持久化待处理命令；相同 command_id 可安全重放。
    pub async fn record_job_command(
        &self,
        agent_id: &str,
        command: &JobCommand,
    ) -> Result<(), DatabaseError> {
        let command_id = command_id(&command.command_id)?;
        let catalog_revision = command_catalog_revision(command)?;
        let catalog_revision = i64::try_from(catalog_revision).map_err(|_| {
            DatabaseError::InvalidJobCommand("catalog revision exceeds i64".to_owned())
        })?;
        let now = unix_micros()?;
        let transaction = self.connection().begin().await?;
        if let Some(previous) = command_record::Entity::find_by_id(&command_id)
            .one(&transaction)
            .await?
        {
            if previous.agent_id != agent_id || previous.command_payload != command.encode_to_vec()
            {
                return Err(DatabaseError::InvalidJobCommand(
                    "command id is already bound to different content".to_owned(),
                ));
            }
        } else {
            command_record::Entity::insert(command_record::ActiveModel {
                command_id: Set(command_id.clone()),
                agent_id: Set(agent_id.to_owned()),
                catalog_revision: Set(catalog_revision),
                command_payload: Set(command.encode_to_vec()),
                result_status: Set(None),
                result_payload: Set(None),
                created_at: Set(now),
                completed_at: Set(None),
            })
            .exec(&transaction)
            .await?;
        }

        // A ReplaceAll snapshot is the only command that can satisfy a Web catalog operation.
        if let Some(command_action::Action::ReplaceAll(snapshot)) = command.action.as_ref() {
            let digest = catalog_digest(snapshot).to_vec();
            let operations = web_job_operation::Entity::find()
                .filter(web_job_operation::Column::AgentId.eq(agent_id))
                .filter(web_job_operation::Column::TargetCatalogRevision.eq(catalog_revision))
                .filter(web_job_operation::Column::State.is_in([
                    WEB_OPERATION_PENDING.to_owned(),
                    WEB_OPERATION_WAITING_AGENT.to_owned(),
                    WEB_OPERATION_SENT.to_owned(),
                    WEB_OPERATION_BLOCKED.to_owned(),
                ]))
                .all(&transaction)
                .await?;
            for operation in operations {
                let matches_desired = operation.desired_digest == digest;
                let mut active: web_job_operation::ActiveModel = operation.into();
                active.command_id = Set(Some(command_id.clone()));
                active.state = Set(if matches_desired {
                    WEB_OPERATION_SENT.to_owned()
                } else {
                    WEB_OPERATION_BLOCKED.to_owned()
                });
                active.reason = Set((!matches_desired).then(|| {
                    "Agent policy or capability filtered the requested catalog".to_owned()
                }));
                active.sent_at = Set(Some(now));
                active.updated_at = Set(now);
                active.update(&transaction).await?;
            }
        }
        transaction.commit().await?;
        Ok(())
    }

    /// 将 Agent 的异步处理结果绑定回先前保存的命令。
    pub async fn complete_job_command(
        &self,
        agent_id: &str,
        result: &JobCommandResult,
    ) -> Result<(), DatabaseError> {
        let command_id = command_id(&result.command_id)?;
        let now = unix_micros()?;
        let transaction = self.connection().begin().await?;
        let Some(model) = command_record::Entity::find_by_id(&command_id)
            .one(&transaction)
            .await?
        else {
            return Err(DatabaseError::InvalidJobCommand(
                "Agent returned a result for an unknown command".to_owned(),
            ));
        };
        if model.agent_id != agent_id {
            return Err(DatabaseError::InvalidJobCommand(
                "Agent returned a result for another Agent command".to_owned(),
            ));
        }
        let mut active: command_record::ActiveModel = model.into();
        active.result_status = Set(Some(result.status));
        active.result_payload = Set(Some(result.encode_to_vec()));
        active.completed_at = Set(Some(now));
        active.update(&transaction).await?;

        let (state, completed_at, clear_command) = match result.status() {
            JobCommandStatus::Applied => (WEB_OPERATION_APPLIED, Some(now), false),
            JobCommandStatus::Rejected => (WEB_OPERATION_REJECTED, Some(now), false),
            JobCommandStatus::ResyncRequired => (WEB_OPERATION_PENDING, None, true),
            JobCommandStatus::Unspecified => (WEB_OPERATION_UNKNOWN, Some(now), false),
        };
        let mut operation_update = web_job_operation::Entity::update_many()
            .col_expr(web_job_operation::Column::State, Expr::value(state))
            .col_expr(web_job_operation::Column::UpdatedAt, Expr::value(now))
            .col_expr(
                web_job_operation::Column::CompletedAt,
                Expr::value(completed_at),
            )
            .col_expr(
                web_job_operation::Column::Reason,
                Expr::value(result.error.as_ref().and_then(|error| {
                    JobCommandErrorCode::try_from(error.code)
                        .ok()
                        .map(|code| code.as_str_name().to_owned())
                })),
            )
            .filter(web_job_operation::Column::CommandId.eq(command_id))
            .filter(web_job_operation::Column::State.is_in([
                WEB_OPERATION_PENDING.to_owned(),
                WEB_OPERATION_WAITING_AGENT.to_owned(),
                WEB_OPERATION_SENT.to_owned(),
            ]));
        if clear_command {
            operation_update = operation_update.col_expr(
                web_job_operation::Column::CommandId,
                Expr::value(Option::<String>::None),
            );
        }
        operation_update.exec(&transaction).await?;
        transaction.commit().await?;
        Ok(())
    }
}

fn command_id(bytes: &[u8]) -> Result<String, DatabaseError> {
    Uuid::from_slice(bytes)
        .map(|value| value.to_string())
        .map_err(|_| {
            DatabaseError::InvalidJobCommand("command id must contain 16 bytes".to_owned())
        })
}

fn command_catalog_revision(command: &JobCommand) -> Result<u64, DatabaseError> {
    let Some(action) = command.action.as_ref() else {
        return Err(DatabaseError::InvalidJobCommand(
            "Job command action is required".to_owned(),
        ));
    };
    Ok(match action {
        command_action::Action::ReplaceAll(value) => value.catalog_revision,
        command_action::Action::Upsert(value) => value.catalog_revision,
        command_action::Action::Delete(value) => value.catalog_revision,
        command_action::Action::RunNow(_) => 0,
    })
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
    use crate::{
        config::DatabaseConfig,
        database::{WebJobOperationDraft, entity::agent},
    };
    use sea_orm::Set;
    use smalux_protocol::agent::v1::{ReplaceAllJobs, job_command};

    async fn database() -> ServerDatabase {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let now = unix_micros().unwrap();
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![4; 32]),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
        database
    }

    fn draft(mutation_id: String, request_hash: &str) -> WebJobOperationDraft {
        WebJobOperationDraft {
            operation_id: Uuid::new_v4().to_string(),
            actor_user_id: "web-user-a".to_owned(),
            method: "PUT".to_owned(),
            client_mutation_id: mutation_id,
            request_hash: request_hash.to_owned(),
            agent_id: "agent-a".to_owned(),
            action: "replace_catalog".to_owned(),
            target_job_id: None,
            response_json: "{\"accepted\":true}".to_owned(),
        }
    }

    async fn create_web_operation(database: &ServerDatabase, revision: u64) -> String {
        let operation = draft(
            Uuid::new_v4().to_string(),
            format!("{revision:064x}").as_str(),
        );
        database
            .replace_agent_job_catalog_for_web("agent-a", Vec::new(), revision, operation)
            .await
            .unwrap()
            .operation
            .operation_id
    }

    fn replace_all_command(catalog_revision: u64) -> JobCommand {
        JobCommand {
            command_id: Uuid::new_v4().as_bytes().to_vec(),
            action: Some(job_command::Action::ReplaceAll(ReplaceAllJobs {
                catalog_revision,
                jobs: Vec::new(),
            })),
        }
    }

    #[tokio::test]
    async fn replace_all_command_links_web_operation_and_results_control_retry_state() {
        let database = database().await;
        let operation_id = create_web_operation(&database, 0).await;
        let first_command = replace_all_command(1);
        let first_command_id = Uuid::from_slice(&first_command.command_id)
            .unwrap()
            .to_string();
        database
            .record_job_command("agent-a", &first_command)
            .await
            .unwrap();
        let linked = database
            .get_web_job_operation(&operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            linked.command_id.as_deref(),
            Some(first_command_id.as_str())
        );
        assert_eq!(linked.state, WEB_OPERATION_SENT);

        database
            .complete_job_command(
                "agent-a",
                &JobCommandResult {
                    command_id: first_command.command_id.clone(),
                    status: JobCommandStatus::ResyncRequired as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let retryable = database
            .get_web_job_operation(&operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retryable.state, WEB_OPERATION_PENDING);
        assert_eq!(retryable.command_id, None);

        let retry_command = replace_all_command(1);
        let retry_command_id = Uuid::from_slice(&retry_command.command_id)
            .unwrap()
            .to_string();
        database
            .record_job_command("agent-a", &retry_command)
            .await
            .unwrap();
        assert_eq!(
            database
                .get_web_job_operation(&operation_id)
                .await
                .unwrap()
                .unwrap()
                .command_id
                .as_deref(),
            Some(retry_command_id.as_str())
        );
        database
            .complete_job_command(
                "agent-a",
                &JobCommandResult {
                    command_id: retry_command.command_id,
                    status: JobCommandStatus::Applied as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            database
                .get_web_job_operation(&operation_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            WEB_OPERATION_APPLIED
        );
    }

    #[tokio::test]
    async fn late_command_ack_does_not_overwrite_superseded_web_operation() {
        let database = database().await;
        let old_operation_id = create_web_operation(&database, 0).await;
        let old_command = replace_all_command(1);
        database
            .record_job_command("agent-a", &old_command)
            .await
            .unwrap();
        let new_operation_id = create_web_operation(&database, 1).await;
        assert_eq!(
            database
                .get_web_job_operation(&old_operation_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            super::super::web_job_operations::WEB_OPERATION_SUPERSEDED
        );
        assert_eq!(
            database
                .get_web_job_operation(&new_operation_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            WEB_OPERATION_WAITING_AGENT
        );

        database
            .complete_job_command(
                "agent-a",
                &JobCommandResult {
                    command_id: old_command.command_id,
                    status: JobCommandStatus::Applied as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            database
                .get_web_job_operation(&old_operation_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            super::super::web_job_operations::WEB_OPERATION_SUPERSEDED
        );
    }
}
