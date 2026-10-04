//! Server 下发 JobCommand 与 Agent 处理结果的持久化 Adapter。

use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sea_orm::{ActiveModelTrait, ActiveValue::Set, EntityTrait};
use smalux_protocol::agent::v1::{JobCommand, JobCommandResult, job_command as command_action};
use uuid::Uuid;

use super::{DatabaseError, ServerDatabase, entity::job_command as command_record};

impl ServerDatabase {
    /// 在写入加密流前持久化待处理命令；相同 command_id 可安全重放。
    pub async fn record_job_command(
        &self,
        agent_id: &str,
        command: &JobCommand,
    ) -> Result<(), DatabaseError> {
        let command_id = command_id(&command.command_id)?;
        let catalog_revision = command_catalog_revision(command)?;
        let now = unix_micros()?;
        if let Some(previous) = command_record::Entity::find_by_id(&command_id)
            .one(self.connection())
            .await?
        {
            if previous.agent_id != agent_id || previous.command_payload != command.encode_to_vec()
            {
                return Err(DatabaseError::InvalidJobCommand(
                    "command id is already bound to different content".to_owned(),
                ));
            }
            return Ok(());
        }
        command_record::Entity::insert(command_record::ActiveModel {
            command_id: Set(command_id),
            agent_id: Set(agent_id.to_owned()),
            catalog_revision: Set(i64::try_from(catalog_revision).map_err(|_| {
                DatabaseError::InvalidJobCommand("catalog revision exceeds i64".to_owned())
            })?),
            command_payload: Set(command.encode_to_vec()),
            result_status: Set(None),
            result_payload: Set(None),
            created_at: Set(now),
            completed_at: Set(None),
        })
        .exec(self.connection())
        .await?;
        Ok(())
    }

    /// 将 Agent 的异步处理结果绑定回先前保存的命令。
    pub async fn complete_job_command(
        &self,
        agent_id: &str,
        result: &JobCommandResult,
    ) -> Result<(), DatabaseError> {
        let command_id = command_id(&result.command_id)?;
        let Some(model) = command_record::Entity::find_by_id(&command_id)
            .one(self.connection())
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
        active.completed_at = Set(Some(unix_micros()?));
        active.update(self.connection()).await?;
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
