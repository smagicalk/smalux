//! 成功 TaskReport 的幂等持久化 Adapter。

use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    sea_query::OnConflict,
};
use smalux_protocol::agent::v1::{TaskReport, task_result};
use uuid::Uuid;

use super::{
    DatabaseError, ServerDatabase,
    entity::{agent_job_version, task_report},
};

/// 用于管理查询的 TaskReport 元数据和原始 Proto payload。
#[derive(Clone, Debug)]
pub struct TaskReportRecord {
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

impl ServerDatabase {
    /// 幂等保存一条成功执行报告。重复投递相同执行身份时不新增历史行。
    pub async fn append_task_report(
        &self,
        agent_id: &str,
        report: &TaskReport,
    ) -> Result<(), DatabaseError> {
        if agent_id.is_empty() {
            return Err(DatabaseError::InvalidTaskReport(
                "agent id is required".to_owned(),
            ));
        }
        let job_id = Uuid::from_slice(&report.job_id).map_err(|_| {
            DatabaseError::InvalidTaskReport("Job id must contain 16 bytes".to_owned())
        })?;
        let run_id = Uuid::from_slice(&report.run_id).map_err(|_| {
            DatabaseError::InvalidTaskReport("run id must contain 16 bytes".to_owned())
        })?;
        if report.job_revision == 0 || report.attempt == 0 {
            return Err(DatabaseError::InvalidTaskReport(
                "Job revision and attempt must be greater than zero".to_owned(),
            ));
        }
        let result_kind = result_kind(report)?;
        let job_revision = i64::try_from(report.job_revision)
            .map_err(|_| DatabaseError::InvalidTaskReport("Job revision exceeds i64".to_owned()))?;
        let Some(job_version) = agent_job_version::Entity::find()
            .filter(agent_job_version::Column::AgentId.eq(agent_id))
            .filter(agent_job_version::Column::JobId.eq(report.job_id.clone()))
            .filter(agent_job_version::Column::Revision.eq(job_revision))
            .one(self.connection())
            .await?
        else {
            return Err(DatabaseError::InvalidTaskReport(
                "Job revision does not belong to this Agent".to_owned(),
            ));
        };
        validate_result_matches_job(report, &result_kind, &job_version)?;
        let report_id = format!(
            "{agent_id}:{job_id}:{}:{run_id}:{}",
            report.job_revision, report.attempt
        );
        let payload = report.encode_to_vec();
        if let Some(previous) = task_report::Entity::find_by_id(&report_id)
            .one(self.connection())
            .await?
        {
            if previous.payload != payload {
                return Err(DatabaseError::InvalidTaskReport(
                    "duplicate execution identity has different payload".to_owned(),
                ));
            }
            return Ok(());
        }
        let insert = task_report::Entity::insert(task_report::ActiveModel {
            report_id: Set(report_id.clone()),
            agent_id: Set(agent_id.to_owned()),
            job_id: Set(report.job_id.clone()),
            job_revision: Set(job_revision),
            run_id: Set(report.run_id.clone()),
            attempt: Set(i32::try_from(report.attempt)
                .map_err(|_| DatabaseError::InvalidTaskReport("attempt exceeds i32".to_owned()))?),
            scheduled_at: Set(timestamp_micros(report.scheduled_at.as_ref())?),
            started_at: Set(timestamp_micros(report.started_at.as_ref())?),
            result_kind: Set(result_kind),
            payload: Set(payload.clone()),
            received_at: Set(unix_micros()?),
        })
        .on_conflict(
            OnConflict::column(task_report::Column::ReportId)
                .do_nothing()
                .to_owned(),
        )
        .exec(self.connection())
        .await;
        match insert {
            Ok(_) => Ok(()),
            Err(sea_orm::DbErr::RecordNotInserted) => {
                let Some(previous) = task_report::Entity::find_by_id(report_id)
                    .one(self.connection())
                    .await?
                else {
                    return Err(DatabaseError::InvalidTaskReport(
                        "duplicate TaskReport insert did not produce a row".to_owned(),
                    ));
                };
                if previous.payload == payload {
                    Ok(())
                } else {
                    Err(DatabaseError::InvalidTaskReport(
                        "duplicate execution identity has different payload".to_owned(),
                    ))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    /// 按接收时间倒序列出报告；payload 仅由显式管理查询返回。
    pub async fn list_task_reports(
        &self,
        agent_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskReportRecord>, DatabaseError> {
        let mut query = task_report::Entity::find();
        if let Some(agent_id) = agent_id {
            query = query.filter(task_report::Column::AgentId.eq(agent_id));
        }
        Ok(query
            .order_by_desc(task_report::Column::ReceivedAt)
            .limit(limit.clamp(1, 500))
            .all(self.connection())
            .await?
            .into_iter()
            .map(|row| TaskReportRecord {
                report_id: row.report_id,
                agent_id: row.agent_id,
                job_id: row.job_id,
                job_revision: row.job_revision,
                run_id: row.run_id,
                attempt: row.attempt,
                scheduled_at: row.scheduled_at,
                started_at: row.started_at,
                result_kind: row.result_kind,
                payload: row.payload,
                received_at: row.received_at,
            })
            .collect())
    }
}

fn result_kind(report: &TaskReport) -> Result<String, DatabaseError> {
    let result = report
        .result
        .as_ref()
        .and_then(|value| value.result.as_ref())
        .ok_or_else(|| DatabaseError::InvalidTaskReport("Task result is required".to_owned()))?;
    Ok(match result {
        task_result::Result::System(_) => "system",
        task_result::Result::Cpu(_) => "cpu",
        task_result::Result::Memory(_) => "memory",
        task_result::Result::Load(_) => "load",
        task_result::Result::Host(_) => "host",
        task_result::Result::DiskIo(_) => "disk_io",
        task_result::Result::NetworkIo(_) => "network_io",
        task_result::Result::LocalIp(_) => "local_ip",
        task_result::Result::PublicIp(_) => "public_ip",
        task_result::Result::Process(_) => "process",
        task_result::Result::Socket(_) => "socket",
        task_result::Result::Probe(_) => "probe",
        task_result::Result::Plugin(_) => "plugin",
    }
    .to_owned())
}

/// 检查报告结果的 oneof 类型是否与历史 Job 定义一致。
fn validate_result_matches_job(
    report: &TaskReport,
    result_kind: &str,
    job_version: &agent_job_version::Model,
) -> Result<(), DatabaseError> {
    let expected = job_version.task_kind.as_str();
    let valid_builtin = matches!(
        (expected, result_kind),
        ("smalux.collect.system.v1", "system")
            | ("smalux.collect.cpu.v1", "cpu")
            | ("smalux.collect.memory.v1", "memory")
            | ("smalux.collect.load.v1", "load")
            | ("smalux.collect.host.v1", "host")
            | ("smalux.collect.disk_io.v1", "disk_io")
            | ("smalux.collect.network_io.v1", "network_io")
            | ("smalux.collect.local_ip.v1", "local_ip")
            | ("smalux.collect.public_ip.v1", "public_ip")
            | ("smalux.collect.process.v1", "process")
            | ("smalux.collect.socket.v1", "socket")
            | ("smalux.probe.network.v1", "probe")
    );
    if valid_builtin {
        return Ok(());
    }
    let Some(task_result::Result::Plugin(plugin_result)) = report
        .result
        .as_ref()
        .and_then(|value| value.result.as_ref())
    else {
        return Err(DatabaseError::InvalidTaskReport(
            "Task result kind does not match the historical Job definition".to_owned(),
        ));
    };
    let definition =
        smalux_protocol::agent::v1::JobDefinition::decode(job_version.definition.as_slice())
            .map_err(|error| {
                DatabaseError::InvalidTaskReport(format!(
                    "historical Job definition cannot be decoded: {error}"
                ))
            })?;
    let Some(smalux_protocol::agent::v1::task_definition::Task::Plugin(plugin)) =
        definition.task.and_then(|task| task.task)
    else {
        return Err(DatabaseError::InvalidTaskReport(
            "plugin result does not match the historical Job definition".to_owned(),
        ));
    };
    if plugin.task_kind != expected
        || plugin_result.task_kind != expected
        || plugin_result.plugin_id != plugin.plugin_id
        || plugin_result.plugin_version != plugin.plugin_version
    {
        return Err(DatabaseError::InvalidTaskReport(
            "plugin result identity does not match the historical Job definition".to_owned(),
        ));
    }
    Ok(())
}

fn timestamp_micros(
    timestamp: Option<&prost_types::Timestamp>,
) -> Result<Option<i64>, DatabaseError> {
    let Some(timestamp) = timestamp else {
        return Ok(None);
    };
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(DatabaseError::InvalidTaskReport(
            "timestamp nanos are invalid".to_owned(),
        ));
    }
    Ok(Some(
        timestamp
            .seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(timestamp.nanos) / 1_000),
    ))
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
    use crate::config::DatabaseConfig;
    use crate::database::entity::agent;
    use sea_orm::Set;
    use smalux_protocol::agent::v1::{
        CpuSnapshot, JobDefinition, TaskDefinition, TaskResult, task_definition,
    };

    #[tokio::test]
    async fn duplicate_execution_report_is_idempotent() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("database should connect");
        let now = unix_micros().unwrap();
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![8; 32]),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
        let job_id = Uuid::new_v4();
        database
            .replace_agent_job_catalog(
                "agent-a",
                vec![JobDefinition {
                    job_id: job_id.as_bytes().to_vec(),
                    revision: 1,
                    enabled: true,
                    task: Some(TaskDefinition {
                        task: Some(task_definition::Task::Cpu(Default::default())),
                    }),
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let report = TaskReport {
            job_id: job_id.as_bytes().to_vec(),
            job_revision: 1,
            run_id: Uuid::new_v4().as_bytes().to_vec(),
            attempt: 1,
            result: Some(TaskResult {
                result: Some(task_result::Result::Cpu(CpuSnapshot::default())),
                ..Default::default()
            }),
            ..Default::default()
        };
        database
            .append_task_report("agent-a", &report)
            .await
            .unwrap();
        database
            .append_task_report("agent-a", &report)
            .await
            .unwrap();
        let rows = task_report::Entity::find()
            .all(database.connection())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn accepts_historical_job_revision_after_current_catalog_is_replaced() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let now = unix_micros().unwrap();
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![6; 32]),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
        let job_id = Uuid::new_v4();
        let first = JobDefinition {
            job_id: job_id.as_bytes().to_vec(),
            revision: 1,
            enabled: true,
            task: Some(TaskDefinition {
                task: Some(task_definition::Task::Cpu(Default::default())),
            }),
            ..Default::default()
        };
        database
            .replace_agent_job_catalog("agent-a", vec![first])
            .await
            .unwrap();
        database
            .replace_agent_job_catalog(
                "agent-a",
                vec![JobDefinition {
                    job_id: job_id.as_bytes().to_vec(),
                    revision: 2,
                    enabled: true,
                    task: Some(TaskDefinition {
                        task: Some(task_definition::Task::Cpu(Default::default())),
                    }),
                    ..Default::default()
                }],
            )
            .await
            .unwrap();
        let report = TaskReport {
            job_id: job_id.as_bytes().to_vec(),
            job_revision: 1,
            run_id: Uuid::new_v4().as_bytes().to_vec(),
            attempt: 1,
            result: Some(TaskResult {
                result: Some(task_result::Result::Cpu(CpuSnapshot::default())),
                ..Default::default()
            }),
            ..Default::default()
        };
        database
            .append_task_report("agent-a", &report)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rejects_report_for_unknown_agent_job_revision() {
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
        let report = TaskReport {
            job_id: Uuid::new_v4().as_bytes().to_vec(),
            job_revision: 1,
            run_id: Uuid::new_v4().as_bytes().to_vec(),
            attempt: 1,
            result: Some(TaskResult {
                result: Some(task_result::Result::Cpu(CpuSnapshot::default())),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(matches!(
            database.append_task_report("agent-a", &report).await,
            Err(DatabaseError::InvalidTaskReport(_))
        ));
    }
}
