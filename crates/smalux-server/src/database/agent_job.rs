//! 单 Agent 权威远程 Job 目录的数据库 Adapter。

use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, EntityTrait, QueryFilter, QueryOrder, TransactionTrait,
    sea_query::{Expr, OnConflict},
};
use smalux_protocol::agent::v1::{
    AgentCapabilitySnapshot, AgentJobPolicySnapshot, AgentPluginInventory, JobDefinition,
    ReplaceAllJobs, task_definition,
};
use uuid::Uuid;

use super::{
    DatabaseError, ServerDatabase,
    entity::{agent_job, agent_job_catalog, agent_job_version},
};

/// 数据库目录提交后的完整权威快照。
#[derive(Clone, Debug)]
pub struct AgentJobCatalogRecord {
    /// 该权威目录所属的稳定 Agent ID。
    pub agent_id: String,
    /// 当前目录的完整 ReplaceAll 快照及 catalog revision。
    pub catalog: ReplaceAllJobs,
    /// 数据库写入时间，Unix epoch 微秒。
    pub updated_at: i64,
}

impl ServerDatabase {
    /// 兼容旧调用方的完整替换入口；未提供期望版本时沿用无条件写入语义。
    pub async fn replace_agent_job_catalog(
        &self,
        agent_id: &str,
        jobs: Vec<JobDefinition>,
    ) -> Result<AgentJobCatalogRecord, DatabaseError> {
        self.replace_agent_job_catalog_if_revision(agent_id, jobs, None)
            .await
    }

    /// 原子替换一个 Agent 的全部远程 Job，并推进该 Agent 的目录版本。
    ///
    /// `JobDefinition.revision` 由控制面拥有；目录 revision 由数据库在每次权威替换时递增。
    /// 当 `expected_revision` 为 `Some` 时，提交只允许基于调用方读取到的版本进行，
    /// 版本不一致会在删除旧目录前返回 [`DatabaseError::RevisionConflict`]。
    pub async fn replace_agent_job_catalog_if_revision(
        &self,
        agent_id: &str,
        jobs: Vec<JobDefinition>,
        expected_revision: Option<u64>,
    ) -> Result<AgentJobCatalogRecord, DatabaseError> {
        validate_agent_id(agent_id)?;
        let mut validated = Vec::with_capacity(jobs.len());
        for definition in jobs {
            let task_kind = task_kind(&definition)?;
            validated.push((definition, task_kind));
        }
        validated.sort_by(|left, right| left.0.job_id.cmp(&right.0.job_id));
        if validated
            .windows(2)
            .any(|pair| pair[0].0.job_id == pair[1].0.job_id)
        {
            return Err(DatabaseError::InvalidJobCatalog(
                "Agent Job catalog contains duplicate job ids".to_owned(),
            ));
        }

        let transaction = self.connection().begin().await?;
        let now = unix_micros()?;
        let expected_revision = expected_revision
            .map(|value| {
                i64::try_from(value).map_err(|_| {
                    DatabaseError::InvalidJobCatalog(
                        "expected catalog revision exceeds i64".to_owned(),
                    )
                })
            })
            .transpose()?;
        let previous = agent_job_catalog::Entity::find_by_id(agent_id)
            .one(&transaction)
            .await?;
        let actual_revision = previous.as_ref().map(|value| value.revision).unwrap_or(0);
        if let Some(expected_revision) = expected_revision
            && actual_revision != expected_revision
        {
            return Err(DatabaseError::RevisionConflict {
                resource: "Agent Job catalog",
                expected: expected_revision,
                actual: actual_revision,
            });
        }

        // 首次创建没有可供条件 UPDATE 锁定的行，先用主键 INSERT 抢占目录。多个
        // Server 实例同时创建时，只有一个事务能成功；另一个会转换成 RevisionConflict。
        let created_catalog_with_cas = expected_revision == Some(0) && previous.is_none();
        if created_catalog_with_cas {
            let insert = agent_job_catalog::Entity::insert(agent_job_catalog::ActiveModel {
                agent_id: Set(agent_id.to_owned()),
                revision: Set(1),
                updated_at: Set(now),
            })
            .on_conflict(
                OnConflict::column(agent_job_catalog::Column::AgentId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&transaction)
            .await;
            match insert {
                Ok(_) => {}
                Err(sea_orm::DbErr::RecordNotInserted) => {
                    let actual = agent_job_catalog::Entity::find_by_id(agent_id)
                        .one(&transaction)
                        .await?
                        .map(|value| value.revision)
                        .unwrap_or(0);
                    return Err(DatabaseError::RevisionConflict {
                        resource: "Agent Job catalog",
                        expected: 0,
                        actual,
                    });
                }
                Err(error) => return Err(error.into()),
            }
        }
        let revision = previous
            .as_ref()
            .map(|value| value.revision)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| {
                DatabaseError::InvalidJobCatalog("catalog revision overflow".to_owned())
            })?;

        // 已有目录使用带 revision 条件的 UPDATE 抢占版本；即使另一个 Server 进程
        // 在前面的读取之后提交了更新，也只有一个调用可以成功推进版本。
        if let Some(expected_revision) = expected_revision
            && previous.is_some()
        {
            let updated = agent_job_catalog::Entity::update_many()
                .col_expr(agent_job_catalog::Column::Revision, Expr::value(revision))
                .col_expr(agent_job_catalog::Column::UpdatedAt, Expr::value(now))
                .filter(agent_job_catalog::Column::AgentId.eq(agent_id))
                .filter(agent_job_catalog::Column::Revision.eq(expected_revision))
                .exec(&transaction)
                .await?;
            if updated.rows_affected != 1 {
                let actual = agent_job_catalog::Entity::find_by_id(agent_id)
                    .one(&transaction)
                    .await?
                    .map(|value| value.revision)
                    .unwrap_or(0);
                return Err(DatabaseError::RevisionConflict {
                    resource: "Agent Job catalog",
                    expected: expected_revision,
                    actual,
                });
            }
        }

        // Agent 不接受业务版本倒退。检查历史最高版本，而非仅检查当前目录，
        // 因为 Agent 可能尚未收到删除命令，就又收到同一 Job 的旧版本。
        for (definition, _) in &validated {
            let revision = i64::try_from(definition.revision).map_err(|_| {
                DatabaseError::InvalidJobCatalog("Job revision exceeds i64".to_owned())
            })?;
            if let Some(latest) = agent_job_version::Entity::find()
                .filter(agent_job_version::Column::AgentId.eq(agent_id))
                .filter(agent_job_version::Column::JobId.eq(definition.job_id.clone()))
                .order_by_desc(agent_job_version::Column::Revision)
                .one(&transaction)
                .await?
                && revision < latest.revision
            {
                return Err(DatabaseError::InvalidJobCatalog(
                    "Job revision must not move backwards".to_owned(),
                ));
            }
        }

        agent_job::Entity::delete_many()
            .filter(agent_job::Column::AgentId.eq(agent_id))
            .exec(&transaction)
            .await?;
        for (definition, task_kind) in &validated {
            let job_id = Uuid::from_slice(&definition.job_id).map_err(|_| {
                DatabaseError::InvalidJobCatalog("Job id must contain 16 bytes".to_owned())
            })?;
            let definition_payload = definition.encode_to_vec();
            let version_key = format!("{agent_id}:{job_id}:{}", definition.revision);
            if let Some(previous_version) = agent_job_version::Entity::find_by_id(&version_key)
                .one(&transaction)
                .await?
            {
                if previous_version.task_kind != *task_kind
                    || previous_version.definition != definition_payload
                {
                    return Err(DatabaseError::InvalidJobCatalog(
                        "Job revision was previously stored with different content".to_owned(),
                    ));
                }
            } else {
                agent_job_version::Entity::insert(agent_job_version::ActiveModel {
                    version_key: Set(version_key),
                    agent_id: Set(agent_id.to_owned()),
                    job_id: Set(definition.job_id.clone()),
                    revision: Set(i64::try_from(definition.revision).map_err(|_| {
                        DatabaseError::InvalidJobCatalog("Job revision exceeds i64".to_owned())
                    })?),
                    task_kind: Set(task_kind.clone()),
                    definition: Set(definition_payload.clone()),
                    created_at: Set(now),
                })
                .exec(&transaction)
                .await?;
            }
            agent_job::Entity::insert(agent_job::ActiveModel {
                job_key: Set(format!("{agent_id}:{job_id}")),
                agent_id: Set(agent_id.to_owned()),
                job_id: Set(definition.job_id.clone()),
                revision: Set(i64::try_from(definition.revision).map_err(|_| {
                    DatabaseError::InvalidJobCatalog("Job revision exceeds i64".to_owned())
                })?),
                enabled: Set(definition.enabled),
                task_kind: Set(task_kind.clone()),
                definition: Set(definition_payload),
                created_at: Set(now),
                updated_at: Set(now),
            })
            .exec(&transaction)
            .await?;
        }

        match previous {
            Some(previous) => {
                // 带 expected_revision 的路径已经通过条件 UPDATE 推进版本；无 CAS
                // 的兼容路径仍由 ActiveModel 完成原有更新。
                if expected_revision.is_none() {
                    let mut active: agent_job_catalog::ActiveModel = previous.into();
                    active.revision = Set(revision);
                    active.updated_at = Set(now);
                    active.update(&transaction).await?;
                }
            }
            None => {
                if !created_catalog_with_cas {
                    agent_job_catalog::Entity::insert(agent_job_catalog::ActiveModel {
                        agent_id: Set(agent_id.to_owned()),
                        revision: Set(revision),
                        updated_at: Set(now),
                    })
                    .exec(&transaction)
                    .await?;
                }
            }
        }
        transaction.commit().await?;

        Ok(AgentJobCatalogRecord {
            agent_id: agent_id.to_owned(),
            catalog: ReplaceAllJobs {
                catalog_revision: revision as u64,
                jobs: validated.into_iter().map(|(job, _)| job).collect(),
            },
            updated_at: now,
        })
    }

    /// 返回 Agent 当前可接收的完整目录；本地黑名单在 Server 下发前就被应用。
    pub async fn load_agent_job_catalog(
        &self,
        agent_id: &str,
        policy: &AgentJobPolicySnapshot,
    ) -> Result<Option<AgentJobCatalogRecord>, DatabaseError> {
        let transaction = self.begin_snapshot_read().await?;
        let Some(catalog) = agent_job_catalog::Entity::find_by_id(agent_id)
            .one(&transaction)
            .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        let rows = agent_job::Entity::find()
            .filter(agent_job::Column::AgentId.eq(agent_id))
            .order_by_asc(agent_job::Column::JobId)
            .all(&transaction)
            .await?;
        transaction.commit().await?;
        let jobs = rows
            .into_iter()
            .filter(|row| !policy.denies_all_or_kind(&row.task_kind))
            .map(|row| {
                JobDefinition::decode(row.definition.as_slice()).map_err(|error| {
                    DatabaseError::InvalidJobCatalog(format!(
                        "stored Job {} cannot be decoded: {error}",
                        row.job_key
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(AgentJobCatalogRecord {
            agent_id: agent_id.to_owned(),
            catalog: ReplaceAllJobs {
                catalog_revision: catalog.revision as u64,
                jobs,
            },
            updated_at: catalog.updated_at,
        }))
    }

    /// 在 Agent 本地黑名单之外，再按本次连接的 Capability 与 Plugin inventory 过滤目录。
    pub async fn load_agent_job_catalog_for_session(
        &self,
        agent_id: &str,
        policy: &AgentJobPolicySnapshot,
        capability: &AgentCapabilitySnapshot,
        inventory: &AgentPluginInventory,
    ) -> Result<Option<AgentJobCatalogRecord>, DatabaseError> {
        let Some(mut catalog) = self.load_agent_job_catalog(agent_id, policy).await? else {
            return Ok(None);
        };
        catalog.catalog.jobs.retain(|job| {
            let Ok(kind) = task_kind(job) else {
                return false;
            };
            capability.task_kinds.iter().any(|value| value == &kind)
                || inventory
                    .plugins
                    .iter()
                    .any(|plugin| plugin.task_kinds.iter().any(|value| value == &kind))
        });
        Ok(Some(catalog))
    }
}

trait PolicyFilter {
    fn denies_all_or_kind(&self, task_kind: &str) -> bool;
}

impl PolicyFilter for AgentJobPolicySnapshot {
    fn denies_all_or_kind(&self, task_kind: &str) -> bool {
        self.deny_all
            || self
                .denied_task_kinds
                .iter()
                .any(|value| value == task_kind)
    }
}

fn validate_agent_id(agent_id: &str) -> Result<(), DatabaseError> {
    if agent_id.is_empty() || agent_id.len() > 64 {
        return Err(DatabaseError::InvalidJobCatalog(
            "Agent id is required for Job catalog".to_owned(),
        ));
    }
    Ok(())
}

fn task_kind(definition: &JobDefinition) -> Result<String, DatabaseError> {
    if definition.revision == 0 {
        return Err(DatabaseError::InvalidJobCatalog(
            "Job revision must be greater than zero".to_owned(),
        ));
    }
    if definition.job_id.len() != 16 {
        return Err(DatabaseError::InvalidJobCatalog(
            "Job id must contain 16 bytes".to_owned(),
        ));
    }
    let Some(task) = definition
        .task
        .as_ref()
        .and_then(|value| value.task.as_ref())
    else {
        return Err(DatabaseError::InvalidJobCatalog(
            "Job task definition is required".to_owned(),
        ));
    };
    Ok(match task {
        task_definition::Task::System(_) => "smalux.collect.system.v1",
        task_definition::Task::Cpu(_) => "smalux.collect.cpu.v1",
        task_definition::Task::Memory(_) => "smalux.collect.memory.v1",
        task_definition::Task::Load(_) => "smalux.collect.load.v1",
        task_definition::Task::Host(_) => "smalux.collect.host.v1",
        task_definition::Task::DiskIo(_) => "smalux.collect.disk_io.v1",
        task_definition::Task::NetworkIo(_) => "smalux.collect.network_io.v1",
        task_definition::Task::LocalIp(_) => "smalux.collect.local_ip.v1",
        task_definition::Task::PublicIp(_) => "smalux.collect.public_ip.v1",
        task_definition::Task::Process(_) => "smalux.collect.process.v1",
        task_definition::Task::Socket(_) => "smalux.collect.socket.v1",
        task_definition::Task::Probe(_) => "smalux.probe.network.v1",
        task_definition::Task::Plugin(config) if !config.task_kind.is_empty() => {
            return Ok(config.task_kind.clone());
        }
        task_definition::Task::Plugin(_) => {
            return Err(DatabaseError::InvalidJobCatalog(
                "Plugin Job task kind is required".to_owned(),
            ));
        }
    }
    .to_owned())
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
    use smalux_protocol::agent::v1::{TaskDefinition, task_definition};

    fn job(revision: u64) -> JobDefinition {
        JobDefinition {
            job_id: Uuid::new_v4().as_bytes().to_vec(),
            revision,
            enabled: true,
            task: Some(TaskDefinition {
                task: Some(task_definition::Task::Cpu(Default::default())),
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn replaces_catalog_and_filters_agent_policy() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("database should connect");
        let now = unix_micros().unwrap();
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![7; 32]),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
        let first = job(1);
        let stored = database
            .replace_agent_job_catalog("agent-a", vec![first.clone()])
            .await
            .unwrap();
        assert_eq!(stored.catalog.catalog_revision, 1);
        let policy = AgentJobPolicySnapshot {
            denied_task_kinds: vec!["smalux.collect.cpu.v1".to_owned()],
            ..Default::default()
        };
        let filtered = database
            .load_agent_job_catalog("agent-a", &policy)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(filtered.catalog.catalog_revision, 1);
        assert!(filtered.catalog.jobs.is_empty());
    }

    #[tokio::test]
    async fn catalog_cas_rejects_stale_revision_before_replacing_rows() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
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

        let first = database
            .replace_agent_job_catalog("agent-a", vec![job(1)])
            .await
            .unwrap();
        let stale = database
            .replace_agent_job_catalog_if_revision("agent-a", vec![job(2)], Some(0))
            .await
            .unwrap_err();
        assert!(matches!(
            stale,
            DatabaseError::RevisionConflict {
                expected: 0,
                actual: 1,
                ..
            }
        ));
        let current = database
            .load_agent_job_catalog("agent-a", &Default::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            current.catalog.catalog_revision,
            first.catalog.catalog_revision
        );

        let updated = database
            .replace_agent_job_catalog_if_revision("agent-a", Vec::new(), Some(1))
            .await
            .unwrap();
        assert_eq!(updated.catalog.catalog_revision, 2);
        assert!(updated.catalog.jobs.is_empty());
    }

    async fn regression_database() -> ServerDatabase {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        for (id, key) in [("agent-a", 1), ("agent-b", 2)] {
            agent::Entity::insert(agent::ActiveModel {
                agent_id: Set(id.to_owned()),
                name: Set(id.to_owned()),
                public_key: Set(vec![key; 32]),
                status: Set("active".to_owned()),
                created_at: Set(1),
                updated_at: Set(1),
                revoked_at: Set(None),
            })
            .exec(database.connection())
            .await
            .unwrap();
        }
        database
    }

    #[tokio::test]
    async fn regression_job_revision_rejects_rollback_without_changing_catalog() {
        let database = regression_database().await;
        let initial = job(2);
        let original = database
            .replace_agent_job_catalog("agent-a", vec![initial.clone()])
            .await
            .unwrap();
        let mut stale = initial.clone();
        stale.revision = 1;
        for expected in [None, Some(original.catalog.catalog_revision)] {
            assert!(
                database
                    .replace_agent_job_catalog_if_revision("agent-a", vec![stale.clone()], expected)
                    .await
                    .is_err()
            );
            assert_eq!(
                database
                    .load_agent_job_catalog("agent-a", &Default::default())
                    .await
                    .unwrap()
                    .unwrap()
                    .catalog,
                original.catalog
            );
        }
        assert!(
            agent_job_version::Entity::find()
                .filter(agent_job_version::Column::Revision.eq(1))
                .one(database.connection())
                .await
                .unwrap()
                .is_none()
        );
        // 同内容重放允许；同版本篡改内容仍必须拒绝。
        let replay = database
            .replace_agent_job_catalog("agent-a", vec![initial.clone()])
            .await
            .unwrap();
        let mut changed = initial.clone();
        changed.enabled = !changed.enabled;
        assert!(
            database
                .replace_agent_job_catalog("agent-a", vec![changed])
                .await
                .is_err()
        );
        assert_eq!(
            database
                .load_agent_job_catalog("agent-a", &Default::default())
                .await
                .unwrap()
                .unwrap()
                .catalog,
            replay.catalog
        );
        let mut newer = initial.clone();
        newer.revision = 3;
        database
            .replace_agent_job_catalog("agent-a", vec![newer])
            .await
            .unwrap();
        let cleared = database
            .replace_agent_job_catalog("agent-a", Vec::new())
            .await
            .unwrap();
        assert!(
            database
                .replace_agent_job_catalog("agent-a", vec![initial])
                .await
                .is_err()
        );
        assert_eq!(
            database
                .load_agent_job_catalog("agent-a", &Default::default())
                .await
                .unwrap()
                .unwrap()
                .catalog,
            cleared.catalog
        );
        // 版本边界属于单 Agent，不影响另一个 Agent 使用同一 Job ID。
        database
            .replace_agent_job_catalog("agent-b", vec![stale])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn regression_catalog_snapshot_remains_consistent_during_updates_and_clears() {
        let (database, directory) = super::super::connection::snapshot_test_database().await;
        assert!(
            database
                .load_agent_job_catalog("agent-a", &Default::default())
                .await
                .unwrap()
                .is_none()
        );
        let initial = job(1);
        database
            .replace_agent_job_catalog("agent-a", vec![initial.clone()])
            .await
            .unwrap();
        let writer_database = database.clone();
        let writer = tokio::spawn(async move {
            for revision in 2..=101u64 {
                let mut definition = initial.clone();
                definition.revision = revision;
                let jobs = if revision.is_multiple_of(3) {
                    Vec::new()
                } else {
                    vec![definition]
                };
                assert_eq!(
                    writer_database
                        .replace_agent_job_catalog("agent-a", jobs)
                        .await
                        .unwrap()
                        .catalog
                        .catalog_revision,
                    revision
                );
                tokio::task::yield_now().await;
            }
        });
        for _ in 0..300 {
            let snapshot = database
                .load_agent_job_catalog("agent-a", &Default::default())
                .await
                .unwrap()
                .unwrap()
                .catalog;
            if snapshot.catalog_revision.is_multiple_of(3) {
                assert!(snapshot.jobs.is_empty());
            } else {
                assert_eq!(snapshot.jobs.len(), 1);
                assert_eq!(snapshot.jobs[0].revision, snapshot.catalog_revision);
            }
            tokio::task::yield_now().await;
        }
        writer.await.unwrap();
        database.connection().clone().close().await.unwrap();
        drop(database);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
