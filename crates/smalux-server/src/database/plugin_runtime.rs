//! Plus Worker 运行时快照的持久化 Adapter。

use std::time::{SystemTime, UNIX_EPOCH};

use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, EntityTrait, QueryFilter, QueryOrder, TransactionTrait,
    sea_query::{Expr, OnConflict},
};
use smalux_protocol::agent::v1::{
    AgentPluginInventory, PluginRuntimeConfig, PluginRuntimeSnapshot,
};

use super::{
    DatabaseError, ServerDatabase,
    entity::{agent_plugin_runtime, agent_plugin_runtime_catalog},
};

/// 无持久化配置时发布的空快照版本；首次保存必须严格高于它。
const EMPTY_RUNTIME_REVISION: i64 = 1;

/// 控制面保存的单插件运行时配置，Schema hash 保证配置只下发给同一插件构建。
#[derive(Clone, Debug)]
pub struct StoredPluginRuntime {
    pub plugin_id: String,
    pub plugin_version: String,
    pub schema_hash: Vec<u8>,
    pub schema_version: u32,
    pub config: Vec<u8>,
    pub requested_concurrency: u32,
}

/// 一个 Agent 当前保存的完整期望 runtime 快照。
#[derive(Clone, Debug)]
pub struct AgentPluginRuntimeRecord {
    /// 该 runtime 快照所属的稳定 Agent ID。
    pub agent_id: String,
    /// Server 当前期望的 runtime revision。
    pub revision: u64,
    /// 按插件 ID 和版本排序的完整配置集合。
    pub runtimes: Vec<StoredPluginRuntime>,
}

impl ServerDatabase {
    /// 兼容旧调用方的完整替换入口；未提供期望版本时沿用无条件写入语义。
    pub async fn replace_agent_plugin_runtime(
        &self,
        agent_id: &str,
        runtimes: Vec<StoredPluginRuntime>,
    ) -> Result<u64, DatabaseError> {
        self.replace_agent_plugin_runtime_if_revision(agent_id, runtimes, None)
            .await
    }

    /// 原子替换某 Agent 的全部期望 Worker 运行时配置，并推进 snapshot revision。
    /// 当 `expected_revision` 为 `Some` 时，版本不一致会在删除旧 runtime 前返回冲突。
    pub async fn replace_agent_plugin_runtime_if_revision(
        &self,
        agent_id: &str,
        runtimes: Vec<StoredPluginRuntime>,
        expected_revision: Option<u64>,
    ) -> Result<u64, DatabaseError> {
        if agent_id.is_empty() || agent_id.len() > 64 {
            return Err(DatabaseError::InvalidPluginRuntime(
                "Agent id is required".to_owned(),
            ));
        }
        let mut runtimes = runtimes;
        runtimes.sort_by(|left, right| {
            (&left.plugin_id, &left.plugin_version).cmp(&(&right.plugin_id, &right.plugin_version))
        });
        if runtimes.windows(2).any(|pair| {
            pair[0].plugin_id == pair[1].plugin_id
                && pair[0].plugin_version == pair[1].plugin_version
        }) {
            return Err(DatabaseError::InvalidPluginRuntime(
                "duplicate plugin runtime identity".to_owned(),
            ));
        }
        for runtime in &runtimes {
            validate_runtime(runtime)?;
        }

        let transaction = self.connection().begin().await?;
        let now = unix_micros()?;
        let expected_revision = expected_revision
            .map(|value| {
                i64::try_from(value).map_err(|_| {
                    DatabaseError::InvalidPluginRuntime(
                        "expected runtime revision exceeds i64".to_owned(),
                    )
                })
            })
            .transpose()?;
        let previous = agent_plugin_runtime_catalog::Entity::find_by_id(agent_id)
            .one(&transaction)
            .await?;
        let actual_revision = previous.as_ref().map(|value| value.revision).unwrap_or(0);
        if let Some(expected_revision) = expected_revision
            && actual_revision != expected_revision
        {
            return Err(DatabaseError::RevisionConflict {
                resource: "Agent plugin runtime",
                expected: expected_revision,
                actual: actual_revision,
            });
        }
        let revision = previous
            .as_ref()
            .map(|value| value.revision)
            .unwrap_or(EMPTY_RUNTIME_REVISION)
            .checked_add(1)
            .ok_or_else(|| {
                DatabaseError::InvalidPluginRuntime("runtime revision overflow".to_owned())
            })?;
        // 已有 runtime 目录使用条件 UPDATE 抢占版本，跨 Server 进程也不会同时通过
        // 同一个 expected_revision。首次创建则由唯一主键 INSERT 负责竞争。
        if let Some(expected_revision) = expected_revision
            && previous.is_some()
        {
            let updated = agent_plugin_runtime_catalog::Entity::update_many()
                .col_expr(
                    agent_plugin_runtime_catalog::Column::Revision,
                    Expr::value(revision),
                )
                .col_expr(
                    agent_plugin_runtime_catalog::Column::UpdatedAt,
                    Expr::value(now),
                )
                .filter(agent_plugin_runtime_catalog::Column::AgentId.eq(agent_id))
                .filter(agent_plugin_runtime_catalog::Column::Revision.eq(expected_revision))
                .exec(&transaction)
                .await?;
            if updated.rows_affected != 1 {
                let actual = agent_plugin_runtime_catalog::Entity::find_by_id(agent_id)
                    .one(&transaction)
                    .await?
                    .map(|value| value.revision)
                    .unwrap_or(0);
                return Err(DatabaseError::RevisionConflict {
                    resource: "Agent plugin runtime",
                    expected: expected_revision,
                    actual,
                });
            }
        }
        let created_catalog_with_cas = expected_revision == Some(0) && previous.is_none();
        if created_catalog_with_cas {
            let insert = agent_plugin_runtime_catalog::Entity::insert(
                agent_plugin_runtime_catalog::ActiveModel {
                    agent_id: Set(agent_id.to_owned()),
                    revision: Set(revision),
                    updated_at: Set(now),
                },
            )
            .on_conflict(
                OnConflict::column(agent_plugin_runtime_catalog::Column::AgentId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(&transaction)
            .await;
            match insert {
                Ok(_) => {}
                Err(sea_orm::DbErr::RecordNotInserted) => {
                    let actual = agent_plugin_runtime_catalog::Entity::find_by_id(agent_id)
                        .one(&transaction)
                        .await?
                        .map(|value| value.revision)
                        .unwrap_or(0);
                    return Err(DatabaseError::RevisionConflict {
                        resource: "Agent plugin runtime",
                        expected: 0,
                        actual,
                    });
                }
                Err(error) => return Err(error.into()),
            }
        }
        agent_plugin_runtime::Entity::delete_many()
            .filter(agent_plugin_runtime::Column::AgentId.eq(agent_id))
            .exec(&transaction)
            .await?;
        for runtime in &runtimes {
            agent_plugin_runtime::Entity::insert(agent_plugin_runtime::ActiveModel {
                runtime_key: Set(format!(
                    "{agent_id}:{}:{}",
                    runtime.plugin_id, runtime.plugin_version
                )),
                agent_id: Set(agent_id.to_owned()),
                plugin_id: Set(runtime.plugin_id.clone()),
                plugin_version: Set(runtime.plugin_version.clone()),
                schema_hash: Set(runtime.schema_hash.clone()),
                schema_version: Set(runtime.schema_version as i32),
                config: Set(runtime.config.clone()),
                requested_concurrency: Set(runtime.requested_concurrency as i32),
                updated_at: Set(now),
            })
            .exec(&transaction)
            .await?;
        }
        match previous {
            Some(previous) => {
                // CAS 分支已推进目录版本，不再重复执行无条件 UPDATE。
                if expected_revision.is_none() {
                    let mut active: agent_plugin_runtime_catalog::ActiveModel = previous.into();
                    active.revision = Set(revision);
                    active.updated_at = Set(now);
                    active.update(&transaction).await?;
                }
            }
            None => {
                if !created_catalog_with_cas {
                    agent_plugin_runtime_catalog::Entity::insert(
                        agent_plugin_runtime_catalog::ActiveModel {
                            agent_id: Set(agent_id.to_owned()),
                            revision: Set(revision),
                            updated_at: Set(now),
                        },
                    )
                    .exec(&transaction)
                    .await?;
                }
            }
        }
        transaction.commit().await?;
        Ok(revision as u64)
    }

    /// 构造仅包含 Agent 当前已安装且 Schema hash 匹配插件的运行时快照。
    pub async fn load_agent_plugin_runtime(
        &self,
        agent_id: &str,
        inventory: &AgentPluginInventory,
    ) -> Result<PluginRuntimeSnapshot, DatabaseError> {
        let Some(record) = self.get_agent_plugin_runtime(agent_id).await? else {
            return Ok(PluginRuntimeSnapshot {
                revision: EMPTY_RUNTIME_REVISION as u64,
                plugins: Vec::new(),
            });
        };
        let revision = record.revision;
        let plugins = record
            .runtimes
            .into_iter()
            .filter(|runtime| {
                inventory.plugins.iter().any(|installed| {
                    installed.plugin_id == runtime.plugin_id
                        && installed.version == runtime.plugin_version
                        && installed.schema_hash == runtime.schema_hash
                })
            })
            .map(|runtime| PluginRuntimeConfig {
                plugin_id: runtime.plugin_id,
                version: runtime.plugin_version,
                schema_version: runtime.schema_version,
                config: runtime.config,
                requested_concurrency: runtime.requested_concurrency,
            })
            .collect();
        Ok(PluginRuntimeSnapshot { revision, plugins })
    }

    /// 读取管理面使用的未过滤 runtime 快照；不会依赖当前在线 inventory。
    pub async fn get_agent_plugin_runtime(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentPluginRuntimeRecord>, DatabaseError> {
        let transaction = self.begin_snapshot_read().await?;
        let Some(catalog) = agent_plugin_runtime_catalog::Entity::find_by_id(agent_id)
            .one(&transaction)
            .await?
        else {
            transaction.commit().await?;
            return Ok(None);
        };
        let rows = agent_plugin_runtime::Entity::find()
            .filter(agent_plugin_runtime::Column::AgentId.eq(agent_id))
            .order_by_asc(agent_plugin_runtime::Column::PluginId)
            .order_by_asc(agent_plugin_runtime::Column::PluginVersion)
            .all(&transaction)
            .await?;
        transaction.commit().await?;
        Ok(Some(AgentPluginRuntimeRecord {
            agent_id: agent_id.to_owned(),
            revision: catalog.revision as u64,
            runtimes: rows
                .into_iter()
                .map(|row| StoredPluginRuntime {
                    plugin_id: row.plugin_id,
                    plugin_version: row.plugin_version,
                    schema_hash: row.schema_hash,
                    schema_version: row.schema_version as u32,
                    config: row.config,
                    requested_concurrency: row.requested_concurrency as u32,
                })
                .collect(),
        }))
    }
}

fn validate_runtime(runtime: &StoredPluginRuntime) -> Result<(), DatabaseError> {
    if runtime.plugin_id.is_empty()
        || runtime.plugin_id.len() > 256
        || runtime.plugin_version.is_empty()
        || runtime.plugin_version.len() > 64
        || runtime.schema_hash.len() != 32
        || runtime.schema_version == 0
        || runtime.requested_concurrency == 0
    {
        return Err(DatabaseError::InvalidPluginRuntime(
            "plugin runtime identity or limits are invalid".to_owned(),
        ));
    }
    Ok(())
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
    use sea_orm::{ActiveValue::Set, EntityTrait};
    use smalux_protocol::agent::v1::PluginInventoryEntry;

    #[tokio::test]
    async fn runtime_is_sent_only_to_matching_inventory_hash() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![2; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
        database
            .replace_agent_plugin_runtime(
                "agent-a",
                vec![StoredPluginRuntime {
                    plugin_id: "example.plugin".to_owned(),
                    plugin_version: "1.0.0".to_owned(),
                    schema_hash: vec![7; 32],
                    schema_version: 1,
                    config: vec![1],
                    requested_concurrency: 2,
                }],
            )
            .await
            .unwrap();
        let matched = database
            .load_agent_plugin_runtime(
                "agent-a",
                &AgentPluginInventory {
                    revision: 1,
                    plugins: vec![PluginInventoryEntry {
                        plugin_id: "example.plugin".to_owned(),
                        version: "1.0.0".to_owned(),
                        task_kinds: Vec::new(),
                        schema_hash: vec![7; 32],
                        schema_format_version: 1,
                    }],
                },
            )
            .await
            .unwrap();
        assert_eq!(matched.revision, 2);
        assert_eq!(matched.plugins.len(), 1);
        let mismatched = database
            .load_agent_plugin_runtime(
                "agent-a",
                &AgentPluginInventory {
                    revision: 2,
                    plugins: vec![PluginInventoryEntry {
                        plugin_id: "example.plugin".to_owned(),
                        version: "1.0.0".to_owned(),
                        task_kinds: Vec::new(),
                        schema_hash: vec![8; 32],
                        schema_format_version: 1,
                    }],
                },
            )
            .await
            .unwrap();
        assert!(mismatched.plugins.is_empty());
    }

    #[tokio::test]
    async fn runtime_cas_rejects_stale_revision_before_replacing_rows() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        agent::Entity::insert(agent::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            name: Set("Agent A".to_owned()),
            public_key: Set(vec![3; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            revoked_at: Set(None),
        })
        .exec(database.connection())
        .await
        .unwrap();
        let runtime = StoredPluginRuntime {
            plugin_id: "example.plugin".to_owned(),
            plugin_version: "1.0.0".to_owned(),
            schema_hash: vec![9; 32],
            schema_version: 1,
            config: vec![1],
            requested_concurrency: 1,
        };
        assert_eq!(
            database
                .replace_agent_plugin_runtime("agent-a", vec![runtime.clone()])
                .await
                .unwrap(),
            2
        );
        let stale = database
            .replace_agent_plugin_runtime_if_revision("agent-a", Vec::new(), Some(0))
            .await
            .unwrap_err();
        assert!(matches!(
            stale,
            DatabaseError::RevisionConflict {
                expected: 0,
                actual: 2,
                ..
            }
        ));
        let current = database
            .get_agent_plugin_runtime("agent-a")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.runtimes.len(), 1);
        assert_eq!(current.runtimes[0].plugin_id, runtime.plugin_id);
        assert_eq!(
            database
                .replace_agent_plugin_runtime_if_revision("agent-a", Vec::new(), Some(2))
                .await
                .unwrap(),
            3
        );
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

    fn regression_runtime() -> StoredPluginRuntime {
        StoredPluginRuntime {
            plugin_id: "example.plugin".to_owned(),
            plugin_version: "1.0.0".to_owned(),
            schema_hash: vec![7; 32],
            schema_version: 1,
            config: Vec::new(),
            requested_concurrency: 2,
        }
    }

    fn regression_inventory() -> AgentPluginInventory {
        AgentPluginInventory {
            revision: 1,
            plugins: vec![PluginInventoryEntry {
                plugin_id: "example.plugin".to_owned(),
                version: "1.0.0".to_owned(),
                task_kinds: Vec::new(),
                schema_hash: vec![7; 32],
                schema_format_version: 1,
            }],
        }
    }

    #[tokio::test]
    async fn regression_first_runtime_advances_beyond_the_empty_snapshot() {
        use smalux_agent::plugins::{PluginRuntimeState, RuntimeSnapshotResult};
        let database = regression_database().await;
        let mut state = PluginRuntimeState::default();
        let empty = database
            .load_agent_plugin_runtime("agent-a", &regression_inventory())
            .await
            .unwrap();
        assert_eq!(empty.revision, 1);
        assert!(empty.plugins.is_empty());
        state.confirm_snapshot(&empty);
        let revision = database
            .replace_agent_plugin_runtime("agent-a", vec![regression_runtime()])
            .await
            .unwrap();
        assert_eq!(revision, 2);
        let first = database
            .load_agent_plugin_runtime("agent-a", &regression_inventory())
            .await
            .unwrap();
        assert_eq!(
            state.validate_snapshot(&first).unwrap(),
            RuntimeSnapshotResult::Applied
        );
        state.confirm_snapshot(&first);
        assert_eq!(
            state.validate_snapshot(&first).unwrap(),
            RuntimeSnapshotResult::IgnoredStale
        );
        let mut conflict = first.clone();
        conflict.plugins.clear();
        assert!(state.validate_snapshot(&conflict).is_err());
        assert_eq!(
            database
                .replace_agent_plugin_runtime_if_revision(
                    "agent-b",
                    vec![regression_runtime()],
                    Some(0)
                )
                .await
                .unwrap(),
            2
        );
        assert!(
            database
                .replace_agent_plugin_runtime_if_revision("agent-b", Vec::new(), Some(0))
                .await
                .is_err()
        );
        assert_eq!(
            database
                .get_agent_plugin_runtime("agent-b")
                .await
                .unwrap()
                .unwrap()
                .revision,
            2
        );
    }

    #[tokio::test]
    async fn regression_legacy_runtime_revision_one_is_preserved_until_updated() {
        let database = regression_database().await;
        agent_plugin_runtime_catalog::Entity::insert(agent_plugin_runtime_catalog::ActiveModel {
            agent_id: Set("agent-a".to_owned()),
            revision: Set(1),
            updated_at: Set(1),
        })
        .exec(database.connection())
        .await
        .unwrap();
        assert_eq!(
            database
                .get_agent_plugin_runtime("agent-a")
                .await
                .unwrap()
                .unwrap()
                .revision,
            1
        );
        assert_eq!(
            database
                .replace_agent_plugin_runtime_if_revision(
                    "agent-a",
                    vec![regression_runtime()],
                    Some(1)
                )
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn regression_runtime_snapshots_remain_consistent_during_updates_and_clears() {
        let (database, directory) = super::super::connection::snapshot_test_database().await;
        assert!(
            database
                .get_agent_plugin_runtime("agent-a")
                .await
                .unwrap()
                .is_none()
        );
        database
            .replace_agent_plugin_runtime("agent-a", vec![regression_runtime()])
            .await
            .unwrap();
        let writer_database = database.clone();
        let writer = tokio::spawn(async move {
            for revision in 3..=102u64 {
                let mut runtime = regression_runtime();
                runtime.requested_concurrency = revision as u32;
                let runtimes = if revision.is_multiple_of(3) {
                    Vec::new()
                } else {
                    vec![runtime]
                };
                assert_eq!(
                    writer_database
                        .replace_agent_plugin_runtime("agent-a", runtimes)
                        .await
                        .unwrap(),
                    revision
                );
                tokio::task::yield_now().await;
            }
        });
        for _ in 0..300 {
            let snapshot = database
                .load_agent_plugin_runtime("agent-a", &regression_inventory())
                .await
                .unwrap();
            if snapshot.revision.is_multiple_of(3) {
                assert!(snapshot.plugins.is_empty());
            } else {
                assert_eq!(snapshot.plugins.len(), 1);
                assert_eq!(
                    u64::from(snapshot.plugins[0].requested_concurrency),
                    snapshot.revision
                );
            }
            let record = database
                .get_agent_plugin_runtime("agent-a")
                .await
                .unwrap()
                .unwrap();
            if record.revision.is_multiple_of(3) {
                assert!(record.runtimes.is_empty());
            } else {
                assert_eq!(record.runtimes.len(), 1);
                assert_eq!(
                    u64::from(record.runtimes[0].requested_concurrency),
                    record.revision
                );
            }
            tokio::task::yield_now().await;
        }
        writer.await.unwrap();
        database.connection().clone().close().await.unwrap();
        drop(database);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
