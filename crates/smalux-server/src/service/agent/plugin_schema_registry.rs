//! Plus 参数 Schema 的内容寻址注册表。

use std::{collections::HashMap, sync::Arc};

use smalux_plus_core::{PluginSchemaBundle, SchemaHash};
use smalux_protocol::agent::v1::{AgentPluginInventory, PluginInventoryEntry};
use tokio::sync::RwLock;

use crate::database::ServerDatabase;

const MAX_CACHED_SCHEMAS: usize = 256;

/// 数据库作为事实来源、内存作为解析缓存的 Schema 注册表。
pub struct PluginSchemaRegistry {
    database: Arc<ServerDatabase>,
    cache: RwLock<HashMap<SchemaHash, Arc<PluginSchemaBundle>>>,
}

impl PluginSchemaRegistry {
    pub fn new(database: Arc<ServerDatabase>) -> Self {
        Self {
            database,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// 返回当前 Server 尚未保存的 Schema Hash。
    pub async fn missing_for_inventory(
        &self,
        inventory: &AgentPluginInventory,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut unresolved = Vec::new();
        let mut resolved = HashMap::new();
        let cache = self.cache.read().await;
        for plugin in &inventory.plugins {
            let hash = schema_hash(&plugin.schema_hash)?;
            if let Some(bundle) = cache.get(&hash) {
                validate_identity(bundle, plugin)?;
                resolved.insert(hash, Arc::clone(bundle));
            } else {
                unresolved.push(hash);
            }
        }
        drop(cache);

        unresolved.sort_unstable();
        unresolved.dedup();
        for record in self.database.load_plugin_schemas(&unresolved).await? {
            let hash = record.hash;
            let bundle = Arc::new(record.bundle);
            self.cache_if_room(hash, Arc::clone(&bundle)).await;
            resolved.insert(hash, bundle);
        }

        let mut missing = Vec::new();
        for plugin in &inventory.plugins {
            let hash = schema_hash(&plugin.schema_hash)?;
            if let Some(bundle) = resolved.get(&hash) {
                validate_identity(bundle, plugin)?;
            } else {
                missing.push(plugin.schema_hash.clone());
            }
        }
        missing.sort_unstable();
        missing.dedup();
        Ok(missing)
    }

    /// 校验 Agent 对待请求 Hash 的响应并保存；相同内容重复提交是幂等操作。
    pub async fn store_response(
        &self,
        inventory: &AgentPluginInventory,
        claimed_hash: &[u8],
        payload: &[u8],
    ) -> anyhow::Result<SchemaHash> {
        let claimed_hash = schema_hash(claimed_hash)?;
        let bundle = PluginSchemaBundle::decode_checked(payload)?;
        let actual_hash = bundle.hash()?;
        if actual_hash != claimed_hash {
            anyhow::bail!("Plus schema payload does not match its claimed hash");
        }
        let expected = inventory
            .plugins
            .iter()
            .find(|plugin| plugin.schema_hash.as_slice() == claimed_hash)
            .ok_or_else(|| {
                anyhow::anyhow!("Plus schema was not declared by the current inventory")
            })?;
        validate_identity(&bundle, expected)?;
        self.database.insert_plugin_schema(&bundle).await?;
        self.cache_if_room(actual_hash, Arc::new(bundle)).await;
        Ok(actual_hash)
    }

    async fn cache_if_room(&self, hash: SchemaHash, bundle: Arc<PluginSchemaBundle>) {
        let mut cache = self.cache.write().await;
        if cache.len() < MAX_CACHED_SCHEMAS || cache.contains_key(&hash) {
            cache.insert(hash, bundle);
        }
    }
}

fn schema_hash(bytes: &[u8]) -> anyhow::Result<SchemaHash> {
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("Plus schema hash must contain exactly 32 bytes"))
}

fn validate_identity(
    bundle: &PluginSchemaBundle,
    expected: &PluginInventoryEntry,
) -> anyhow::Result<()> {
    let task_kinds = bundle
        .tasks
        .iter()
        .map(|task| task.task_kind.as_str())
        .collect::<Vec<_>>();
    let expected_task_kinds = expected
        .task_kinds
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    if bundle.plugin_id != expected.plugin_id
        || bundle.plugin_version != expected.version
        || bundle.format_version != expected.schema_format_version
        || task_kinds != expected_task_kinds
    {
        anyhow::bail!("Plus schema identity does not match the current inventory");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::PluginSchemaRegistry;
    use crate::{config::DatabaseConfig, database::ServerDatabase};
    use prost::Message;
    use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
    use smalux_plus_core::{PluginSchemaBundle, PluginTaskSchema, SCHEMA_FORMAT_VERSION};
    use smalux_protocol::agent::v1::{AgentPluginInventory, PluginInventoryEntry};
    use std::sync::Arc;

    fn bundle() -> PluginSchemaBundle {
        PluginSchemaBundle {
            format_version: SCHEMA_FORMAT_VERSION,
            plugin_id: "smalux.plus.echo".to_owned(),
            plugin_version: "0.1.0".to_owned(),
            descriptor_set: FileDescriptorSet {
                file: vec![FileDescriptorProto {
                    name: Some("echo.proto".to_owned()),
                    package: Some("smalux.plus.echo.v1".to_owned()),
                    message_type: vec![DescriptorProto {
                        name: Some("EchoTaskConfig".to_owned()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
            .encode_to_vec(),
            tasks: vec![PluginTaskSchema {
                task_kind: "smalux.plus.echo.v1".to_owned(),
                schema_version: 1,
                config_message: "smalux.plus.echo.v1.EchoTaskConfig".to_owned(),
                ..Default::default()
            }],
        }
    }

    fn inventory(bundle: &PluginSchemaBundle) -> AgentPluginInventory {
        AgentPluginInventory {
            revision: 1,
            plugins: vec![PluginInventoryEntry {
                plugin_id: bundle.plugin_id.clone(),
                version: bundle.plugin_version.clone(),
                task_kinds: bundle
                    .tasks
                    .iter()
                    .map(|task| task.task_kind.clone())
                    .collect(),
                schema_hash: bundle.hash().unwrap().to_vec(),
                schema_format_version: bundle.format_version,
            }],
        }
    }

    #[tokio::test]
    async fn missing_schema_is_requested_once_then_loaded_from_database() {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .unwrap(),
        );
        let registry = PluginSchemaRegistry::new(Arc::clone(&database));
        let bundle = bundle();
        let inventory = inventory(&bundle);

        let missing = registry.missing_for_inventory(&inventory).await.unwrap();
        assert_eq!(missing, vec![bundle.hash().unwrap().to_vec()]);

        registry
            .store_response(&inventory, &missing[0], &bundle.encode_to_vec())
            .await
            .unwrap();
        assert!(
            registry
                .missing_for_inventory(&inventory)
                .await
                .unwrap()
                .is_empty()
        );

        // 新 Registry 没有内存缓存，仍应从数据库命中而不是再次请求 Agent。
        let restarted = PluginSchemaRegistry::new(database);
        assert!(
            restarted
                .missing_for_inventory(&inventory)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn response_must_match_requested_hash_and_inventory_identity() {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .unwrap(),
        );
        let registry = PluginSchemaRegistry::new(database);
        let bundle = bundle();
        let inventory = inventory(&bundle);

        let mut wrong_hash = bundle.hash().unwrap();
        wrong_hash[0] ^= 0xff;
        assert!(
            registry
                .store_response(&inventory, &wrong_hash, &bundle.encode_to_vec())
                .await
                .is_err()
        );

        let mut wrong_identity = bundle.clone();
        wrong_identity.plugin_id = "smalux.plus.other".to_owned();
        let wrong_identity_hash = wrong_identity.hash().unwrap();
        let mut mismatched_inventory = inventory.clone();
        mismatched_inventory.plugins[0].schema_hash = wrong_identity_hash.to_vec();
        assert!(
            registry
                .store_response(
                    &mismatched_inventory,
                    &wrong_identity_hash,
                    &wrong_identity.encode_to_vec(),
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn plugin_version_cannot_be_rebound_to_a_different_schema() {
        let database = Arc::new(
            ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
                .await
                .unwrap(),
        );
        let registry = PluginSchemaRegistry::new(database);
        let original = bundle();
        let original_inventory = inventory(&original);
        registry
            .store_response(
                &original_inventory,
                &original.hash().unwrap(),
                &original.encode_to_vec(),
            )
            .await
            .unwrap();

        let mut changed = original;
        changed.tasks[0].display_name = "Changed contract".to_owned();
        let changed_inventory = inventory(&changed);
        assert!(
            registry
                .store_response(
                    &changed_inventory,
                    &changed.hash().unwrap(),
                    &changed.encode_to_vec(),
                )
                .await
                .is_err()
        );
    }
}
