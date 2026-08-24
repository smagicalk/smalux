//! Plus 参数 Schema 的数据库适配器。

use std::time::{SystemTime, UNIX_EPOCH};

use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};
use smalux_plus_core::{PluginSchemaBundle, SchemaHash};

use super::{DatabaseError, ServerDatabase, entity::plugin_schema};

/// 数据库中已经校验过的 Schema Bundle。
#[derive(Clone, Debug)]
pub struct PluginSchemaRecord {
    pub hash: SchemaHash,
    pub bundle: PluginSchemaBundle,
    pub created_at: i64,
}

impl ServerDatabase {
    /// 按内容 Hash 读取 Schema；不存在时返回 None。
    pub async fn load_plugin_schema(
        &self,
        hash: &SchemaHash,
    ) -> Result<Option<PluginSchemaRecord>, DatabaseError> {
        let Some(model) = plugin_schema::Entity::find()
            .filter(plugin_schema::Column::SchemaHash.eq(hash.to_vec()))
            .one(self.connection())
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(restore_record(model)?))
    }

    /// 一次查询读取多个内容 Hash，供 Agent inventory 对账使用。
    pub async fn load_plugin_schemas(
        &self,
        hashes: &[SchemaHash],
    ) -> Result<Vec<PluginSchemaRecord>, DatabaseError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        plugin_schema::Entity::find()
            .filter(
                plugin_schema::Column::SchemaHash.is_in(hashes.iter().map(|hash| hash.to_vec())),
            )
            .all(self.connection())
            .await?
            .into_iter()
            .map(restore_record)
            .collect()
    }

    /// 插入一个新的不可变 Schema；重复 Hash 或同一插件版本重复提交均幂等。
    pub async fn insert_plugin_schema(
        &self,
        bundle: &PluginSchemaBundle,
    ) -> Result<SchemaHash, DatabaseError> {
        let payload = bundle
            .encode_checked()
            .map_err(|error| DatabaseError::InvalidPluginSchema(error.to_string()))?;
        let hash = bundle
            .hash()
            .map_err(|error| DatabaseError::InvalidPluginSchema(error.to_string()))?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as i64;
        let model = plugin_schema::ActiveModel {
            schema_hash: Set(hash.to_vec()),
            plugin_id: Set(bundle.plugin_id.clone()),
            plugin_version: Set(bundle.plugin_version.clone()),
            format_version: Set(bundle.format_version as i32),
            schema_payload: Set(payload),
            created_at: Set(now),
        };
        plugin_schema::Entity::insert(model)
            .on_conflict(
                OnConflict::columns([
                    plugin_schema::Column::PluginId,
                    plugin_schema::Column::PluginVersion,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec(self.connection())
            .await?;
        let Some(record) = self.load_plugin_schema(&hash).await? else {
            return Err(DatabaseError::InvalidPluginSchema(
                "Schema insert was ignored by a conflicting plugin version".to_owned(),
            ));
        };
        if record.bundle.plugin_id != bundle.plugin_id
            || record.bundle.plugin_version != bundle.plugin_version
        {
            return Err(DatabaseError::InvalidPluginSchema(
                "plugin version is already bound to a different Schema".to_owned(),
            ));
        }
        Ok(hash)
    }
}

fn restore_record(model: plugin_schema::Model) -> Result<PluginSchemaRecord, DatabaseError> {
    let hash: SchemaHash = model.schema_hash.as_slice().try_into().map_err(|_| {
        DatabaseError::InvalidPluginSchema("stored Schema hash must contain 32 bytes".to_owned())
    })?;
    let bundle = PluginSchemaBundle::decode_checked(&model.schema_payload)
        .map_err(|error| DatabaseError::InvalidPluginSchema(error.to_string()))?;
    let actual = bundle
        .hash()
        .map_err(|error| DatabaseError::InvalidPluginSchema(error.to_string()))?;
    if actual != hash || model.format_version != bundle.format_version as i32 {
        return Err(DatabaseError::InvalidPluginSchema(
            "stored Schema hash or format version does not match payload".to_owned(),
        ));
    }
    Ok(PluginSchemaRecord {
        hash,
        bundle,
        created_at: model.created_at,
    })
}
