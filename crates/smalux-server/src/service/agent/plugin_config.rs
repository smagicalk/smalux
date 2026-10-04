//! Plus 私有参数的动态 Protobuf 编码。
//!
//! Server 从已经校验并保存的 `schema.pb` 找到消息描述符，把管理端的 Protobuf JSON
//! 转为 bytes；不会依赖或加载任何插件二进制。

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use serde_json::Value;
use smalux_plus_core::{PluginRuntimeSchema, PluginSchemaBundle, PluginTaskSchema, SchemaHash};

use crate::database::{DatabaseError, ServerDatabase};

/// 将一个插件 runtime JSON 文档编码为对应的私有 Protobuf bytes。
pub async fn encode_runtime_config(
    database: &ServerDatabase,
    plugin_id: &str,
    plugin_version: &str,
    schema_hash: &SchemaHash,
    input: &Value,
) -> Result<(u32, Vec<u8>), DatabaseError> {
    let bundle = load_bundle(database, plugin_id, plugin_version, schema_hash).await?;
    let runtime = bundle.runtime.as_ref().ok_or_else(|| {
        DatabaseError::InvalidPluginRuntime(
            "Plugin Schema does not declare runtime config".to_owned(),
        )
    })?;
    Ok((
        runtime.schema_version,
        encode_message(&bundle, runtime, input).map_err(DatabaseError::InvalidPluginRuntime)?,
    ))
}

/// 将一个 Plugin Task JSON 文档编码为对应的私有 Protobuf bytes。
pub async fn encode_task_config(
    database: &ServerDatabase,
    plugin_id: &str,
    plugin_version: &str,
    schema_hash: &SchemaHash,
    task_kind: &str,
    input: &Value,
) -> Result<(u32, Vec<u8>), DatabaseError> {
    let bundle = load_bundle(database, plugin_id, plugin_version, schema_hash).await?;
    let task = bundle
        .tasks
        .iter()
        .find(|task| task.task_kind == task_kind)
        .ok_or_else(|| {
            DatabaseError::InvalidPluginRuntime(
                "Plugin Schema does not declare task kind".to_owned(),
            )
        })?;
    Ok((
        task.schema_version,
        encode_message(&bundle, task, input).map_err(DatabaseError::InvalidPluginRuntime)?,
    ))
}

async fn load_bundle(
    database: &ServerDatabase,
    plugin_id: &str,
    plugin_version: &str,
    schema_hash: &SchemaHash,
) -> Result<PluginSchemaBundle, DatabaseError> {
    let record = database
        .load_plugin_schema(schema_hash)
        .await?
        .ok_or_else(|| {
            DatabaseError::InvalidPluginRuntime("Plugin Schema hash is unknown".to_owned())
        })?;
    if record.bundle.plugin_id != plugin_id || record.bundle.plugin_version != plugin_version {
        return Err(DatabaseError::InvalidPluginRuntime(
            "Plugin Schema identity does not match runtime request".to_owned(),
        ));
    }
    Ok(record.bundle)
}

trait MessageSchema {
    fn config_message(&self) -> &str;
}

impl MessageSchema for PluginRuntimeSchema {
    fn config_message(&self) -> &str {
        &self.config_message
    }
}

impl MessageSchema for PluginTaskSchema {
    fn config_message(&self) -> &str {
        &self.config_message
    }
}

fn encode_message(
    bundle: &PluginSchemaBundle,
    schema: &impl MessageSchema,
    input: &Value,
) -> Result<Vec<u8>, String> {
    let pool = DescriptorPool::decode(bundle.descriptor_set.as_slice())
        .map_err(|error| format!("Plugin descriptor set is invalid: {error}"))?;
    let descriptor = pool
        .get_message_by_name(schema.config_message().trim_start_matches('.'))
        .ok_or_else(|| "Plugin config message is absent from descriptor set".to_owned())?;
    let json = serde_json::to_vec(input).map_err(|error| error.to_string())?;
    let mut deserializer = serde_json::Deserializer::from_slice(&json);
    let message = DynamicMessage::deserialize(descriptor, &mut deserializer)
        .map_err(|error| format!("Plugin config JSON does not match schema: {error}"))?;
    deserializer
        .end()
        .map_err(|error| format!("Plugin config JSON has trailing data: {error}"))?;
    Ok(message.encode_to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::DatabaseConfig, database::ServerDatabase};
    use prost::Message;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto,
    };
    use smalux_plus_core::{
        PluginRuntimeSchema, PluginSchemaBundle, PluginTaskSchema, SCHEMA_FORMAT_VERSION,
    };

    fn bundle() -> PluginSchemaBundle {
        let field = |name: &str| FieldDescriptorProto {
            name: Some(name.to_owned()),
            number: Some(1),
            label: Some(field_descriptor_proto::Label::Optional as i32),
            r#type: Some(field_descriptor_proto::Type::String as i32),
            ..Default::default()
        };
        let descriptors = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("example.proto".to_owned()),
                package: Some("example.plugin.v1".to_owned()),
                message_type: vec![
                    DescriptorProto {
                        name: Some("RuntimeConfig".to_owned()),
                        field: vec![field("endpoint")],
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("TaskConfig".to_owned()),
                        field: vec![field("message")],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
        };
        PluginSchemaBundle {
            format_version: SCHEMA_FORMAT_VERSION,
            plugin_id: "example.plugin".to_owned(),
            plugin_version: "1.0.0".to_owned(),
            descriptor_set: descriptors.encode_to_vec(),
            runtime: Some(PluginRuntimeSchema {
                schema_version: 1,
                config_message: "example.plugin.v1.RuntimeConfig".to_owned(),
                ..Default::default()
            }),
            tasks: vec![PluginTaskSchema {
                task_kind: "example.plugin.task.v1".to_owned(),
                schema_version: 1,
                config_message: "example.plugin.v1.TaskConfig".to_owned(),
                ..Default::default()
            }],
        }
    }

    #[tokio::test]
    async fn encodes_runtime_and_task_json_with_uploaded_schema() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .unwrap();
        let bundle = bundle();
        let hash = database.insert_plugin_schema(&bundle).await.unwrap();
        let (_, runtime) = encode_runtime_config(
            &database,
            "example.plugin",
            "1.0.0",
            &hash,
            &serde_json::json!({"endpoint": "https://example.test"}),
        )
        .await
        .unwrap();
        let (_, task) = encode_task_config(
            &database,
            "example.plugin",
            "1.0.0",
            &hash,
            "example.plugin.task.v1",
            &serde_json::json!({"message": "hello"}),
        )
        .await
        .unwrap();
        assert_eq!(runtime, b"\n\x14https://example.test");
        assert_eq!(task, b"\n\x05hello");
    }
}
