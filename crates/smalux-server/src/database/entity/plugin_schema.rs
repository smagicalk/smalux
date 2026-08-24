//! Plus 参数 Schema Bundle 的不可变持久化模型。

use sea_orm::entity::prelude::*;

/// `plugin_schema_bundles` 表的 SeaORM 实体。
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "plugin_schema_bundles")]
pub struct Model {
    /// Schema Bundle 的 SHA-256，作为内容地址主键。
    #[sea_orm(primary_key, auto_increment = false)]
    pub schema_hash: Vec<u8>,
    /// 插件稳定身份。
    pub plugin_id: String,
    /// 插件版本；同一版本只允许绑定一个 hash。
    pub plugin_version: String,
    /// Schema Bundle 格式版本。
    pub format_version: i32,
    /// `smalux-plus-core::PluginSchemaBundle` 的 Protobuf 编码。
    pub schema_payload: Vec<u8>,
    /// 首次写入时间，Unix 微秒。
    pub created_at: i64,
}

impl ActiveModelBehavior for ActiveModel {}
