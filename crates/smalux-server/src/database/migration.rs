//! 可滚动追加的 SeaORM 迁移集合。
//!
//! 当前项目尚未发布，初始 schema 合并在一个迁移中；发布后每次 schema 变化都应
//! 新增带时间序列的 `mYYYYMMDD_NNNNNN_*` 模块并追加到 `Migrator::migrations()`，
//! 不再修改这个初始迁移。

mod m20260804_000001_create_server_database;

use sea_orm_migration::prelude::*;

/// Server 当前所有数据库迁移的唯一入口。
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20260804_000001_create_server_database::Migration)]
    }
}
