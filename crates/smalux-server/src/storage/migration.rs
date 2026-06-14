//! 数据库迁移模块入口，负责组织 server schema 初始化和版本升级迁移。
//!
//! 这里保留单文件入口，具体迁移实现放到同名目录下，避免后续 migration 增长后把
//! 所有表结构都塞进一个文件里。

use sea_orm_migration::prelude::*;

pub mod m20260614_000001_create_agents;

/// server 迁移入口。
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(m20260614_000001_create_agents::Migration)]
    }
}
