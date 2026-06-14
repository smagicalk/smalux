//! `agents` 表迁移。
//!
//! 当前使用 SeaORM 推荐的时间前缀命名，保证后续 migration 顺序清晰。

use sea_orm_migration::prelude::*;

/// 第一条最小迁移。
///
/// 当前不创建任何表，只用于验证 migration 调用链和日志输出。
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        tracing::info!("migration up: m20260614_000001_create_agents");
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        tracing::info!("migration down: m20260614_000001_create_agents");
        Ok(())
    }
}
