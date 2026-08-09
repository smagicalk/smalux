//! 创建 Server 初始数据库 schema。
//!
//! 当前项目尚未发布，因此把注册中心、级联外键和 Server Noise 密钥环合并为一个
//! 初始迁移。后续发布后只能继续追加新的迁移，不能再修改这个迁移的名称或结构。
//! SeaORM 会把这里的 schema 编译为 SQLite、PostgreSQL 和 MySQL 各自的 SQL。

use sea_orm_migration::{prelude::*, schema::*};

/// Server 初始数据库迁移。
pub struct Migration;

// 显式实现迁移名称，避免 IDE 未展开宏时误报 `MigrationName` 约束。
// 该名称会写入 seaql_migrations；项目发布后不能再修改。
impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20260804_000001_create_server_database"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Agents::Table)
                    .if_not_exists()
                    .col(string_len(Agents::AgentId, 64).primary_key())
                    .col(string_len(Agents::Name, 256))
                    .col(binary_len_uniq(Agents::PublicKey, 32))
                    .col(string_len(Agents::Status, 32))
                    .col(big_integer(Agents::CreatedAt))
                    .col(big_integer(Agents::UpdatedAt))
                    .col(big_integer_null(Agents::RevokedAt))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(RegistrationTokens::Table)
                    .if_not_exists()
                    .col(string_len(RegistrationTokens::TokenId, 128).primary_key())
                    .col(binary_len(RegistrationTokens::Psk, 32))
                    .col(string_len(RegistrationTokens::Status, 32))
                    .col(big_integer(RegistrationTokens::CreatedAt))
                    .col(big_integer(RegistrationTokens::UpdatedAt))
                    .col(big_integer_null(RegistrationTokens::ExpiresAt))
                    .col(big_integer_null(RegistrationTokens::UsedAt))
                    .to_owned(),
            )
            .await?;

        let mut token_foreign_key = ForeignKey::create()
            .name("fk_agent_registrations_token_id")
            .from(AgentRegistrations::Table, AgentRegistrations::TokenId)
            .to(RegistrationTokens::Table, RegistrationTokens::TokenId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        let mut agent_foreign_key = ForeignKey::create()
            .name("fk_agent_registrations_agent_id")
            .from(AgentRegistrations::Table, AgentRegistrations::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentRegistrations::Table)
                    .if_not_exists()
                    .col(string_len(AgentRegistrations::RegistrationId, 64).primary_key())
                    .col(string_len(AgentRegistrations::TokenId, 128))
                    // prepare 阶段 Agent 尚未落库；commit 时再回填这个外键。
                    .col(
                        ColumnDef::new(AgentRegistrations::AgentId)
                            .string_len(64)
                            .null(),
                    )
                    // 预分配 ID 保证 prepare 响应稳定，但它不是外键。
                    .col(string_len(AgentRegistrations::ReservedAgentId, 64))
                    .col(string_len(AgentRegistrations::AgentName, 256))
                    .col(binary_len(AgentRegistrations::AgentPublicKey, 32))
                    .col(string_len(AgentRegistrations::Status, 32))
                    .col(big_integer(AgentRegistrations::CreatedAt))
                    .col(big_integer(AgentRegistrations::UpdatedAt))
                    .col(big_integer_null(AgentRegistrations::ExpiresAt))
                    .col(big_integer_null(AgentRegistrations::CommittedAt))
                    .foreign_key(&mut token_foreign_key)
                    .foreign_key(&mut agent_foreign_key)
                    .to_owned(),
            )
            .await?;
        // 一个 Agent 公钥同一时间只能有一条注册尝试；过期尝试会被清理后才能重试。
        manager
            .create_index(
                Index::create()
                    .name("uq_agent_registrations_public_key")
                    .table(AgentRegistrations::Table)
                    .col(AgentRegistrations::AgentPublicKey)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // 每个注册 Token 只能绑定一个注册事务。唯一索引既加速查询，也在多个 Server
        // 实例并发 prepare 时提供最终一致的数据库约束，不能只依赖应用层先查后写。
        manager
            .create_index(
                Index::create()
                    .name("uq_agent_registrations_token_id")
                    .table(AgentRegistrations::Table)
                    .col(AgentRegistrations::TokenId)
                    .unique()
                    .to_owned(),
            )
            .await?;
        // Agent ID 的普通索引用于按 Agent 查询注册历史。
        manager
            .create_index(
                Index::create()
                    .name("idx_agent_registrations_agent_id")
                    .table(AgentRegistrations::Table)
                    .col(AgentRegistrations::AgentId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ServerKeyrings::Table)
                    .if_not_exists()
                    .col(string_len(ServerKeyrings::KeyringId, 32).primary_key())
                    .col(binary_len(ServerKeyrings::CurrentPrivateKey, 32))
                    .col(binary_len(ServerKeyrings::CurrentPublicKey, 32))
                    .col(binary_len_null(ServerKeyrings::NextPrivateKey, 32))
                    .col(binary_len_null(ServerKeyrings::NextPublicKey, 32))
                    .col(binary_len_null(ServerKeyrings::PreviousPrivateKey, 32))
                    .col(binary_len_null(ServerKeyrings::PreviousPublicKey, 32))
                    .col(binary_len_null(ServerKeyrings::RotationId, 16))
                    // 单调递增版本用于多个 Server 实例之间的乐观并发控制。
                    .col(big_integer(ServerKeyrings::Revision).default(0))
                    .col(big_integer(ServerKeyrings::CreatedAt))
                    .col(big_integer(ServerKeyrings::UpdatedAt))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 先删除不依赖其他业务表的密钥环，再按外键依赖的反向顺序删除注册表。
        manager
            .drop_table(
                Table::drop()
                    .table(ServerKeyrings::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(AgentRegistrations::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(RegistrationTokens::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(Agents::Table).if_exists().to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum Agents {
    Table,
    AgentId,
    Name,
    PublicKey,
    Status,
    CreatedAt,
    UpdatedAt,
    RevokedAt,
}

#[derive(DeriveIden)]
enum RegistrationTokens {
    Table,
    TokenId,
    Psk,
    Status,
    CreatedAt,
    UpdatedAt,
    ExpiresAt,
    UsedAt,
}

#[derive(DeriveIden)]
enum AgentRegistrations {
    Table,
    RegistrationId,
    TokenId,
    AgentId,
    ReservedAgentId,
    AgentName,
    AgentPublicKey,
    Status,
    CreatedAt,
    UpdatedAt,
    ExpiresAt,
    CommittedAt,
}

#[derive(DeriveIden)]
enum ServerKeyrings {
    Table,
    KeyringId,
    CurrentPrivateKey,
    CurrentPublicKey,
    NextPrivateKey,
    NextPublicKey,
    PreviousPrivateKey,
    PreviousPublicKey,
    RotationId,
    Revision,
    CreatedAt,
    UpdatedAt,
}
