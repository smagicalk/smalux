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
                    .col(
                        ColumnDef::new(RegistrationTokens::DisplayName)
                            .string_len(256)
                            .null(),
                    )
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
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(PluginSchemaBundles::Table)
                    .if_not_exists()
                    .col(binary_len(PluginSchemaBundles::SchemaHash, 32).primary_key())
                    .col(string_len(PluginSchemaBundles::PluginId, 256))
                    .col(string_len(PluginSchemaBundles::PluginVersion, 64))
                    .col(integer(PluginSchemaBundles::FormatVersion))
                    .col(binary(PluginSchemaBundles::SchemaPayload))
                    .col(big_integer(PluginSchemaBundles::CreatedAt))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_plugin_schema_bundles_plugin_version")
                    .table(PluginSchemaBundles::Table)
                    .col(PluginSchemaBundles::PluginId)
                    .col(PluginSchemaBundles::PluginVersion)
                    .unique()
                    .to_owned(),
            )
            .await?;

        let mut catalog_agent_foreign_key = ForeignKey::create()
            .name("fk_agent_job_catalogs_agent_id")
            .from(AgentJobCatalogs::Table, AgentJobCatalogs::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentJobCatalogs::Table)
                    .if_not_exists()
                    .col(string_len(AgentJobCatalogs::AgentId, 64).primary_key())
                    .col(big_integer(AgentJobCatalogs::Revision))
                    .col(big_integer(AgentJobCatalogs::UpdatedAt))
                    .foreign_key(&mut catalog_agent_foreign_key)
                    .to_owned(),
            )
            .await?;

        let mut runtime_catalog_agent_foreign_key = ForeignKey::create()
            .name("fk_agent_plugin_runtime_catalogs_agent_id")
            .from(
                AgentPluginRuntimeCatalogs::Table,
                AgentPluginRuntimeCatalogs::AgentId,
            )
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentPluginRuntimeCatalogs::Table)
                    .if_not_exists()
                    .col(string_len(AgentPluginRuntimeCatalogs::AgentId, 64).primary_key())
                    .col(big_integer(AgentPluginRuntimeCatalogs::Revision))
                    .col(big_integer(AgentPluginRuntimeCatalogs::UpdatedAt))
                    .foreign_key(&mut runtime_catalog_agent_foreign_key)
                    .to_owned(),
            )
            .await?;

        let mut capability_agent_foreign_key = ForeignKey::create()
            .name("fk_agent_capabilities_agent_id")
            .from(AgentCapabilities::Table, AgentCapabilities::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentCapabilities::Table)
                    .if_not_exists()
                    .col(string_len(AgentCapabilities::AgentId, 64).primary_key())
                    .col(big_integer(AgentCapabilities::Revision))
                    .col(string_len(AgentCapabilities::AgentVersion, 128))
                    .col(binary(AgentCapabilities::Payload))
                    .col(big_integer(AgentCapabilities::UpdatedAt))
                    .foreign_key(&mut capability_agent_foreign_key)
                    .to_owned(),
            )
            .await?;

        let mut inventory_agent_foreign_key = ForeignKey::create()
            .name("fk_agent_plugin_inventories_agent_id")
            .from(
                AgentPluginInventories::Table,
                AgentPluginInventories::AgentId,
            )
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentPluginInventories::Table)
                    .if_not_exists()
                    .col(string_len(AgentPluginInventories::AgentId, 64).primary_key())
                    .col(big_integer(AgentPluginInventories::Revision))
                    .col(binary(AgentPluginInventories::Payload))
                    .col(big_integer(AgentPluginInventories::UpdatedAt))
                    .foreign_key(&mut inventory_agent_foreign_key)
                    .to_owned(),
            )
            .await?;

        let mut runtime_agent_foreign_key = ForeignKey::create()
            .name("fk_agent_plugin_runtimes_agent_id")
            .from(AgentPluginRuntimes::Table, AgentPluginRuntimes::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentPluginRuntimes::Table)
                    .if_not_exists()
                    .col(string_len(AgentPluginRuntimes::RuntimeKey, 512).primary_key())
                    .col(string_len(AgentPluginRuntimes::AgentId, 64))
                    .col(string_len(AgentPluginRuntimes::PluginId, 256))
                    .col(string_len(AgentPluginRuntimes::PluginVersion, 64))
                    .col(binary_len(AgentPluginRuntimes::SchemaHash, 32))
                    .col(integer(AgentPluginRuntimes::SchemaVersion))
                    .col(binary(AgentPluginRuntimes::Config))
                    .col(integer(AgentPluginRuntimes::RequestedConcurrency))
                    .col(big_integer(AgentPluginRuntimes::UpdatedAt))
                    .foreign_key(&mut runtime_agent_foreign_key)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_agent_plugin_runtimes_identity")
                    .table(AgentPluginRuntimes::Table)
                    .col(AgentPluginRuntimes::AgentId)
                    .col(AgentPluginRuntimes::PluginId)
                    .col(AgentPluginRuntimes::PluginVersion)
                    .unique()
                    .to_owned(),
            )
            .await?;

        let mut job_agent_foreign_key = ForeignKey::create()
            .name("fk_agent_jobs_agent_id")
            .from(AgentJobs::Table, AgentJobs::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentJobs::Table)
                    .if_not_exists()
                    .col(string_len(AgentJobs::JobKey, 256).primary_key())
                    .col(string_len(AgentJobs::AgentId, 64))
                    .col(binary_len(AgentJobs::JobId, 16))
                    .col(big_integer(AgentJobs::Revision))
                    .col(boolean(AgentJobs::Enabled))
                    .col(string_len(AgentJobs::TaskKind, 256))
                    .col(binary(AgentJobs::Definition))
                    .col(big_integer(AgentJobs::CreatedAt))
                    .col(big_integer(AgentJobs::UpdatedAt))
                    .foreign_key(&mut job_agent_foreign_key)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_agent_jobs_agent_job")
                    .table(AgentJobs::Table)
                    .col(AgentJobs::AgentId)
                    .col(AgentJobs::JobId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        let mut job_version_agent_foreign_key = ForeignKey::create()
            .name("fk_agent_job_versions_agent_id")
            .from(AgentJobVersions::Table, AgentJobVersions::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(AgentJobVersions::Table)
                    .if_not_exists()
                    .col(string_len(AgentJobVersions::VersionKey, 320).primary_key())
                    .col(string_len(AgentJobVersions::AgentId, 64))
                    .col(binary_len(AgentJobVersions::JobId, 16))
                    .col(big_integer(AgentJobVersions::Revision))
                    .col(string_len(AgentJobVersions::TaskKind, 256))
                    .col(binary(AgentJobVersions::Definition))
                    .col(big_integer(AgentJobVersions::CreatedAt))
                    .foreign_key(&mut job_version_agent_foreign_key)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_agent_job_versions_identity")
                    .table(AgentJobVersions::Table)
                    .col(AgentJobVersions::AgentId)
                    .col(AgentJobVersions::JobId)
                    .col(AgentJobVersions::Revision)
                    .unique()
                    .to_owned(),
            )
            .await?;

        let mut command_agent_foreign_key = ForeignKey::create()
            .name("fk_job_commands_agent_id")
            .from(JobCommands::Table, JobCommands::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(JobCommands::Table)
                    .if_not_exists()
                    .col(string_len(JobCommands::CommandId, 64).primary_key())
                    .col(string_len(JobCommands::AgentId, 64))
                    .col(big_integer(JobCommands::CatalogRevision))
                    .col(binary(JobCommands::CommandPayload))
                    .col(integer_null(JobCommands::ResultStatus))
                    .col(binary_null(JobCommands::ResultPayload))
                    .col(big_integer(JobCommands::CreatedAt))
                    .col(big_integer_null(JobCommands::CompletedAt))
                    .foreign_key(&mut command_agent_foreign_key)
                    .to_owned(),
            )
            .await?;

        let mut report_agent_foreign_key = ForeignKey::create()
            .name("fk_task_reports_agent_id")
            .from(TaskReports::Table, TaskReports::AgentId)
            .to(Agents::Table, Agents::AgentId)
            .on_delete(ForeignKeyAction::Cascade)
            .to_owned();
        manager
            .create_table(
                Table::create()
                    .table(TaskReports::Table)
                    .if_not_exists()
                    .col(string_len(TaskReports::ReportId, 256).primary_key())
                    .col(string_len(TaskReports::AgentId, 64))
                    .col(binary_len(TaskReports::JobId, 16))
                    .col(big_integer(TaskReports::JobRevision))
                    .col(binary_len(TaskReports::RunId, 16))
                    .col(integer(TaskReports::Attempt))
                    .col(big_integer_null(TaskReports::ScheduledAt))
                    .col(big_integer_null(TaskReports::StartedAt))
                    .col(string_len(TaskReports::ResultKind, 256))
                    .col(binary(TaskReports::Payload))
                    .col(big_integer(TaskReports::ReceivedAt))
                    .foreign_key(&mut report_agent_foreign_key)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_task_reports_execution")
                    .table(TaskReports::Table)
                    .col(TaskReports::AgentId)
                    .col(TaskReports::JobId)
                    .col(TaskReports::JobRevision)
                    .col(TaskReports::RunId)
                    .col(TaskReports::Attempt)
                    .unique()
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_task_reports_agent_received")
                    .table(TaskReports::Table)
                    .col(TaskReports::AgentId)
                    .col(TaskReports::ReceivedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(JobEvents::Table)
                    .if_not_exists()
                    .col(string_len(JobEvents::EventId, 128).primary_key())
                    .col(string_len(JobEvents::AgentId, 64))
                    .col(binary_len(JobEvents::InstanceId, 16))
                    .col(big_integer(JobEvents::Sequence))
                    .col(integer(JobEvents::Kind))
                    .col(binary(JobEvents::JobId))
                    .col(big_integer(JobEvents::Revision))
                    .col(binary(JobEvents::RunId))
                    .col(integer(JobEvents::Attempt))
                    .col(big_integer(JobEvents::EmittedAt))
                    .col(big_integer_null(JobEvents::RunAt))
                    .col(big_integer(JobEvents::DurationMs))
                    .col(string_len(JobEvents::Message, 4096))
                    .col(boolean(JobEvents::WillRetry))
                    .col(integer(JobEvents::PendingCount))
                    .col(boolean(JobEvents::GapDetected))
                    .col(binary(JobEvents::Payload))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_job_events_agent_emitted")
                    .table(JobEvents::Table)
                    .col(JobEvents::AgentId)
                    .col(JobEvents::EmittedAt)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("uq_job_events_agent_instance_sequence")
                    .table(JobEvents::Table)
                    .col(JobEvents::AgentId)
                    .col(JobEvents::InstanceId)
                    .col(JobEvents::Sequence)
                    .unique()
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
            .drop_table(Table::drop().table(JobEvents::Table).if_exists().to_owned())
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(TaskReports::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(JobCommands::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(AgentJobs::Table).if_exists().to_owned())
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(AgentJobVersions::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(AgentJobCatalogs::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(AgentPluginRuntimes::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(AgentPluginRuntimeCatalogs::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(AgentPluginInventories::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(AgentCapabilities::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(PluginSchemaBundles::Table)
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
    DisplayName,
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

#[derive(DeriveIden)]
enum PluginSchemaBundles {
    Table,
    SchemaHash,
    PluginId,
    PluginVersion,
    FormatVersion,
    SchemaPayload,
    CreatedAt,
}

#[derive(DeriveIden)]
enum JobEvents {
    Table,
    EventId,
    AgentId,
    InstanceId,
    Sequence,
    Kind,
    JobId,
    Revision,
    RunId,
    Attempt,
    EmittedAt,
    RunAt,
    DurationMs,
    Message,
    WillRetry,
    PendingCount,
    GapDetected,
    Payload,
}

#[derive(DeriveIden)]
enum AgentJobCatalogs {
    Table,
    AgentId,
    Revision,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum AgentJobs {
    Table,
    JobKey,
    AgentId,
    JobId,
    Revision,
    Enabled,
    TaskKind,
    Definition,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum AgentJobVersions {
    Table,
    VersionKey,
    AgentId,
    JobId,
    Revision,
    TaskKind,
    Definition,
    CreatedAt,
}

#[derive(DeriveIden)]
enum AgentPluginRuntimeCatalogs {
    Table,
    AgentId,
    Revision,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum AgentPluginRuntimes {
    Table,
    RuntimeKey,
    AgentId,
    PluginId,
    PluginVersion,
    SchemaHash,
    SchemaVersion,
    Config,
    RequestedConcurrency,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum AgentCapabilities {
    Table,
    AgentId,
    Revision,
    AgentVersion,
    Payload,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum AgentPluginInventories {
    Table,
    AgentId,
    Revision,
    Payload,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum JobCommands {
    Table,
    CommandId,
    AgentId,
    CatalogRevision,
    CommandPayload,
    ResultStatus,
    ResultPayload,
    CreatedAt,
    CompletedAt,
}

#[derive(DeriveIden)]
enum TaskReports {
    Table,
    ReportId,
    AgentId,
    JobId,
    JobRevision,
    RunId,
    Attempt,
    ScheduledAt,
    StartedAt,
    ResultKind,
    Payload,
    ReceivedAt,
}
