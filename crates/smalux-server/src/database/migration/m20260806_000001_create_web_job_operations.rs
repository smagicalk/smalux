//! Persist durable Web Job mutations and their Agent command lifecycle.

use sea_orm_migration::{prelude::*, schema::*};

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20260806_000001_create_web_job_operations"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(WebJobOperations::Table)
                    .if_not_exists()
                    .col(string_len(WebJobOperations::OperationId, 36).primary_key())
                    .col(string_len(WebJobOperations::ActorUserId, 36))
                    .col(string_len(WebJobOperations::Method, 64))
                    .col(string_len(WebJobOperations::ClientMutationId, 36))
                    .col(string_len(WebJobOperations::RequestHash, 64))
                    .col(string_len(WebJobOperations::AgentId, 64))
                    .col(string_len_null(WebJobOperations::TargetJobId, 36))
                    .col(string_len(WebJobOperations::ActionKind, 64))
                    .col(big_integer(WebJobOperations::ExpectedCatalogRevision))
                    .col(big_integer(WebJobOperations::TargetCatalogRevision))
                    .col(binary_len(WebJobOperations::DesiredDigest, 32))
                    .col(string_len_null(WebJobOperations::CommandId, 36))
                    .col(string_len(WebJobOperations::State, 24))
                    .col(string_len_null(WebJobOperations::Reason, 128))
                    // JSON bytes contain only the immutable, sanitized initial HTTP response DTO.
                    .col(binary(WebJobOperations::InitialResponseJson))
                    .col(big_integer(WebJobOperations::CreatedAt))
                    .col(big_integer(WebJobOperations::UpdatedAt))
                    .col(big_integer_null(WebJobOperations::SentAt))
                    .col(big_integer_null(WebJobOperations::CompletedAt))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("uq_web_job_operations_idempotency")
                    .table(WebJobOperations::Table)
                    .col(WebJobOperations::ActorUserId)
                    .col(WebJobOperations::Method)
                    .col(WebJobOperations::ClientMutationId)
                    .unique()
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_web_job_operations_agent_revision")
                    .table(WebJobOperations::Table)
                    .col(WebJobOperations::AgentId)
                    .col(WebJobOperations::TargetCatalogRevision)
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_web_job_operations_agent_created")
                    .table(WebJobOperations::Table)
                    .col(WebJobOperations::AgentId)
                    .col(WebJobOperations::CreatedAt)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(WebJobOperations::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum WebJobOperations {
    Table,
    OperationId,
    ActorUserId,
    Method,
    ClientMutationId,
    RequestHash,
    AgentId,
    TargetJobId,
    ActionKind,
    ExpectedCatalogRevision,
    TargetCatalogRevision,
    DesiredDigest,
    CommandId,
    State,
    Reason,
    InitialResponseJson,
    CreatedAt,
    UpdatedAt,
    SentAt,
    CompletedAt,
}
