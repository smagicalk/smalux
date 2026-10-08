//! Web 登录身份、会话、脱敏审计和唯一 bootstrap claim。
use sea_orm_migration::{prelude::*, schema::*};

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20260805_000001_create_web_auth"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(WebUsers::Table)
                    .if_not_exists()
                    .col(string_len(WebUsers::UserId, 36).primary_key())
                    .col(string_len_uniq(WebUsers::Username, 64))
                    .col(string(WebUsers::PasswordHash))
                    .col(string_len(WebUsers::Role, 16))
                    .col(boolean(WebUsers::Enabled).default(true))
                    .col(big_integer(WebUsers::CreatedAt))
                    .col(big_integer(WebUsers::UpdatedAt))
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(WebBootstrap::Table)
                    .if_not_exists()
                    .col(string_len(WebBootstrap::Claim, 16).primary_key())
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(WebSessions::Table)
                    .if_not_exists()
                    .col(string_len(WebSessions::SessionId, 36).primary_key())
                    .col(string_len_uniq(WebSessions::SecretHash, 64))
                    .col(string_len(WebSessions::UserId, 36))
                    .col(big_integer(WebSessions::CreatedAt))
                    .col(big_integer(WebSessions::LastSeenAt))
                    .col(big_integer(WebSessions::ExpiresAt))
                    .col(big_integer(WebSessions::IdleExpiresAt))
                    .col(big_integer_null(WebSessions::RevokedAt))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_web_sessions_user")
                    .table(WebSessions::Table)
                    .col(WebSessions::UserId)
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(WebAuthEvents::Table)
                    .if_not_exists()
                    .col(string_len(WebAuthEvents::EventId, 36).primary_key())
                    .col(string_len(WebAuthEvents::Action, 32))
                    .col(string_len(WebAuthEvents::Outcome, 16))
                    .col(string_len_null(WebAuthEvents::UserId, 36))
                    .col(big_integer(WebAuthEvents::CreatedAt))
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_web_auth_events_created")
                    .table(WebAuthEvents::Table)
                    .col(WebAuthEvents::CreatedAt)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(WebAuthEvents::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(WebSessions::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(WebBootstrap::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(WebUsers::Table).if_exists().to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum WebUsers {
    Table,
    UserId,
    Username,
    PasswordHash,
    Role,
    Enabled,
    CreatedAt,
    UpdatedAt,
}
#[derive(DeriveIden)]
enum WebBootstrap {
    Table,
    Claim,
}
#[derive(DeriveIden)]
enum WebSessions {
    Table,
    SessionId,
    SecretHash,
    UserId,
    CreatedAt,
    LastSeenAt,
    ExpiresAt,
    IdleExpiresAt,
    RevokedAt,
}
#[derive(DeriveIden)]
enum WebAuthEvents {
    Table,
    EventId,
    Action,
    Outcome,
    UserId,
    CreatedAt,
}
