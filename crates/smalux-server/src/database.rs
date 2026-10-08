//! Server 数据库层。
//!
//! 这一层负责建立 SeaORM 连接池、执行版本化迁移、暴露实体模型，并提供 Server
//! Noise 密钥环和 Agent 注册的持久化方法。注册中心、任务服务等业务模块应该调用
//! 这里提供的领域持久化接口，不应直接使用连接池、实体或 SeaORM 查询。

pub mod entity;
pub mod migration;

mod agent_job;
mod agent_registration;
mod agent_snapshot;
mod connection;
mod error;
mod job_command;
mod job_event;
mod keyring;
mod management;
mod plugin_runtime;
mod plugin_schema;
pub(crate) use plugin_runtime::{AgentPluginRuntimeRecord, StoredPluginRuntime};
mod task_report;
mod web_job_operations;

pub use crate::config::{
    DATABASE_ACQUIRE_TIMEOUT_ENV, DATABASE_CONNECT_TIMEOUT_ENV, DATABASE_IDLE_TIMEOUT_ENV,
    DATABASE_MAX_CONNECTIONS_ENV, DATABASE_MAX_LIFETIME_ENV, DATABASE_MIN_CONNECTIONS_ENV,
    DATABASE_PASSWORD_ENV, DATABASE_RECORD_STMT_IN_SPANS_ENV, DATABASE_SQLX_LOGGING_ENV,
    DATABASE_URL_ENV, DATABASE_USERNAME_ENV, DatabaseBackend, DatabaseConfig, DatabaseConfigError,
    DatabasePoolConfig, default_database_url,
};
pub(crate) use agent_job::AgentJobCatalogRecord;
pub(crate) use agent_registration::{
    PendingAgentRegistration, PersistedAgentAuthorization, PrepareAgentRegistrationError,
};
pub use connection::ServerDatabase;
pub use error::DatabaseError;
pub(crate) use job_event::{JobEventCursor, JobEventRecord};
pub use keyring::ServerKeyRingRecord;
pub(crate) use management::{AgentRecord, RegistrationTokenRecord, RevokeTokenOutcome};
pub use plugin_schema::PluginSchemaRecord;
pub(crate) use task_report::{TaskReportCursor, TaskReportRecord};
pub(crate) use web_job_operations::{WebJobOperationDraft, WebJobOperationRecord};
