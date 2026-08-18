//! Server 数据库层。
//!
//! 这一层负责建立 SeaORM 连接池、执行版本化迁移、暴露实体模型，并提供 Server
//! Noise 密钥环和 Agent 注册的持久化方法。注册中心、任务服务等业务模块应该调用
//! 这里提供的领域持久化接口，不应直接使用连接池、实体或 SeaORM 查询。

pub mod entity;
pub mod migration;

mod agent_registration;
mod config;
mod connection;
mod keyring;

pub(crate) use agent_registration::{
    PendingAgentRegistration, PersistedAgentAuthorization, PrepareAgentRegistrationError,
};
pub use config::{
    DATABASE_ACQUIRE_TIMEOUT_ENV, DATABASE_CONNECT_TIMEOUT_ENV, DATABASE_IDLE_TIMEOUT_ENV,
    DATABASE_MAX_CONNECTIONS_ENV, DATABASE_MAX_LIFETIME_ENV, DATABASE_MIN_CONNECTIONS_ENV,
    DATABASE_PASSWORD_ENV, DATABASE_RECORD_STMT_IN_SPANS_ENV, DATABASE_SQLX_LOGGING_ENV,
    DATABASE_URL_ENV, DATABASE_USERNAME_ENV, DatabaseBackend, DatabaseConfig, DatabaseError,
    DatabasePoolConfig, default_database_url,
};
pub use connection::ServerDatabase;
pub use keyring::ServerKeyRingRecord;
