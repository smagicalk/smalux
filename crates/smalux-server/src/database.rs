//! Server 数据库层。
//!
//! 这一层负责建立 SeaORM 连接池、执行版本化迁移、暴露实体模型，并提供 Server
//! Noise 密钥环的快照读写方法。注册中心、任务服务等业务模块应该通过这里提供的
//! 连接或 repository 访问数据，不应自行创建连接或执行迁移。

pub mod entity;
pub mod migration;

mod connection;
mod keyring;

pub use connection::{
    DATABASE_ACQUIRE_TIMEOUT_ENV, DATABASE_CONNECT_TIMEOUT_ENV, DATABASE_IDLE_TIMEOUT_ENV,
    DATABASE_MAX_CONNECTIONS_ENV, DATABASE_MAX_LIFETIME_ENV, DATABASE_MIN_CONNECTIONS_ENV,
    DATABASE_PASSWORD_ENV, DATABASE_RECORD_STMT_IN_SPANS_ENV, DATABASE_SQLX_LOGGING_ENV,
    DATABASE_URL_ENV, DATABASE_USERNAME_ENV, DatabaseBackend, DatabaseConfig, DatabaseError,
    DatabasePoolConfig, ServerDatabase, default_database_url,
};
pub use keyring::ServerKeyRingRecord;
