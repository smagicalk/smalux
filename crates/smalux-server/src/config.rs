use std::env;

use serde::{Deserialize, Serialize};
use smalux_core::config::default::{DEFAULT_ADDRESS, DEFAULT_PORT};
use thiserror::Error;

pub(crate) mod database;
pub(crate) mod default;

pub use database::{
    DATABASE_ACQUIRE_TIMEOUT_ENV, DATABASE_CONNECT_TIMEOUT_ENV, DATABASE_IDLE_TIMEOUT_ENV,
    DATABASE_MAX_CONNECTIONS_ENV, DATABASE_MAX_LIFETIME_ENV, DATABASE_MIN_CONNECTIONS_ENV,
    DATABASE_PASSWORD_ENV, DATABASE_RECORD_STMT_IN_SPANS_ENV, DATABASE_SQLX_LOGGING_ENV,
    DATABASE_URL_ENV, DATABASE_USERNAME_ENV, DatabaseBackend, DatabaseConfig, DatabaseConfigError,
    DatabasePoolConfig, default_database_url,
};
pub(crate) use default::{
    DEFAULT_MAX_AGENT_SESSIONS, DEFAULT_MAX_GRPC_MESSAGE_BYTES, DEFAULT_MAX_REGISTRATION_SESSIONS,
    DEFAULT_SHUTDOWN_GRACE_SECONDS,
};

const MAX_AGENT_SESSIONS_ENV: &str = "SMALUX_AGENT_MAX_SESSIONS";
const MAX_REGISTRATION_SESSIONS_ENV: &str = "SMALUX_AGENT_MAX_REGISTRATION_SESSIONS";
const MAX_GRPC_MESSAGE_BYTES_ENV: &str = "SMALUX_AGENT_MAX_MESSAGE_BYTES";
const SHUTDOWN_GRACE_SECONDS_ENV: &str = "SMALUX_SERVER_SHUTDOWN_GRACE_SECONDS";
const LISTEN_ADDRESS_ENV: &str = "SMALUX_SERVER_LISTEN_ADDRESS";
const LISTEN_PORT_ENV: &str = "SMALUX_SERVER_LISTEN_PORT";

/// Server 启动配置错误。
///
/// 数据库子配置保留它自己的精确错误，Server 级环境变量则由
/// `InvalidEnvironment` 表达，避免把会话上限等错误误报为数据库错误。
#[derive(Debug, Error)]
pub(crate) enum ServerConfigError {
    /// 数据库配置解析或校验失败。
    #[error(transparent)]
    Database(#[from] DatabaseConfigError),
    /// Server 环境变量不是所需的正整数。
    #[error("invalid Server environment variable {name}: {value}")]
    InvalidEnvironment { name: &'static str, value: String },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(default)]
pub(crate) struct ServerConfig {
    pub(crate) address: String,
    pub(crate) port: u16,
    /// 启动阶段使用的数据库配置；不会直接放入运行态 AppState。
    pub(crate) database: DatabaseConfig,
    /// 同时存在的 Agent gRPC 会话上限。
    pub(crate) max_agent_sessions: usize,
    /// 同时处于注册业务阶段的会话上限。
    pub(crate) max_registration_sessions: usize,
    /// 单个 gRPC protobuf 消息允许的最大字节数。
    pub(crate) max_grpc_message_bytes: usize,
    /// Server 收到关闭信号后等待长期会话退出的最长秒数。
    pub(crate) shutdown_grace_seconds: u64,
}

/// 已经进入运行态的安全 Server 配置。
///
/// 数据库 URL、用户名和密码只在启动阶段用于创建连接，不进入这个结构，避免
/// handler 或 gRPC service 通过公共状态读取敏感连接信息。
#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub(crate) address: String,
    pub(crate) port: u16,
    pub(crate) max_agent_sessions: usize,
    pub(crate) max_registration_sessions: usize,
    pub(crate) max_grpc_message_bytes: usize,
}

impl ServerConfig {
    /// 从环境变量和默认值加载基础启动配置；CLI 覆盖随后由 `RunArgs::resolve` 应用。
    pub(crate) fn from_env() -> Result<Self, ServerConfigError> {
        Ok(Self {
            address: env::var(LISTEN_ADDRESS_ENV)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_ADDRESS.to_owned()),
            port: read_positive_u16(LISTEN_PORT_ENV, DEFAULT_PORT)?,
            database: DatabaseConfig::from_env()?,
            max_agent_sessions: read_positive_usize(
                MAX_AGENT_SESSIONS_ENV,
                DEFAULT_MAX_AGENT_SESSIONS,
            )?,
            max_registration_sessions: read_positive_usize(
                MAX_REGISTRATION_SESSIONS_ENV,
                DEFAULT_MAX_REGISTRATION_SESSIONS,
            )?,
            max_grpc_message_bytes: read_positive_usize(
                MAX_GRPC_MESSAGE_BYTES_ENV,
                DEFAULT_MAX_GRPC_MESSAGE_BYTES,
            )?,
            shutdown_grace_seconds: read_positive_u64(
                SHUTDOWN_GRACE_SECONDS_ENV,
                DEFAULT_SHUTDOWN_GRACE_SECONDS,
            )?,
        })
    }

    /// 提取不包含数据库秘密的运行态配置。
    pub(crate) fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig {
            address: self.address.clone(),
            port: self.port,
            max_agent_sessions: self.max_agent_sessions,
            max_registration_sessions: self.max_registration_sessions,
            max_grpc_message_bytes: self.max_grpc_message_bytes,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: DEFAULT_ADDRESS.to_string(),
            port: DEFAULT_PORT,
            database: DatabaseConfig::default(),
            max_agent_sessions: DEFAULT_MAX_AGENT_SESSIONS,
            max_registration_sessions: DEFAULT_MAX_REGISTRATION_SESSIONS,
            max_grpc_message_bytes: DEFAULT_MAX_GRPC_MESSAGE_BYTES,
            shutdown_grace_seconds: DEFAULT_SHUTDOWN_GRACE_SECONDS,
        }
    }
}

fn read_positive_usize(name: &'static str, default: usize) -> Result<usize, ServerConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ServerConfigError::InvalidEnvironment { name, value }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(value)) => Err(ServerConfigError::InvalidEnvironment {
            name,
            value: value.to_string_lossy().into_owned(),
        }),
    }
}

fn read_positive_u64(name: &'static str, default: u64) -> Result<u64, ServerConfigError> {
    read_positive_usize(name, default as usize).map(|value| value as u64)
}

fn read_positive_u16(name: &'static str, default: u16) -> Result<u16, ServerConfigError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u16>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ServerConfigError::InvalidEnvironment { name, value }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(value)) => Err(ServerConfigError::InvalidEnvironment {
            name,
            value: value.to_string_lossy().into_owned(),
        }),
    }
}
