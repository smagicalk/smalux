use std::env;

use serde::{Deserialize, Serialize};
use smalux_core::config::default::{DEFAULT_ADDRESS, DEFAULT_PORT};

use crate::database::{DatabaseConfig, DatabaseError};

pub(crate) mod default;

pub(crate) const DEFAULT_MAX_AGENT_SESSIONS: usize = 256;
pub(crate) const DEFAULT_MAX_REGISTRATION_SESSIONS: usize = 32;
pub(crate) const DEFAULT_MAX_GRPC_MESSAGE_BYTES: usize = 1024 * 1024;
pub(crate) const DEFAULT_SHUTDOWN_GRACE_SECONDS: u64 = 15;

const MAX_AGENT_SESSIONS_ENV: &str = "SMALUX_AGENT_MAX_SESSIONS";
const MAX_REGISTRATION_SESSIONS_ENV: &str = "SMALUX_AGENT_MAX_REGISTRATION_SESSIONS";
const MAX_GRPC_MESSAGE_BYTES_ENV: &str = "SMALUX_AGENT_MAX_MESSAGE_BYTES";
const SHUTDOWN_GRACE_SECONDS_ENV: &str = "SMALUX_SERVER_SHUTDOWN_GRACE_SECONDS";

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
    /// 从环境变量加载完整启动配置。
    // TODO: 引入 CLI 后按“CLI 参数 > 环境变量 > 默认值”的优先级统一构造配置。
    pub(crate) fn from_env() -> Result<Self, DatabaseError> {
        Ok(Self {
            address: DEFAULT_ADDRESS.to_string(),
            port: DEFAULT_PORT,
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

fn read_positive_usize(name: &'static str, default: usize) -> Result<usize, DatabaseError> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| DatabaseError::InvalidEnvironment { name, value }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(value)) => Err(DatabaseError::InvalidEnvironment {
            name,
            value: value.to_string_lossy().into_owned(),
        }),
    }
}

fn read_positive_u64(name: &'static str, default: u64) -> Result<u64, DatabaseError> {
    read_positive_usize(name, default as usize).map(|value| value as u64)
}
