//! Server 数据库配置、环境变量和跨后端连接参数解析。

use std::{collections::BTreeMap, env, fmt, path::Path, time::Duration};

use sea_orm::ConnectOptions;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use url::Url;

/// 覆盖默认 SQLite 数据库地址的环境变量。
pub const DATABASE_URL_ENV: &str = "SMALUX_DATABASE_URL";
/// 覆盖数据库用户名的环境变量。
pub const DATABASE_USERNAME_ENV: &str = "SMALUX_DATABASE_USERNAME";
/// 覆盖数据库密码的环境变量。
pub const DATABASE_PASSWORD_ENV: &str = "SMALUX_DATABASE_PASSWORD";
/// 覆盖最大连接数的环境变量。
pub const DATABASE_MAX_CONNECTIONS_ENV: &str = "SMALUX_DATABASE_MAX_CONNECTIONS";
/// 覆盖最小连接数的环境变量。
pub const DATABASE_MIN_CONNECTIONS_ENV: &str = "SMALUX_DATABASE_MIN_CONNECTIONS";
/// 覆盖连接超时时间的环境变量，单位为秒。
pub const DATABASE_CONNECT_TIMEOUT_ENV: &str = "SMALUX_DATABASE_CONNECT_TIMEOUT_SECONDS";
/// 覆盖获取连接超时时间的环境变量，单位为秒。
pub const DATABASE_ACQUIRE_TIMEOUT_ENV: &str = "SMALUX_DATABASE_ACQUIRE_TIMEOUT_SECONDS";
/// 覆盖空闲连接回收时间的环境变量，单位为秒。
pub const DATABASE_IDLE_TIMEOUT_ENV: &str = "SMALUX_DATABASE_IDLE_TIMEOUT_SECONDS";
/// 覆盖连接最大生命周期的环境变量，单位为秒。
pub const DATABASE_MAX_LIFETIME_ENV: &str = "SMALUX_DATABASE_MAX_LIFETIME_SECONDS";
/// 是否启用 SQLx 语句日志的环境变量。
pub const DATABASE_SQLX_LOGGING_ENV: &str = "SMALUX_DATABASE_SQLX_LOGGING";
/// 是否记录 SQL 语句 span 的环境变量。
pub const DATABASE_RECORD_STMT_IN_SPANS_ENV: &str = "SMALUX_DATABASE_RECORD_STMT_IN_SPANS";

const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_MIN_CONNECTIONS: u32 = 1;
const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 5;

/// 数据库层自己的稳定错误边界，避免启动代码依赖 SeaORM 的内部错误结构。
#[derive(Debug, Error)]
pub enum DatabaseError {
    /// 解析应用数据目录失败。
    #[error("failed to resolve application data directory: {0}")]
    Directory(#[from] smalux_core::config::DirectoryError),
    /// 创建默认 SQLite 目录失败。
    #[error("failed to prepare default SQLite directory: {0}")]
    Io(#[from] std::io::Error),
    /// SeaORM 或底层 SQLx 驱动返回的连接/迁移错误。
    #[error("database operation failed: {0}")]
    SeaOrm(#[from] sea_orm::DbErr),
    /// URL 库无法构造默认 SQLite 地址；这是程序配置错误。
    #[error("failed to build default SQLite URL: {0}")]
    InvalidDefaultUrl(String),
    /// 数据库连接配置不满足跨后端约束。
    #[error("invalid database configuration: {0}")]
    InvalidConfig(String),
    /// 数据库 URL 无法解析。
    #[error("invalid database URL: {0}")]
    InvalidUrl(String),
    /// URL scheme 不在当前编译的支持范围内。
    #[error("unsupported database URL scheme: {0}")]
    UnsupportedBackend(String),
    /// 后端 options 中出现当前 adapter 不认识的参数。
    #[error("unsupported {backend} database option: {key}")]
    UnsupportedOption { backend: &'static str, key: String },
    /// 后端 option 的 JSON 类型不正确。
    #[error("invalid {backend} database option '{key}': {reason}")]
    InvalidOption {
        backend: &'static str,
        key: String,
        reason: String,
    },
    /// URL 查询参数与 options map 中的同名参数冲突。
    #[error("database option '{key}' conflicts with an existing URL query parameter")]
    OptionConflict { key: String },
    /// 环境变量中的数值或布尔值无法解析。
    #[error("invalid database environment variable {name}: {value}")]
    InvalidEnvironment { name: &'static str, value: String },
    /// 数据库中保存的 Server 密钥环结构不完整或不一致。
    #[error("invalid persisted Server keyring: {0}")]
    InvalidServerKeyring(String),
    /// Server 密钥环记录不存在，通常表示迁移后数据库尚未完成初始化。
    #[error("persisted Server keyring record is missing")]
    MissingServerKeyring,
    /// Server 密钥环写入时发现其他进程已经提交了更新。
    #[error("Server keyring revision conflict: expected {expected}, actual {actual:?}")]
    ServerKeyringRevisionConflict {
        /// 调用方读取到的 revision。
        expected: i64,
        /// 数据库当前 revision；数据库记录被删除时为 None。
        actual: Option<i64>,
    },
    /// revision 达到 i64 上限，无法再安全递增。
    #[error("Server keyring revision overflow")]
    ServerKeyringRevisionOverflow,
    /// 读取系统时间失败，无法安全比较注册 Token 或注册事务的过期时间。
    #[error("failed to read database clock: {0}")]
    Clock(#[from] std::time::SystemTimeError),
    /// 注册相关记录违反持久化不变量，例如数据库中的 PSK 长度错误。
    #[error("invalid persisted Agent registration data: {0}")]
    InvalidAgentRegistration(String),
    /// 恢复或校验 Noise 身份时失败。
    #[error("Noise keyring operation failed: {0}")]
    Noise(#[from] smalux_protocol::noise::NoiseError),
}

/// SeaORM 支持的运行时数据库后端。
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum DatabaseBackend {
    /// 使用 SQLite 文件或内存数据库。
    Sqlite,
    /// 使用 PostgreSQL 数据库。
    Postgres,
    /// 使用 MySQL 或兼容协议的数据库。
    MySql,
}

impl DatabaseBackend {
    /// 将 URL scheme 转换为数据库后端。
    fn from_url(url: &Url) -> Result<Self, DatabaseError> {
        match url.scheme().to_ascii_lowercase().as_str() {
            "sqlite" => Ok(Self::Sqlite),
            "postgres" | "postgresql" => Ok(Self::Postgres),
            "mysql" => Ok(Self::MySql),
            scheme => Err(DatabaseError::UnsupportedBackend(scheme.to_owned())),
        }
    }

    /// 返回脱敏日志标签。
    pub fn label(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
        }
    }
}

/// 数据库连接池的跨后端通用参数。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct DatabasePoolConfig {
    /// 连接池允许创建的最大连接数。
    pub max_connections: u32,
    /// 连接池保持的最小连接数。
    pub min_connections: u32,
    /// 建立连接时的超时时间，单位为秒。
    pub connect_timeout_seconds: u64,
    /// 从连接池获取连接的超时时间，单位为秒。
    pub acquire_timeout_seconds: Option<u64>,
    /// 空闲连接回收时间，单位为秒。
    pub idle_timeout_seconds: Option<u64>,
    /// 单个连接最大生命周期，单位为秒。
    pub max_lifetime_seconds: Option<u64>,
    /// 是否启用 SQLx 语句日志；默认关闭，避免业务 SQL 进入日志。
    pub sqlx_logging: bool,
    /// 是否将 SQL 语句记录到 tracing span；默认关闭。
    pub record_stmt_in_spans: bool,
}

impl Default for DatabasePoolConfig {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            min_connections: DEFAULT_MIN_CONNECTIONS,
            connect_timeout_seconds: DEFAULT_CONNECT_TIMEOUT_SECONDS,
            acquire_timeout_seconds: None,
            idle_timeout_seconds: None,
            max_lifetime_seconds: None,
            sqlx_logging: false,
            record_stmt_in_spans: false,
        }
    }
}

/// Server 数据库连接配置。
///
/// `url`、`username` 和 `password` 是连接身份的通用字段；`options` 只保存当前 URL
/// 对应后端的专属参数。连接建立前会严格校验 URL scheme、认证字段、连接池范围和
/// options key，连接成功后原始配置不会进入 `ServerDatabase` 或 Axum 状态。
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct DatabaseConfig {
    url: String,
    username: Option<String>,
    /// 密码只允许从配置读取，序列化时跳过；Debug 实现也不会输出它。
    #[serde(skip_serializing)]
    password: Option<String>,
    pub pool: DatabasePoolConfig,
    /// 后端专属配置的值只允许使用标量 JSON 类型。
    pub options: BTreeMap<String, Value>,
}

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let option_keys = self.options.keys().collect::<Vec<_>>();
        formatter
            .debug_struct("DatabaseConfig")
            .field("url", &"<redacted>")
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("pool", &self.pool)
            .field("option_keys", &option_keys)
            .finish()
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        // `Default` 不执行文件系统访问；正式启动使用 `from_env`，测试可安全使用内存库。
        Self::new("sqlite::memory:")
    }
}

impl DatabaseConfig {
    /// 使用指定 URL 创建配置，连接池参数采用 Server 默认值。
    ///
    /// URL 应只包含 scheme、主机、端口、数据库名或 SQLite 路径，不要在 URL 中嵌入
    /// username/password；认证信息通过 [`Self::with_credentials`] 注入。
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username: None,
            password: None,
            pool: DatabasePoolConfig::default(),
            options: BTreeMap::new(),
        }
    }

    /// 从环境变量读取完整数据库配置；未设置 URL 时使用应用数据目录下的 SQLite 文件。
    pub fn from_env() -> Result<Self, DatabaseError> {
        let url = env::var(DATABASE_URL_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(Ok)
            .unwrap_or_else(default_database_url)?;
        let mut config = Self::new(url);
        config.username = optional_env(DATABASE_USERNAME_ENV);
        config.password = optional_env(DATABASE_PASSWORD_ENV);
        config.pool = DatabasePoolConfig::from_env()?;
        config.validate()?;
        Ok(config)
    }

    /// 设置数据库认证信息。
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// 添加一个后端专属 option；真正连接时会由后端 adapter 校验和应用。
    pub fn with_option(mut self, key: impl Into<String>, value: Value) -> Self {
        self.options.insert(key.into(), value);
        self
    }

    /// 返回原始数据库 URL，供连接层使用。
    pub fn url(&self) -> &str {
        &self.url
    }

    /// 返回配置中声明的用户名；密码没有公开 getter，避免被普通调用方传播。
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// 根据 URL 返回数据库后端；非法 URL 返回稳定的配置错误。
    pub fn backend(&self) -> Result<DatabaseBackend, DatabaseError> {
        let url =
            Url::parse(&self.url).map_err(|error| DatabaseError::InvalidUrl(error.to_string()))?;
        DatabaseBackend::from_url(&url)
    }

    /// 返回不包含用户名、密码或完整文件路径的日志标签。
    pub fn backend_label(&self) -> &'static str {
        self.backend()
            .map(DatabaseBackend::label)
            .unwrap_or("unknown")
    }

    /// 校验配置但不建立网络连接。
    pub fn validate(&self) -> Result<(), DatabaseError> {
        let (_, backend) = self.resolve_connection_url()?;
        self.validate_options(backend)?;
        if self.pool.max_connections == 0 {
            return Err(DatabaseError::InvalidConfig(
                "max_connections must be greater than zero".to_owned(),
            ));
        }
        if self.pool.min_connections == 0 {
            return Err(DatabaseError::InvalidConfig(
                "min_connections must be greater than zero".to_owned(),
            ));
        }
        if self.pool.min_connections > self.pool.max_connections {
            return Err(DatabaseError::InvalidConfig(
                "min_connections must not exceed max_connections".to_owned(),
            ));
        }
        if self.pool.connect_timeout_seconds == 0 {
            return Err(DatabaseError::InvalidConfig(
                "connect_timeout_seconds must be greater than zero".to_owned(),
            ));
        }
        for (name, value) in [
            ("acquire_timeout_seconds", self.pool.acquire_timeout_seconds),
            ("idle_timeout_seconds", self.pool.idle_timeout_seconds),
            ("max_lifetime_seconds", self.pool.max_lifetime_seconds),
        ] {
            if value == Some(0) {
                return Err(DatabaseError::InvalidConfig(format!(
                    "{name} must be greater than zero when configured"
                )));
            }
        }
        Ok(())
    }

    /// 将配置解析成 SeaORM 使用的连接 URL，并返回已确定的后端类型。
    pub(super) fn resolve_connection_url(
        &self,
    ) -> Result<(String, DatabaseBackend), DatabaseError> {
        let mut url =
            Url::parse(&self.url).map_err(|error| DatabaseError::InvalidUrl(error.to_string()))?;
        let backend = DatabaseBackend::from_url(&url)?;

        let has_url_username = !url.username().is_empty();
        let has_url_password = url.password().is_some();
        if has_url_username || has_url_password {
            return Err(DatabaseError::InvalidConfig(
                "database URL must not contain username or password; use separate fields"
                    .to_owned(),
            ));
        }
        if self.password.is_some() && self.username.is_none() {
            return Err(DatabaseError::InvalidConfig(
                "database password requires a database username".to_owned(),
            ));
        }
        if backend == DatabaseBackend::Sqlite
            && (self.username.is_some() || self.password.is_some())
        {
            return Err(DatabaseError::InvalidConfig(
                "SQLite does not accept username or password fields".to_owned(),
            ));
        }

        if let Some(username) = &self.username {
            url.set_username(username).map_err(|_| {
                DatabaseError::InvalidConfig("database username cannot be encoded".to_owned())
            })?;
        }
        if let Some(password) = &self.password {
            url.set_password(Some(password)).map_err(|_| {
                DatabaseError::InvalidConfig("database password cannot be encoded".to_owned())
            })?;
        }

        self.apply_url_options(&mut url, backend)?;
        Ok((url.to_string(), backend))
    }

    /// 将 SQLite/MySQL 和 PostgreSQL 的 URL 级 option 安全地加入 URL。
    fn apply_url_options(
        &self,
        url: &mut Url,
        backend: DatabaseBackend,
    ) -> Result<(), DatabaseError> {
        for (key, value) in &self.options {
            let is_url_option = match backend {
                DatabaseBackend::Sqlite => matches!(key.as_str(), "mode" | "cache" | "immutable"),
                DatabaseBackend::Postgres => matches!(key.as_str(), "sslmode"),
                DatabaseBackend::MySql => matches!(key.as_str(), "charset" | "ssl-mode"),
            };
            if !is_url_option {
                continue;
            }
            if url
                .query_pairs()
                .any(|(existing, _)| existing == key.as_str())
            {
                return Err(DatabaseError::OptionConflict { key: key.clone() });
            }
            let encoded = option_scalar_as_string(backend, key, value)?;
            url.query_pairs_mut().append_pair(key, &encoded);
        }
        Ok(())
    }

    /// 将通用连接池字段和 SeaORM 已知的后端 setter 应用到 ConnectOptions。
    pub(super) fn apply_connect_options(
        &self,
        options: &mut ConnectOptions,
        backend: DatabaseBackend,
    ) -> Result<(), DatabaseError> {
        options
            .max_connections(self.pool.max_connections)
            .min_connections(self.pool.min_connections)
            .connect_timeout(Duration::from_secs(self.pool.connect_timeout_seconds))
            .sqlx_logging(self.pool.sqlx_logging)
            .record_stmt_in_spans(self.pool.record_stmt_in_spans);
        if let Some(seconds) = self.pool.acquire_timeout_seconds {
            options.acquire_timeout(Duration::from_secs(seconds));
        }
        if let Some(seconds) = self.pool.idle_timeout_seconds {
            options.idle_timeout(Duration::from_secs(seconds));
        }
        if let Some(seconds) = self.pool.max_lifetime_seconds {
            options.max_lifetime(Duration::from_secs(seconds));
        }

        match backend {
            DatabaseBackend::Postgres => {
                if let Some(value) = self.options.get("application_name") {
                    options.set_application_name(option_string(
                        backend,
                        "application_name",
                        value,
                    )?);
                }
                if let Some(value) = self.options.get("search_path") {
                    options.set_schema_search_path(option_string(backend, "search_path", value)?);
                }
                if let Some(value) = self.options.get("statement_timeout_seconds") {
                    let seconds = option_u64(backend, "statement_timeout_seconds", value)?;
                    if seconds == 0 {
                        return Err(DatabaseError::InvalidOption {
                            backend: backend.label(),
                            key: "statement_timeout_seconds".to_owned(),
                            reason: "must be greater than zero".to_owned(),
                        });
                    }
                    options.statement_timeout(Duration::from_secs(seconds));
                }
            }
            DatabaseBackend::Sqlite | DatabaseBackend::MySql => {}
        }

        self.validate_options(backend)
    }

    /// 校验当前后端允许的 options key。
    fn validate_options(&self, backend: DatabaseBackend) -> Result<(), DatabaseError> {
        for key in self.options.keys() {
            let supported = match backend {
                DatabaseBackend::Sqlite => {
                    matches!(key.as_str(), "mode" | "cache" | "immutable")
                }
                DatabaseBackend::Postgres => matches!(
                    key.as_str(),
                    "application_name" | "search_path" | "statement_timeout_seconds" | "sslmode"
                ),
                DatabaseBackend::MySql => matches!(key.as_str(), "charset" | "ssl-mode"),
            };
            if !supported {
                return Err(DatabaseError::UnsupportedOption {
                    backend: backend.label(),
                    key: key.clone(),
                });
            }

            let value = self
                .options
                .get(key)
                .expect("option key came from the same map");
            match (backend, key.as_str()) {
                (DatabaseBackend::Sqlite, "mode" | "cache" | "immutable")
                | (DatabaseBackend::Postgres, "sslmode")
                | (DatabaseBackend::MySql, "charset" | "ssl-mode") => {
                    option_scalar_as_string(backend, key, value)?;
                }
                (DatabaseBackend::Postgres, "application_name" | "search_path") => {
                    option_string(backend, key, value)?;
                }
                (DatabaseBackend::Postgres, "statement_timeout_seconds") => {
                    let seconds = option_u64(backend, key, value)?;
                    if seconds == 0 {
                        return Err(DatabaseError::InvalidOption {
                            backend: backend.label(),
                            key: key.clone(),
                            reason: "must be greater than zero".to_owned(),
                        });
                    }
                }
                _ => unreachable!("supported option should have a validation branch"),
            }
        }
        Ok(())
    }
}

impl DatabasePoolConfig {
    /// 使用环境变量覆盖默认连接池参数。
    fn from_env() -> Result<Self, DatabaseError> {
        let defaults = Self::default();
        Ok(Self {
            max_connections: env_u32(DATABASE_MAX_CONNECTIONS_ENV, defaults.max_connections)?,
            min_connections: env_u32(DATABASE_MIN_CONNECTIONS_ENV, defaults.min_connections)?,
            connect_timeout_seconds: env_u64(
                DATABASE_CONNECT_TIMEOUT_ENV,
                defaults.connect_timeout_seconds,
            )?,
            acquire_timeout_seconds: env_optional_u64(DATABASE_ACQUIRE_TIMEOUT_ENV)?,
            idle_timeout_seconds: env_optional_u64(DATABASE_IDLE_TIMEOUT_ENV)?,
            max_lifetime_seconds: env_optional_u64(DATABASE_MAX_LIFETIME_ENV)?,
            sqlx_logging: env_bool(DATABASE_SQLX_LOGGING_ENV, defaults.sqlx_logging)?,
            record_stmt_in_spans: env_bool(
                DATABASE_RECORD_STMT_IN_SPANS_ENV,
                defaults.record_stmt_in_spans,
            )?,
        })
    }
}

/// 将 option 的标量值转换成 URL query 可接受的字符串。
fn option_scalar_as_string(
    backend: DatabaseBackend,
    key: &str,
    value: &Value,
) -> Result<String, DatabaseError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => Err(DatabaseError::InvalidOption {
            backend: backend.label(),
            key: key.to_owned(),
            reason: "expected a string, boolean, or number".to_owned(),
        }),
    }
}

fn option_string(
    backend: DatabaseBackend,
    key: &str,
    value: &Value,
) -> Result<String, DatabaseError> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| DatabaseError::InvalidOption {
            backend: backend.label(),
            key: key.to_owned(),
            reason: "expected a string".to_owned(),
        })
}

fn option_u64(backend: DatabaseBackend, key: &str, value: &Value) -> Result<u64, DatabaseError> {
    value.as_u64().ok_or_else(|| DatabaseError::InvalidOption {
        backend: backend.label(),
        key: key.to_owned(),
        reason: "expected an unsigned integer".to_owned(),
    })
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn env_u32(name: &'static str, default: u32) -> Result<u32, DatabaseError> {
    let Some(value) = optional_env(name) else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| DatabaseError::InvalidEnvironment { name, value })
}

fn env_u64(name: &'static str, default: u64) -> Result<u64, DatabaseError> {
    let Some(value) = optional_env(name) else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| DatabaseError::InvalidEnvironment { name, value })
}

fn env_optional_u64(name: &'static str) -> Result<Option<u64>, DatabaseError> {
    let Some(value) = optional_env(name) else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|_| DatabaseError::InvalidEnvironment { name, value })
}

fn env_bool(name: &'static str, default: bool) -> Result<bool, DatabaseError> {
    let Some(value) = optional_env(name) else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| DatabaseError::InvalidEnvironment { name, value })
}

/// 返回默认数据库 URL，并确保公共应用目录存在。
pub fn default_database_url() -> Result<String, DatabaseError> {
    let directories = smalux_core::config::AppDirectories::discover()?;
    directories.ensure_all()?;
    let path = directories.data_dir().join("server.db");
    sqlite_url_for_path(&path)
}

/// 将跨平台文件路径转换为 SQLx 可以识别的 SQLite URL。
fn sqlite_url_for_path(path: &Path) -> Result<String, DatabaseError> {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let mut url = Url::parse("sqlite:///")
        .map_err(|error| DatabaseError::InvalidDefaultUrl(error.to_string()))?;
    // Url 负责对空格、井号等文件名字符做百分号编码。
    url.set_path(&normalized);
    url.set_query(Some("mode=rwc"));
    let value = url.to_string();

    // Windows 绝对路径需要保留 C:/；SQLx 解析时会去掉 sqlite scheme，
    // 因此把 sqlite:///C:/... 调整为 sqlite://C:/...。
    #[cfg(windows)]
    if normalized.as_bytes().get(1) == Some(&b':') {
        return Ok(value.replacen("sqlite:///", "sqlite://", 1));
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use sea_orm::{ActiveModelTrait, EntityTrait, Set};
    use sea_orm_migration::{MigratorTrait, SchemaManager};
    use serde_json::json;

    use super::DatabaseConfig;
    use crate::database::entity::{agent, agent_registration, registration_token};
    use crate::database::{ServerDatabase, migration::Migrator};

    #[test]
    fn database_config_reports_backend_without_exposing_url() {
        let config = DatabaseConfig::new("postgres://example/db")
            .with_credentials("user", "secret")
            .with_option("application_name", json!("smalux-test"));
        assert_eq!(config.backend_label(), "postgres");
        assert_eq!(config.username(), Some("user"));
        assert!(config.validate().is_ok());
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("postgres://example/db"));
        let serialized = serde_json::to_string(&config).expect("config should serialize");
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn database_config_detects_all_supported_backends() {
        assert_eq!(
            DatabaseConfig::new("sqlite::memory:").backend_label(),
            "sqlite"
        );
        assert_eq!(
            DatabaseConfig::new("postgresql://example/db").backend_label(),
            "postgres"
        );
        assert_eq!(
            DatabaseConfig::new("mysql://example/db").backend_label(),
            "mysql"
        );
        assert_eq!(
            DatabaseConfig::new("mssql://example/db").backend_label(),
            "unknown"
        );
    }

    #[test]
    fn database_config_rejects_credentials_embedded_in_url() {
        let config = DatabaseConfig::new("postgres://user:secret@example/db");
        let error = config
            .validate()
            .expect_err("embedded credentials should be rejected");
        assert!(error.to_string().contains("separate fields"));
    }

    #[test]
    fn database_config_rejects_sqlite_credentials_and_invalid_pool() {
        let sqlite = DatabaseConfig::new("sqlite::memory:").with_credentials("user", "secret");
        assert!(sqlite.validate().is_err());

        let mut pool = super::DatabasePoolConfig::default();
        pool.min_connections = pool.max_connections + 1;
        let invalid_pool = DatabaseConfig {
            pool,
            ..DatabaseConfig::default()
        };
        assert!(invalid_pool.validate().is_err());
    }

    #[test]
    fn database_config_rejects_unknown_backend_options() {
        let config =
            DatabaseConfig::new("sqlite::memory:").with_option("not_supported", json!(true));
        let error = config
            .validate()
            .expect_err("unknown backend option should be rejected");
        assert!(error.to_string().contains("not_supported"));
    }

    #[test]
    fn database_config_accepts_sqlite_url_options() {
        let config = DatabaseConfig::new("sqlite::memory:")
            .with_option("mode", json!("memory"))
            .with_option("cache", json!("shared"));
        config
            .validate()
            .expect("supported SQLite URL options should validate");
    }

    #[test]
    fn database_config_rejects_duplicate_url_options() {
        let config =
            DatabaseConfig::new("sqlite::memory:?mode=ro").with_option("mode", json!("rwc"));
        let error = config
            .validate()
            .expect_err("duplicate URL and map options should be rejected");
        assert!(error.to_string().contains("conflicts"));
    }

    #[test]
    fn database_config_validates_backend_option_types_before_connecting() {
        let config =
            DatabaseConfig::new("postgres://example/db").with_option("application_name", json!(42));
        let error = config
            .validate()
            .expect_err("PostgreSQL option type should be checked during validation");
        assert!(error.to_string().contains("application_name"));

        let config = DatabaseConfig::new("postgres://example/db")
            .with_option("statement_timeout_seconds", json!(0));
        let error = config
            .validate()
            .expect_err("zero statement timeout should be rejected");
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn database_config_rejects_zero_optional_pool_timeouts() {
        let pool = super::DatabasePoolConfig {
            acquire_timeout_seconds: Some(0),
            ..super::DatabasePoolConfig::default()
        };
        let config = DatabaseConfig {
            pool,
            ..DatabaseConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn sqlite_connection_runs_migrations_idempotently() {
        let database = ServerDatabase::connect(DatabaseConfig::new("sqlite::memory:"))
            .await
            .expect("in-memory database should connect");
        assert_eq!(database.backend_label(), "sqlite");
        let manager = SchemaManager::new(database.connection());

        for table in [
            "agents",
            "registration_tokens",
            "agent_registrations",
            "server_keyrings",
        ] {
            assert!(
                manager
                    .has_table(table)
                    .await
                    .expect("table lookup should work"),
                "migration should create {table}"
            );
        }
        assert!(
            manager
                .has_column("server_keyrings", "revision")
                .await
                .expect("keyring revision column lookup should work")
        );

        // 先写入真实数据，验证最终 schema 的级联外键会自动清理注册事务。
        agent::ActiveModel {
            agent_id: Set("agent-1".to_owned()),
            name: Set("Agent One".to_owned()),
            public_key: Set(vec![1; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            revoked_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("agent should be inserted");
        agent::ActiveModel {
            agent_id: Set("agent-2".to_owned()),
            // 名称只是展示字段，不同稳定 ID 可以使用相同名称。
            name: Set("Agent One".to_owned()),
            public_key: Set(vec![2; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            revoked_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("second agent should be inserted");
        registration_token::ActiveModel {
            token_id: Set("token-1".to_owned()),
            psk: Set(vec![3; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            used_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("token should be inserted");
        registration_token::ActiveModel {
            token_id: Set("token-2".to_owned()),
            psk: Set(vec![4; 32]),
            status: Set("active".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            used_at: Set(None),
        }
        .insert(database.connection())
        .await
        .expect("second token should be inserted");
        for (registration_id, token_id, agent_id, agent_name) in [
            ("registration-1", "token-1", "agent-1", "Agent One"),
            ("registration-2", "token-2", "agent-2", "Agent One"),
        ] {
            agent_registration::ActiveModel {
                registration_id: Set(registration_id.to_owned()),
                token_id: Set(token_id.to_owned()),
                agent_id: Set(Some(agent_id.to_owned())),
                reserved_agent_id: Set(agent_id.to_owned()),
                agent_name: Set(agent_name.to_owned()),
                agent_public_key: Set(vec![
                    if registration_id == "registration-1" {
                        5
                    } else {
                        6
                    };
                    32
                ]),
                status: Set("prepared".to_owned()),
                created_at: Set(1),
                updated_at: Set(1),
                expires_at: Set(None),
                committed_at: Set(None),
            }
            .insert(database.connection())
            .await
            .expect("registration should be inserted");
        }

        // 即使两个 Server 实例同时越过应用层查询，数据库也必须拒绝同一 Token
        // 绑定第二条注册事务。
        let duplicate_token_registration = agent_registration::ActiveModel {
            registration_id: Set("registration-duplicate-token".to_owned()),
            token_id: Set("token-1".to_owned()),
            agent_id: Set(Some("agent-2".to_owned())),
            reserved_agent_id: Set("agent-2".to_owned()),
            agent_name: Set("Agent One".to_owned()),
            agent_public_key: Set(vec![7; 32]),
            status: Set("prepared".to_owned()),
            created_at: Set(1),
            updated_at: Set(1),
            expires_at: Set(None),
            committed_at: Set(None),
        }
        .insert(database.connection())
        .await;
        assert!(
            duplicate_token_registration.is_err(),
            "one registration Token must have at most one registration transaction"
        );

        // 删除父表记录会自动删除对应注册事务，但不会误删另一个 Agent 的事务。
        agent::Entity::delete_by_id("agent-1")
            .exec(database.connection())
            .await
            .expect("agent deletion should succeed");
        assert!(
            agent_registration::Entity::find_by_id("registration-1")
                .one(database.connection())
                .await
                .expect("registration lookup should work")
                .is_none()
        );
        assert!(
            agent_registration::Entity::find_by_id("registration-2")
                .one(database.connection())
                .await
                .expect("second registration lookup should work")
                .is_some()
        );
        registration_token::Entity::delete_by_id("token-2")
            .exec(database.connection())
            .await
            .expect("token deletion should succeed");
        assert!(
            agent_registration::Entity::find_by_id("registration-2")
                .one(database.connection())
                .await
                .expect("second registration lookup should work")
                .is_none()
        );

        // Server 重启或多个启动钩子重复调用 migrate 都不能重复建表。
        Migrator::up(database.connection(), None)
            .await
            .expect("running migrations twice should be safe");

        // 全部迁移可以完整回滚并重新创建，便于测试环境重置 schema。
        Migrator::down(database.connection(), None)
            .await
            .expect("all migrations should roll back");
        for table in [
            "agents",
            "registration_tokens",
            "agent_registrations",
            "server_keyrings",
        ] {
            assert!(
                !manager
                    .has_table(table)
                    .await
                    .expect("table lookup should work"),
                "rollback should remove {table}"
            );
        }
        Migrator::up(database.connection(), None)
            .await
            .expect("initial migration should re-apply");
    }
}
